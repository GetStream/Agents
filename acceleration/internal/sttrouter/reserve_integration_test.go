//go:build integration

package sttrouter

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/config"
	"github.com/GetStream/Vision-Agents/acceleration/internal/policy"
	"github.com/GetStream/Vision-Agents/acceleration/internal/policy/policytest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stt"
	"github.com/GetStream/Vision-Agents/acceleration/internal/usage"
)

// MoneySuite covers a session whose organization's money is refused while it streams, through
// the real enforcer.
type MoneySuite struct {
	suite.Suite
}

func TestMoneySuite(t *testing.T) {
	suite.Run(t, new(MoneySuite))
}

func (s *MoneySuite) SetupTest() {
	previous := leaseRenew
	leaseRenew = 50 * time.Millisecond
	s.T().Cleanup(func() { leaseRenew = previous })
}

// lastWordsSTT is a router-measured transcriber that writes its last words as it closes, which
// waits for closing when it is set.
type lastWordsSTT struct {
	emitter *stt.Emitter
	closing chan struct{}

	mu    sync.Mutex
	heard int
}

func (l *lastWordsSTT) Start(context.Context) error { return nil }
func (l *lastWordsSTT) Events() <-chan stt.Event    { return l.emitter.Events() }
func (l *lastWordsSTT) Provider() string            { return "stub" }
func (l *lastWordsSTT) Model() string               { return "stub-model" }
func (*lastWordsSTT) Metering() stt.Metering        { return stt.MeteredByRouter }

func (l *lastWordsSTT) ProcessAudio(stt.PcmData, stt.Participant) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.heard++
	return nil
}

func (l *lastWordsSTT) Close() error {
	if l.closing != nil {
		<-l.closing
	}
	l.emitter.Send(stt.Transcript{Text: "the last words", Mode: stt.ModeFinal})
	l.emitter.Close()
	return nil
}

func (l *lastWordsSTT) frames() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.heard
}

func listening(t testing.TB, money *policytest.Money, owner routing.Owner, provider stt.STT) *Session {
	t.Helper()
	// $1 an hour of audio.
	config := routing.ProviderConfig{Provider: "stub", Model: "stub-model", Price: routing.Price{PerAudioHour: 1}}
	prices, err := routing.Prices(routing.Config{routing.STT: {Providers: []routing.ProviderConfig{config}}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	session := newSession(context.Background(), provider, config, owner, money.Recorder(routing.STT, prices), nil, nil)
	t.Cleanup(func() { _ = session.Close() })
	return session
}

// refused speaks into the session until it refuses a frame.
func (s *MoneySuite) refused(session *Session) error {
	var refused error
	s.Require().Eventually(func() bool {
		refused = session.ProcessAudio(speech(20), stt.Participant{})
		return refused != nil
	}, 5*time.Second, 5*time.Millisecond, "the session never refused")
	return refused
}

// said reads the session's events to their end, and answers what was transcribed.
func (s *MoneySuite) said(session *Session) []string {
	var said []string
	deadline := time.After(5 * time.Second)
	for {
		select {
		case event, open := <-session.Events():
			if !open {
				return said
			}
			if transcript, ok := event.(stt.Transcript); ok {
				said = append(said, transcript.Text)
			}
			if failure, ok := event.(stt.Error); ok {
				s.Fail("the vendor failed nothing", "%v", failure.Err)
			}
		case <-deadline:
			s.FailNow("the session's events did not end within 5s")
		}
	}
}

func (s *MoneySuite) billed(money *policytest.Money, owner routing.Owner) int64 {
	var ms int64
	var rows []store.Request
	s.Require().Eventually(func() bool {
		rows, ms = nil, 0
		s.Require().NoError(money.Store.GlobalDB().NewSelect().Model(&rows).Where("customer_id = ?", owner.CustomerID).Scan(s.T().Context()))
		for _, row := range rows {
			s.True(row.Success)
			ms += row.AudioMs
		}
		return len(rows) > 0
	}, 5*time.Second, 10*time.Millisecond, "nothing was billed")
	return ms
}

func (s *MoneySuite) TestASessionWhoseMoneyRunsOutEndsAfterItsLastWordsAndBillsWhatItTook() {
	money := policytest.Start(s.T(), "sttrouter_reserve")
	owner := money.Owner(s.T(), 1_000_000)
	provider := &lastWordsSTT{emitter: stt.NewEmitter(64)}
	session := listening(s.T(), money, owner, provider)
	s.Require().NoError(session.ProcessAudio(speech(20), stt.Participant{}))
	provider.emitter.Send(stt.Transcript{Text: "the first words", Mode: stt.ModeReplacement})

	spent := uuid.Must(uuid.NewV7())
	s.Require().NoError(money.Enforcer.Reserve(owner, spent, 1_000_000, time.Minute))
	s.Require().ErrorIs(s.refused(session), policy.ErrNoCredits)
	taken := provider.frames()

	s.NoError(session.ProcessAudio(speech(20), stt.Participant{}), "why is said once")
	s.Equal([]string{"the first words", "the last words"}, s.said(session), "what was heard is kept, and the events end")
	s.NoError(session.ProcessAudio(speech(20), stt.Participant{}))
	s.Equal(taken, provider.frames(), "the vendor hears nothing more")
	s.EqualValues(taken*20, s.billed(money, owner), "every frame it took is billed")

	money.Enforcer.Release(spent)
	s.Eventually(func() bool { return money.Spend(s.T(), owner) == nil }, 2*time.Second, 10*time.Millisecond)
	held := uuid.Must(uuid.NewV7())
	// All but a tenth of a cent, which a lease's minute ($0.0167) would not leave room for.
	s.Require().NoError(money.Enforcer.Reserve(owner, held, 999_000, time.Minute))
	s.NoError(money.Spend(s.T(), owner), "the lease is let go")
}

func (s *MoneySuite) TestASuspensionEndsASession() {
	money := policytest.Start(s.T(), "sttrouter_reserve")
	owner := money.Owner(s.T(), 9_000_000)
	provider := &lastWordsSTT{emitter: stt.NewEmitter(64)}
	session := listening(s.T(), money, owner, provider)
	s.Require().NoError(session.ProcessAudio(speech(20), stt.Participant{}))

	money.Usage.Suspend(s.T(), owner.OrganizationID, "a test")

	s.Require().ErrorIs(s.refused(session), policy.ErrSuspended)
	s.Equal([]string{"the last words"}, s.said(session))
}

func (s *MoneySuite) TestARefusalDoesNotWaitForTheVendorToClose() {
	money := policytest.Start(s.T(), "sttrouter_reserve")
	owner := money.Owner(s.T(), 0)
	provider := &lastWordsSTT{emitter: stt.NewEmitter(64), closing: make(chan struct{})}
	session := listening(s.T(), money, owner, provider)

	answered := make(chan error, 1)
	go func() { answered <- session.ProcessAudio(speech(20), stt.Participant{}) }()
	select {
	case err := <-answered:
		s.ErrorIs(err, policy.ErrNoCredits)
	case <-time.After(5 * time.Second):
		s.Fail("the refusal waited for the vendor")
	}
	s.Zero(provider.frames(), "refused from its first minute, the vendor hears nothing")
	close(provider.closing)
	s.said(session)
}

func (s *MoneySuite) TestMoneyThatCannotBeCheckedRefusesNoAudio() {
	redis, sharing := policytest.SharedHolds(s.T())
	money := policytest.Start(s.T(), "sttrouter_reserve", sharing, func(settings *policy.Settings) { settings.FlushMaxLag = 200 * time.Millisecond })
	owner := money.Owner(s.T(), 9_000_000)
	provider := &lastWordsSTT{emitter: stt.NewEmitter(64)}
	session := listening(s.T(), money, owner, provider)

	redis.Stop()
	s.Require().Eventually(func() bool { return errors.Is(session.lease.Err(), policy.ErrSpendCheckUnavailable) },
		5*time.Second, 10*time.Millisecond, "a renewal could not be checked")
	for range 10 {
		s.Require().NoError(session.ProcessAudio(speech(20), stt.Participant{}))
	}
	s.Equal(10, provider.frames())
	redis.Restore()
	s.Require().Eventually(func() bool { return session.lease.Err() == nil }, 5*time.Second, 10*time.Millisecond)
	s.NoError(session.ProcessAudio(speech(20), stt.Participant{}))
	s.NoError(session.Close())
	s.Equal([]string{"the last words"}, s.said(session))
	s.EqualValues(220, s.billed(money, owner), "the audio it took unchecked is billed")
}

func (s *MoneySuite) TestARouterThatDoesNotEnforceItsLimitsRefusesNoAudio() {
	for _, mode := range []config.LimitsMode{config.LimitsOff, config.LimitsShadow} {
		money := policytest.Start(s.T(), "sttrouter_reserve", func(settings *policy.Settings) { settings.Mode = mode })
		app, organization := "app-"+uuid.NewString(), "org-"+uuid.NewString()
		money.Usage.Suspend(s.T(), organization, "a test")
		owner := routing.Owner{CustomerID: app, Attribution: routing.Attribution{OrganizationID: organization}}
		money.Enforcer.Join(s.T().Context(), app, organization, usage.DefaultTier)
		provider := &lastWordsSTT{emitter: stt.NewEmitter(64)}
		session := listening(s.T(), money, owner, provider)
		if mode == config.LimitsShadow {
			s.Require().Eventually(func() bool {
				_, read := money.Enforcer.Balance(app, organization)
				return read
			}, 5*time.Second, 5*time.Millisecond, "the organization's standing is read")
		}
		// Past a renewal of the lease.
		renewed := time.Now().Add(4 * leaseRenew)
		for time.Now().Before(renewed) {
			s.Require().NoError(session.ProcessAudio(speech(20), stt.Participant{}), string(mode))
			time.Sleep(5 * time.Millisecond)
		}
		s.Require().NoError(session.Close())
		s.Equal([]string{"the last words"}, s.said(session))
	}
}

func BenchmarkProcessAudio(b *testing.B) {
	logged := slog.Default()
	slog.SetDefault(slog.New(slog.DiscardHandler))
	b.Cleanup(func() { slog.SetDefault(logged) })
	money := policytest.Start(b, "sttrouter_reserve")
	session := listening(b, money, money.Owner(b, 1_000_000_000), &quietSTT{emitter: stt.NewEmitter(1)})
	frame := speech(20)
	b.ReportAllocs()
	for b.Loop() {
		if err := session.ProcessAudio(frame, stt.Participant{ID: "caller"}); err != nil {
			b.Fatal(err)
		}
	}
}
