package sttrouter

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/policy"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing/gatetest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stt"
)

// RefusalsSuite covers when the refusals of a session's lease end it, on a fake clock: the lease
// is asked for as the session opens and every 30 s after.
type RefusalsSuite struct {
	suite.Suite
}

func TestRefusalsSuite(t *testing.T) {
	suite.Run(t, new(RefusalsSuite))
}

var (
	paidModel = routing.ProviderConfig{Provider: "stub", Model: "paid", Price: routing.Price{PerAudioHour: 1}}
	// otherModel costs twice as much.
	otherModel = routing.ProviderConfig{Provider: "stub", Model: "other", Price: routing.Price{PerAudioHour: 2}}
	freeModel  = routing.ProviderConfig{Provider: "stub", Model: "free", Local: true}
	// ownKeyModel is the paid model on the customer's own key.
	ownKeyModel = routing.ProviderConfig{Provider: "stub", Model: "paid", BYOK: true}
)

// speech is that much caller audio at the rate every transcriber is fed.
func speech(ms int) stt.PcmData {
	return stt.PcmData{Samples: make([]int16, stt.SampleRate*ms/1000), SampleRate: stt.SampleRate, Channels: 1}
}

// asked runs body on a fake clock with a session on the model, whose gate it is given.
func (s *RefusalsSuite) asked(model routing.ProviderConfig, first error, body func(gate *gatetest.Answers, session *Session)) {
	outer := s.T()
	synctest.Test(outer, func(t *testing.T) {
		s.SetT(t)
		defer s.SetT(outer)
		discard := slog.New(slog.NewTextHandler(io.Discard, nil))
		prices, err := routing.Prices(routing.Config{routing.STT: {Providers: []routing.ProviderConfig{paidModel, otherModel, freeModel}}}, discard)
		s.Require().NoError(err)
		gate := &gatetest.Answers{}
		gate.Refuse(first)
		writer := gate.Writer()
		defer writer.Close(context.Background())
		session := newSession(t.Context(), &quietSTT{emitter: stt.NewEmitter(64)}, model, routing.Owner{CustomerID: "acme"},
			routing.NewRecorderOn(routing.STT, writer, prices, discard), nil, nil)
		body(gate, session)
		s.NoError(session.Close())
		synctest.Wait()
		s.Zero(gate.Held(), "nothing is held once it ended")
	})
}

// renewals answers each of the next renewals of the lease in turn.
func (s *RefusalsSuite) renewals(gate *gatetest.Answers, answers ...error) {
	for _, answer := range answers {
		gate.Refuse(answer)
		time.Sleep(30 * time.Second)
		synctest.Wait()
	}
}

// goesOn checks that the session takes audio still.
func (s *RefusalsSuite) goesOn(session *Session) {
	s.Require().NoError(session.ProcessAudio(speech(20), stt.Participant{}))
	s.Require().False(session.failed.Load(), "the audio was taken, not dropped")
}

// ends checks that the session answers refusal to its next audio, once, and ends.
func (s *RefusalsSuite) ends(session *Session, refusal error) {
	s.Require().ErrorIs(session.ProcessAudio(speech(20), stt.Participant{}), refusal)
	s.NoError(session.ProcessAudio(speech(20), stt.Participant{}), "why is said once")
	synctest.Wait()
	_, open := <-session.Events()
	s.False(open, "its events end")
}

func (s *RefusalsSuite) TestOneRefusalChangesNothing() {
	s.asked(paidModel, nil, func(gate *gatetest.Answers, session *Session) {
		s.renewals(gate, policy.ErrNoCredits)
		s.goesOn(session)
		time.Sleep(29 * time.Second)
		s.goesOn(session)
	})
}

func (s *RefusalsSuite) TestTwoRefusalsInARowEndIt() {
	s.asked(paidModel, nil, func(gate *gatetest.Answers, session *Session) {
		s.renewals(gate, policy.ErrNoCredits, policy.ErrBudgetSpent)
		s.ends(session, policy.ErrBudgetSpent)
	})
}

func (s *RefusalsSuite) TestTheFirstMinuteRefusedCounts() {
	s.asked(paidModel, policy.ErrNoCredits, func(gate *gatetest.Answers, session *Session) {
		s.goesOn(session)
		s.renewals(gate, policy.ErrNoCredits)
		s.ends(session, policy.ErrNoCredits)
	})
}

func (s *RefusalsSuite) TestAGrantInBetweenStartsTheCountAgain() {
	s.asked(paidModel, nil, func(gate *gatetest.Answers, session *Session) {
		s.renewals(gate, policy.ErrNoCredits, nil, policy.ErrNoCredits)
		s.goesOn(session)
		s.renewals(gate, nil)
		s.True(session.RefusedSince().IsZero())
		s.renewals(gate, policy.ErrNoCredits, policy.ErrNoCredits)
		s.ends(session, policy.ErrNoCredits)
	})
}

func (s *RefusalsSuite) TestMoneyThatCouldNotBeCheckedNeverCounts() {
	s.asked(paidModel, nil, func(gate *gatetest.Answers, session *Session) {
		s.renewals(gate, gatetest.Unchecked{}, gatetest.Unchecked{}, gatetest.Unchecked{})
		s.goesOn(session)
		s.renewals(gate, policy.ErrNoCredits, gatetest.Unchecked{}, gatetest.Unchecked{})
		s.goesOn(session)
	})
}

func (s *RefusalsSuite) TestMoneyThatCouldNotBeCheckedDoesNotStartTheCountAgain() {
	s.asked(paidModel, nil, func(gate *gatetest.Answers, session *Session) {
		s.renewals(gate, policy.ErrNoCredits, gatetest.Unchecked{}, policy.ErrNoCredits)
		s.ends(session, policy.ErrNoCredits)
	})
}

func (s *RefusalsSuite) TestASuspensionEndsItAtOnce() {
	s.asked(paidModel, nil, func(gate *gatetest.Answers, session *Session) {
		s.renewals(gate, policy.ErrSuspended)
		s.ends(session, policy.ErrSuspended)
	})
}

func (s *RefusalsSuite) TestASuspensionAfterARefusalEndsItAtOnce() {
	s.asked(paidModel, policy.ErrNoCredits, func(gate *gatetest.Answers, session *Session) {
		s.renewals(gate, gatetest.Unchecked{}, policy.ErrSuspended)
		s.ends(session, policy.ErrSuspended)
	})
}

// takesOver puts another model in place of the one serving the session.
func (s *RefusalsSuite) takesOver(session *Session, model routing.ProviderConfig) {
	synctest.Wait()
	s.Require().True(session.swap(session.current(), &quietSTT{emitter: stt.NewEmitter(64)}, model))
	synctest.Wait()
}

func (s *RefusalsSuite) TestTheNextModelKeepsTheCount() {
	s.asked(paidModel, nil, func(gate *gatetest.Answers, session *Session) {
		s.renewals(gate, policy.ErrNoCredits)
		s.takesOver(session, otherModel)
		s.goesOn(session)
		s.renewals(gate, policy.ErrNoCredits)
		s.Require().ErrorIs(session.ProcessAudio(speech(20), stt.Participant{}), policy.ErrNoCredits)
	})
}

func (s *RefusalsSuite) TestTheNextModelsMinuteIsHeldAtItsPrice() {
	s.asked(paidModel, nil, func(gate *gatetest.Answers, session *Session) {
		s.EqualValues(16_667, gate.Held())
		s.takesOver(session, otherModel)
		s.renewals(gate, nil)
		s.EqualValues(33_333, gate.Held())
	})
}

func (s *RefusalsSuite) TestAModelThatBillsNothingIsNeverEnded() {
	for _, model := range []routing.ProviderConfig{freeModel, ownKeyModel} {
		s.asked(model, policy.ErrNoCredits, func(gate *gatetest.Answers, session *Session) {
			s.renewals(gate, policy.ErrNoCredits, policy.ErrNoCredits, policy.ErrNoCredits)
			s.goesOn(session)
		})
	}
}

func (s *RefusalsSuite) TestAModelThatBillsNothingTakingOverEndsNothing() {
	for _, model := range []routing.ProviderConfig{freeModel, ownKeyModel} {
		s.asked(paidModel, nil, func(gate *gatetest.Answers, session *Session) {
			s.renewals(gate, policy.ErrNoCredits)
			s.takesOver(session, model)
			s.Zero(gate.Held(), "nothing is held for it")
			s.renewals(gate, policy.ErrNoCredits, policy.ErrNoCredits, policy.ErrNoCredits)
			s.goesOn(session)
		})
	}
}

func (s *RefusalsSuite) TestAPaidModelTakingOverFromOneThatBillsNothingIsHeldAndEnded() {
	s.asked(freeModel, nil, func(gate *gatetest.Answers, session *Session) {
		s.Zero(gate.Held())
		s.takesOver(session, paidModel)
		s.EqualValues(16_667, gate.Held())
		s.renewals(gate, policy.ErrNoCredits)
		s.goesOn(session)
		s.renewals(gate, policy.ErrNoCredits)
		s.Require().ErrorIs(session.ProcessAudio(speech(20), stt.Participant{}), policy.ErrNoCredits)
	})
}
