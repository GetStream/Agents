//go:build integration

package stsrouter

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/quota"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/sts"
	_ "github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv/redisready"
)

// STSQuotaSuite covers what a conversation does about a daily limit. It needs a Redis to
// count in but no provider credentials: the stub replies when told to, so what is under test
// is the session's own decision rather than a model's.
type STSQuotaSuite struct {
	suite.Suite
	ctx    context.Context
	redis  rueidis.Client
	caller routing.Caller
}

func TestSTSQuotaSuite(t *testing.T) {
	suite.Run(t, new(STSQuotaSuite))
}

func (s *STSQuotaSuite) SetupSuite() {
	address := os.Getenv("ROUTER_REDIS_ADDR")
	if address == "" {
		s.T().Skip("ROUTER_REDIS_ADDR must be set")
	}
	client, err := redisready.NewClient(rueidis.ClientOption{
		InitAddress:  []string{address},
		DisableCache: true,
	})
	s.Require().NoError(err)
	s.redis = client
	s.ctx = context.Background()
}

func (s *STSQuotaSuite) TearDownSuite() {
	if s.redis != nil {
		s.redis.Close()
	}
}

func (s *STSQuotaSuite) SetupTest() {
	// A fresh user per test, so no test spends another's allowance.
	s.caller = routing.Caller{UserID: fmt.Sprintf("user-%d", time.Now().UnixNano())}
}

func (s *STSQuotaSuite) limiter(messages, tokens int64) *quota.Limiter {
	limiter, err := quota.New(s.redis,
		quota.Limits{MessagesPerDay: messages, TokensPerDay: tokens}, nil, slog.Default())
	s.Require().NoError(err)
	return limiter
}

// session returns a conversation over the stub provider, counted by limiter and owned by
// the caller under test.
func (s *STSQuotaSuite) session(limiter *quota.Limiter, caller routing.Caller) (*Session, *stubSTS) {
	provider := newStubSTS()
	recorder := routing.NewRecorder(routing.STS, nil, nil, nil, slog.Default())
	config := routing.ProviderConfig{Provider: "stub", Model: "stub-model"}
	session := newSession(s.ctx, provider, config, routing.Owner{CustomerID: "acme", Caller: caller},
		recorder, recorder.For(routing.LLM), limiter)
	s.T().Cleanup(func() {
		_ = session.Close()
		recorder.Close()
	})
	go func() {
		for range session.Events() {
		}
	}()
	return session, provider
}

// answerOnce has the stub answer once, using the tokens given.
func answerOnce(provider *stubSTS, id string, generation int, input, output int64) {
	provider.emitter.Send(sts.ResponseStarted{ResponseID: id, Generation: generation, At: time.Now()})
	provider.emitter.Send(sts.ResponseComplete{ResponseID: id, Generation: generation,
		Usage: sts.Usage{InputTokens: input, OutputTokens: output}})
}

// refused waits until the conversation refuses the caller's audio for a spent day.
func (s *STSQuotaSuite) refused(session *Session) {
	s.Require().Eventually(func() bool {
		return errors.Is(session.ProcessAudio(heard(20), sts.Participant{}), quota.ErrExhausted)
	}, 15*time.Second, 10*time.Millisecond, "the conversation went on taking turns past the day")
}

func (s *STSQuotaSuite) TestAConversationTakesNoTurnOnceItsRepliesSpendTheDay() {
	session, provider := s.session(s.limiter(2, 0), s.caller)
	s.Require().NoError(session.ProcessAudio(heard(20), sts.Participant{}))

	answerOnce(provider, "r1", 1, 10, 5)
	answerOnce(provider, "r2", 2, 10, 5)

	s.refused(session)
	s.True(provider.closed, "a conversation the day has nothing left for is closed")
	s.ErrorIs(session.SendText("and a text", sts.Participant{}), quota.ErrExhausted)
	s.ErrorIs(session.Prompt("say something"), quota.ErrExhausted)
}

func (s *STSQuotaSuite) TestARepliesTokensAreCountedOnTheDay() {
	session, provider := s.session(s.limiter(0, 40), s.caller)

	answerOnce(provider, "r1", 1, 30, 12)

	s.refused(session)
}

func (s *STSQuotaSuite) TestAReplyWithinTheDayLeavesTheConversationOpen() {
	limiter := s.limiter(2, 0)
	session, provider := s.session(limiter, s.caller)

	answerOnce(provider, "r1", 1, 10, 5)

	s.Require().Eventually(func() bool {
		status, _ := limiter.Allow(s.ctx, "acme", s.caller)
		return status.Remaining == 1
	}, 15*time.Second, 10*time.Millisecond, "the reply was never counted")
	s.NoError(session.ProcessAudio(heard(20), sts.Participant{}))
	s.False(provider.closed)
}

func (s *STSQuotaSuite) TestACallerWhoHasSpentTheirDayIsRefusedAConversation() {
	limiter := s.limiter(1, 0)
	limiter.Debit(s.ctx, "acme", s.caller, 0)
	var built []routing.Spec
	router := s.router(limiter, &built)

	_, err := router.Start(s.ctx, Request{CustomerID: "acme", Caller: s.caller, Target: "sts-fast"})

	s.ErrorIs(err, quota.ErrExhausted)
	s.Empty(built, "no model was opened")
}

func (s *STSQuotaSuite) TestACallerWithADayLeftIsGivenAConversation() {
	var built []routing.Spec
	router := s.router(s.limiter(1, 0), &built)

	session, err := router.Start(s.ctx, Request{CustomerID: "acme", Caller: s.caller, Target: "sts-fast"})

	s.Require().NoError(err)
	_ = session.Close()
}

func (s *STSQuotaSuite) TestAConversationTheRouterOpensCountsItsRepliesOnTheCallersDay() {
	var built []routing.Spec
	router := s.router(s.limiter(1, 0), &built)
	session, err := router.Start(s.ctx, Request{CustomerID: "acme", Caller: s.caller, Target: "sts-fast"})
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = session.Close() })
	go func() {
		for range session.Events() {
		}
	}()
	provider, ok := session.provider.(*stubSTS)
	s.Require().True(ok)

	answerOnce(provider, "r1", 1, 10, 5)

	s.refused(session)
}

// router routes over the built-in config with every model a stub, counted by limiter.
func (s *STSQuotaSuite) router(limiter *quota.Limiter, built *[]routing.Spec) *Router {
	config, err := routing.DefaultConfig()
	s.Require().NoError(err)
	registry := NewRegistry()
	for _, provider := range config[routing.STS].Providers {
		name := provider.Provider
		registry.Register(name, func(spec routing.Spec) (sts.STS, error) {
			*built = append(*built, spec)
			stub := newStubSTS()
			stub.capabilities, _ = capabilitiesFor(name, spec.Model)
			return stub, nil
		})
	}
	router, err := New(Options{Config: config[routing.STS], Registry: registry, Quota: limiter})
	s.Require().NoError(err)
	s.T().Cleanup(router.Close)
	return router
}
