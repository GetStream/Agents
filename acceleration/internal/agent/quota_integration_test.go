//go:build integration

package agent

import (
	"context"
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
	"github.com/GetStream/Vision-Agents/acceleration/internal/stsrouter"
	_ "github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv/redisready"
)

// AgentQuotaSuite covers a voice agent's speech-to-speech model and the daily limit of the
// caller it talks to. It needs a Redis to count in.
type AgentQuotaSuite struct {
	suite.Suite
	ctx    context.Context
	redis  rueidis.Client
	caller routing.Caller
}

func TestAgentQuotaSuite(t *testing.T) {
	suite.Run(t, new(AgentQuotaSuite))
}

func (s *AgentQuotaSuite) SetupSuite() {
	address := os.Getenv("ROUTER_REDIS_ADDR")
	if address == "" {
		s.T().Skip("ROUTER_REDIS_ADDR must be set")
	}
	client, err := redisready.NewClient(rueidis.ClientOption{InitAddress: []string{address}, DisableCache: true})
	s.Require().NoError(err)
	s.redis = client
	s.ctx = context.Background()
}

func (s *AgentQuotaSuite) TearDownSuite() {
	if s.redis != nil {
		s.redis.Close()
	}
}

func (s *AgentQuotaSuite) SetupTest() {
	// A fresh user per test, so no test spends another's allowance.
	s.caller = routing.Caller{UserID: fmt.Sprintf("user-%d", time.Now().UnixNano())}
}

// join has a voice agent for the caller under test join a call, its speech-to-speech model
// counted on a day of one message.
func (s *AgentQuotaSuite) join(spent bool) error {
	limiter, err := quota.New(s.redis, quota.Limits{MessagesPerDay: 1}, nil, slog.Default())
	s.Require().NoError(err)
	if spent {
		limiter.Debit(s.ctx, "acme", s.caller, 0)
	}
	config, err := routing.DefaultConfig()
	s.Require().NoError(err)
	models := stsrouter.NewRegistry()
	models.Register("openai", func(spec routing.Spec) (sts.STS, error) { return newStubSTS(spec.STS.Instructions), nil })
	conversing, err := stsrouter.New(stsrouter.Options{Config: config[routing.STS], Registry: models, Quota: limiter})
	s.Require().NoError(err)
	s.T().Cleanup(conversing.Close)

	agent, err := New(Options{
		Edge: newLoopbackEdge(), CustomerID: "acme", Caller: s.caller,
		STS: conversing, STSTarget: "openai/gpt-realtime-2",
	})
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = agent.Close() })
	return agent.Join(s.ctx)
}

func (s *AgentQuotaSuite) TestAVoiceAgentDoesNotOpenItsModelForACallerWhoHasSpentTheirDay() {
	s.ErrorIs(s.join(true), quota.ErrExhausted)
}

func (s *AgentQuotaSuite) TestAVoiceAgentOpensItsModelForACallerWithADayLeft() {
	s.NoError(s.join(false))
}
