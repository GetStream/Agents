//go:build integration

package agent

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stt"
)

// dsnEnvVar and redisEnvVar are where this looks for a Postgres to count turns into and a
// Redis to keep them in.
const (
	dsnEnvVar   = "ROUTER_POSTGRES_DSN"
	redisEnvVar = "ROUTER_REDIS_ADDR"
)

// writeGrace is how long the asynchronous turn writer is given to drain, since a
// conversation hands turns over rather than waiting on the database.
const writeGrace = 10 * time.Second

// TurnRecordingSuite runs a whole exchange through the in-process loopback edge and
// checks what it leaves behind, which is the part unit tests cannot reach.
type TurnRecordingSuite struct {
	suite.Suite
	// agents is the agent fixture, not embedded so its own tests are not run again here.
	agents    *AgentSuite
	pgStore   *store.Store
	startedAt time.Time
}

func TestTurnRecordingSuite(t *testing.T) {
	suite.Run(t, new(TurnRecordingSuite))
}

func (s *TurnRecordingSuite) SetupSuite() {
	dsn, redisAddr := os.Getenv(dsnEnvVar), os.Getenv(redisEnvVar)
	if dsn == "" || redisAddr == "" {
		s.T().Skipf("%s and %s must be set", dsnEnvVar, redisEnvVar)
	}

	pgStore, err := store.OpenShards(store.Options{DSN: dsn, Redis: store.Redis{
		Address: redisAddr, Username: os.Getenv("ROUTER_REDIS_USERNAME"), Password: os.Getenv("ROUTER_REDIS_PASSWORD"),
	}})
	s.Require().NoError(err)
	s.pgStore = pgStore
	s.Require().NoError(pgStore.Ping(context.Background()))
	s.Require().NoError(pgStore.Migrate(context.Background()))
}

func (s *TurnRecordingSuite) SetupTest() {
	s.agents = new(AgentSuite)
	s.agents.SetT(s.T())
	s.agents.SetupTest()
	// A fresh agent per test, so one run's turns are not another's.
	s.agents.agentID = fmt.Sprintf("agent-test-%d", time.Now().UnixNano())
	s.agents.records = s.pgStore
	s.startedAt = time.Now().Add(-time.Minute)
}

func (s *TurnRecordingSuite) TearDownSuite() {
	if s.pgStore != nil {
		s.Require().NoError(s.pgStore.Close())
	}
}

func (s *TurnRecordingSuite) TestAFinishedExchangeIsKeptAndCounted() {
	agents := s.agents
	agents.join(true)

	participant := stt.Participant{ID: "alice", UserID: "alice"}
	agents.speak(participant)
	agents.says(participant, "hello there")

	agents.eventually(func() bool { return len(agents.voice.spoken()) > 0 }, "the agent never answered")

	// Closing drains the writer, so what follows is not racing it.
	agents.agent.Close()

	turns, err := s.pgStore.Turns.Select(context.Background(), "acme", s.agents.agentID, s.startedAt, nil)
	s.Require().NoError(err)
	s.Require().Len(turns, 1, "one exchange, one turn")
	s.Equal("acme", turns[0].CustomerID)
	s.Require().NotNil(turns[0].RoundtripMs)
	s.Positive(*turns[0].RoundtripMs, "the caller waited a measurable amount of time")
	s.Require().NotNil(turns[0].LLMTTFTMs)
	s.False(turns[0].Interrupted)

	stats, err := s.pgStore.TurnBuckets.Select(context.Background(), "acme", store.TurnBucketFilter{AgentID: s.agents.agentID, Granularity: store.Hourly, From: s.startedAt.Add(-time.Hour), To: time.Now().Add(time.Hour)})
	s.Require().NoError(err)
	s.Require().Len(stats, 1)
	s.EqualValues(1, stats[0].TurnCount)
	s.NotNil(stats[0].RoundtripP50Ms, "the turn's wait is in its hour's percentiles")
}

// TestPersistReportsAWriteTheDeadlineCut is what lets the recorder count a lost row: a
// write whose context is gone must say so.
func (s *TurnRecordingSuite) TestPersistReportsAWriteTheDeadlineCut() {
	row := store.Turn{CustomerID: "acme", AgentID: s.agents.agentID, TurnID: "turn-persist", StartedAt: time.Now().UTC()}
	owner := routing.Owner{CustomerID: "acme", AgentID: s.agents.agentID}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	r := buildTurnRecorder(s.pgStore, owner, slog.New(slog.DiscardHandler))
	s.Error(r.persist(cancelled, row), "a write cut by its context is an error")
	s.NoError(r.persist(context.Background(), row), "a write that goes through is not")
}

// TestDecisionsReachRedis pins what the recorder writes with by default: a judgement
// recorded and flushed by Close must be readable back from the store.
func (s *TurnRecordingSuite) TestDecisionsReachRedis() {
	callID := fmt.Sprintf("call-decisions-%d", time.Now().UnixNano())
	owner := routing.Owner{CustomerID: "acme", AgentID: s.agents.agentID, CallID: callID}
	r := newDecisionRecorder(s.pgStore.CallEvents, owner, slog.New(slog.DiscardHandler))
	r.Record(Decided{Kind: "reply", Reason: "asked a question", TurnID: "turn-1", Text: "hello"})
	r.Close()

	events, err := s.pgStore.CallEvents.Select(context.Background(), "acme", callID, s.startedAt, nil, 10)
	s.Require().NoError(err)
	s.Require().Len(events, 1)
	s.Equal("reply", events[0].Kind)
	s.Equal("turn-1", events[0].TurnID)
	s.Equal("hello", events[0].Said)
}
