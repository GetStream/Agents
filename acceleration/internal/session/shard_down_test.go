//go:build integration

package session

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/agent"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm/llmtest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/streamapp"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
)

// ShardDownSuite opens sessions on a router whose peer shard's database is down.
type ShardDownSuite struct {
	suite.Suite
	ctx     context.Context
	manager *Manager
}

func TestShardDownSuite(t *testing.T) {
	suite.Run(t, new(ShardDownSuite))
}

func (s *ShardDownSuite) SetupSuite() {
	dsn := os.Getenv("ROUTER_POSTGRES_DSN")
	if dsn == "" {
		s.T().Skip("ROUTER_POSTGRES_DSN is not set")
	}
	s.ctx = context.Background()
	own := testenv.Database(dsn, "shard_down")
	migrated, err := store.Open(own)
	s.Require().NoError(err)
	s.Require().NoError(migrated.Migrate(s.ctx))
	s.Require().NoError(migrated.Close())

	// Nothing answers on port 1: the peer's dial is refused at once.
	db, err := store.OpenShards(store.Options{DSN: own, Shards: map[string]string{
		"here": own, "gone": "postgres://postgres:postgres@127.0.0.1:1/gone?sslmode=disable",
	}})
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.Require().NoError(db.Close()) })

	manager, err := NewManager(ManagerOptions{
		LLM:    screenedBy(&s.Suite, stubConfig(), &llmtest.Thinker{Answer: "Sure."}),
		Store:  db,
		Logger: slog.New(slog.DiscardHandler),
		Edge: func(context.Context, Spec, streamapp.Bound, *slog.Logger) (agent.Edge, error) {
			return newQuietEdge(), nil
		},
	})
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = manager.Shutdown() })
	s.manager = manager
}

func (s *ShardDownSuite) TestASessionWithAnIdTheRouterGeneratesOpens() {
	created, err := s.manager.Create(s.ctx, Spec{Text: true, CustomerID: "shard-down-" + uuid.NewString(), LLMTarget: "en-low-latency"})
	s.Require().NoError(err)
	s.NotEmpty(created.ID())
	s.Require().NoError(created.Close())
}

// The id a caller chose may be another session's on the shard that is down, so it is not
// taken on trust.
func (s *ShardDownSuite) TestASessionWithAnIdTheCallerChoseIsNotOpened() {
	_, err := s.manager.Create(s.ctx, Spec{ID: uuid.NewString(), Text: true, CustomerID: "shard-down-" + uuid.NewString(), LLMTarget: "en-low-latency"})
	s.Require().Error(err)
	s.NotErrorIs(err, ErrSessionExists)
}
