//go:build integration

package node

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv/redisready"
)

// ShardsSuite covers the shard list a deployment's routers agree on in Redis.
type ShardsSuite struct {
	suite.Suite
	client rueidis.Client
	key    string
}

func TestShardsSuite(t *testing.T) {
	suite.Run(t, new(ShardsSuite))
}

func (s *ShardsSuite) SetupTest() {
	address := os.Getenv("ROUTER_REDIS_ADDR")
	if address == "" {
		s.T().Skip("ROUTER_REDIS_ADDR not set")
	}
	s.Require().NoError(redisready.Ready(address))
	client, err := redisready.NewClient(rueidis.ClientOption{InitAddress: []string{address}, DisableCache: true})
	s.Require().NoError(err)
	s.T().Cleanup(client.Close)
	s.client = client
	s.key = "shards-test:" + time.Now().Format(time.RFC3339Nano)
	s.T().Cleanup(func() {
		s.NoError(client.Do(context.WithoutCancel(s.T().Context()), client.B().Del().Key(s.key).Build()).Error())
	})
}

func (s *ShardsSuite) TestARouterKnowingAnotherShardIsRefused() {
	s.Require().NoError(s.agree("us-east4", "us-west1"))

	s.ErrorIs(s.agree("us-east4", "us-west1", "europe-west4"), ErrShardsDisagree)
}

func (s *ShardsSuite) TestARouterKnowingNoShardsIsRefusedWhereTheRunningOnesKnowSome() {
	s.Require().NoError(s.agree("us-east4"))

	s.ErrorIs(s.agree(), ErrShardsDisagree)
}

func (s *ShardsSuite) TestARouterKnowingTheSameShardsInAnotherOrderStarts() {
	s.Require().NoError(s.agree("us-west1", "us-east4"))

	s.NoError(s.agree("us-east4", "us-west1"))
}

// A list that never lapsed could not be changed: every router with the new one would be
// refused forever.
func (s *ShardsSuite) TestTheListLapsesUnlessARouterRenewsIt() {
	s.Require().NoError(s.agree("us-east4"))

	ttl, err := s.client.Do(s.T().Context(), s.client.B().Pttl().Key(s.key).Build()).AsInt64()
	s.Require().NoError(err)
	s.Greater(ttl, int64(0))
	s.LessOrEqual(ttl, holdFor.Milliseconds())
}

// A list left to lapse while its routers run would let a router with another list start.
func (s *ShardsSuite) TestARunningRouterRenewsTheList() {
	s.Require().NoError(s.agree("us-east4"))
	short := renewEvery + 5*time.Second
	s.Require().NoError(s.client.Do(s.T().Context(), s.client.B().Pexpire().Key(s.key).Milliseconds(short.Milliseconds()).Build()).Error())

	s.Eventually(func() bool {
		ttl, err := s.client.Do(s.T().Context(), s.client.B().Pttl().Key(s.key).Build()).AsInt64()
		return err == nil && ttl > short.Milliseconds()
	}, renewEvery+5*time.Second, 100*time.Millisecond)
}

func (s *ShardsSuite) agree(shards ...string) error {
	return AgreeOnShards(s.T().Context(), s.client, s.key, shards, slog.New(slog.DiscardHandler))
}
