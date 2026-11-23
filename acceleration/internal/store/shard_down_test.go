//go:build integration

package store

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// goneShardDSN is a shard whose database nothing answers on: the dial is refused at once.
const goneShardDSN = "postgres://postgres:postgres@127.0.0.1:1/gone?sslmode=disable"

// withAShardDown is a store on the suite's databases beside a peer shard that is down, and
// what it logs.
func (s *StoreSuite) withAShardDown() (*Store, *syncBuffer) {
	down, err := OpenShards(Options{DSN: s.dsn, ShardDSN: s.shardDSN, Shards: map[string]string{"here": s.shardDSN, "gone": goneShardDSN}})
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.NoError(down.Close()) })

	logged := &syncBuffer{}
	before := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logged, nil)))
	s.T().Cleanup(func() { slog.SetDefault(before) })
	return down, logged
}

func (s *StoreSuite) TestABridgeOnAShardThatAnswersIsClaimedWhileAnotherIsDown() {
	down, logged := s.withAShardDown()
	token := newID()
	s.Require().NoError(s.store.CallBridges.Insert(s.ctx, &CallBridge{Token: token, CustomerID: "acme", Vendor: "plivo",
		TrunkURI: "sip:x@example.com", CallID: "call", ExpiresAt: time.Now().Add(time.Hour)}))

	claimed, err := down.CallBridges.Claim(s.ctx, token)
	s.Require().NoError(err)
	s.Equal("sip:x@example.com", claimed.TrunkURI)
	s.Contains(logged.String(), "shard=gone", "the shard that did not answer is logged")
	_, err = down.CallBridges.Claim(s.ctx, token)
	s.Error(err, "a token still works once")
}

func (s *StoreSuite) TestABridgeNoAnsweringShardHoldsIsNotClaimedWhileAnotherIsDown() {
	down, _ := s.withAShardDown()

	_, err := down.CallBridges.Claim(s.ctx, newID())
	s.Require().Error(err)
	s.NotContains(err.Error(), "not waiting to be claimed", "the shard that is down may hold it")
}

func (s *StoreSuite) TestACallPlacedOnAShardThatAnswersIsFoundWhileAnotherIsDown() {
	down, logged := s.withAShardDown()
	callID := newID()
	s.Require().NoError(s.store.CallResources.Insert(s.ctx, &CallResource{
		TrunkID: "trunk-1", RouteID: "route-1", CallType: "default", CallID: callID, CustomerID: "acme",
	}))

	placed, err := down.CallResources.Select(s.ctx, AppScope{Unpinned: true}, "default", callID)
	s.Require().NoError(err)
	s.Require().Len(placed, 1)
	s.Equal("trunk-1", placed[0].TrunkID)
	s.Contains(logged.String(), "shard=gone", "the shard that did not answer is logged")

	_, err = down.CallResources.Select(s.ctx, AppScope{Unpinned: true}, "default", callID)
	s.Require().NoError(err)
	s.Equal(1, strings.Count(logged.String(), "shard=gone"), "a shard still down is logged once a minute, not on every call")
}

func (s *StoreSuite) TestACallNoAnsweringShardPlacedIsNotAnsweredWhileAnotherIsDown() {
	down, _ := s.withAShardDown()

	placed, err := down.CallResources.Select(s.ctx, AppScope{Unpinned: true}, "default", newID())
	s.Require().Error(err, "the shard that is down may have placed it")
	s.Empty(placed)
}

// syncBuffer is a log the store's other goroutines may write to at the same time.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
