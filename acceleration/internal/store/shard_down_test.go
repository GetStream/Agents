//go:build integration

package store

import (
	"bytes"
	"log/slog"
	"net"
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
	// Logged as it fails, which may be after the answer that settled the read.
	s.Eventually(func() bool { return strings.Contains(logged.String(), "shard=gone") }, 5*time.Second, 10*time.Millisecond,
		"the shard that did not answer is logged")
	_, err = down.CallBridges.Claim(s.ctx, token)
	s.Error(err, "a token still works once")
}

func (s *StoreSuite) TestABridgeNoAnsweringShardHoldsIsNotClaimedWhileAnotherIsDown() {
	down, _ := s.withAShardDown()

	_, err := down.CallBridges.Claim(s.ctx, newID())
	s.Require().ErrorIs(err, ErrShardUnanswered, "the shard that is down may hold it")
	_, err = s.store.CallBridges.Claim(s.ctx, newID())
	s.Require().Error(err)
	s.NotErrorIs(err, ErrShardUnanswered, "with every shard answering, it is not there")
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
	// Logged as it fails, which may be after the answer that settled the read.
	s.Eventually(func() bool { return strings.Contains(logged.String(), "shard=gone") }, 5*time.Second, 10*time.Millisecond,
		"the shard that did not answer is logged")

	_, err = down.CallResources.Select(s.ctx, AppScope{Unpinned: true}, "default", callID)
	s.Require().NoError(err)
	// The second read's failure is logged, if at all, as the first was: by then it has had
	// time to.
	s.Never(func() bool { return strings.Count(logged.String(), "shard=gone") > 1 }, 200*time.Millisecond, 10*time.Millisecond,
		"a shard still down is logged once a minute, not on every call")
}

func (s *StoreSuite) TestACallNoAnsweringShardPlacedIsNotAnsweredWhileAnotherIsDown() {
	down, _ := s.withAShardDown()

	placed, err := down.CallResources.Select(s.ctx, AppScope{Unpinned: true}, "default", newID())
	s.Require().Error(err, "the shard that is down may have placed it")
	s.Empty(placed)
}

// withAShardThatNeverAnswers is a store on the suite's databases beside a peer shard whose
// host accepts a connection and then says nothing, as a partitioned one can.
func (s *StoreSuite) withAShardThatNeverAnswers() *Store {
	silent, err := net.Listen("tcp", "127.0.0.1:0")
	s.Require().NoError(err)
	var held []net.Conn
	var mu sync.Mutex
	go func() {
		for {
			conn, err := silent.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			held = append(held, conn)
			mu.Unlock()
		}
	}()
	s.T().Cleanup(func() {
		_ = silent.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, conn := range held {
			_ = conn.Close()
		}
	})
	dsn := "postgres://postgres:postgres@" + silent.Addr().String() + "/silent?sslmode=disable"
	down, err := OpenShards(Options{DSN: s.dsn, ShardDSN: s.shardDSN, Shards: map[string]string{"here": s.shardDSN, "silent": dsn}})
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.NoError(down.Close()) })
	return down
}

func (s *StoreSuite) TestACallPlacedOnAShardThatAnswersIsFoundWithoutWaitingOnOneThatDoesNot() {
	down := s.withAShardThatNeverAnswers()
	callID := newID()
	s.Require().NoError(s.store.CallResources.Insert(s.ctx, &CallResource{
		TrunkID: "trunk-1", RouteID: "route-1", CallType: "default", CallID: callID, CustomerID: "acme",
	}))

	began := time.Now()
	placed, err := down.CallResources.Select(s.ctx, AppScope{Unpinned: true}, "default", callID)
	s.Require().NoError(err)
	s.Len(placed, 1)
	s.Less(time.Since(began), peerReadTimeout, "the answer that settled it was not held for the silent shard")
}

func (s *StoreSuite) TestABridgeNoShardHoldsIsRefusedOnceASilentShardRunsOutOfTime() {
	down := s.withAShardThatNeverAnswers()

	began := time.Now()
	_, err := down.CallBridges.Claim(s.ctx, newID())
	s.Require().Error(err)
	s.Less(time.Since(began), peerReadTimeout+time.Second, "the silent shard was waited on past its read's bound")
}

// A peer that never answers fails after the answer that settled the read, and is still logged.
func (s *StoreSuite) TestASilentShardIsLoggedAfterTheReadItDidNotHoldUp() {
	down := s.withAShardThatNeverAnswers()
	logged := &syncBuffer{}
	before := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logged, nil)))
	s.T().Cleanup(func() { slog.SetDefault(before) })
	callID := newID()
	s.Require().NoError(s.store.CallResources.Insert(s.ctx, &CallResource{
		TrunkID: "trunk-1", RouteID: "route-1", CallType: "default", CallID: callID, CustomerID: "acme",
	}))

	_, err := down.CallResources.Select(s.ctx, AppScope{Unpinned: true}, "default", callID)
	s.Require().NoError(err)
	s.Eventually(func() bool { return strings.Contains(logged.String(), "shard=silent") }, peerReadTimeout+2*time.Second, 20*time.Millisecond,
		"the silent shard is logged once its read runs out of time")
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
