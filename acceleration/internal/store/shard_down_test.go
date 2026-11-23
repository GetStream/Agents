//go:build integration

package store

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/uptrace/bun"
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
	down, _ := s.withAShardDown()
	token := newID()
	s.Require().NoError(s.store.CallBridges.Insert(s.ctx, &CallBridge{Token: token, CustomerID: "acme", Vendor: "plivo",
		TrunkURI: "sip:x@example.com", CallID: "call", ExpiresAt: time.Now().Add(time.Hour)}))

	claimed, err := down.CallBridges.Claim(s.ctx, token)
	s.Require().NoError(err)
	s.Equal("sip:x@example.com", claimed.TrunkURI)
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
	down, _ := s.withAShardDown()
	callID := newID()
	s.Require().NoError(s.store.CallResources.Insert(s.ctx, &CallResource{
		TrunkID: "trunk-1", RouteID: "route-1", CallType: "default", CallID: callID, CustomerID: "acme",
	}))

	placed, err := down.CallResources.Select(s.ctx, AppScope{Unpinned: true}, "default", callID)
	s.Require().NoError(err)
	s.Require().Len(placed, 1)
	s.Equal("trunk-1", placed[0].TrunkID)

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
	s.Less(time.Since(began), claimReadTimeout+time.Second, "the silent shard was waited on past its read's bound")
}

// A shard that failed before the answer settled the read is logged, once a minute; one the
// settle cut off is not, since it was not known to be down.
func (s *StoreSuite) TestOnlyAShardThatFailedBeforeTheReadSettledIsLogged() {
	down, logged := s.withAShardDown()
	failedFirst := func(ctx context.Context, db bun.IDB) (bool, error) {
		if db == bun.IDB(down.shard) {
			time.Sleep(50 * time.Millisecond)
			return true, nil
		}
		return false, errors.New("refused")
	}
	for range 2 {
		_, failed := answeringShards(s.ctx, down, "test", peerReadTimeout, func(ok bool) bool { return ok }, failedFirst)
		s.Error(failed[1])
	}
	s.Equal(1, strings.Count(logged.String(), "shard=gone"), "logged, and once a minute rather than on every read")

	cutOff := &syncBuffer{}
	slog.SetDefault(slog.New(slog.NewTextHandler(cutOff, nil)))
	_, failed := answeringShards(s.ctx, down, "test", peerReadTimeout, func(ok bool) bool { return ok },
		func(ctx context.Context, db bun.IDB) (bool, error) {
			if db == bun.IDB(down.shard) {
				return true, nil
			}
			<-ctx.Done()
			return false, ctx.Err()
		})
	s.NoError(failed[1], "a read still running when the answer settled is not waited for")
	s.NotContains(cutOff.String(), "shard=", "a read the settle cut off is not logged")
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
