//go:build integration

package store

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// turn is a minute's lease on the channel cid in app, held by holder.
func channelTurn(app int64, cid, holder string) ChannelTurn {
	return ChannelTurn{StreamAppPK: app, CID: cid, LeasedBy: holder, Lease: time.Minute}
}

// leaseLeft is how long the lease on the channel cid in app has to run, on Postgres's clock.
func (s *StoreSuite) leaseLeft(app int64, cid string) time.Duration {
	var seconds float64
	s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx,
		"SELECT EXTRACT(EPOCH FROM lease_until - now()) FROM channel_turns WHERE stream_app_pk = ? AND cid = ?", app, cid).Scan(&seconds))
	return time.Duration(seconds * float64(time.Second))
}

// A router's clock is no input to the lease: one five seconds behind Postgres (or ahead) used to
// write a lease_until that now() judged five seconds short (or long). The lease is counted from
// now() where it is judged.
func (s *StoreSuite) TestAChannelTurnsLeaseLastsItsFullLengthOnTheDatabasesClock() {
	cid := "callouts:" + newID()

	taken, err := s.store.ChannelTurns.Take(s.ctx, channelTurn(7, cid, "router-a"))

	s.Require().NoError(err)
	s.Require().True(taken)
	s.InDelta(time.Minute, s.leaseLeft(7, cid), float64(time.Second))
}

// Two routers are two pools on one database; only one may run a channel's turn, in that app.
func (s *StoreSuite) TestTwoRoutersCannotHoldOneChannelsTurnAtOnce() {
	other, err := s.openRouter()
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = other.Close() })
	cid := "callouts:" + newID()

	first, err := s.store.ChannelTurns.Take(s.ctx, channelTurn(7, cid, "router-a"))
	s.Require().NoError(err)
	second, err := other.ChannelTurns.Take(s.ctx, channelTurn(7, cid, "router-b"))
	s.Require().NoError(err)
	elsewhere, err := other.ChannelTurns.Take(s.ctx, channelTurn(8, cid, "router-b"))
	s.Require().NoError(err)
	s.True(first)
	s.False(second, "router b waits while router a answers")
	s.True(elsewhere, "a channel of the same id in another app is another channel")

	s.Require().NoError(other.ChannelTurns.Release(s.ctx, 7, cid, "router-b"))
	again, err := other.ChannelTurns.Take(s.ctx, channelTurn(7, cid, "router-b"))
	s.Require().NoError(err)
	s.False(again, "a release by a holder that has no lease releases nothing")
	held, err := s.store.ChannelTurns.Take(s.ctx, channelTurn(8, cid, "router-c"))
	s.Require().NoError(err)
	s.False(held, "a release in one app lets go of no lease in another")

	s.Require().NoError(s.store.ChannelTurns.Release(s.ctx, 7, cid, "router-a"))
	after, err := other.ChannelTurns.Take(s.ctx, channelTurn(7, cid, "router-b"))
	s.Require().NoError(err)
	s.True(after)
}

func (s *StoreSuite) TestTwoRoutersAskingAtOnceGetOneChannelTurn() {
	cid := "callouts:" + newID()
	pools := []*Store{s.store}
	for range 3 {
		pool, err := s.openRouter()
		s.Require().NoError(err)
		s.T().Cleanup(func() { _ = pool.Close() })
		pools = append(pools, pool)
	}
	var wg sync.WaitGroup
	taken := make([]bool, len(pools))
	for i, pool := range pools {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := pool.ChannelTurns.Take(s.ctx, channelTurn(7, cid, fmt.Sprintf("router-%d", i)))
			s.NoError(err)
			taken[i] = ok
		}()
	}
	wg.Wait()

	count := 0
	for _, ok := range taken {
		if ok {
			count++
		}
	}
	s.Equal(1, count)
}

// A router that stopped mid-turn leaves its lease; it runs out and the next router answers.
func (s *StoreSuite) TestAChannelTurnWhoseLeaseRanOutCanBeTaken() {
	cid := "callouts:" + newID()
	_, err := s.store.ChannelTurns.Take(s.ctx, channelTurn(7, cid, "router-a"))
	s.Require().NoError(err)
	_, err = s.store.GlobalDB().ExecContext(s.ctx, "UPDATE channel_turns SET lease_until = now() - interval '1 second' WHERE cid = ?", cid)
	s.Require().NoError(err)

	taken, err := s.store.ChannelTurns.Take(s.ctx, channelTurn(7, cid, "router-b"))

	s.Require().NoError(err)
	s.True(taken)
}

// The holder of a turn takes it again to stretch its lease: a long reply outlasts no lease.
func (s *StoreSuite) TestTheHolderOfAChannelTurnExtendsItsLease() {
	cid := "callouts:" + newID()
	_, err := s.store.ChannelTurns.Take(s.ctx, channelTurn(7, cid, "router-a"))
	s.Require().NoError(err)
	later := channelTurn(7, cid, "router-a")
	later.Lease = time.Hour

	again, err := s.store.ChannelTurns.Take(s.ctx, later)

	s.Require().NoError(err)
	s.True(again)
	s.InDelta(time.Hour, s.leaseLeft(7, cid), float64(time.Second))
}

func (s *StoreSuite) TestAChannelTurnNoOneHoldsIsNotWritten() {
	cid := "callouts:" + newID()

	_, err := s.store.ChannelTurns.Take(s.ctx, channelTurn(7, cid, ""))

	var invalid *ValidationError
	s.Require().True(errors.As(err, &invalid))
	s.Equal("leased_by", invalid.Field)
	var rows int
	s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx, "SELECT count(*) FROM channel_turns WHERE cid = ?", cid).Scan(&rows))
	s.Zero(rows)
}

func (s *StoreSuite) TestChannelTurnQueryBudgets() {
	cid := "callouts:" + newID()

	s.Equal(1, s.countQueries(func() {
		taken, err := s.store.ChannelTurns.Take(s.ctx, channelTurn(7, cid, "router-a"))
		s.Require().NoError(err)
		s.True(taken)
	}))
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.ChannelTurns.Release(s.ctx, 7, cid, "router-a")) }))
}
