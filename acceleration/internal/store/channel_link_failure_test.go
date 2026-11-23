//go:build integration

package store

import (
	"sync"
	"time"
)

// Codes reserved at the same moment from many numbers are held to the line's count: none is
// reserved on a total the others have not yet added to.
func (s *StoreSuite) TestCodesReservedAtOnceFromManyNumbersStopAtTheLinesCount() {
	line, window := newID(), s.linkWindow()

	reserved := s.reserveAtOnce(60, func(n int) (bool, error) {
		return s.store.ChannelLinkFailures.Reserve(s.ctx, line, "+1555"+newID(), window, window, 5, 20)
	})

	s.Equal(20, reserved)
	s.Equal(20, s.linkFailures(line, window), "a refused code counts nothing")
}

// Codes one number reserves at the same moment, from routers of their own, are held to its count.
func (s *StoreSuite) TestCodesReservedAtOnceFromOneNumberStopAtItsCount() {
	line, window := newID(), s.linkWindow()

	reserved := s.reserveAtOnce(10, func(int) (bool, error) {
		return s.store.ChannelLinkFailures.Reserve(s.ctx, line, "+15550001", window, window, 5, 20)
	})

	s.Equal(5, reserved)
	s.Equal(5, s.linkFailures(line, window), "a refused code counts nothing")
}

// A router that reserves a code while another has the line's count uncommitted waits for it, and
// is then refused on the count it left: it never reserves on a total the other had not finished.
func (s *StoreSuite) TestACodeIsReservedOnlyOnceAnotherRoutersCountOfTheLineIsCommitted() {
	line, window := newID(), s.linkWindow()
	other, err := Open(s.dsn)
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.Require().NoError(other.Close()) })

	tx, err := s.store.global.BeginTx(s.ctx, nil)
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = tx.Rollback() })
	s.Require().NoError(lockUntilCommit(s.ctx, tx, line, channelLinkFailureLockSeed))
	_, err = tx.NewInsert().Model(&channelLinkFailure{AccountID: line, FromAddress: "+15550001", WindowStart: window, Failures: 20}).Exec(s.ctx)
	s.Require().NoError(err)

	type reservation struct {
		reserved bool
		err      error
	}
	done := make(chan reservation, 1)
	go func() {
		reserved, err := other.ChannelLinkFailures.Reserve(s.ctx, line, "+15550002", window, window, 5, 20)
		done <- reservation{reserved, err}
	}()

	select {
	case got := <-done:
		s.Failf("reserved before the count was committed", "reserved %v, error %v", got.reserved, got.err)
	case <-time.After(500 * time.Millisecond):
	}

	s.Require().NoError(tx.Commit())
	select {
	case got := <-done:
		s.Require().NoError(got.err)
		s.False(got.reserved, "the committed count is the line's twentieth")
	case <-time.After(10 * time.Second):
		s.Fail("still waiting after the count was committed")
	}
	s.Equal(20, s.linkFailures(line, window))
}

// A line's count is its own and its window's: another line, or the next window, starts at none.
func (s *StoreSuite) TestALinkCodeIsCountedOnItsOwnLineAndWindow() {
	line, other, window := newID(), newID(), s.linkWindow()
	for n := range 20 {
		reserved, err := s.store.ChannelLinkFailures.Reserve(s.ctx, line, "+1555"+newID(), window, window, 5, 20)
		s.Require().NoError(err)
		s.Require().True(reserved, "code %d", n)
	}

	refused, err := s.store.ChannelLinkFailures.Reserve(s.ctx, line, "+1555"+newID(), window, window, 5, 20)
	s.Require().NoError(err)
	onOther, err := s.store.ChannelLinkFailures.Reserve(s.ctx, other, "+1555"+newID(), window, window, 5, 20)
	s.Require().NoError(err)
	next, err := s.store.ChannelLinkFailures.Reserve(s.ctx, line, "+1555"+newID(), window.Add(15*time.Minute), window, 5, 20)
	s.Require().NoError(err)

	s.False(refused)
	s.True(onOther, "another line")
	s.True(next, "the next window")
}

// A code given back is no longer counted: the number may try one more.
func (s *StoreSuite) TestAReleasedLinkCodeIsCountedNoMore() {
	line, window := newID(), s.linkWindow()
	for range 5 {
		reserved, err := s.store.ChannelLinkFailures.Reserve(s.ctx, line, "+15550001", window, window, 5, 20)
		s.Require().NoError(err)
		s.Require().True(reserved)
	}

	other, before := newID(), window.Add(-15*time.Minute)
	for _, reserve := range []struct {
		line, from string
		window     time.Time
	}{{line, "+15550002", window}, {other, "+15550001", window}, {line, "+15550001", before}} {
		reserved, err := s.store.ChannelLinkFailures.Reserve(s.ctx, reserve.line, reserve.from, reserve.window, before, 5, 20)
		s.Require().NoError(err)
		s.Require().True(reserved)
	}

	s.Require().NoError(s.store.ChannelLinkFailures.Release(s.ctx, line, "+15550001", window))

	s.Equal(4+1, s.linkFailures(line, window), "the other number's code is still counted")
	s.Equal(1, s.linkFailures(other, window), "another line's")
	s.Equal(1, s.linkFailures(line, before), "the window before's")
	again, err := s.store.ChannelLinkFailures.Reserve(s.ctx, line, "+15550001", window, window, 5, 20)
	s.Require().NoError(err)
	s.True(again)
	refused, err := s.store.ChannelLinkFailures.Reserve(s.ctx, line, "+15550001", window, window, 5, 20)
	s.Require().NoError(err)
	s.False(refused)
}

func (s *StoreSuite) TestReservingALinkCodeForgetsWindowsPastTheirTime() {
	line := newID()
	old := s.linkWindow().Add(-time.Hour)
	_, err := s.store.ChannelLinkFailures.Reserve(s.ctx, line, "+15550001", old, old.Add(-time.Hour), 5, 20)
	s.Require().NoError(err)
	s.Require().Equal(1, s.linkFailures(line, old))

	_, err = s.store.ChannelLinkFailures.Reserve(s.ctx, newID(), "+15550002", old.Add(time.Hour), old.Add(time.Minute), 5, 20)
	s.Require().NoError(err)

	kept, err := s.store.global.NewSelect().Model((*channelLinkFailure)(nil)).Where("account_id = ?", line).Count(s.ctx)
	s.Require().NoError(err)
	s.Zero(kept)
}

// A code of no number, or on no line, is refused rather than counted for every such message together.
func (s *StoreSuite) TestALinkCodeOfNoNumberOrNoLineIsNotReserved() {
	window := s.linkWindow()

	_, noNumber := s.store.ChannelLinkFailures.Reserve(s.ctx, newID(), "", window, window, 5, 20)
	_, noLine := s.store.ChannelLinkFailures.Reserve(s.ctx, "", "+15550001", window, window, 5, 20)

	var invalid *ValidationError
	s.Require().ErrorAs(noNumber, &invalid)
	s.Equal("from_address", invalid.Field)
	s.Require().ErrorAs(noLine, &invalid)
	s.Equal("account_id", invalid.Field)
}

func (s *StoreSuite) TestChannelLinkFailureQueryBudgets() {
	line, window := newID(), s.linkWindow()

	s.Equal(4, s.countQueries(func() {
		_, err := s.store.ChannelLinkFailures.Reserve(s.ctx, line, "+15550001", window, window, 5, 20)
		s.Require().NoError(err)
	}), "Reserve: begin, lock, count and write, commit")
	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.ChannelLinkFailures.Release(s.ctx, line, "+15550001", window))
	}), "Release")
}

// linkWindow is the window link codes are counted in now.
func (s *StoreSuite) linkWindow() time.Time { return time.Now().UTC().Truncate(15 * time.Minute) }

// reserveAtOnce runs reserve n times at the same moment and is how many reserved a code.
func (s *StoreSuite) reserveAtOnce(n int, reserve func(n int) (bool, error)) int {
	var (
		wait     sync.WaitGroup
		mu       sync.Mutex
		reserved int
		failed   []error
	)
	start := make(chan struct{})
	for i := range n {
		wait.Go(func() {
			<-start
			ok, err := reserve(i)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				failed = append(failed, err)
			}
			if ok {
				reserved++
			}
		})
	}
	close(start)
	wait.Wait()
	s.Require().Empty(failed)
	return reserved
}

// linkFailures is how many codes a line has counted in a window, from every number.
func (s *StoreSuite) linkFailures(line string, window time.Time) int {
	var total int
	s.Require().NoError(s.store.global.NewSelect().Model((*channelLinkFailure)(nil)).
		ColumnExpr("COALESCE(SUM(failures), 0)").
		Where("account_id = ? AND window_start = ?", line, window).
		Scan(s.ctx, &total))
	return total
}
