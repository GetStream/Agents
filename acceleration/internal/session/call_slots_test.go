package session

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

// CallSlotsSuite is the cap on a connection's connector calls in flight in one process, and
// the memory it keeps: none for a connection with no call waiting or in flight.
type CallSlotsSuite struct {
	suite.Suite
	slots callSlots
}

func TestCallSlotsSuite(t *testing.T) {
	suite.Run(t, new(CallSlotsSuite))
}

func (s *CallSlotsSuite) SetupTest() {
	s.slots = callSlots{}
}

func (s *CallSlotsSuite) TestNoLimitHoldsNothing() {
	for range 3 {
		_, _, err := s.slots.take(s.T().Context(), "conn", 0)
		s.Require().NoError(err)
	}

	s.Empty(s.slots.byConnection)
}

func (s *CallSlotsSuite) TestAConnectionIsForgottenAfterItsLastCallEnds() {
	first, waited, err := s.slots.take(s.T().Context(), "conn", 2)
	s.Require().NoError(err)
	s.False(waited, "a slot was free")
	second, waited, err := s.slots.take(s.T().Context(), "conn", 2)
	s.Require().NoError(err)
	s.False(waited, "a slot was free")

	first()
	s.Len(s.slots.byConnection, 1, "one call is still in flight")
	second()

	s.Empty(s.slots.byConnection)
}

func (s *CallSlotsSuite) TestACallThatStopsWaitingIsCountedOut() {
	held, _, err := s.slots.take(s.T().Context(), "conn", 1)
	s.Require().NoError(err)
	waiting, cancel := context.WithTimeout(s.T().Context(), 20*time.Millisecond)
	defer cancel()

	_, _, err = s.slots.take(waiting, "conn", 1)
	s.Require().ErrorIs(err, context.DeadlineExceeded)
	held()

	s.Empty(s.slots.byConnection)
}

func (s *CallSlotsSuite) TestACallThatFoundNoSlotFreeSaysItWaited() {
	held, _, err := s.slots.take(s.T().Context(), "conn", 1)
	s.Require().NoError(err)
	type taken struct {
		release func()
		waited  bool
	}
	got := make(chan taken, 1)
	go func() {
		release, waited, err := s.slots.take(s.T().Context(), "conn", 1)
		if err == nil {
			got <- taken{release, waited}
		}
	}()
	s.Require().Eventually(func() bool {
		s.slots.mu.Lock()
		defer s.slots.mu.Unlock()
		return s.slots.byConnection["conn"].users == 2
	}, time.Second, time.Millisecond)
	held()

	select {
	case second := <-got:
		s.True(second.waited)
		second.release()
	case <-time.After(time.Second):
		s.FailNow("the waiting call never took the freed slot")
	}
}
