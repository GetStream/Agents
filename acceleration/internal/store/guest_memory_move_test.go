//go:build integration

package store

import (
	"time"
)

// queuedMove queues a move of guest's memories to account for acme and claims it.
func (s *StoreSuite) claimedMove(guest, account string) GuestMemoryMove {
	s.Require().NoError(s.store.GuestMemoryMoves.Queue(s.ctx, GuestMemoryMove{CustomerID: "acme", GuestID: guest, UserID: account}))
	s.timePasses("guest_memory_moves", time.Minute)
	claimed, err := s.store.GuestMemoryMoves.Claim(s.ctx, time.Minute, 1, nil)
	s.Require().NoError(err)
	s.Require().Len(claimed, 1)
	return claimed[0]
}

func (s *StoreSuite) TestAMoveWithoutBothUsersIsRefused() {
	for _, move := range []GuestMemoryMove{
		{GuestID: "guest", UserID: "account"},
		{CustomerID: "acme", UserID: "account"},
		{CustomerID: "acme", GuestID: "guest"},
		{CustomerID: "acme", GuestID: "same", UserID: "same"},
	} {
		s.Error(s.store.GuestMemoryMoves.Queue(s.ctx, move), "%+v", move)
	}
}

func (s *StoreSuite) TestASlowClaimCannotWriteAMoveAnotherClaimTook() {
	// A router that ran past its lease and the one that claimed the move after it would both
	// copy the guest's memories, without the claim being part of every write.
	first := s.claimedMove("guest", "account")
	s.timePasses("guest_memory_moves", 2*time.Minute)
	second, err := s.store.GuestMemoryMoves.Claim(s.ctx, time.Minute, 1, nil)
	s.Require().NoError(err)
	s.Require().Len(second, 1, "due again once the first lease ran out")

	held, err := s.store.GuestMemoryMoves.Held(s.ctx, first)
	s.Require().NoError(err)
	s.False(held)
	for name, write := range map[string]func(GuestMemoryMove) (bool, error){
		"Passed":  func(move GuestMemoryMove) (bool, error) { return s.store.GuestMemoryMoves.Passed(s.ctx, move, 0) },
		"Failed":  func(move GuestMemoryMove) (bool, error) { return s.store.GuestMemoryMoves.Failed(s.ctx, move, 0) },
		"Release": func(move GuestMemoryMove) (bool, error) { return s.store.GuestMemoryMoves.Release(s.ctx, move) },
		"Finish":  func(move GuestMemoryMove) (bool, error) { return s.store.GuestMemoryMoves.Finish(s.ctx, move) },
	} {
		written, err := write(first)
		s.Require().NoError(err, name)
		s.False(written, "%s by the first claim", name)
	}

	passed, err := s.store.GuestMemoryMoves.Passed(s.ctx, second[0], time.Hour)
	s.Require().NoError(err)
	s.True(passed)
	move, pending, err := s.store.GuestMemoryMoves.Pending(s.ctx, "acme", "guest")
	s.Require().NoError(err)
	s.Require().True(pending)
	s.Equal(1, move.Passes, "only the claim holding it counted a pass")
}

func (s *StoreSuite) TestAMoveUnderItsLeaseIsNotClaimedAgain() {
	s.claimedMove("guest", "account")

	again, err := s.store.GuestMemoryMoves.Claim(s.ctx, time.Minute, 1, nil)
	s.Require().NoError(err)
	s.Empty(again)
}

func (s *StoreSuite) TestAClaimSkipsTheMovesItIsToldTo() {
	ran := s.claimedMove("guest-1", "account")
	_, err := s.store.GuestMemoryMoves.Release(s.ctx, ran)
	s.Require().NoError(err)
	s.Require().NoError(s.store.GuestMemoryMoves.Queue(s.ctx, GuestMemoryMove{CustomerID: "acme", GuestID: "guest-2", UserID: "account"}))
	s.timePasses("guest_memory_moves", time.Minute)

	claimed, err := s.store.GuestMemoryMoves.Claim(s.ctx, time.Minute, 2, []int64{ran.ID})

	s.Require().NoError(err)
	s.Require().Len(claimed, 1)
	s.Equal("guest-2", claimed[0].GuestID)
}

func (s *StoreSuite) TestAFailedPassIsCountedAndAPassThatRanClearsTheCount() {
	move := s.claimedMove("guest", "account")
	_, err := s.store.GuestMemoryMoves.Failed(s.ctx, move, 0)
	s.Require().NoError(err)
	failed, _, err := s.store.GuestMemoryMoves.Pending(s.ctx, "acme", "guest")
	s.Require().NoError(err)
	s.Equal(1, failed.Failures)

	s.timePasses("guest_memory_moves", time.Minute)
	again, err := s.store.GuestMemoryMoves.Claim(s.ctx, time.Minute, 1, nil)
	s.Require().NoError(err)
	s.Require().Len(again, 1)
	_, err = s.store.GuestMemoryMoves.Passed(s.ctx, again[0], 0)
	s.Require().NoError(err)

	passed, _, err := s.store.GuestMemoryMoves.Pending(s.ctx, "acme", "guest")
	s.Require().NoError(err)
	s.Zero(passed.Failures)
	s.Equal(1, passed.Passes)
}

func (s *StoreSuite) TestAMoveQueuedAgainStartsItsPassesOver() {
	move := s.claimedMove("guest", "account")
	_, err := s.store.GuestMemoryMoves.Passed(s.ctx, move, 0)
	s.Require().NoError(err)

	s.Require().NoError(s.store.GuestMemoryMoves.Queue(s.ctx, GuestMemoryMove{CustomerID: "acme", GuestID: "guest", UserID: "account"}))

	queued, _, err := s.store.GuestMemoryMoves.Pending(s.ctx, "acme", "guest")
	s.Require().NoError(err)
	s.Zero(queued.Passes, "a guest's session ending leaves memories still to extract")
	s.Equal(move.ID, queued.ID, "one move per guest")
}

func (s *StoreSuite) TestAMoveIsOnlyItsCustomers() {
	s.Require().NoError(s.store.GuestMemoryMoves.Queue(s.ctx, GuestMemoryMove{CustomerID: "acme", GuestID: "guest", UserID: "account"}))
	s.Require().NoError(s.store.GuestMemoryMoves.Queue(s.ctx, GuestMemoryMove{CustomerID: "initech", GuestID: "guest", UserID: "account"}))

	_, pending, err := s.store.GuestMemoryMoves.Pending(s.ctx, "globex", "guest")
	s.Require().NoError(err)
	s.False(pending)
	guests, err := s.store.GuestMemoryMoves.DeleteFor(s.ctx, "acme", "account")
	s.Require().NoError(err)
	s.Equal([]string{"guest"}, guests)
	_, pending, err = s.store.GuestMemoryMoves.Pending(s.ctx, "initech", "guest")
	s.Require().NoError(err)
	s.True(pending, "another customer's guest of the same id keeps its move")
}

func (s *StoreSuite) TestGuestMemoryMoveQueryBudgets() {
	move := s.claimedMove("guest", "account")
	s.withinQueryBudgets([]queryBudget{
		{"Queue", 1, 0, func() error {
			return s.store.GuestMemoryMoves.Queue(s.ctx, GuestMemoryMove{CustomerID: "acme", GuestID: "other", UserID: "account"})
		}},
		{"Claim", 1, 0, func() error {
			_, err := s.store.GuestMemoryMoves.Claim(s.ctx, time.Minute, 1, []int64{move.ID})
			return err
		}},
		{"Held", 1, 0, func() error { _, err := s.store.GuestMemoryMoves.Held(s.ctx, move); return err }},
		{"Passed", 1, 0, func() error { _, err := s.store.GuestMemoryMoves.Passed(s.ctx, move, 0); return err }},
		{"Pending", 1, 0, func() error { _, _, err := s.store.GuestMemoryMoves.Pending(s.ctx, "acme", "guest"); return err }},
		{"DeleteFor", 1, 0, func() error { _, err := s.store.GuestMemoryMoves.DeleteFor(s.ctx, "acme", "account"); return err }},
	})
}
