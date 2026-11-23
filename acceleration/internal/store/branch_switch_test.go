//go:build integration

package store

import (
	"fmt"
	"time"
)

// switched records a switch of session "one" on into, hiding from from at at.
func (s *StoreSuite) switched(into *Store, id string, from, at time.Time, state string) {
	s.Require().NoError(into.BranchSwitches.Insert(s.ctx, &AgentBranchSwitch{
		ID: id, SessionID: "one", CustomerID: "app", Kind: SwitchRegenerate, TargetResponseID: "second",
		ForkMessageID: "m-" + id, HiddenFrom: from, State: state, CreatedAt: at,
	}))
}

func (s *StoreSuite) TestAnotherCustomersSwitchUnderASwitchIdIsRefused() {
	s.opened("one", "app", s.base, nil)
	s.opened("theirs", "somebody-else", s.base, nil)
	s.switched(s.store, "one", s.base, s.base.Add(time.Minute), "")

	err := s.store.BranchSwitches.Insert(s.ctx, &AgentBranchSwitch{
		ID: "one", SessionID: "theirs", CustomerID: "somebody-else", Kind: SwitchEdit, TargetResponseID: "theirs",
		ForkMessageID: "m-theirs", HiddenFrom: s.base, State: SwitchDone, CreatedAt: s.base.Add(time.Hour),
	})

	s.ErrorIs(err, ErrIDTaken, "the writer is told the id is not its own")
	unfinished, err := s.store.BranchSwitches.Unfinished(s.ctx, "app", "one")
	s.Require().NoError(err)
	s.Require().Len(unfinished, 1)
	s.Equal(SwitchSnapshotted, unfinished[0].State, "app's switch is as it was")
	s.Equal("m-one", unfinished[0].ForkMessageID)
	theirs, err := s.store.BranchSwitches.Unfinished(s.ctx, "somebody-else", "theirs")
	s.Require().NoError(err)
	s.Empty(theirs)
}

func (s *StoreSuite) TestASwitchOnlyMovesForward() {
	s.opened("one", "app", s.base, nil)
	s.switched(s.store, "switch", s.base, s.base.Add(time.Minute), "")

	s.Require().NoError(s.store.BranchSwitches.Advance(s.ctx, "app", "switch", SwitchResending))
	s.Require().NoError(s.store.BranchSwitches.Advance(s.ctx, "app", "switch", SwitchRewound))

	unfinished, err := s.store.BranchSwitches.Unfinished(s.ctx, "app", "one")
	s.Require().NoError(err)
	s.Require().Len(unfinished, 1)
	s.Equal(SwitchResending, unfinished[0].State, "a late router finishing an earlier step does not move it back")

	s.Require().NoError(s.store.BranchSwitches.Advance(s.ctx, "app", "switch", SwitchDone))
	unfinished, err = s.store.BranchSwitches.Unfinished(s.ctx, "app", "one")
	s.Require().NoError(err)
	s.Empty(unfinished)
}

func (s *StoreSuite) TestStartingASwitchAgainKeepsHowFarItGot() {
	s.opened("one", "app", s.base, nil)
	s.switched(s.store, "switch", s.base, s.base.Add(time.Minute), "")
	s.Require().NoError(s.store.BranchSwitches.Advance(s.ctx, "app", "switch", SwitchRewound))

	s.switched(s.store, "switch", s.base, s.base.Add(time.Minute), "")

	unfinished, err := s.store.BranchSwitches.Unfinished(s.ctx, "app", "one")
	s.Require().NoError(err)
	s.Require().Len(unfinished, 1)
	s.Equal(SwitchRewound, unfinished[0].State)
}

func (s *StoreSuite) TestAdvancingToAStateThereIsNotIsRefused() {
	s.opened("one", "app", s.base, nil)
	s.switched(s.store, "switch", s.base, s.base.Add(time.Minute), "")

	s.ErrorContains(s.store.BranchSwitches.Advance(s.ctx, "app", "switch", "abandoned"), "unknown state")
}

func (s *StoreSuite) TestEachMessageSentAgainIsCounted() {
	s.opened("one", "app", s.base, nil)
	s.switched(s.store, "switch", s.base, s.base.Add(time.Minute), "")

	first, err := s.store.BranchSwitches.CountResent(s.ctx, "app", "switch")
	s.Require().NoError(err)
	second, err := s.store.BranchSwitches.CountResent(s.ctx, "app", "switch")
	s.Require().NoError(err)

	s.Equal([2]int{1, 2}, [2]int{first, second})
}

func (s *StoreSuite) TestAnAbandonedSwitchTakesOnlyItsOwnCopiesWithIt() {
	s.opened("one", "app", s.base, nil)
	s.switched(s.store, "earlier", s.base, s.base.Add(time.Minute), SwitchDone)
	s.switched(s.store, "refused", s.base.Add(2*time.Minute), s.base.Add(3*time.Minute), "")
	s.Require().NoError(s.store.BranchMessages.UpsertMany(s.ctx, []AgentBranchMessage{
		hidden("m1", "second", "earlier", s.base, "hidden"),
		hidden("m2", "third", "refused", s.base.Add(2*time.Minute), "still visible"),
	}))

	s.Require().NoError(s.store.BranchSwitches.Abandon(s.ctx, "app", "one", "refused"))

	left, err := s.store.BranchMessages.Select(s.ctx, "app", "one", nil)
	s.Require().NoError(err)
	s.Require().Len(left, 1)
	s.Equal("m1", left[0].OriginalMessageID)
	unfinished, err := s.store.BranchSwitches.Unfinished(s.ctx, "app", "one")
	s.Require().NoError(err)
	s.Empty(unfinished)
	count, err := s.store.BranchSwitches.MergedRangeCount(s.ctx, "app", "one")
	s.Require().NoError(err)
	s.Equal(1, count)
}

func (s *StoreSuite) TestUnfinishedSwitchesAreOldestFirst() {
	s.opened("one", "app", s.base, nil)
	s.switched(s.store, "later", s.base, s.base.Add(2*time.Minute), "")
	s.switched(s.store, "earlier", s.base, s.base.Add(time.Minute), SwitchResending)
	s.switched(s.store, "finished", s.base, s.base.Add(30*time.Second), SwitchDone)

	unfinished, err := s.store.BranchSwitches.Unfinished(s.ctx, "app", "one")
	s.Require().NoError(err)
	s.Require().Len(unfinished, 2)
	s.Equal("earlier", unfinished[0].ID)
	s.Equal("later", unfinished[1].ID)
}

func (s *StoreSuite) TestTheRangesRewindsHidAreCountedMerged() {
	s.opened("one", "app", s.base, nil)
	// Two rewinds whose ranges overlap, one that starts where the previous one ended, one
	// apart from the rest, and one that has not rewound the channel yet.
	s.switched(s.store, "a", s.base, s.base.Add(10*time.Minute), SwitchDone)
	s.switched(s.store, "b", s.base.Add(5*time.Minute), s.base.Add(15*time.Minute), SwitchDone)
	s.switched(s.store, "c", s.base.Add(15*time.Minute), s.base.Add(20*time.Minute), SwitchRewound)
	s.switched(s.store, "d", s.base.Add(time.Hour), s.base.Add(2*time.Hour), SwitchResending)
	s.switched(s.store, "e", s.base.Add(3*time.Hour), s.base.Add(4*time.Hour), SwitchSnapshotted)

	count, err := s.store.BranchSwitches.MergedRangeCount(s.ctx, "app", "one")
	s.Require().NoError(err)

	s.Equal(2, count)
}

// An activation of a branch whose sibling never landed hides nothing, but is journaled all the
// same so its messages are sent again only once: it is no range of Chat's.
func (s *StoreSuite) TestAnActivationThatHidNothingIsNoRange() {
	s.opened("one", "app", s.base, nil)
	s.switched(s.store, "regenerate", s.base, s.base.Add(10*time.Minute), SwitchDone)
	s.Require().NoError(s.store.BranchMessages.UpsertMany(s.ctx, []AgentBranchMessage{hidden("m1", "second", "regenerate", s.base, "gone")}))
	s.Require().NoError(s.store.BranchSwitches.Insert(s.ctx, &AgentBranchSwitch{
		ID: "nothing-hidden", SessionID: "one", CustomerID: "app", Kind: SwitchActivate, TargetResponseID: "second",
		ForkMessageID: "m-last", HiddenFrom: s.base.Add(time.Hour), State: SwitchDone, CreatedAt: s.base.Add(2 * time.Hour),
	}))
	s.Require().NoError(s.store.BranchSwitches.Insert(s.ctx, &AgentBranchSwitch{
		ID: "hid-some", SessionID: "one", CustomerID: "app", Kind: SwitchActivate, TargetResponseID: "second",
		ForkMessageID: "m-fork", HiddenFrom: s.base.Add(3 * time.Hour), State: SwitchDone, CreatedAt: s.base.Add(4 * time.Hour),
	}))
	s.Require().NoError(s.store.BranchMessages.UpsertMany(s.ctx, []AgentBranchMessage{hidden("m2", "third", "hid-some", s.base.Add(3*time.Hour), "gone")}))

	count, err := s.store.BranchSwitches.MergedRangeCount(s.ctx, "app", "one")

	s.Require().NoError(err)
	s.Equal(2, count, "the regenerate and the activation that hid a message")
}

func (s *ShardedStoreSuite) TestTheRangesOfEveryShardAreCountedTogether() {
	s.opened("one", "app", s.base, nil)
	s.Require().NoError(s.other.Sessions.Upsert(s.ctx, &AgentSession{ID: "one", CustomerID: "app", AgentName: "docs", UserID: "jlahey", CallerKind: "user", CreatedAt: s.base}))
	s.switched(s.store, "here", s.base, s.base.Add(10*time.Minute), SwitchDone)
	s.switched(s.other, "there", s.base.Add(5*time.Minute), s.base.Add(15*time.Minute), SwitchDone)
	s.switched(s.other, "apart", s.base.Add(time.Hour), s.base.Add(2*time.Hour), SwitchDone)

	count, err := s.store.BranchSwitches.MergedRangeCount(s.ctx, "app", "one")
	s.Require().NoError(err)

	s.Equal(2, count)
}

func (s *StoreSuite) TestDeletingASessionTakesItsBranchesWithIt() {
	s.branchedAfterFirst()
	s.Require().NoError(s.store.BranchMessages.UpsertMany(s.ctx, []AgentBranchMessage{hidden("m1", "asked", "switch", s.base, "gone")}))
	s.switched(s.store, "switch", s.base, s.base.Add(time.Minute), SwitchRewound)

	s.Require().NoError(s.store.Sessions.Delete(s.ctx, "app", "one"))

	messages, err := s.store.BranchMessages.Select(s.ctx, "app", "one", nil)
	s.Require().NoError(err)
	s.Empty(messages)
	unfinished, err := s.store.BranchSwitches.Unfinished(s.ctx, "app", "one")
	s.Require().NoError(err)
	s.Empty(unfinished)
	count, err := s.store.BranchSwitches.MergedRangeCount(s.ctx, "app", "one")
	s.Require().NoError(err)
	s.Zero(count)
}

func (s *ShardedStoreSuite) TestDeletingASessionTakesItsBranchesWithItInEveryShard() {
	s.opened("one", "app", s.base, nil)
	s.Require().NoError(s.other.Sessions.Upsert(s.ctx, &AgentSession{ID: "one", CustomerID: "app", AgentName: "docs", UserID: "jlahey", CallerKind: "user", CreatedAt: s.base}))
	s.Require().NoError(s.store.BranchMessages.UpsertMany(s.ctx, []AgentBranchMessage{hidden("here", "second", "switch", s.base, "gone")}))
	s.Require().NoError(s.other.BranchMessages.UpsertMany(s.ctx, []AgentBranchMessage{hidden("there", "second", "switch", s.base, "gone")}))
	s.switched(s.store, "here", s.base, s.base.Add(time.Minute), SwitchRewound)
	s.switched(s.other, "there", s.base, s.base.Add(time.Minute), SwitchRewound)
	messages, err := s.store.BranchMessages.Select(s.ctx, "app", "one", nil)
	s.Require().NoError(err)
	s.Require().Len(messages, 2)

	s.Require().NoError(s.store.Sessions.Delete(s.ctx, "app", "one"))

	messages, err = s.store.BranchMessages.Select(s.ctx, "app", "one", nil)
	s.Require().NoError(err)
	s.Empty(messages)
	unfinished, err := s.store.BranchSwitches.Unfinished(s.ctx, "app", "one")
	s.Require().NoError(err)
	s.Empty(unfinished)
}

func (s *StoreSuite) TestBranchesAreOnlyTheirOwnCustomers() {
	s.branchedAfterFirst()
	s.Require().NoError(s.store.BranchMessages.UpsertMany(s.ctx, []AgentBranchMessage{hidden("m1", "asked", "switch", s.base, "mine")}))
	s.switched(s.store, "switch", s.base, s.base.Add(time.Minute), SwitchRewound)
	s.Require().NoError(s.store.Responses.Rewind(s.ctx, "app", "one", "first", s.base.Add(5*time.Minute)))

	_, err := s.store.Responses.Siblings(s.ctx, "somebody-else", "one", "asked", SiblingsOfReply)
	s.ErrorIs(err, ErrUnknownResponse)
	_, err = s.store.Responses.BranchTail(s.ctx, "somebody-else", "one", "first")
	s.ErrorIs(err, ErrUnknownResponse)
	s.ErrorIs(s.store.Responses.MoveActivePath(s.ctx, "somebody-else", "one", "asked", s.base.Add(6*time.Minute)), ErrUnknownResponse)
	s.Equal([]string{"first"}, s.conversation(), "another customer moves nothing")

	messages, err := s.store.BranchMessages.Select(s.ctx, "somebody-else", "one", nil)
	s.Require().NoError(err)
	s.Empty(messages)
	deleted, err := s.store.BranchMessages.DeleteUser(s.ctx, "somebody-else", "jlahey")
	s.Require().NoError(err)
	s.Zero(deleted)
	stolen := hidden("m1", "asked", "switch", s.base, "theirs")
	stolen.CustomerID = "somebody-else"
	s.Require().NoError(s.store.BranchMessages.UpsertMany(s.ctx, []AgentBranchMessage{stolen}))
	messages, err = s.store.BranchMessages.Select(s.ctx, "app", "one", nil)
	s.Require().NoError(err)
	s.Require().Len(messages, 1)
	s.Equal("mine", messages[0].Message["text"], "another customer cannot overwrite a copy")

	s.Require().NoError(s.store.BranchSwitches.Advance(s.ctx, "somebody-else", "switch", SwitchDone))
	_, err = s.store.BranchSwitches.CountResent(s.ctx, "somebody-else", "switch")
	s.ErrorContains(err, "no switch")
	unfinished, err := s.store.BranchSwitches.Unfinished(s.ctx, "somebody-else", "one")
	s.Require().NoError(err)
	s.Empty(unfinished)
	count, err := s.store.BranchSwitches.MergedRangeCount(s.ctx, "somebody-else", "one")
	s.Require().NoError(err)
	s.Zero(count)

	unfinished, err = s.store.BranchSwitches.Unfinished(s.ctx, "app", "one")
	s.Require().NoError(err)
	s.Require().Len(unfinished, 1)
	s.Equal(SwitchRewound, unfinished[0].State)
	s.Zero(unfinished[0].Resent)
	s.Equal([]string{"first"}, s.conversation())
}

func (s *StoreSuite) TestBranchSwitchQueryBudgets() {
	at := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	s.opened("switching", "app", at, nil)
	for i := range 30 {
		s.Require().NoError(s.store.BranchSwitches.Insert(s.ctx, &AgentBranchSwitch{ID: fmt.Sprintf("seeded-%02d", i),
			SessionID: "switching", CustomerID: "app", Kind: SwitchRegenerate, State: SwitchDone}))
	}
	switched := &AgentBranchSwitch{ID: "switch", SessionID: "switching", CustomerID: "app", Kind: SwitchEdit,
		State: SwitchSnapshotted}
	s.withinQueryBudgets([]queryBudget{
		{"BranchSwitches.Insert", 1, 0, func() error { return s.store.BranchSwitches.Insert(s.ctx, switched) }},
		{"BranchSwitches.Advance", 0, 1, func() error {
			return s.store.BranchSwitches.Advance(s.ctx, "app", switched.ID, SwitchResending)
		}},
		{"BranchSwitches.CountResent", 1, 0, func() error {
			_, err := s.store.BranchSwitches.CountResent(s.ctx, "app", switched.ID)
			return err
		}},
		{"BranchSwitches.Unfinished", 0, 1, func() error {
			_, err := s.store.BranchSwitches.Unfinished(s.ctx, "app", "switching")
			return err
		}},
		{"BranchSwitches.MergedRangeCount", 0, 1, func() error {
			_, err := s.store.BranchSwitches.MergedRangeCount(s.ctx, "app", "switching")
			return err
		}},
		{"BranchSwitches.Abandon", 0, 2, func() error {
			return s.store.BranchSwitches.Abandon(s.ctx, "app", "switching", switched.ID)
		}},
	})
}

func (s *StoreSuite) TestASwitchOfNoKnownKindIsNotWritten() {
	err := s.store.BranchSwitches.Insert(s.ctx, &AgentBranchSwitch{ID: "invalid", SessionID: "one", CustomerID: "app", Kind: "undo"})
	s.refusedUnwritten(err, "agent_branch_switches", "id", "invalid")
}
