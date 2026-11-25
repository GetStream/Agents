//go:build integration

package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uptrace/bun"
)

func (s *StoreSuite) TestOnlyTheRefusedCallersLegIsRefused() {
	refused := CallLeg{CustomerID: "acme", StreamAppPK: 7, CallType: "phone", CallID: "number-call", StreamSessionID: "session-1"}
	s.Equal(1, s.countRedis(func() {
		s.Require().NoError(s.store.CallLegs.MarkRefused(s.ctx, "acme", 7, "phone", "number-call", "session-1", time.Hour))
	}))

	var found bool
	s.Equal(1, s.countRedis(func() {
		var err error
		found, err = s.store.CallLegs.Refused(s.ctx, refused)
		s.Require().NoError(err)
	}))
	s.True(found)
	for _, other := range []CallLeg{
		{CustomerID: "acme", StreamAppPK: 7, CallType: "phone", CallID: "number-call", StreamSessionID: "session-2"},
		{CustomerID: "acme", StreamAppPK: 8, CallType: "phone", CallID: "number-call", StreamSessionID: "session-1"},
		{CustomerID: "other", StreamAppPK: 7, CallType: "phone", CallID: "number-call", StreamSessionID: "session-1"},
	} {
		found, err := s.store.CallLegs.Refused(s.ctx, other)
		s.Require().NoError(err)
		s.False(found, "%+v", other)
	}
}

func (s *StoreSuite) TestARefusalIsForgottenAfterItsTTL() {
	leg := CallLeg{CustomerID: "acme", StreamAppPK: 7, CallType: "phone", CallID: "call", StreamSessionID: "session-1"}
	s.Require().NoError(s.store.CallLegs.MarkRefused(s.ctx, "acme", 7, "phone", "call", "session-1", time.Second))

	s.Eventually(func() bool {
		found, err := s.store.CallLegs.Refused(s.ctx, leg)
		s.Require().NoError(err)
		return !found
	}, 5*time.Second, 50*time.Millisecond)
}

// placedLeg places a call's leg at s.base, defaulting the fields a test does not care about.
func (s *StoreSuite) placedLeg(token string, change func(*CallLeg)) CallLeg {
	placed := s.base
	leg := CallLeg{
		Token:        token,
		CustomerID:   "app",
		CallType:     "default",
		CallID:       "call",
		Vendor:       "telnyx",
		Direction:    Outbound,
		LocalNumber:  "+15125550100",
		RemoteNumber: "+15125550199",
		Reports:      true,
		PlacedAt:     &placed,
	}
	if change != nil {
		change(&leg)
	}
	s.Require().NoError(s.store.CallLegs.Place(s.ctx, &leg))
	return leg
}

// unchanged reads a placed leg without writing it.
func unchanged(*CallLeg) bool { return false }

func (s *StoreSuite) TestAPlacedLegIsFoundByItsToken() {
	s.placedLeg("one", nil)
	s.placedLeg("two", func(leg *CallLeg) { leg.CallID = "other" })

	var found CallLeg
	s.Equal(1, s.countQueries(func() {
		var err error
		found, err = s.store.CallLegs.GetByToken(s.ctx, "two")
		s.Require().NoError(err)
	}))
	s.Equal("other", found.CallID)
	s.Equal(s.base, found.PlacedAt.UTC())

	_, err := s.store.CallLegs.ChangeOutcome(s.ctx, PlacedWhere{Token: "three"}, unchanged)
	s.ErrorIs(err, ErrNoPlacedCall)
	_, err = s.store.CallLegs.GetByToken(s.ctx, "three")
	s.ErrorIs(err, ErrNoPlacedCall)
}

func (s *StoreSuite) TestAPlacedLegIsFoundByItsTokenWithOnlyItsHashStored() {
	token := newID()
	placed := s.placedLeg(token, nil)

	found, err := s.store.CallLegs.GetByToken(s.ctx, token)
	s.Require().NoError(err)
	s.Equal(placed.ID, found.ID)
	var row string
	s.Require().NoError(s.store.shard.NewSelect().ColumnExpr("to_jsonb(cl)::text").TableExpr("call_legs AS cl").
		Where("cl.id = ?", placed.ID).Scan(s.ctx, &row))
	s.NotContains(row, token, "the leg holds the token a status post presents")
}

func (s *StoreSuite) TestAPlacedLegIsFoundByItsCallAndNumbers() {
	s.placedLeg("first", nil)
	again := s.placedLeg("again", nil)
	s.placedLeg("transfer", func(leg *CallLeg) { leg.RemoteNumber = "+15125550142" })
	where := PlacedWhere{CustomerID: "app", CallType: "default", CallID: "call", LocalNumber: "+15125550100", RemoteNumber: "+15125550199"}

	found, err := s.store.CallLegs.ChangeOutcome(s.ctx, where, unchanged)
	s.Require().NoError(err)
	s.Equal(again.ID, found.ID, "the newest match")

	where.StreamAppPK = 7
	_, err = s.store.CallLegs.ChangeOutcome(s.ctx, where, unchanged)
	s.ErrorIs(err, ErrNoPlacedCall, "another Stream app's call of the same id")
}

func (s *StoreSuite) TestAChangeThatSaysNoWritesNothing() {
	s.placedLeg("one", nil)

	_, err := s.store.CallLegs.ChangeOutcome(s.ctx, PlacedWhere{Token: "one"}, func(leg *CallLeg) bool {
		leg.Outcome = "busy"
		return false
	})
	s.Require().NoError(err)

	found, err := s.store.CallLegs.GetByToken(s.ctx, "one")
	s.Require().NoError(err)
	s.Empty(found.Outcome)
}

func (s *StoreSuite) TestAnOutcomeChangeWritesOnlyTheOutcome() {
	s.placedLeg("one", nil)
	answered := s.base.Add(time.Minute)

	changed, err := s.store.CallLegs.ChangeOutcome(s.ctx, PlacedWhere{Token: "one"}, func(leg *CallLeg) bool {
		leg.Outcome, leg.AnsweredAt, leg.VendorCallID = "completed", &answered, "vendor-1"
		leg.EndedAt, leg.OrganizationID = &answered, "someone else's"
		return true
	})
	s.Require().NoError(err)

	found, err := s.store.CallLegs.GetByToken(s.ctx, "one")
	s.Require().NoError(err)
	s.Equal(changed, found)
	s.Equal("completed", found.Outcome)
	s.Equal(answered, found.AnsweredAt.UTC())
	s.Equal("vendor-1", found.VendorCallID)
	s.Nil(found.EndedAt, "the leg's billed end is the leg's own")
	s.Empty(found.OrganizationID)
}

func (s *StoreSuite) TestAnOutcomeChangeTheTableWouldRefuseIsNotWritten() {
	s.placedLeg("one", nil)

	_, err := s.store.CallLegs.ChangeOutcome(s.ctx, PlacedWhere{Token: "one"}, func(leg *CallLeg) bool {
		leg.Outcome = "maybe"
		return true
	})
	var invalid *ValidationError
	s.Require().ErrorAs(err, &invalid)
	s.Equal("outcome", invalid.Field)
}

func (s *StoreSuite) TestTwoOutcomeChangesAtOnceSeeEachOther() {
	s.placedLeg("one", nil)

	var wg sync.WaitGroup
	for _, word := range []string{"a", "b"} {
		wg.Go(func() {
			_, err := s.store.CallLegs.ChangeOutcome(s.ctx, PlacedWhere{Token: "one"}, func(leg *CallLeg) bool {
				leg.VendorStatus += word
				return true
			})
			s.NoError(err)
		})
	}
	wg.Wait()

	found, err := s.store.CallLegs.GetByToken(s.ctx, "one")
	s.Require().NoError(err)
	s.ElementsMatch([]rune("ab"), []rune(found.VendorStatus), "neither write lost")
}

func (s *StoreSuite) TestUnplacingALegLeavesNoRow() {
	s.placedLeg("one", nil)

	s.Require().NoError(s.store.CallLegs.Unplace(s.ctx, "one"))

	legs, err := s.store.CallLegs.SelectPlaced(s.ctx, "app", []string{"call"})
	s.Require().NoError(err)
	s.Empty(legs)
}

func (s *StoreSuite) TestUnplacingNeverDeletesADialledLeg() {
	leg := s.placedLeg("one", nil)
	leg.OrganizationID = "org"
	_, err := s.store.CallLegs.Open(s.ctx, &leg)
	s.Require().NoError(err)

	s.Require().NoError(s.store.CallLegs.Unplace(s.ctx, "one"))

	found, err := s.store.CallLegs.GetByToken(s.ctx, "one")
	s.Require().NoError(err)
	s.Equal(leg.ID, found.ID)
}

func (s *StoreSuite) TestOpeningAPlacedLegDialsItOnce() {
	placed := s.placedLeg("one", nil)
	_, err := s.store.CallLegs.ChangeOutcome(s.ctx, PlacedWhere{Token: "one"}, func(leg *CallLeg) bool {
		leg.VendorStatus = "queued"
		return true
	})
	s.Require().NoError(err)

	dialed := CallLeg{Token: "one", CustomerID: "app", OrganizationID: "org", APIKey: "key", CallType: "default", CallID: "call",
		Vendor: "telnyx", VendorCallID: "vendor-1", Direction: Outbound, LocalNumber: "+15125550100", RemoteNumber: "+15125550199",
		Country: "US", DialedAt: s.base.Add(time.Second)}
	opened, err := s.store.CallLegs.Open(s.ctx, &dialed)
	s.Require().NoError(err)
	s.True(opened)

	s.Equal(placed.ID, dialed.ID, "the placed leg is the one dialled")
	s.Equal("org", dialed.OrganizationID)
	s.Equal("key", dialed.APIKey)
	s.Equal(s.base.Add(time.Second), dialed.DialedAt.UTC())
	s.Equal("queued", dialed.VendorStatus, "what the vendor said before the dial answered is kept")
	s.True(dialed.Reports)
	s.Equal(s.base, dialed.PlacedAt.UTC())

	again := dialed
	again.ID = 0
	opened, err = s.store.CallLegs.Open(s.ctx, &again)
	s.ErrorIs(err, ErrNoPlacedCall)
	s.False(opened)
}

// A transfer leg written again, as a write whose answer was lost is, is the one leg: a second
// ringing leg could be given to a phone of the call and bill the transfer twice.
func (s *StoreSuite) TestATransferLegWrittenTwiceIsOneLeg() {
	transfer := CallLeg{CustomerID: "app", StreamAppPK: 7, CallType: "default", CallID: "call", Vendor: "telnyx",
		VendorCallID: "vendor-1", Direction: Outbound, LocalNumber: "+15125550100", RemoteNumber: "+15125550199",
		DialedAt: s.base}
	first, again := transfer, transfer

	opened, err := s.store.CallLegs.Open(s.ctx, &first)
	s.Require().NoError(err)
	s.True(opened)
	opened, err = s.store.CallLegs.Open(s.ctx, &again)
	s.Require().NoError(err)
	s.False(opened)

	legs, err := s.store.shard.NewSelect().Model((*CallLeg)(nil)).Where("call_id = ?", "call").Count(s.ctx)
	s.Require().NoError(err)
	s.Equal(1, legs)
	other := transfer
	other.VendorCallID = "vendor-2"
	opened, err = s.store.CallLegs.Open(s.ctx, &other)
	s.Require().NoError(err)
	s.True(opened, "another transfer in the same call is a leg of its own")
}

func (s *StoreSuite) TestALegNotDialledYetRingsForNobody() {
	s.placedLeg("one", nil)
	scope := AppScope{}

	ringing, err := s.store.CallLegs.Ringing(s.ctx, scope, "default", "call")
	s.Require().NoError(err)
	s.False(ringing)
	_, connection, err := s.store.CallLegs.Connect(s.ctx, scope, "default", "call", "session", "phone", "+15125550199", s.base, nil)
	s.Require().NoError(err)
	s.Equal(NothingWaiting, connection)
	ended, err := s.store.CallLegs.EndCall(s.ctx, scope, "default", "call", s.base.Add(time.Minute))
	s.Require().NoError(err)
	s.Empty(ended)
	swept, err := s.store.CallLegs.Sweep(s.ctx, s.base.Add(48*time.Hour), time.Hour, 10)
	s.Require().NoError(err)
	s.Empty(swept)
}

func (s *StoreSuite) TestClearingTokensKeepsTheRows() {
	s.placedLeg("old", func(leg *CallLeg) { leg.CallID = "old" })
	s.placedLeg("new", func(leg *CallLeg) {
		leg.CallID = "new"
		later := s.base.Add(time.Hour)
		leg.PlacedAt = &later
	})

	var cleared int64
	s.Equal(1, s.countQueries(func() {
		var err error
		cleared, err = s.store.CallLegs.ClearTokens(s.ctx, s.base.Add(time.Minute))
		s.Require().NoError(err)
	}))
	s.EqualValues(1, cleared)

	_, err := s.store.CallLegs.GetByToken(s.ctx, "old")
	s.ErrorIs(err, ErrNoPlacedCall)
	legs, err := s.store.CallLegs.SelectPlaced(s.ctx, "app", []string{"old", "new"})
	s.Require().NoError(err)
	s.Len(legs, 2)
}

func (s *StoreSuite) TestSelectPlacedReadsTheNewestPerCall() {
	s.placedLeg("first", nil)
	s.placedLeg("second", func(leg *CallLeg) {
		later := s.base.Add(time.Minute)
		leg.PlacedAt = &later
	})
	s.placedLeg("other", func(leg *CallLeg) { leg.CallID = "other" })
	s.placedLeg("theirs", func(leg *CallLeg) {
		leg.CustomerID = "someone else"
		later := s.base.Add(time.Hour)
		leg.PlacedAt = &later
	})
	inbound := CallLeg{CustomerID: "app", CallType: "default", CallID: "unknown", Vendor: "telnyx", Direction: Inbound,
		LocalNumber: "+15125550100", RemoteNumber: "+15125550199", DialedAt: s.base}
	_, err := s.store.CallLegs.Open(s.ctx, &inbound)
	s.Require().NoError(err)

	var legs []CallLeg
	s.Equal(len(s.store.shards()), s.countQueries(func() {
		legs, err = s.store.CallLegs.SelectPlaced(s.ctx, "app", []string{"call", "other", "unknown"})
		s.Require().NoError(err)
	}), "one query a shard, however many calls")
	tokens := map[string]string{}
	for _, leg := range legs {
		tokens[leg.CallID] = leg.TokenHash
	}
	s.Equal(map[string]string{"call": callTokenHash("second"), "other": callTokenHash("other")}, tokens,
		"an arriving call's leg is no placed call")
}

func (s *StoreSuite) TestAPlacedLegNeedsItsTokenAndPlacing() {
	for field, broken := range map[string]func(*CallLeg){
		"token":     func(leg *CallLeg) { leg.Token = "" },
		"placed_at": func(leg *CallLeg) { leg.PlacedAt = nil },
		"direction": func(leg *CallLeg) { leg.Direction = Inbound },
		"dialed_at": func(leg *CallLeg) { leg.DialedAt = s.base },
	} {
		placed := s.base
		leg := CallLeg{Token: "t", CustomerID: "app", CallType: "default", CallID: "call", Vendor: "telnyx",
			Direction: Outbound, PlacedAt: &placed}
		broken(&leg)
		var invalid *ValidationError
		s.Require().ErrorAs(s.store.CallLegs.Place(s.ctx, &leg), &invalid, field)
		s.Equal(field, invalid.Field)
	}
	legs, err := s.store.CallLegs.SelectPlaced(s.ctx, "app", []string{"call"})
	s.Require().NoError(err)
	s.Empty(legs)
}

func (s *ShardedStoreSuite) TestALegAnotherShardPlacedIsChangedThere() {
	placed := s.base
	leg := CallLeg{Token: "there", CustomerID: "app", CallType: "default", CallID: "call", Vendor: "telnyx",
		Direction: Outbound, LocalNumber: "+15125550100", RemoteNumber: "+15125550199", PlacedAt: &placed}
	s.Require().NoError(s.other.CallLegs.Place(s.ctx, &leg))

	_, err := s.store.CallLegs.ChangeOutcome(s.ctx, PlacedWhere{Token: "there"}, func(leg *CallLeg) bool {
		leg.Outcome = "busy"
		return true
	})
	s.Require().NoError(err)

	found, err := s.other.CallLegs.GetByToken(s.ctx, "there")
	s.Require().NoError(err)
	s.Equal("busy", found.Outcome)
}

func (s *StoreSuite) TestCallLegQueryBudgets() {
	shards := len(s.store.shards())
	scope := AppScope{App: 7}
	open := func(callID, remote string, direction string) CallLeg {
		leg := CallLeg{CustomerID: "acme", OrganizationID: "org", StreamAppPK: 7, CallType: "default", CallID: callID,
			Vendor: "telnyx", Direction: direction, LocalNumber: "+15125550100", RemoteNumber: remote, DialedAt: s.base}
		opened, err := s.store.CallLegs.Open(s.ctx, &leg)
		s.Require().NoError(err)
		s.Require().True(opened)
		return leg
	}
	// Opening looks for the phone's join kept before it, by its remote number, then, outbound,
	// by the number it was placed from: one statement for each, in each shard, each its own transaction.
	s.Equal(1+2*shards*3, s.countQueries(func() { open("call-1", "+15125550101", Outbound) }), "Open of an outbound leg")
	s.Equal(1+shards*3, s.countQueries(func() { open("call-1", "+15125550102", Inbound) }), "Open of an inbound leg")
	open("call-1", "+15125550103", Outbound)

	s.Equal(shards, s.countQueries(func() {
		ringing, err := s.store.CallLegs.Ringing(s.ctx, scope, "default", "call-1")
		s.Require().NoError(err)
		s.True(ringing)
	}), "Ringing")
	s.Equal(shards+1, s.countQueries(func() {
		_, connection, err := s.store.CallLegs.Connect(s.ctx, scope, "default", "call-1", "session", "phone-1", "+15125550101", s.base, nil)
		s.Require().NoError(err)
		s.Equal(LegConnected, connection)
	}), "Connect asks every shard whether the phone has a leg, then gives it the leg of its number")
	s.Equal(shards, s.countQueries(func() {
		known, err := s.store.CallLegs.HasParticipant(s.ctx, scope, "default", "call-1", "phone-1")
		s.Require().NoError(err)
		s.True(known)
	}), "HasParticipant")
	s.Equal(shards, s.countQueries(func() {
		total, inCall, err := s.store.CallLegs.CountOpen(s.ctx, "org", s.base.Add(-time.Minute), "default", "call-1")
		s.Require().NoError(err)
		s.Equal(3, total)
		s.Equal(3, inCall)
	}), "CountOpen")
	s.Equal(shards, s.countQueries(func() {
		ended, err := s.store.CallLegs.EndParticipant(s.ctx, scope, "default", "call-1", "phone-1", s.base.Add(time.Minute))
		s.Require().NoError(err)
		s.Len(ended, 1)
	}), "EndParticipant")
	s.Equal(shards, s.countQueries(func() {
		ended, err := s.store.CallLegs.EndCall(s.ctx, scope, "default", "call-1", s.base.Add(time.Minute))
		s.Require().NoError(err)
		s.Len(ended, 2)
	}), "EndCall ends every open leg of the call in one statement a shard")
	s.Equal(1, s.countQueries(func() {
		ended, err := s.store.CallLegs.SelectEnded(s.ctx, s.base, s.base.Add(time.Hour), 10)
		s.Require().NoError(err)
		s.Len(ended, 1, "the inbound leg never started")
	}), "SelectEnded")

	for i := range 3 {
		answered := open(fmt.Sprintf("long-%d", i), "+15125550104", Inbound)
		_, _, err := s.store.CallLegs.Connect(s.ctx, scope, "default", answered.CallID, "session", "phone", "+15125550104", s.base, nil)
		s.Require().NoError(err)
		open(fmt.Sprintf("rang-%d", i), "+15125550105", Outbound)
	}
	s.Equal(2, s.countQueries(func() {
		swept, err := s.store.CallLegs.Sweep(s.ctx, s.base.Add(time.Hour), 30*time.Minute, 10)
		s.Require().NoError(err)
		s.Len(swept, 6)
	}), "Sweep ends the long calls, then the ones that rang out")

	leg := s.placedLeg("token", func(leg *CallLeg) { leg.VendorCallID = "v-1" })
	s.Equal(1, s.countQueries(func() { s.placedLeg("other", nil) }), "Place")
	// The transaction, its lock, the count in each shard, the insert and the commit.
	s.Equal(4+shards, s.countQueries(func() { s.Require().NoError(s.placeWithin(s.store, newID(), 1)) }), "PlaceWithin")
	s.Equal(1, s.countQueries(func() {
		placed, err := s.store.CallLegs.Placed(s.ctx, leg.CustomerID, leg.Vendor, "v-1")
		s.Require().NoError(err)
		s.True(placed)
	}), "Placed finds a leg of this shard there")
	s.Equal(2+2, s.countQueries(func() {
		_, err := s.store.CallLegs.ChangeOutcome(s.ctx, PlacedWhere{Token: "token"}, func(leg *CallLeg) bool {
			leg.Outcome = "completed"
			return true
		})
		s.Require().NoError(err)
	}), "ChangeOutcome locks the leg in its shard, then writes it")
	s.Equal(shards, s.countQueries(func() { s.Require().NoError(s.store.CallLegs.Unplace(s.ctx, "other")) }), "Unplace")
}

func (s *StoreSuite) TestAPlacedLegNotDialedYetIsACallUp() {
	organization := newID()
	s.placedLeg(newID(), func(leg *CallLeg) { leg.OrganizationID = organization })

	total, _, err := s.store.CallLegs.CountOpen(s.ctx, organization, s.base.Add(-time.Minute), "", "")
	s.Require().NoError(err)
	s.Equal(1, total, "placed now, so ringing")
	total, _, err = s.store.CallLegs.CountOpen(s.ctx, organization, s.base.Add(time.Minute), "", "")
	s.Require().NoError(err)
	s.Zero(total, "placed longer ago than anything rings")
}

// placeWithin places a leg of organization at s.base, no more than limit at once.
func (s *StoreSuite) placeWithin(on *Store, organization string, limit int) error {
	placed := s.base
	return on.CallLegs.PlaceWithin(s.ctx, &CallLeg{
		Token: newID(), CustomerID: "app", OrganizationID: organization, CallType: "default", CallID: "call-" + newID(),
		Vendor: "telnyx", Direction: Outbound, LocalNumber: "+15125550100", RemoteNumber: "+15125550199", PlacedAt: &placed,
	}, CallsAtOnce{Limit: limit, Ringing: time.Minute})
}

func (s *StoreSuite) TestALegPastItsOrganizationsCallsAtOnceIsNotPlaced() {
	organization := newID()
	s.Require().NoError(s.placeWithin(s.store, organization, 2))
	s.Require().NoError(s.placeWithin(s.store, organization, 2))

	s.Require().ErrorIs(s.placeWithin(s.store, organization, 2), ErrCallsAtOnce)
	s.Require().NoError(s.placeWithin(s.store, newID(), 2), "another organization's calls are its own")
	total, _, err := s.store.CallLegs.CountOpen(s.ctx, organization, s.base.Add(-time.Minute), "", "")
	s.Require().NoError(err)
	s.Equal(2, total)
}

func (s *StoreSuite) TestCallsPlacedAllAtOnceKeepToTheLimit() {
	organization := newID()
	var placed, refused int
	var mu sync.Mutex
	var wg sync.WaitGroup
	for range 30 {
		wg.Go(func() {
			err := s.placeWithin(s.store, organization, 4)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				placed++
			case errors.Is(err, ErrCallsAtOnce):
				refused++
			default:
				s.Fail("placing failed", "%v", err)
			}
		})
	}
	wg.Wait()

	s.Equal(4, placed)
	s.Equal(26, refused)
}

func (s *ShardedStoreSuite) TestALegAnotherShardPlacedCountsAgainstTheLimitHere() {
	organization := newID()
	s.Require().NoError(s.placeWithin(s.other, organization, 2))
	s.Require().NoError(s.placeWithin(s.store, organization, 2))

	s.Require().ErrorIs(s.placeWithin(s.store, organization, 2), ErrCallsAtOnce)
	s.Require().ErrorIs(s.placeWithin(s.other, organization, 2), ErrCallsAtOnce)
}

func (s *StoreSuite) TestManyRoutersPlacingAtOnceKeepToTheLimit() {
	organization := newID()
	routers := []*Store{s.store}
	for range 7 {
		router, err := s.openRouter()
		s.Require().NoError(err)
		s.T().Cleanup(func() { s.Require().NoError(router.Close()) })
		routers = append(routers, router)
	}
	var placed atomic.Int64
	var wg sync.WaitGroup
	for _, router := range routers {
		for range 4 {
			wg.Go(func() {
				err := s.placeWithin(router, organization, 3)
				if err == nil {
					placed.Add(1)
					return
				}
				s.ErrorIs(err, ErrCallsAtOnce)
			})
		}
	}
	wg.Wait()

	s.EqualValues(3, placed.Load())
}

// holdPlacing holds, from a connection of its own, the lock each organization's calls are placed
// under, until the release it returns is called.
func (s *StoreSuite) holdPlacing(organizations ...string) (release func()) {
	held, err := s.store.GlobalDB().BeginTx(s.ctx, nil)
	s.Require().NoError(err)
	for _, organization := range organizations {
		s.Require().NoError(lockUntilCommit(s.ctx, held, organization, callsAtOnceLockSeed))
	}
	released := false
	release = func() {
		if !released {
			released = true
			s.Require().NoError(held.Rollback())
		}
	}
	s.T().Cleanup(release)
	return release
}

// placingInUse is how many connections of router's shared pool are in use.
func placingInUse(router *Store) int { return router.global.Stats().InUse }

func (s *StoreSuite) TestAPlacementWaitsForItsOrganizationsLock() {
	organization := newID()
	release := s.holdPlacing(organization)

	placed := make(chan error, 1)
	go func() { placed <- s.placeWithin(s.store, organization, 1) }()
	s.Never(func() bool { return len(placed) > 0 }, 500*time.Millisecond, 20*time.Millisecond,
		"placed while another held the organization's lock")
	release()

	s.Require().NoError(<-placed)
}

func (s *StoreSuite) TestAPlacementThatCannotTakeItsLockInTimeGivesUp() {
	organization := newID()
	s.holdPlacing(organization)

	err := s.placeWithin(s.store, organization, 1)

	s.Require().Error(err)
	s.NotErrorIs(err, ErrCallsAtOnce)
	total, _, err := s.store.CallLegs.CountOpen(s.ctx, organization, s.base.Add(-time.Minute), "", "")
	s.Require().NoError(err)
	s.Zero(total)
}

func (s *StoreSuite) TestARouterWaitsOnOneConnectionForAnOrganizationsLock() {
	router, err := s.openRouter()
	s.Require().NoError(err)
	defer router.Close()
	organization := newID()
	release := s.holdPlacing(organization)

	var wg sync.WaitGroup
	for range 6 {
		wg.Go(func() { s.NoError(s.placeWithin(router, organization, 10)) })
	}
	time.Sleep(300 * time.Millisecond)
	s.Equal(1, placingInUse(router), "one placement of the organization waits for its lock, the others for their turn")
	release()
	wg.Wait()
}

func (s *StoreSuite) TestARouterHoldsAtMostItsPlacementsAtOnce() {
	router, err := s.openRouter()
	s.Require().NoError(err)
	defer router.Close()
	organizations := make([]string, 3*placingAtOnce)
	for i := range organizations {
		organizations[i] = newID()
	}
	release := s.holdPlacing(organizations...)

	var wg sync.WaitGroup
	for _, organization := range organizations {
		wg.Go(func() { s.NoError(s.placeWithin(router, organization, 1)) })
	}
	s.Require().Eventually(func() bool { return placingInUse(router) == placingAtOnce }, 2*time.Second, 10*time.Millisecond,
		"as many placements as a router holds started, each waiting for its organization's lock")
	s.Never(func() bool { return placingInUse(router) > placingAtOnce }, 300*time.Millisecond, 10*time.Millisecond,
		"the shared pool keeps room for every other request")
	release()
	wg.Wait()
}

// cancelAfterInsert ends a context once a leg is inserted.
type cancelAfterInsert struct{ cancel context.CancelFunc }

func (h cancelAfterInsert) BeforeQuery(ctx context.Context, _ *bun.QueryEvent) context.Context {
	return ctx
}

func (h cancelAfterInsert) AfterQuery(_ context.Context, event *bun.QueryEvent) {
	if strings.Contains(event.Query, `INSERT INTO "call_legs"`) {
		h.cancel()
	}
}

func (s *ShardedStoreSuite) TestALegItsShardKeptForAPlacementThatFailedHoldsNoCall() {
	router, err := s.openRouter()
	s.Require().NoError(err)
	defer router.Close()
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	// The request goes away once the shard kept the leg, so the lock's commit fails.
	router.shard.AddQueryHook(cancelAfterInsert{cancel})
	organization := newID()
	placed := s.base

	err = router.CallLegs.PlaceWithin(ctx, &CallLeg{
		Token: newID(), CustomerID: "app", OrganizationID: organization, CallType: "default", CallID: "call-" + newID(),
		Vendor: "telnyx", Direction: Outbound, LocalNumber: "+15125550100", RemoteNumber: "+15125550199", PlacedAt: &placed,
	}, CallsAtOnce{Limit: 1, Ringing: time.Minute})

	s.Require().Error(err)
	total, _, err := s.store.CallLegs.CountOpen(s.ctx, organization, s.base.Add(-time.Minute), "", "")
	s.Require().NoError(err)
	s.Zero(total, "the leg the shard kept was deleted")
	s.NoError(s.placeWithin(s.store, organization, 1))
}

func (s *StoreSuite) TestADialNamingNoOrganizationKeepsTheOneTheCallWasPlacedUnder() {
	s.placedLeg("placed", func(leg *CallLeg) { leg.OrganizationID = "org" })

	dialed := CallLeg{Token: "placed", CustomerID: "app", CallType: "default", CallID: "call", Vendor: "telnyx",
		Direction: Outbound, LocalNumber: "+15125550100", RemoteNumber: "+15125550199", DialedAt: s.base.Add(time.Second)}
	opened, err := s.store.CallLegs.Open(s.ctx, &dialed)
	s.Require().NoError(err)
	s.Require().True(opened)

	s.Equal("org", dialed.OrganizationID, "still counted among the organization's calls")
}

// ownerOf is the customer the call is, read as Claim reads it.
func (s *StoreSuite) ownerOf(scope AppScope, callID string) string {
	owner, err := callOwner(s.ctx, s.store.shards(), scope, "agent", callID)
	s.Require().NoError(err)
	return owner
}

// claims says a customer's session is on the call, or how that is refused.
func (s *StoreSuite) claims(customer, callID string, on NamedCall) error {
	return s.store.CallLegs.Claim(s.ctx, customer, "agent", callID, on, func(context.Context) error { return nil })
}

// places keeps a leg of a call a customer is about to dial, unless it is another's.
func (s *StoreSuite) places(customer, callID string, on NamedCall, atOnce *CallsAtOnce) error {
	placed := time.Now()
	return s.store.CallLegs.PlaceOn(s.ctx, &CallLeg{Token: newID(), CustomerID: customer, OrganizationID: "org", CallType: "agent", CallID: callID,
		Vendor: "telnyx", Direction: Outbound, LocalNumber: "+15125550150", RemoteNumber: "+15125550151", PlacedAt: &placed}, on, atOnce)
}

// onCall opens a leg of customer's on the call in app, dialled at.
func (s *StoreSuite) onCall(customer, callID string, app int64, at time.Time) CallLeg {
	leg := CallLeg{CustomerID: customer, StreamAppPK: app, CallType: "agent", CallID: callID, Vendor: "telnyx",
		VendorCallID: customer + at.String(), Direction: Outbound, LocalNumber: "+15125550150", RemoteNumber: "+15125550151", DialedAt: at}
	opened, err := s.store.CallLegs.Open(s.ctx, &leg)
	s.Require().NoError(err)
	s.Require().True(opened)
	return leg
}

func (s *StoreSuite) TestACallIsOwnedInItsOwnStreamApp() {
	s.onCall("theirs", "call", 7, s.base)
	s.onCall("mine", "call", 8, s.base.Add(time.Second))
	s.onCall("pinned", "shared", 1, s.base)

	s.Equal("mine", s.ownerOf(AppOf(8, 1), "call"))
	s.Empty(s.ownerOf(AppOf(0, 1), "call"))
	s.Equal("pinned", s.ownerOf(AppOf(0, 1), "shared"), "the deployment's app, pinned to its id")
	s.Equal("pinned", s.ownerOf(AppOf(1, 1), "shared"))
	s.onCall("unpinned", "older", 0, s.base)
	s.Equal("unpinned", s.ownerOf(AppOf(1, 1), "older"))
}

func (s *StoreSuite) TestACallSomebodyWasOnStaysTheirsAndOneNobodyAnsweredDoesNot() {
	s.onCall("answered", "held", 0, s.base)
	s.onCall("unanswered", "rang", 0, s.base)
	scope := AppScope{Unpinned: true}
	_, connection, err := s.store.CallLegs.Connect(s.ctx, scope, "agent", "held", "session", "phone", "+15125550151", s.base, nil)
	s.Require().NoError(err)
	s.Require().Equal(LegConnected, connection)
	for _, call := range []string{"held", "rang"} {
		_, err := s.store.CallLegs.EndCall(s.ctx, scope, "agent", call, s.base.Add(time.Minute))
		s.Require().NoError(err)
	}

	s.Equal("answered", s.ownerOf(scope, "held"))
	s.Empty(s.ownerOf(scope, "rang"))
}

func (s *StoreSuite) TestACallIsTheCustomersWhoseLegWasDialledOnItFirst() {
	s.onCall("a later", "call", 0, s.base.Add(time.Second))
	s.onCall("first", "call", 0, s.base)
	// Of two dialled at once, the first by id, whichever row came first.
	s.onCall("b", "tied", 0, s.base)
	s.onCall("a", "tied", 0, s.base)

	s.Equal("first", s.ownerOf(AppScope{Unpinned: true}, "call"))
	s.Equal("a", s.ownerOf(AppScope{Unpinned: true}, "tied"))
	s.Empty(s.ownerOf(AppScope{Unpinned: true}, "another call"))
	other, err := callOwner(s.ctx, s.store.shards(), AppScope{Unpinned: true}, "default", "call")
	s.Require().NoError(err)
	s.Empty(other, "a call of another type")
}

func (s *StoreSuite) TestALegPlacedAndNeverDialledIsOnItsCallForTenMinutes() {
	on := NamedCall{Scope: AppScope{Unpinned: true}}
	s.Require().NoError(s.places("app", "call", on, nil))
	s.ErrorIs(s.places("other", "call", on, nil), ErrCallTaken)
	s.ErrorIs(s.claims("other", "call", on), ErrCallTaken)
	s.Require().NoError(s.places("app", "call", on, nil), "the customer's own")

	_, err := s.store.shard.NewUpdate().Model((*CallLeg)(nil)).Set("placed_at = now() - interval '11 minutes'").Where("call_id = 'call'").Exec(s.ctx)
	s.Require().NoError(err)
	s.NoError(s.places("other", "call", on, nil))
}

func (s *StoreSuite) TestARefusedPlacementKeepsNoLeg() {
	on := NamedCall{Scope: AppScope{Unpinned: true}}
	s.onCall("theirs", "call", 0, s.base)

	s.ErrorIs(s.places("mine", "call", on, &CallsAtOnce{Limit: 9, Ringing: time.Hour}), ErrCallTaken)

	kept, err := s.store.CallLegs.SelectPlaced(s.ctx, "mine", []string{"call"})
	s.Require().NoError(err)
	s.Empty(kept)
	s.ErrorIs(s.places("mine", "free", on, &CallsAtOnce{Limit: 0, Ringing: time.Hour}), ErrCallsAtOnce)
}

func (s *StoreSuite) TestACallARunningSessionHoldsIsItsCustomers() {
	holder, failing := "theirs", error(nil)
	on := NamedCall{Scope: AppScope{Unpinned: true}, Holder: func(context.Context) (string, error) { return holder, failing }}

	s.ErrorIs(s.places("mine", "call", on, nil), ErrCallTaken)
	s.ErrorIs(s.claims("mine", "call", on), ErrCallTaken)
	s.NoError(s.claims("theirs", "call", on))
	holder = ""
	s.NoError(s.claims("mine", "call", on))
	failing = errors.New("redis is down")
	s.ErrorIs(s.claims("mine", "call", on), ErrCallUnchecked)
	s.ErrorIs(s.places("mine", "call", on, nil), ErrCallUnchecked)
	failing = nil
	s.ErrorIs(s.store.CallLegs.Claim(s.ctx, "mine", "agent", "call", on, func(context.Context) error { return errors.New("redis is down") }),
		ErrCallUnchecked)
}

// Whoever claims first is let in and the other refused, however long the first takes to keep
// what it claimed with.
func (s *StoreSuite) TestOfTwoCustomersClaimingACallAtOnceOneIsLetIn() {
	on := NamedCall{Scope: AppScope{Unpinned: true}}
	for i := range 10 {
		call := fmt.Sprintf("call-%d", i)
		var held atomic.Pointer[string]
		holding := on
		holding.Holder = func(context.Context) (string, error) {
			if customer := held.Load(); customer != nil {
				return *customer, nil
			}
			return "", nil
		}
		hold := func(customer string) func(context.Context) error {
			return func(context.Context) error {
				time.Sleep(20 * time.Millisecond)
				held.Store(&customer)
				return nil
			}
		}
		errs := make([]error, 3)
		var wg sync.WaitGroup
		wg.Go(func() { errs[0] = s.store.CallLegs.Claim(s.ctx, "a", "agent", call, holding, hold("a")) })
		wg.Go(func() { errs[1] = s.store.CallLegs.Claim(s.ctx, "b", "agent", call, holding, hold("b")) })
		wg.Go(func() { errs[2] = s.places("c", call, holding, nil) })
		wg.Wait()

		in := 0
		for _, err := range errs {
			if err == nil {
				in++
				continue
			}
			s.ErrorIs(err, ErrCallTaken)
		}
		s.Equal(1, in, "%v", errs)
	}
}

// The deployment's app is one app, pinned to its id or to none, and another app's call of the
// name is claimed beside it.
func (s *StoreSuite) TestACallIsClaimedInItsOwnStreamApp() {
	s.Require().NoError(s.places("theirs", "call", NamedCall{Scope: AppOf(0, 1)}, nil))

	s.ErrorIs(s.claims("mine", "call", NamedCall{Scope: AppOf(1, 1)}), ErrCallTaken)
	s.NoError(s.claims("mine", "call", NamedCall{Scope: AppOf(7, 1)}))
}

// A customer's leg kept in another shard is on the call too.
func (s *ShardedStoreSuite) TestACallIsClaimedAcrossShards() {
	on := NamedCall{Scope: AppScope{Unpinned: true}}
	place := func(store *Store, customer string) error {
		placed := time.Now()
		return store.CallLegs.PlaceOn(s.ctx, &CallLeg{Token: newID(), CustomerID: customer, CallType: "agent", CallID: "call", Vendor: "telnyx",
			Direction: Outbound, LocalNumber: "+15125550150", RemoteNumber: "+15125550151", PlacedAt: &placed}, on, nil)
	}
	errs := make([]error, 2)
	var wg sync.WaitGroup
	wg.Go(func() { errs[0] = place(s.store, "a") })
	wg.Go(func() { errs[1] = place(s.other, "b") })
	wg.Wait()

	s.True(errors.Is(errs[0], ErrCallTaken) != errors.Is(errs[1], ErrCallTaken), "%v", errs)
}

// Of legs two shards hold, the first dialled is the call's owner, and of two dialled at once the
// first by id, whichever shard asks.
func (s *ShardedStoreSuite) TestACallsOwnerIsTheSameOnEveryShard() {
	open := func(store *Store, customer, call string, at time.Time) {
		leg := CallLeg{CustomerID: customer, CallType: "agent", CallID: call, Vendor: "telnyx", VendorCallID: customer, Direction: Outbound,
			LocalNumber: "+15125550150", RemoteNumber: "+15125550151", DialedAt: at}
		_, err := store.CallLegs.Open(s.ctx, &leg)
		s.Require().NoError(err)
	}
	open(s.store, "a-later", "call", s.base.Add(time.Second))
	open(s.other, "first", "call", s.base)
	open(s.store, "b", "tied", s.base)
	open(s.other, "a", "tied", s.base)
	for _, store := range []*Store{s.store, s.other} {
		for call, want := range map[string]string{"call": "first", "tied": "a"} {
			owner, err := callOwner(s.ctx, store.shards(), AppScope{Unpinned: true}, "agent", call)
			s.Require().NoError(err)
			s.Equal(want, owner)
		}
	}
}

// A call is locked while it is claimed, in the deployment's app whether a claim knows its id or
// not, and another app's call of the name is not.
func (s *StoreSuite) TestACallIsLockedWhileItIsClaimed() {
	// Another router's, with turns of its own.
	elsewhere := &CallLegs{store: s.store}
	blocked := func(on NamedCall) (err error) {
		unpinned := NamedCall{Scope: AppScope{Unpinned: true}}
		s.Require().NoError(s.store.CallLegs.Claim(s.ctx, "a", "agent", "call", unpinned, func(context.Context) error {
			ctx, cancel := context.WithTimeout(s.ctx, 300*time.Millisecond)
			defer cancel()
			err = elsewhere.Claim(ctx, "b", "agent", "call", on, func(context.Context) error { return nil })
			return nil
		}))
		return err
	}

	s.Error(blocked(NamedCall{Scope: AppScope{App: 1, Unpinned: true}}))
	s.NoError(blocked(NamedCall{Scope: AppScope{App: 7}}))
}

func (s *StoreSuite) TestACallWhoseLegsCannotBeReadIsNotClaimed() {
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	held, asked := false, 0
	on := NamedCall{Scope: AppScope{Unpinned: true}, Holder: func(context.Context) (string, error) {
		// Asked again under the call's lock, just before its legs are read.
		if asked++; asked == 2 {
			cancel()
		}
		return "", nil
	}}

	s.Error(s.store.CallLegs.Claim(ctx, "mine", "agent", "call", on, func(context.Context) error { held = true; return nil }))

	s.Equal(2, asked)
	s.False(held)
}

// A leg counts as on its call until ten minutes after it was placed.
func (s *StoreSuite) TestALegPlacedNineMinutesAgoIsOnItsCall() {
	on := NamedCall{Scope: AppScope{Unpinned: true}}
	s.Require().NoError(s.places("app", "call", on, nil))
	_, err := s.store.shard.NewUpdate().Model((*CallLeg)(nil)).Set("placed_at = now() - interval '9 minutes'").Where("call_id = 'call'").Exec(s.ctx)
	s.Require().NoError(err)

	s.ErrorIs(s.places("other", "call", on, nil), ErrCallTaken)
}

// A leg placed and dialled is on its call from its dial.
func (s *StoreSuite) TestACallIsNotTheCustomersWhoPlacedALegAndDialledItLater() {
	placed := s.base.Add(-time.Minute)
	leg := CallLeg{CustomerID: "placed", CallType: "agent", CallID: "call", Vendor: "telnyx", Direction: Outbound,
		LocalNumber: "+15125550150", RemoteNumber: "+15125550152", PlacedAt: &placed, DialedAt: s.base.Add(time.Minute)}
	_, err := s.store.CallLegs.Open(s.ctx, &leg)
	s.Require().NoError(err)
	s.onCall("dialled", "call", 0, s.base)

	s.Equal("dialled", s.ownerOf(AppScope{Unpinned: true}, "call"))
}

// Named calls of no organization wait for no turn of each other's.
func (s *StoreSuite) TestNamedCallsOfNoOrganizationArePlacedBesideEachOther() {
	asking, asked := make(chan struct{}), make(chan struct{})
	var once sync.Once
	waits := NamedCall{Scope: AppScope{Unpinned: true}, Holder: func(ctx context.Context) (string, error) {
		once.Do(func() { close(asking) })
		select {
		case <-asked:
		case <-ctx.Done():
		}
		return "", ctx.Err()
	}}
	place := func(call string, on NamedCall) error {
		placed := time.Now()
		return s.store.CallLegs.PlaceOn(s.ctx, &CallLeg{Token: newID(), CustomerID: "app", CallType: "agent", CallID: call, Vendor: "telnyx",
			Direction: Outbound, LocalNumber: "+15125550150", RemoteNumber: "+15125550151", PlacedAt: &placed}, on, nil)
	}
	first := make(chan error, 1)
	go func() { first <- place("one", waits) }()
	select {
	case <-asking:
	case <-time.After(10 * time.Second):
		s.FailNow("nobody was asked who is on the first call")
	}

	s.NoError(place("two", NamedCall{Scope: AppScope{Unpinned: true}}))

	close(asked)
	s.NoError(<-first)
}

// Named calls whose hold is not answered give up, and hold up no other placement meanwhile.
func (s *StoreSuite) TestNamedCallsNotAnsweredHoldUpNoOtherPlacement() {
	// A router of its own, closed here: the connections it opens are not the suite's to keep.
	router, err := s.openRouter()
	s.Require().NoError(err)
	defer func() { s.NoError(router.Close()) }()
	on := NamedCall{Scope: AppScope{Unpinned: true}}
	var holding atomic.Int64
	var wg sync.WaitGroup
	began := time.Now()
	for i := range 64 {
		wg.Go(func() {
			s.ErrorIs(router.CallLegs.Claim(s.ctx, "app", "agent", fmt.Sprintf("call-%d", i), on, func(ctx context.Context) error {
				holding.Add(1)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(20 * time.Second):
					return nil
				}
			}), ErrCallUnchecked)
		})
	}
	s.Require().Eventually(func() bool { return holding.Load() >= 8 }, 5*time.Second, 5*time.Millisecond)

	at := time.Now()
	s.Require().NoError(router.CallLegs.PlaceWithin(s.ctx, &CallLeg{Token: newID(), CustomerID: "other", OrganizationID: "org", CallType: "agent",
		CallID: "unrelated", Vendor: "telnyx", Direction: Outbound, LocalNumber: "+15125550150", RemoteNumber: "+15125550151", PlacedAt: &at},
		CallsAtOnce{Limit: 9, Ringing: time.Hour}))
	s.Less(time.Since(at), time.Second)
	wg.Wait()
	s.Less(time.Since(began), 10*time.Second)
}

// Who is on a call is asked before the claim takes a place: one not answered holds none.
func (s *StoreSuite) TestACallWhoseHolderDoesNotAnswerTakesNoPlace() {
	on := NamedCall{Scope: AppScope{Unpinned: true}, Holder: func(context.Context) (string, error) { return "", errors.New("redis is down") }}
	legs := &CallLegs{store: s.store, claims: make(chan struct{})}
	legs.claimsOnce.Do(func() {})

	began := time.Now()
	s.ErrorIs(legs.Claim(s.ctx, "app", "agent", "call", on, func(context.Context) error { return nil }), ErrCallUnchecked)
	s.Less(time.Since(began), time.Second, "it waits for no place to be told so")
}
