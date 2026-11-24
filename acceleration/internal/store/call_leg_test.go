//go:build integration

package store

import (
	"fmt"
	"sync"
	"time"
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
