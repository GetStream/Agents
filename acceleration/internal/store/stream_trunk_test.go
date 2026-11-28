//go:build integration

package store

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// trunkIDs are the ids of the trunks one group sees.
func (s *StoreSuite) trunkIDs(group TrunkGroup) []string {
	var ids []string
	s.Require().NoError(s.store.StreamTrunks.InGroup(s.ctx, group, func(_ context.Context, tx *TrunkGroupTx) error {
		trunks, err := tx.Trunks()
		for _, trunk := range trunks {
			ids = append(ids, trunk.ID)
		}
		return err
	}))
	return ids
}

// savedTrunk stores a trunk in a group, defaulting the fields a test does not care about.
func (s *StoreSuite) savedTrunk(group TrunkGroup, id string) {
	s.Require().NoError(s.store.StreamTrunks.InGroup(s.ctx, group, func(_ context.Context, tx *TrunkGroupTx) error {
		return tx.SaveTrunk(StreamTrunk{
			ID: id, BridgeURI: "sip:" + id + "@sip.example.test",
			BridgeUsername: id, BridgePasswordSealed: []byte("sealed"), BridgePasswordKEKVersion: 1,
		})
	}))
}

func (s *StoreSuite) TestATrunkGroupSeesOnlyItsOwnTrunks() {
	group := TrunkGroup{CustomerID: "acme", StreamAppPK: 7, Vendor: "twilio"}
	s.savedTrunk(group, "trunk-own")
	s.savedTrunk(TrunkGroup{CustomerID: "globex", StreamAppPK: 7, Vendor: "twilio"}, "trunk-other-customer")
	s.savedTrunk(TrunkGroup{CustomerID: "acme", StreamAppPK: 7, Vendor: "telnyx"}, "trunk-other-vendor")
	s.savedTrunk(TrunkGroup{CustomerID: "acme", StreamAppPK: 8, Vendor: "twilio"}, "trunk-other-app")

	s.Equal([]string{"trunk-own"}, s.trunkIDs(group))
}

func (s *StoreSuite) TestATrunkAnotherGroupHoldsIsNotOverwritten() {
	group := TrunkGroup{CustomerID: "acme", StreamAppPK: 7, Vendor: "twilio"}
	s.savedTrunk(group, "trunk-taken")

	for _, other := range []TrunkGroup{
		{CustomerID: "globex", StreamAppPK: 7, Vendor: "twilio"},
		{CustomerID: "acme", StreamAppPK: 8, Vendor: "twilio"},
		{CustomerID: "acme", Vendor: "twilio"},
		{CustomerID: "acme", StreamAppPK: 7, Vendor: "telnyx"},
	} {
		err := s.store.StreamTrunks.InGroup(s.ctx, other, func(_ context.Context, tx *TrunkGroupTx) error {
			return tx.SaveTrunk(StreamTrunk{ID: "trunk-taken",
				BridgeURI: "sip:x@y", BridgeUsername: "x", BridgePasswordSealed: []byte("theirs"), BridgePasswordKEKVersion: 1})
		})
		s.Error(err, other)
	}

	s.Require().NoError(s.store.StreamTrunks.InGroup(s.ctx, group, func(_ context.Context, tx *TrunkGroupTx) error {
		trunks, err := tx.Trunks()
		s.Require().Len(trunks, 1)
		s.Equal("sip:trunk-taken@sip.example.test", trunks[0].BridgeURI)
		s.Equal([]byte("sealed"), trunks[0].BridgePasswordSealed)
		return err
	}))
}

func (s *StoreSuite) TestTheDeploymentsAppIsAGroupOfItsOwn() {
	s.savedTrunk(TrunkGroup{CustomerID: "acme", Vendor: "twilio"}, "trunk-deployment")
	s.savedTrunk(TrunkGroup{CustomerID: "acme", StreamAppPK: 7, Vendor: "twilio"}, "trunk-own-app")

	s.Equal([]string{"trunk-deployment"}, s.trunkIDs(TrunkGroup{CustomerID: "acme", Vendor: "twilio"}))
}

func (s *StoreSuite) TestTwoTransactionsOnOneGroupRunOneAfterTheOther() {
	group := TrunkGroup{CustomerID: "acme", StreamAppPK: 7, Vendor: "twilio"}
	firstHolds, firstMayFinish := make(chan struct{}), make(chan struct{})
	var seen []string
	var wait sync.WaitGroup
	wait.Add(2)

	go func() {
		defer wait.Done()
		s.NoError(s.store.StreamTrunks.InGroup(s.ctx, group, func(_ context.Context, tx *TrunkGroupTx) error {
			close(firstHolds)
			<-firstMayFinish
			return tx.SaveTrunk(StreamTrunk{ID: "trunk-first",
				BridgeURI: "sip:x@y", BridgeUsername: "x", BridgePasswordSealed: []byte("sealed"), BridgePasswordKEKVersion: 1})
		}))
	}()
	<-firstHolds
	go func() {
		defer wait.Done()
		s.NoError(s.store.StreamTrunks.InGroup(s.ctx, group, func(_ context.Context, tx *TrunkGroupTx) error {
			trunks, err := tx.Trunks()
			for _, trunk := range trunks {
				seen = append(seen, trunk.ID)
			}
			return err
		}))
	}()
	// Without the lock the second would read before the first commits and see nothing.
	time.Sleep(100 * time.Millisecond)
	close(firstMayFinish)
	wait.Wait()

	s.Equal([]string{"trunk-first"}, seen, "the second waited for the first and saw its trunk")
}

func (s *StoreSuite) TestSettingALineIsReadBackOnTheNumber() {
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &PhoneNumber{
		E164: "+15125551234", Vendor: "twilio", Country: "US", CustomerID: "acme", PurchasedAt: s.base,
	}))
	group := TrunkGroup{CustomerID: "acme", StreamAppPK: 7, Vendor: "twilio"}

	s.Require().NoError(s.store.StreamTrunks.InGroup(s.ctx, group, func(_ context.Context, tx *TrunkGroupTx) error {
		return tx.SetLine("+15125551234", "trunk-1", "route-1")
	}))

	held, err := s.store.PhoneNumbers.GetByE164(s.ctx, "acme", "+15125551234")
	s.Require().NoError(err)
	s.Equal("trunk-1", held.StreamTrunkID)
	s.Equal("route-1", held.StreamRouteID)
	s.Equal(int64(7), held.StreamAppPK)
	s.Nil(held.AttachedAt, "a line is not an attachment")
}

func (s *StoreSuite) TestATrunksNumbersAreTheNumbersHeldOnIt() {
	group := TrunkGroup{CustomerID: "acme", StreamAppPK: 7, Vendor: "twilio"}
	s.savedTrunk(group, "trunk-1")
	s.savedTrunk(group, "trunk-2")
	s.lined(group, "+15125551234", "trunk-1")
	s.lined(group, "+15125551235", "trunk-1")
	s.lined(group, "+15125551236", "trunk-1")
	s.lined(group, "+15125551237", "trunk-2")
	s.lined(TrunkGroup{CustomerID: "globex", StreamAppPK: 7, Vendor: "twilio"}, "+15125551238", "trunk-1")
	s.Require().NoError(giveBack(s.ctx, s.store, "acme", "+15125551235", s.base))

	numbers := map[string][]string{}
	s.Require().NoError(s.store.StreamTrunks.InGroup(s.ctx, group, func(_ context.Context, tx *TrunkGroupTx) error {
		trunks, err := tx.Trunks()
		for _, trunk := range trunks {
			numbers[trunk.ID] = trunk.Numbers
		}
		return err
	}))

	s.Equal(map[string][]string{
		"trunk-1": {"+15125551234", "+15125551236"},
		"trunk-2": {"+15125551237"},
	}, numbers, "a released number, and another customer's on a trunk of the same id, are not on it")
}

func (s *StoreSuite) TestReleasingANumberClearsItsAttachedAt() {
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &PhoneNumber{
		E164: "+15125551234", Vendor: "twilio", Country: "US", CustomerID: "acme", PurchasedAt: s.base,
	}))
	s.Require().NoError(s.store.PhoneNumbers.Attach(s.ctx, "acme", "+15125551234", NumberAttachment{TrunkID: "trunk-1", CallType: "default"}))
	attached, err := s.store.PhoneNumbers.GetByE164(s.ctx, "acme", "+15125551234")
	s.Require().NoError(err)
	s.Require().NotNil(attached.AttachedAt)

	s.Require().NoError(giveBack(s.ctx, s.store, "acme", "+15125551234", s.base))

	numbers, err := s.store.PhoneNumbers.Select(s.ctx, "acme", PhoneNumberFilter{IncludeReleased: true})
	s.Require().NoError(err)
	s.Require().Len(numbers, 1)
	s.Nil(numbers[0].AttachedAt)
}

func (s *StoreSuite) TestTwoCallResourcesMayShareATrunk() {
	var recorded []*CallResource
	for _, call := range []string{"call-1", "call-2"} {
		resource := &CallResource{
			TrunkID: "shared-trunk", RouteID: "route-1", CallType: "default", CallID: call, CustomerID: "acme", Shared: true,
		}
		s.Require().NoError(s.store.CallResources.Insert(s.ctx, resource))
		recorded = append(recorded, resource)
	}

	s.Require().NoError(s.store.CallResources.Delete(s.ctx, recorded[0].ID))

	gone, err := s.store.CallResources.Select(s.ctx, AppScope{Unpinned: true}, "default", "call-1")
	s.Require().NoError(err)
	s.Empty(gone)
	kept, err := s.store.CallResources.Select(s.ctx, AppScope{Unpinned: true}, "default", "call-2")
	s.Require().NoError(err)
	s.Require().Len(kept, 1)
	s.True(kept[0].Shared)
}

// lined is a number a customer holds, on a trunk with a rule.
func (s *StoreSuite) lined(group TrunkGroup, e164, trunkID string) {
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &PhoneNumber{
		E164: e164, Vendor: group.Vendor, Country: "US", CustomerID: group.CustomerID, PurchasedAt: s.base,
	}))
	s.Require().NoError(s.store.StreamTrunks.InGroup(s.ctx, group, func(_ context.Context, tx *TrunkGroupTx) error {
		return tx.SetLine(e164, trunkID, "route-"+trunkID)
	}))
}

func (s *StoreSuite) TestSettingALineLeavesAnotherCustomersNumberAlone() {
	theirs := TrunkGroup{CustomerID: "globex", StreamAppPK: 7, Vendor: "telnyx"}
	s.lined(theirs, "+15125551234", "trunk-theirs")

	err := s.store.StreamTrunks.InGroup(s.ctx, TrunkGroup{CustomerID: "acme", StreamAppPK: 7, Vendor: "twilio"},
		func(_ context.Context, tx *TrunkGroupTx) error {
			return tx.SetLine("+15125551234", "trunk-ours", "route-ours")
		})

	s.Error(err, "acme holds no such number")
	held, err := s.store.PhoneNumbers.GetByE164(s.ctx, "globex", "+15125551234")
	s.Require().NoError(err)
	s.Equal("trunk-theirs", held.StreamTrunkID)
}

func (s *StoreSuite) TestClearingALineTakesTheNumberOffItsTrunk() {
	group := TrunkGroup{CustomerID: "acme", StreamAppPK: 7, Vendor: "twilio"}
	s.lined(group, "+15125551234", "trunk-1")
	s.Require().NoError(s.store.PhoneNumbers.Attach(s.ctx, "acme", "+15125551234", NumberAttachment{
		TrunkID: "trunk-1", RouteID: "route-trunk-1", StreamAppPK: 7, CallType: "default",
	}))

	s.Require().NoError(s.store.StreamTrunks.InGroup(s.ctx, group, func(_ context.Context, tx *TrunkGroupTx) error {
		return tx.ClearLine("+15125551234")
	}))

	held, err := s.store.PhoneNumbers.GetByE164(s.ctx, "acme", "+15125551234")
	s.Require().NoError(err)
	s.Empty(held.StreamTrunkID)
	s.Empty(held.StreamRouteID)
	s.Nil(held.AttachedAt)
}

func (s *StoreSuite) TestClearingALineLeavesAnotherCustomersNumberAlone() {
	s.lined(TrunkGroup{CustomerID: "globex", StreamAppPK: 7, Vendor: "telnyx"}, "+15125551234", "trunk-theirs")

	err := s.store.StreamTrunks.InGroup(s.ctx, TrunkGroup{CustomerID: "acme", StreamAppPK: 7, Vendor: "twilio"},
		func(_ context.Context, tx *TrunkGroupTx) error {
			return tx.ClearLine("+15125551234")
		})

	s.Error(err, "acme holds no such number")
	held, err := s.store.PhoneNumbers.GetByE164(s.ctx, "globex", "+15125551234")
	s.Require().NoError(err)
	s.Equal("trunk-theirs", held.StreamTrunkID)
}

func (s *StoreSuite) TestDetachClearsAttachedAndKeepsTheLine() {
	group := TrunkGroup{CustomerID: "acme", StreamAppPK: 7, Vendor: "twilio"}
	s.lined(group, "+15125551234", "trunk-1")
	s.Require().NoError(s.store.PhoneNumbers.Attach(s.ctx, "acme", "+15125551234", NumberAttachment{
		TrunkID: "trunk-1", RouteID: "route-trunk-1", StreamAppPK: 7, CallType: "default",
	}))
	before, err := s.store.PhoneNumbers.GetByE164(s.ctx, "acme", "+15125551234")
	s.Require().NoError(err)
	s.Require().NotNil(before.AttachedAt)

	s.Require().NoError(s.store.StreamTrunks.InGroup(s.ctx, group, func(_ context.Context, tx *TrunkGroupTx) error {
		return tx.Detach("+15125551234")
	}))

	after, err := s.store.PhoneNumbers.GetByE164(s.ctx, "acme", "+15125551234")
	s.Require().NoError(err)
	s.Nil(after.AttachedAt)
	s.Nil(after.ReleasedAt)
	s.Equal(before.StreamTrunkID, after.StreamTrunkID)
	s.Equal(before.StreamRouteID, after.StreamRouteID)
}

func (s *StoreSuite) TestDetachOfANumberAnotherCustomerHoldsFails() {
	s.lined(TrunkGroup{CustomerID: "globex", StreamAppPK: 7, Vendor: "telnyx"}, "+15125551234", "trunk-theirs")
	s.Require().NoError(s.store.PhoneNumbers.Attach(s.ctx, "globex", "+15125551234", NumberAttachment{
		TrunkID: "trunk-theirs", RouteID: "route-theirs", StreamAppPK: 7, CallType: "default",
	}))

	err := s.store.StreamTrunks.InGroup(s.ctx, TrunkGroup{CustomerID: "acme", StreamAppPK: 7, Vendor: "twilio"},
		func(_ context.Context, tx *TrunkGroupTx) error {
			return tx.Detach("+15125551234")
		})

	s.Error(err, "acme holds no such number")
	held, err := s.store.PhoneNumbers.GetByE164(s.ctx, "globex", "+15125551234")
	s.Require().NoError(err)
	s.NotNil(held.AttachedAt)
	s.Equal("trunk-theirs", held.StreamTrunkID)
}

func (s *StoreSuite) TestDeletingATrunkForgetsIt() {
	group := TrunkGroup{CustomerID: "acme", StreamAppPK: 7, Vendor: "twilio"}
	s.savedTrunk(group, "trunk-1")
	s.savedTrunk(group, "trunk-2")

	s.Require().NoError(s.store.StreamTrunks.InGroup(s.ctx, group, func(_ context.Context, tx *TrunkGroupTx) error {
		return tx.DeleteTrunk("trunk-1")
	}))

	s.Equal([]string{"trunk-2"}, s.trunkIDs(group))
}

func (s *StoreSuite) TestDeletingATrunkLeavesAnotherCustomersTrunkAlone() {
	theirs := TrunkGroup{CustomerID: "globex", StreamAppPK: 7, Vendor: "twilio"}
	s.savedTrunk(theirs, "trunk-theirs")

	s.Require().NoError(s.store.StreamTrunks.InGroup(s.ctx, TrunkGroup{CustomerID: "acme", StreamAppPK: 7, Vendor: "twilio"},
		func(_ context.Context, tx *TrunkGroupTx) error {
			return tx.DeleteTrunk("trunk-theirs")
		}))

	s.Equal([]string{"trunk-theirs"}, s.trunkIDs(theirs))
}

func (s *StoreSuite) TestDeletingALegKeepsAnotherLegOfTheCallOnTheSameTrunk() {
	legs := make([]*CallResource, 2)
	for i := range legs {
		legs[i] = &CallResource{
			TrunkID: "shared-trunk", RouteID: "route-1", CallType: "default", CallID: "call-1", CustomerID: "acme", Shared: true,
		}
		s.Require().NoError(s.store.CallResources.Insert(s.ctx, legs[i]))
	}

	s.Require().NoError(s.store.CallResources.Delete(s.ctx, legs[1].ID))

	kept, err := s.store.CallResources.Select(s.ctx, AppScope{Unpinned: true}, "default", "call-1")
	s.Require().NoError(err)
	s.Require().Len(kept, 1)
	s.Equal(legs[0].ID, kept[0].ID)
}

// linedOnly puts a number of acme's on a line in the deployment's app, without attaching it.
func (s *StoreSuite) linedOnly(vendor, e164 string) {
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &PhoneNumber{
		E164: e164, Vendor: vendor, Country: "US", CustomerID: "acme", PurchasedAt: s.base,
	}))
	group := TrunkGroup{CustomerID: "acme", Vendor: vendor}
	s.Require().NoError(s.store.StreamTrunks.InGroup(s.ctx, group, func(_ context.Context, tx *TrunkGroupTx) error {
		return tx.SetLine(e164, "trunk-1", "route-1")
	}))
}

func (s *StoreSuite) TestANumberLinedOnlyToPlaceCallsIsNotRung() {
	s.linedOnly("twilio", "+15125551234")

	_, err := s.store.NumberRungInApp(s.ctx, AppScope{Unpinned: true}, "+15125551234")

	s.ErrorContains(err, "no number +15125551234 is attached")
}

func (s *StoreSuite) TestANumberLinedOnlyToPlaceCallsHasNoInboundBridge() {
	s.linedOnly("telnyx", "+15125551234")

	_, err := s.store.PhoneNumbers.AttachedBridge(s.ctx, "telnyx", "+15125551234")

	s.ErrorIs(err, ErrNoNumber)
}

func (s *StoreSuite) TestAStreamTrunkIsReadByIdForItsOwnCustomerOnly() {
	s.savedTrunk(TrunkGroup{CustomerID: "acme", StreamAppPK: 7, Vendor: "twilio"}, "trunk-read")

	trunk, err := s.store.StreamTrunks.Get(s.ctx, "acme", "trunk-read")
	s.Require().NoError(err)
	s.Equal("sip:trunk-read@sip.example.test", trunk.BridgeURI)

	_, err = s.store.StreamTrunks.Get(s.ctx, "globex", "trunk-read")
	s.ErrorIs(err, ErrNoStreamTrunk)
	_, err = s.store.StreamTrunks.Get(s.ctx, "acme", "missing")
	s.ErrorIs(err, ErrNoStreamTrunk)
}

func (s *StoreSuite) TestAnOwnTrunkMadeWithoutAKeyIsSavedWithNoPassword() {
	group := TrunkGroup{CustomerID: "acme", Vendor: "twilio"}
	s.Require().NoError(s.store.StreamTrunks.InGroup(s.ctx, group, func(_ context.Context, tx *TrunkGroupTx) error {
		return tx.SaveTrunk(StreamTrunk{ID: "trunk-unsealed", BridgeURI: "sip:+15125551234@sip.example.test", Own: true})
	}))

	trunk, err := s.store.StreamTrunks.Get(s.ctx, "acme", "trunk-unsealed")
	s.Require().NoError(err)
	s.True(trunk.Own)
	s.Empty(trunk.BridgePasswordSealed)
}

func (s *StoreSuite) TestStreamTrunkQueryBudgets() {
	group := TrunkGroup{CustomerID: "acme", StreamAppPK: 7, Vendor: "twilio"}
	for i := range 3 {
		s.savedTrunk(group, fmt.Sprintf("trunk-%d", i))
		for j := range 3 {
			s.lined(group, fmt.Sprintf("+151255501%d%d", i, j), fmt.Sprintf("trunk-%d", i))
		}
	}
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &PhoneNumber{
		E164: "+15125550199", Vendor: "twilio", Country: "US", CustomerID: "acme", PurchasedAt: s.base,
	}))
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.StreamTrunks.Get(s.ctx, "acme", "trunk-0")
		s.Require().NoError(err)
	}), "Get")
	// InGroup is a transaction, BEGIN to COMMIT, that sets its idle limit and takes the group's
	// lock first.
	in := func(name string, budget int, fn func(tx *TrunkGroupTx) error) {
		s.Equal(4+budget, s.countQueries(func() {
			s.Require().NoError(s.store.StreamTrunks.InGroup(s.ctx, group, func(_ context.Context, tx *TrunkGroupTx) error {
				return fn(tx)
			}))
		}), name)
	}
	in("InGroup", 0, func(*TrunkGroupTx) error { return nil })
	in("Trunks", 1, func(tx *TrunkGroupTx) error {
		trunks, err := tx.Trunks()
		s.Len(trunks, 3)
		for _, trunk := range trunks {
			s.Len(trunk.Numbers, 3, "each trunk's numbers come with it")
		}
		return err
	})
	in("SaveTrunk", 1, func(tx *TrunkGroupTx) error {
		return tx.SaveTrunk(StreamTrunk{ID: "trunk-3", BridgeURI: "sip:trunk-3@sip.example.test"})
	})
	in("Number", 1, func(tx *TrunkGroupTx) error {
		_, err := tx.Number("+15125550199")
		return err
	})
	in("SetLine", 1, func(tx *TrunkGroupTx) error { return tx.SetLine("+15125550199", "trunk-3", "route-3") })
	in("Attach", 1, func(tx *TrunkGroupTx) error {
		return tx.Attach("+15125550199", NumberAttachment{TrunkID: "trunk-3", StreamAppPK: 7, CallType: "default"})
	})
	in("Detach", 1, func(tx *TrunkGroupTx) error { return tx.Detach("+15125550199") })
	in("ClearLine", 1, func(tx *TrunkGroupTx) error { return tx.ClearLine("+15125550199") })
	in("DeleteTrunk", 1, func(tx *TrunkGroupTx) error { return tx.DeleteTrunk("trunk-3") })
	in("ReleaseNumber", 1, func(tx *TrunkGroupTx) error { return tx.ReleaseNumber("+15125550199", s.base) })
}

func (s *StoreSuite) TestATrunkGroupOutlastsTheIdleLimitOfAServingTransaction() {
	limited := s.limitedStore()
	group := TrunkGroup{CustomerID: "acme", StreamAppPK: 7, Vendor: "twilio"}

	err := limited.StreamTrunks.InGroup(s.ctx, group, func(_ context.Context, tx *TrunkGroupTx) error {
		if _, err := tx.Trunks(); err != nil {
			return err
		}
		// A call to Stream slower than the limit, between two of the group's statements.
		time.Sleep(3 * shortLimit)
		return tx.SaveTrunk(StreamTrunk{ID: "trunk-1", BridgeURI: "sip:trunk-1@sip.example.test"})
	})

	s.Require().NoError(err)
	s.Equal([]string{"trunk-1"}, s.trunkIDs(group))
}

func (s *StoreSuite) TestAnotherRoutersTrunkGroupWaitsForTheOneHoldingIt() {
	group := TrunkGroup{CustomerID: "acme", StreamAppPK: 7, Vendor: "twilio"}
	other, err := OpenShards(Options{DSN: s.dsn, ShardDSN: s.shardDSN})
	s.Require().NoError(err)
	defer other.Close()
	holding := make(chan struct{})
	held := make(chan error, 1)
	go func() {
		held <- s.store.StreamTrunks.InGroup(s.ctx, group, func(context.Context, *TrunkGroupTx) error {
			close(holding)
			// Longer than the serving lock limit, as a call to Stream can take.
			time.Sleep(4 * time.Second)
			return nil
		})
	}()
	<-holding

	err = other.StreamTrunks.InGroup(s.ctx, group, func(_ context.Context, tx *TrunkGroupTx) error {
		return tx.SaveTrunk(StreamTrunk{ID: "trunk-1", BridgeURI: "sip:trunk-1@sip.example.test"})
	})

	s.Require().NoError(err, "the second router waited for the group")
	s.Require().NoError(<-held)
	s.Equal([]string{"trunk-1"}, s.trunkIDs(group))
}

func (s *StoreSuite) TestATrunkGroupLeavesItsConnectionWithTheServingLimits() {
	single, err := OpenShards(Options{DSN: s.dsn, ShardDSN: s.shardDSN})
	s.Require().NoError(err)
	defer single.Close()
	single.GlobalDB().SetMaxOpenConns(1)
	s.Require().NoError(single.StreamTrunks.InGroup(s.ctx, TrunkGroup{CustomerID: "acme", Vendor: "twilio"},
		func(context.Context, *TrunkGroupTx) error { return nil }))

	var statement, lock, idle string
	s.Require().NoError(single.GlobalDB().QueryRowContext(s.ctx,
		"SELECT current_setting('statement_timeout'), current_setting('lock_timeout'), current_setting('idle_in_transaction_session_timeout')").
		Scan(&statement, &lock, &idle))
	s.Equal([]string{"8s", "3s", "30s"}, []string{statement, lock, idle}, "the group's own limits ended with its transaction")
}

func (s *StoreSuite) TestAWriteToANumberATrunkGroupHoldsWaitsForTheGroup() {
	group := TrunkGroup{CustomerID: "acme", Vendor: "twilio"}
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &PhoneNumber{
		E164: "+15125551234", Vendor: group.Vendor, Country: "US", CustomerID: group.CustomerID, PurchasedAt: s.base,
	}))
	holding := make(chan struct{})
	held := make(chan error, 1)
	go func() {
		held <- s.store.StreamTrunks.InGroup(s.ctx, group, func(_ context.Context, tx *TrunkGroupTx) error {
			if _, err := tx.Number("+15125551234"); err != nil {
				return err
			}
			close(holding)
			// Longer than the serving lock limit, as a call to Stream can take.
			time.Sleep(4 * time.Second)
			return nil
		})
	}()
	<-holding

	err := s.store.PhoneNumbers.ClearAgent(s.ctx, "acme", "+15125551234")

	s.Require().NoError(err, "the write waited for the group")
	s.Require().NoError(<-held)
}

func (s *StoreSuite) TestATrunkGroupRunsWithTheGroupsLimits() {
	var lock, idle string
	s.Require().NoError(s.store.StreamTrunks.InGroup(s.ctx, TrunkGroup{CustomerID: "acme", Vendor: "twilio"}, func(ctx context.Context, tx *TrunkGroupTx) error {
		return tx.tx.QueryRowContext(ctx, "SELECT current_setting('lock_timeout'), current_setting('idle_in_transaction_session_timeout')").Scan(&lock, &idle)
	}))
	s.Equal([]string{"1min", "1min"}, []string{lock, idle})
}

// savedSealed stores trunk-sealed in the group, with its password sealed under the router's key
// at kekVersion, or under the app's data key at dataKeyVersion when that is not nil.
func (s *StoreSuite) savedSealed(group TrunkGroup, kekVersion int, dataKeyVersion *int) {
	s.Require().NoError(s.store.StreamTrunks.InGroup(s.ctx, group, func(_ context.Context, tx *TrunkGroupTx) error {
		return tx.SaveTrunk(StreamTrunk{
			ID: "trunk-sealed", BridgeURI: "sip:x@y", BridgeUsername: "x", BridgePasswordSealed: []byte("sealed"), BridgePasswordKEKVersion: kekVersion,
			BridgePasswordDataKeyVersion: dataKeyVersion,
		})
	}))
}

// A trunk saved again replaces the key its password is under, both ways, so a row never names
// a key other than the one that sealed its blob.
func (s *StoreSuite) TestATrunksDataKeyVersionIsSavedAndReplacedWithItsPassword() {
	group := TrunkGroup{CustomerID: "acme", Vendor: "twilio"}
	one, two := 1, 2

	s.savedSealed(group, 0, &one)
	trunk, err := s.store.StreamTrunks.Get(s.ctx, "acme", "trunk-sealed")
	s.Require().NoError(err)
	s.Require().NotNil(trunk.BridgePasswordDataKeyVersion)
	s.Equal(1, *trunk.BridgePasswordDataKeyVersion)

	s.savedSealed(group, 3, nil)
	trunk, err = s.store.StreamTrunks.Get(s.ctx, "acme", "trunk-sealed")
	s.Require().NoError(err)
	s.Nil(trunk.BridgePasswordDataKeyVersion, "a password sealed again under the router's key names no data key")
	s.Equal(3, trunk.BridgePasswordKEKVersion)

	s.savedSealed(group, 0, &two)
	trunk, err = s.store.StreamTrunks.Get(s.ctx, "acme", "trunk-sealed")
	s.Require().NoError(err)
	s.Require().NotNil(trunk.BridgePasswordDataKeyVersion)
	s.Equal(2, *trunk.BridgePasswordDataKeyVersion)
}

func (s *StoreSuite) TestAnInboundBridgeNamesTheDataKeyItsPasswordIsUnder() {
	group := TrunkGroup{CustomerID: "acme", StreamAppPK: 7, Vendor: "twilio"}
	one := 1
	s.savedSealed(group, 0, &one)
	s.lined(group, "+15125551234", "trunk-sealed")
	s.Require().NoError(s.store.PhoneNumbers.Attach(s.ctx, "acme", "+15125551234", NumberAttachment{
		TrunkID: "trunk-sealed", RouteID: "route-sealed", StreamAppPK: 7, CallType: "default",
	}))

	bridge, err := s.store.PhoneNumbers.AttachedBridge(s.ctx, "twilio", "+15125551234")
	s.Require().NoError(err)
	s.Require().NotNil(bridge.DataKeyVersion)
	s.Equal(1, *bridge.DataKeyVersion)
	s.Equal([]byte("sealed"), bridge.PasswordSealed)

	s.savedSealed(group, 4, nil)
	bridge, err = s.store.PhoneNumbers.AttachedBridge(s.ctx, "twilio", "+15125551234")
	s.Require().NoError(err)
	s.Nil(bridge.DataKeyVersion, "a password under the router's key names no data key")
	s.Equal(4, bridge.KEKVersion)
}
