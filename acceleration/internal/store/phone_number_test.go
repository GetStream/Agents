//go:build integration

package store

import (
	"context"
	"fmt"
	"time"
)

// giveBack releases a customer's number in db at at, as the phone service does: in a trunk
// group's transaction. The group only names the lock, so any vendor's reaches the number.
func giveBack(ctx context.Context, db *Store, customerID, e164 string, at time.Time) error {
	return db.StreamTrunks.InGroup(ctx, TrunkGroup{CustomerID: customerID, Vendor: "twilio"}, func(_ context.Context, tx *TrunkGroupTx) error {
		return tx.ReleaseNumber(e164, at)
	})
}

func (s *StoreSuite) TestANumberIsHeldUntilItIsReleased() {
	number := &PhoneNumber{
		E164: "+15125551234", Vendor: "twilio", Country: "US",
		Capabilities: []string{"voice"}, MonthlyCostMicros: 1_150_000,
		CustomerID: "acme", PurchasedAt: s.base,
	}
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, number))

	held, err := s.store.PhoneNumbers.Select(s.ctx, "acme", PhoneNumberFilter{})
	s.Require().NoError(err)
	s.Require().Len(held, 1)
	s.Equal("+15125551234", held[0].E164)
	s.Equal([]string{"voice"}, held[0].Capabilities)

	s.Require().NoError(giveBack(s.ctx, s.store, "acme", "+15125551234", s.base.Add(time.Hour)))

	held, err = s.store.PhoneNumbers.Select(s.ctx, "acme", PhoneNumberFilter{})
	s.Require().NoError(err)
	s.Empty(held, "a released number is not one you can be called on")

	// The row stays, because what it cost while it was held is still part of the bill.
	all, err := s.store.PhoneNumbers.Select(s.ctx, "acme", PhoneNumberFilter{IncludeReleased: true})
	s.Require().NoError(err)
	s.Require().Len(all, 1)
	s.Require().NotNil(all[0].ReleasedAt)
}

func (s *StoreSuite) TestANumberSomebodyElseHoldsCannotBeReleased() {
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &PhoneNumber{
		E164: "+15125551234", Vendor: "twilio", Country: "US",
		CustomerID: "acme", PurchasedAt: s.base,
	}))

	err := giveBack(s.ctx, s.store, "globex", "+15125551234", s.base)

	s.ErrorIs(err, ErrNoNumber)
	s.EqualError(err, "store: +15125551234 is not a number globex holds")
}

func (s *StoreSuite) TestANumberSomebodyElseHoldsCannotBeAttached() {
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &PhoneNumber{
		E164: "+15125551234", Vendor: "twilio", Country: "US",
		CustomerID: "acme", PurchasedAt: s.base,
	}))

	err := s.store.PhoneNumbers.Attach(s.ctx, "globex", "+15125551234", NumberAttachment{TrunkID: "trunk-1", CallType: "default"})

	s.ErrorIs(err, ErrNoNumber)
	s.EqualError(err, "store: +15125551234 is not a number globex holds")
}

func (s *StoreSuite) TestANumberIsRecordedOnlyInE164Form() {
	for _, e164 := range []string{"", "15125551234", "5125551234", "+1 512 555 1234", "+05125551234"} {
		err := s.store.PhoneNumbers.Insert(s.ctx, &PhoneNumber{
			E164: e164, Vendor: "plivo", Country: "US", CustomerID: "acme",
		})
		var invalid *ValidationError
		s.Require().ErrorAs(err, &invalid, "%q", e164)
		s.Equal("e164", invalid.Field, "%q", e164)
	}

	held, err := s.store.PhoneNumbers.Select(s.ctx, "acme", PhoneNumberFilter{IncludeReleased: true})
	s.Require().NoError(err)
	s.Empty(held)
}

func (s *StoreSuite) TestANumberWrittenInAnotherFormIsNotHeld() {
	// Written straight to the table, the way a row could be before every write checked
	// the form: the vendor reads it as +15125551234.
	_, err := s.store.global.ExecContext(s.ctx,
		"INSERT INTO phone_numbers (e164, vendor, country, customer_id) VALUES ('15125551234', 'plivo', 'US', 'globex')")
	s.Require().NoError(err)

	_, err = s.store.PhoneNumbers.GetByE164(s.ctx, "globex", "15125551234")

	s.ErrorIs(err, ErrNoNumber)
	s.ErrorContains(err, "is not a number globex holds")
}

func (s *StoreSuite) TestANumberRemembersWhichTrunkItsCallsArriveOn() {
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &PhoneNumber{
		E164: "+15125551234", Vendor: "twilio", Country: "US",
		CustomerID: "acme", PurchasedAt: s.base,
	}))

	s.Require().NoError(s.store.PhoneNumbers.Attach(
		s.ctx, "acme", "+15125551234", NumberAttachment{TrunkID: "trunk-7", CallType: "default", CallID: "phone-+15125551234"}))

	number, err := s.store.PhoneNumbers.GetByE164(s.ctx, "acme", "+15125551234")
	s.Require().NoError(err)
	s.Equal("trunk-7", number.StreamTrunkID)
	s.Equal("phone-+15125551234", number.StreamCallID)
	s.Equal("default", number.StreamCallType)
}

// attachWithBridge holds a number of the vendor for acme and attaches it on a trunk of its own.
func (s *StoreSuite) attachWithBridge(vendor, e164 string) {
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &PhoneNumber{
		E164: e164, Vendor: vendor, Country: "US", CustomerID: "acme", PurchasedAt: s.base,
	}))
	s.Require().NoError(s.store.StreamTrunks.InGroup(s.ctx, TrunkGroup{CustomerID: "acme", Vendor: vendor},
		func(_ context.Context, tx *TrunkGroupTx) error {
			if err := tx.SaveTrunk(StreamTrunk{
				ID: "trunk-7", BridgeURI: "sip:" + e164 + "@sip.example.test", BridgeUsername: "trunk-user",
				BridgePasswordSealed: []byte("sealed"), BridgePasswordKEKVersion: 3, Own: true,
			}); err != nil {
				return err
			}
			return tx.Attach(e164, NumberAttachment{TrunkID: "trunk-7", CallType: "agent", CallID: "{{uuid}}"})
		}))
}

func (s *StoreSuite) TestAnAttachedNumberKeepsItsTrunkCredentials() {
	s.attachWithBridge("telnyx", "+15125551234")

	var bridge AttachedBridge
	var err error
	s.Equal(1, s.countQueries(func() {
		bridge, err = s.store.PhoneNumbers.AttachedBridge(s.ctx, "telnyx", "+15125551234")
	}))

	s.Require().NoError(err)
	s.Equal(AttachedBridge{
		CustomerID: "acme", TrunkID: "trunk-7", Own: true, URI: "sip:+15125551234@sip.example.test", Username: "trunk-user",
		PasswordSealed: []byte("sealed"), KEKVersion: 3,
	}, bridge)
}

func (s *StoreSuite) TestANumberOnASharedTrunkHasTheTrunksCredentials() {
	group := TrunkGroup{CustomerID: "acme", Vendor: "telnyx"}
	s.savedTrunk(group, "trunk-shared")
	s.lined(group, "+15125551234", "trunk-shared")
	s.Require().NoError(s.store.PhoneNumbers.Attach(s.ctx, "acme", "+15125551234", NumberAttachment{
		TrunkID: "trunk-shared", RouteID: "route-trunk-shared", CallType: "agent",
	}))

	bridge, err := s.store.PhoneNumbers.AttachedBridge(s.ctx, "telnyx", "+15125551234")

	s.Require().NoError(err)
	s.Equal(AttachedBridge{
		CustomerID: "acme", TrunkID: "trunk-shared", URI: "sip:trunk-shared@sip.example.test", Username: "trunk-shared",
		PasswordSealed: []byte("sealed"), KEKVersion: 1,
	}, bridge)
}

func (s *StoreSuite) TestAReleasedNumbersTrunkCredentialsAreGone() {
	s.attachWithBridge("telnyx", "+15125551234")
	s.Require().NoError(giveBack(s.ctx, s.store, "acme", "+15125551234", s.base.Add(time.Hour)))

	_, err := s.store.PhoneNumbers.AttachedBridge(s.ctx, "telnyx", "+15125551234")
	s.ErrorIs(err, ErrNoNumber)
}

func (s *StoreSuite) TestAnotherVendorsNumberHasNoTrunkCredentials() {
	s.attachWithBridge("twilio", "+15125551234")

	_, err := s.store.PhoneNumbers.AttachedBridge(s.ctx, "telnyx", "+15125551234")

	s.ErrorIs(err, ErrNoNumber)
}

func (s *StoreSuite) TestANumberAttachedWithoutCredentialsHasNoTrunkCredentials() {
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &PhoneNumber{
		E164: "+15125551234", Vendor: "telnyx", Country: "US", CustomerID: "acme", PurchasedAt: s.base,
	}))
	s.Require().NoError(s.store.PhoneNumbers.Attach(
		s.ctx, "acme", "+15125551234", NumberAttachment{TrunkID: "trunk-7", CallType: "agent", CallID: "{{uuid}}"}))

	_, err := s.store.PhoneNumbers.AttachedBridge(s.ctx, "telnyx", "+15125551234")

	s.ErrorIs(err, ErrNoStoredBridge, "attaching it again keeps them")
	s.NotErrorIs(err, ErrNoNumber, "it is held and attached, so it is not an unknown number")
}

func (s *StoreSuite) TestANumberThatIsHeldButNotAttachedHasNoTrunkCredentials() {
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &PhoneNumber{
		E164: "+15125551234", Vendor: "telnyx", Country: "US", CustomerID: "acme", PurchasedAt: s.base,
	}))

	_, err := s.store.PhoneNumbers.AttachedBridge(s.ctx, "telnyx", "+15125551234")

	s.ErrorIs(err, ErrNoNumber)
}

func (s *StoreSuite) TestTheSameNumberCanBeBoughtAgainAfterBeingReleased() {
	first := &PhoneNumber{
		E164: "+15125551234", Vendor: "twilio", Country: "US",
		CustomerID: "acme", PurchasedAt: s.base,
	}
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, first))
	s.Require().NoError(giveBack(s.ctx, s.store, "acme", "+15125551234", s.base.Add(time.Hour)))

	again := &PhoneNumber{
		E164: "+15125551234", Vendor: "twilio", Country: "US",
		CustomerID: "acme", PurchasedAt: s.base.Add(2 * time.Hour),
	}

	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, again))
}

func (s *StoreSuite) TestPhoneNumberQueryBudgets() {
	config := newID()
	_, err := s.store.global.ExecContext(s.ctx,
		"INSERT INTO agent_configs (id, customer_id, name, instructions) VALUES (?, 'acme', 'answers', 'be brief')", config)
	s.Require().NoError(err)
	for i := range 5 {
		e164 := fmt.Sprintf("+1512555010%d", i)
		s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &PhoneNumber{
			E164: e164, Vendor: "twilio", Country: "US", CustomerID: "acme", PurchasedAt: s.base,
		}))
		s.Require().NoError(s.store.PhoneNumbers.Attach(s.ctx, "acme", e164, NumberAttachment{TrunkID: "trunk-1", CallType: "default"}))
		s.Require().NoError(s.store.PhoneNumbers.SetAgent(s.ctx, "acme", e164, config))
	}
	trunk := s.trunk("acme")

	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &PhoneNumber{
			E164: "+15125550199", Vendor: "twilio", Country: "US", CustomerID: "acme", PurchasedAt: s.base,
		}))
	}), "Insert")
	// The number's holders are read and it is inserted in a transaction, BEGIN to COMMIT, under
	// a lock taken first.
	s.Equal(6, s.countQueries(func() {
		s.Require().NoError(s.trunkNumber("acme", trunk.ID, "+15125550198"))
	}), "InsertOnSIPTrunk reads the trunk, then locks the number and reads who holds it before it inserts")
	s.Equal(1, s.countQueries(func() {
		bought, err := s.store.PhoneNumbers.BoughtElsewhere(s.ctx, "acme", "+15125550198")
		s.Require().NoError(err)
		s.Empty(bought)
	}), "BoughtElsewhere")
	s.Equal(1, s.countQueries(func() {
		held, err := s.store.PhoneNumbers.Holders(s.ctx, "+15125550198")
		s.Require().NoError(err)
		s.Equal([]string{"acme"}, held)
	}), "Holders")
	s.Equal(1, s.countQueries(func() {
		lines, err := s.store.PhoneNumbers.TrunkLinesElsewhere(s.ctx, "other", "+15125550198", 0)
		s.Require().NoError(err)
		s.Empty(lines)
	}), "TrunkLinesElsewhere")
	// A write to a number is a transaction, BEGIN to COMMIT, that waits on the row as long as a
	// trunk group may hold it.
	s.Equal(4, s.countQueries(func() {
		s.Require().NoError(s.store.PhoneNumbers.Attach(s.ctx, "acme", "+15125550199", NumberAttachment{TrunkID: "trunk-1", CallType: "default"}))
	}), "Attach")
	s.Equal(1, s.countQueries(func() {
		numbers, err := s.store.PhoneNumbers.Select(s.ctx, "acme", PhoneNumberFilter{})
		s.Require().NoError(err)
		s.Len(numbers, 7)
	}), "Select")
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.PhoneNumbers.GetByE164(s.ctx, "acme", "+15125550100")
		s.Require().NoError(err)
	}), "GetByE164")
	s.Equal(4, s.countQueries(func() {
		s.Require().NoError(s.store.PhoneNumbers.SetAgent(s.ctx, "acme", "+15125550199", config))
	}), "SetAgent")
	s.Equal(1, s.countQueries(func() {
		numbers, err := s.store.PhoneNumbers.AnsweredBy(s.ctx, "acme", config)
		s.Require().NoError(err)
		s.Len(numbers, 6)
	}), "AnsweredBy")
	s.Equal(4, s.countQueries(func() {
		s.Require().NoError(s.store.PhoneNumbers.ClearAgent(s.ctx, "acme", "+15125550199"))
	}), "ClearAgent")
	s.Equal(1, s.countQueries(func() {
		held, err := s.store.PhoneNumbers.HeldSince(s.ctx, s.base)
		s.Require().NoError(err)
		s.Len(held, 7)
	}), "HeldSince")
}

// lineOn is a number on a customer's own trunk with a line made for it in the app pinned.
func (s *StoreSuite) lineOn(customerID, e164 string, app int64) {
	s.lined(TrunkGroup{CustomerID: customerID, StreamAppPK: app, Vendor: sipTrunkVendor}, e164, "trunk-"+customerID)
}

func (s *StoreSuite) TestTheTrunkLinesElsewhereAreOtherCustomersLinesForTheNumberInTheApp() {
	s.lineOn("globex", "+15125557001", 0)
	s.lineOn("acme", "+15125557001", 0)
	s.lineOn("initech", "+15125557002", 0)
	s.lineOn("umbrella", "+15125557001", 77)
	s.lined(TrunkGroup{CustomerID: "hooli", Vendor: "telnyx"}, "+15125557001", "trunk-hooli")
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &PhoneNumber{
		E164: "+15125557001", Vendor: sipTrunkVendor, Country: "US", CustomerID: "soylent", PurchasedAt: s.base,
	}))
	s.lineOn("wayne", "+15125557001", 0)
	s.Require().NoError(giveBack(s.ctx, s.store, "wayne", "+15125557001", time.Time{}))

	lines, err := s.store.PhoneNumbers.TrunkLinesElsewhere(s.ctx, "acme", "+15125557001", 0)

	s.Require().NoError(err)
	s.Require().Len(lines, 1, "acme's own, another number, another app, a bought number, one with no line and one given back are left")
	s.Equal("globex", lines[0].CustomerID)
	s.Equal("trunk-globex", lines[0].StreamTrunkID)
}

// A line made in the deployment's own app before lines were pinned has no pin; one made since
// has its id.
func (s *StoreSuite) TestATrunkLineInTheDeploymentsAppIsFoundByItsIdOrNoPin() {
	s.store.SetStreamPins(StreamPins{Deployment: func() int64 { return 1 }})
	s.T().Cleanup(func() { s.store.SetStreamPins(StreamPins{}) })
	s.lineOn("globex", "+15125557003", 0)
	s.lineOn("initech", "+15125557003", 1)
	s.lineOn("umbrella", "+15125557003", 77)

	for _, app := range []int64{0, 1} {
		lines, err := s.store.PhoneNumbers.TrunkLinesElsewhere(s.ctx, "acme", "+15125557003", app)
		s.Require().NoError(err)
		s.ElementsMatch([]string{"globex", "initech"}, customersHolding(lines), "app %d", app)
	}
	lines, err := s.store.PhoneNumbers.TrunkLinesElsewhere(s.ctx, "acme", "+15125557003", 77)
	s.Require().NoError(err)
	s.Equal([]string{"umbrella"}, customersHolding(lines))
}

func customersHolding(numbers []PhoneNumber) []string {
	customers := make([]string, 0, len(numbers))
	for _, number := range numbers {
		customers = append(customers, number.CustomerID)
	}
	return customers
}

// Attached lists the attached numbers of one vendor that are held now, every customer's, by id,
// a page at a time, kept to one app or one number when asked.
func (s *StoreSuite) TestAttachedListsTheHeldAttachedNumbersOfOneVendorAPageAtATime() {
	add := func(customer, e164, vendor string, app int64, attached bool) {
		s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &PhoneNumber{
			E164: e164, Vendor: vendor, Country: "US", CustomerID: customer, PurchasedAt: s.base,
		}))
		if attached {
			s.Require().NoError(s.store.PhoneNumbers.Attach(s.ctx, customer, e164,
				NumberAttachment{TrunkID: "trunk-" + e164, StreamAppPK: app, CallType: "default"}))
		}
	}
	add("acme", "+15125550001", "twilio", 0, true)
	add("globex", "+15125550002", "twilio", 7, true)
	add("acme", "+15125550003", "twilio", 0, false)
	add("acme", "+15125550004", "telnyx", 0, true)
	add("acme", "+15125550005", "twilio", 0, true)
	s.Require().NoError(giveBack(s.ctx, s.store, "acme", "+15125550005", s.base))
	add("acme", "+15125550006", "twilio", 7, true)

	listed := func(filter AttachedFilter) []string {
		numbers, err := s.store.PhoneNumbers.Attached(s.ctx, filter)
		s.Require().NoError(err)
		var e164s []string
		for _, number := range numbers {
			e164s = append(e164s, number.E164)
		}
		return e164s
	}
	s.Equal([]string{"+15125550001", "+15125550002", "+15125550006"}, listed(AttachedFilter{Vendor: "twilio", Limit: 10}))

	first, err := s.store.PhoneNumbers.Attached(s.ctx, AttachedFilter{Vendor: "twilio", Limit: 2})
	s.Require().NoError(err)
	s.Require().Len(first, 2)
	s.Equal([]string{"+15125550006"}, listed(AttachedFilter{Vendor: "twilio", After: first[1].ID, Limit: 2}))

	app, deployment := int64(7), int64(0)
	s.Equal([]string{"+15125550002", "+15125550006"}, listed(AttachedFilter{Vendor: "twilio", App: &app, Limit: 10}))
	s.Equal([]string{"+15125550001"}, listed(AttachedFilter{Vendor: "twilio", App: &deployment, Limit: 10}))
	s.Equal([]string{"+15125550002"}, listed(AttachedFilter{Vendor: "twilio", E164: "+15125550002", Limit: 10}))

	_, err = s.store.PhoneNumbers.Attached(s.ctx, AttachedFilter{Limit: 10})
	s.Error(err, "a vendor is required")
	_, err = s.store.PhoneNumbers.Attached(s.ctx, AttachedFilter{Vendor: "twilio"})
	s.Error(err, "a page size is required")
}
