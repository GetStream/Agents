//go:build integration

package store

import (
	"time"
)

func (s *StoreSuite) trunk(customerID string) *SIPTrunk {
	trunk := &SIPTrunk{
		ID: newID(), CustomerID: customerID, Name: "main", Host: "trunk.example.com",
		Port: 5060, Transport: "tcp", Username: "agent",
		PasswordSealed: []byte("sealed"), PasswordKEKVersion: 1,
		LateOffer: true, Codecs: []string{"PCMU", "PCMA"},
	}
	s.Require().NoError(s.store.SIPTrunks.Insert(s.ctx, trunk))
	return trunk
}

func (s *StoreSuite) trunkNumber(customerID, trunkID, e164 string) error {
	return s.store.PhoneNumbers.InsertOnSIPTrunk(s.ctx, &PhoneNumber{
		E164: e164, Vendor: "sip_trunk", Country: "US", CustomerID: customerID, SIPTrunkID: trunkID,
	})
}

func (s *StoreSuite) TestATrunkIsReadBackWhole() {
	created := s.trunk("acme")

	read, err := s.store.SIPTrunks.Get(s.ctx, "acme", created.ID)
	s.Require().NoError(err)
	read.CreatedAt, read.UpdatedAt = created.CreatedAt, created.UpdatedAt
	s.Equal(*created, read)
}

func (s *StoreSuite) TestAnotherCustomersTrunkIsNotFound() {
	created := s.trunk("acme")

	_, err := s.store.SIPTrunks.Get(s.ctx, "other", created.ID)
	s.ErrorIs(err, ErrNoSIPTrunk)

	listed, err := s.store.SIPTrunks.Select(s.ctx, "other")
	s.Require().NoError(err)
	s.Empty(listed)

	updated := *created
	updated.CustomerID = "other"
	s.ErrorIs(s.store.SIPTrunks.Update(s.ctx, &updated), ErrNoSIPTrunk)
	s.ErrorIs(s.store.SIPTrunks.Delete(s.ctx, "other", created.ID), ErrNoSIPTrunk)
}

func (s *StoreSuite) TestUpdatingATrunkChangesEveryField() {
	trunk := s.trunk("acme")
	trunk.Name, trunk.Host, trunk.Port, trunk.Transport = "backup", "other.example.com", 5061, "tls"
	trunk.Username, trunk.PasswordSealed, trunk.PasswordKEKVersion = "agent2", []byte("resealed"), 2
	trunk.LateOffer, trunk.Codecs = false, []string{"G722"}
	s.Require().NoError(s.store.SIPTrunks.Update(s.ctx, trunk))

	read, err := s.store.SIPTrunks.Get(s.ctx, "acme", trunk.ID)
	s.Require().NoError(err)
	read.CreatedAt = trunk.CreatedAt
	s.WithinDuration(trunk.UpdatedAt, read.UpdatedAt, time.Millisecond)
	read.UpdatedAt = trunk.UpdatedAt
	s.Equal(*trunk, read)
}

func (s *StoreSuite) TestATrunkWithANumberCannotBeDeletedUntilTheNumberIsReleased() {
	trunk := s.trunk("acme")
	s.Require().NoError(s.trunkNumber("acme", trunk.ID, "+15550000101"))

	s.ErrorIs(s.store.SIPTrunks.Delete(s.ctx, "acme", trunk.ID), ErrSIPTrunkInUse)

	s.Require().NoError(giveBack(s.ctx, s.store, "acme", "+15550000101", time.Time{}))
	s.Require().NoError(s.store.SIPTrunks.Delete(s.ctx, "acme", trunk.ID))
	_, err := s.store.SIPTrunks.Get(s.ctx, "acme", trunk.ID)
	s.ErrorIs(err, ErrNoSIPTrunk)
}

func (s *StoreSuite) TestANumberOnATrunkIsHeldUnderSIPTrunk() {
	trunk := s.trunk("acme")
	s.Require().NoError(s.trunkNumber("acme", trunk.ID, "+15550000102"))

	held, err := s.store.PhoneNumbers.GetByE164(s.ctx, "acme", "+15550000102")
	s.Require().NoError(err)
	s.Equal("sip_trunk", held.Vendor)
	s.Equal(trunk.ID, held.SIPTrunkID)
	s.Equal(int64(0), held.MonthlyCostMicros)
}

func (s *StoreSuite) TestANumberOnAnotherCustomersTrunkCannotBeAddedToATrunk() {
	acme, other := s.trunk("acme"), s.trunk("other")
	s.Require().NoError(s.trunkNumber("acme", acme.ID, "+15550000103"))

	err := s.trunkNumber("other", other.ID, "+15550000103")

	s.ErrorIs(err, ErrNumberHeldElsewhere)
	s.NotContains(err.Error(), "acme")
	_, err = s.store.PhoneNumbers.GetByE164(s.ctx, "other", "+15550000103")
	s.ErrorIs(err, ErrNoNumber)
	held, err := s.store.PhoneNumbers.GetByE164(s.ctx, "acme", "+15550000103")
	s.Require().NoError(err)
	s.Equal(acme.ID, held.SIPTrunkID)
}

func (s *StoreSuite) TestANumberAnotherCustomerBoughtCannotBeAddedToATrunk() {
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &PhoneNumber{
		E164: "+15550000110", Vendor: "twilio", Country: "US", CustomerID: "acme",
	}))
	trunk := s.trunk("other")

	s.ErrorIs(s.trunkNumber("other", trunk.ID, "+15550000110"), ErrNumberHeldElsewhere)
	_, err := s.store.PhoneNumbers.GetByE164(s.ctx, "other", "+15550000110")
	s.ErrorIs(err, ErrNoNumber)
}

func (s *StoreSuite) TestANumberAnotherCustomerGaveBackCanBeAddedToATrunk() {
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &PhoneNumber{
		E164: "+15550000111", Vendor: "twilio", Country: "US", CustomerID: "acme",
	}))
	s.Require().NoError(giveBack(s.ctx, s.store, "acme", "+15550000111", time.Time{}))
	trunk := s.trunk("other")

	s.Require().NoError(s.trunkNumber("other", trunk.ID, "+15550000111"))
	held, err := s.store.PhoneNumbers.GetByE164(s.ctx, "other", "+15550000111")
	s.Require().NoError(err)
	s.Equal(trunk.ID, held.SIPTrunkID)
}

// Another customer's registration of the number holds the lock its insert keeps, uncommitted,
// so the second one has to wait for it to know whether it holds the number.
func (s *StoreSuite) TestOfTwoCustomersAddingOneNumberAtOnceTheSecondIsRefused() {
	acme, other := s.trunk("acme"), s.trunk("other")
	tx, err := s.store.global.BeginTx(s.ctx, nil)
	s.Require().NoError(err)
	defer func() { _ = tx.Rollback() }()
	held, err := holders(s.ctx, tx, "+15550000112")
	s.Require().NoError(err)
	s.Require().Empty(held)
	s.Require().NoError(insertNumber(s.ctx, tx, &PhoneNumber{
		E164: "+15550000112", Vendor: sipTrunkVendor, Country: "US", CustomerID: "acme", SIPTrunkID: acme.ID,
	}))

	added := make(chan error, 1)
	go func() { added <- s.trunkNumber("other", other.ID, "+15550000112") }()
	s.Require().Eventually(func() bool {
		var waiting int
		s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx, `
SELECT count(*) FROM pg_stat_activity
WHERE datname = current_database() AND wait_event_type = 'Lock'`).Scan(&waiting))
		return waiting > 0
	}, 5*time.Second, 10*time.Millisecond)
	s.Require().NoError(tx.Commit())

	s.ErrorIs(<-added, ErrNumberHeldElsewhere)
}

func (s *StoreSuite) TestANumberOnATrunkDoesNotStopAnotherCustomerBuyingIt() {
	trunk := s.trunk("acme")
	s.Require().NoError(s.trunkNumber("acme", trunk.ID, "+15550000113"))

	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &PhoneNumber{
		E164: "+15550000113", Vendor: "twilio", Country: "US", CustomerID: "other",
	}))

	bought, err := s.store.PhoneNumbers.BoughtElsewhere(s.ctx, "acme", "+15550000113")
	s.Require().NoError(err)
	s.True(bought, "the trunk's customer is told another bought it")
	bought, err = s.store.PhoneNumbers.BoughtElsewhere(s.ctx, "other", "+15550000113")
	s.Require().NoError(err)
	s.False(bought, "the buyer's own number is not another's")
}

func (s *StoreSuite) TestANumberGivenBackIsNotBoughtElsewhere() {
	trunk := s.trunk("acme")
	s.Require().NoError(s.trunkNumber("acme", trunk.ID, "+15550000114"))
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &PhoneNumber{
		E164: "+15550000114", Vendor: "twilio", Country: "US", CustomerID: "other",
	}))
	s.Require().NoError(giveBack(s.ctx, s.store, "other", "+15550000114", time.Time{}))

	bought, err := s.store.PhoneNumbers.BoughtElsewhere(s.ctx, "acme", "+15550000114")
	s.Require().NoError(err)
	s.False(bought)
}

// Two customers' rows on their own trunks, as one written before a number was held by one
// customer at a time leaves them: neither one was sold the number.
func (s *StoreSuite) TestANumberOnAnotherCustomersTrunkIsNotBoughtElsewhere() {
	trunk := s.trunk("acme")
	s.Require().NoError(s.trunkNumber("acme", trunk.ID, "+15550000115"))
	other := s.trunk("other")
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &PhoneNumber{
		E164: "+15550000115", Vendor: sipTrunkVendor, Country: "US", CustomerID: "other", SIPTrunkID: other.ID,
	}))

	bought, err := s.store.PhoneNumbers.BoughtElsewhere(s.ctx, "acme", "+15550000115")
	s.Require().NoError(err)
	s.False(bought)
}

func (s *StoreSuite) TestACustomerCannotAddANumberTwice() {
	trunk := s.trunk("acme")
	s.Require().NoError(s.trunkNumber("acme", trunk.ID, "+15550000104"))

	s.ErrorIs(s.trunkNumber("acme", trunk.ID, "+15550000104"), ErrNumberHeld)
}

func (s *StoreSuite) TestACustomerCannotAddANumberItAlreadyHolds() {
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &PhoneNumber{
		E164: "+15550000105", Vendor: "twilio", Country: "US", CustomerID: "acme",
	}))
	trunk := s.trunk("acme")

	s.ErrorIs(s.trunkNumber("acme", trunk.ID, "+15550000105"), ErrNumberHeld)
}

func (s *StoreSuite) TestAnotherCustomersTrunkCannotTakeANumber() {
	trunk := s.trunk("acme")

	s.ErrorIs(s.trunkNumber("other", trunk.ID, "+15550000106"), ErrNoSIPTrunk)
}

// The trunk is found, then deleted by another router before the number is inserted. The
// delete is held open until the insert waits on it, so the order is fixed.
func (s *StoreSuite) TestATrunkDeletedWhileANumberIsAddedIsNotFound() {
	trunk := s.trunk("acme")
	tx, err := s.router().GlobalDB().BeginTx(s.ctx, nil)
	s.Require().NoError(err)
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(s.ctx, "DELETE FROM sip_trunks WHERE id = ?", trunk.ID)
	s.Require().NoError(err)

	added := make(chan error, 1)
	go func() { added <- s.trunkNumber("acme", trunk.ID, "+15550000109") }()
	s.Require().Eventually(func() bool {
		var waiting int
		s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx, `
SELECT count(*) FROM pg_stat_activity
WHERE datname = current_database() AND wait_event_type = 'Lock'`).Scan(&waiting))
		return waiting > 0
	}, 5*time.Second, 10*time.Millisecond)
	s.Require().NoError(tx.Commit())

	s.ErrorIs(<-added, ErrNoSIPTrunk)
}

func (s *StoreSuite) TestANumberNobodyHoldsMatchesErrNoNumber() {
	_, err := s.store.PhoneNumbers.GetByE164(s.ctx, "acme", "+15550000108")
	s.ErrorIs(err, ErrNoNumber)
	s.EqualError(err, "store: +15550000108 is not a number acme holds")
}

func (s *StoreSuite) TestABoughtNumberIsStillHeldByOneCustomerPerVendor() {
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &PhoneNumber{
		E164: "+15550000107", Vendor: "twilio", Country: "US", CustomerID: "acme",
	}))

	err := s.store.PhoneNumbers.Insert(s.ctx, &PhoneNumber{
		E164: "+15550000107", Vendor: "twilio", Country: "US", CustomerID: "other",
	})
	s.True(isViolation(err, uniqueViolation), "the rewritten index still stops a vendor's number being recorded twice: %v", err)
}

func (s *StoreSuite) TestSIPTrunkQueryBudgets() {
	for range 3 {
		s.trunk("acme")
	}
	var trunk *SIPTrunk
	s.Equal(1, s.countQueries(func() { trunk = s.trunk("acme") }), "Insert")
	s.Equal(1, s.countQueries(func() {
		trunks, err := s.store.SIPTrunks.Select(s.ctx, "acme")
		s.Require().NoError(err)
		s.Len(trunks, 4)
	}), "Select")
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.SIPTrunks.Get(s.ctx, "acme", trunk.ID)
		s.Require().NoError(err)
	}), "Get")
	trunk.Name = "renamed"
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.SIPTrunks.Update(s.ctx, trunk)) }), "Update")
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.SIPTrunks.Delete(s.ctx, "acme", trunk.ID)) }), "Delete")
}
