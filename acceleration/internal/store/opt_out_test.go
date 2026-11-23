//go:build integration

package store

import (
	"errors"
	"fmt"
)

// START lifts what STOP did, and the record of both stays.
func (s *StoreSuite) TestAnOptOutFromEveryChannelBlocksEachUntilRevoked() {
	customerID := newID()
	first := OptOut{CustomerID: customerID, Recipient: "+15551230000", Channel: OptOutAll, Source: "api"}
	again := first
	s.Require().NoError(s.store.OptOuts.Insert(s.ctx, &first))
	s.Require().NoError(s.store.OptOuts.Insert(s.ctx, &again))
	s.Equal(first.ID, again.ID, "asking twice keeps the first")

	blocked, err := s.store.OptOuts.OptedOut(s.ctx, customerID, "+15551230000", "sms")
	s.Require().NoError(err)
	s.True(blocked)

	s.Require().NoError(s.store.OptOuts.RevokeAll(s.ctx, customerID, "+15551230000", "sms"))
	blocked, err = s.store.OptOuts.OptedOut(s.ctx, customerID, "+15551230000", "voice")
	s.Require().NoError(err)
	s.False(blocked)
	s.True(errors.Is(s.store.OptOuts.Revoke(s.ctx, customerID, first.ID), ErrUnknownOptOut), "already revoked")
}

func (s *StoreSuite) TestOptOutQueryBudgets() {
	customerID := newID()
	for i := range 4 {
		s.Require().NoError(s.store.OptOuts.Insert(s.ctx, &OptOut{
			CustomerID: customerID, Recipient: fmt.Sprintf("+1555123000%d", i), Channel: "sms", Source: "api",
		}))
	}
	optOut := OptOut{CustomerID: customerID, Recipient: "+15551239999", Channel: "sms", Source: "api"}
	s.Equal(2, s.countQueries(func() { s.Require().NoError(s.store.OptOuts.Insert(s.ctx, &optOut)) }),
		"Insert writes, then reads back whichever opt-out was kept")
	s.Equal(1, s.countQueries(func() {
		listed, err := s.store.OptOuts.Select(s.ctx, customerID, 10, nil)
		s.Require().NoError(err)
		s.Len(listed, 5)
	}), "Select")
	s.Equal(1, s.countQueries(func() {
		opted, err := s.store.OptOuts.OptedOut(s.ctx, customerID, "+15551239999", "sms")
		s.Require().NoError(err)
		s.True(opted)
	}), "OptedOut")
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.OptOuts.Revoke(s.ctx, customerID, optOut.ID)) }), "Revoke")
	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.OptOuts.RevokeAll(s.ctx, customerID, "+15551230000", "sms"))
	}), "RevokeAll")
}
