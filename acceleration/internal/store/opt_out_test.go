//go:build integration

package store

import "errors"

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
