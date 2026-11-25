//go:build integration

package store

import "time"

// A provider that did not hear back in time sends the same message again.
func (s *StoreSuite) TestAMessageAlreadyTakenIsNotClaimedTwice() {
	account := s.line(newID(), "+1555"+newID()[:7])
	message := "wamid." + newID()

	first, err := s.store.ChannelDeliveries.Claim(s.ctx, account.ID, message, "+15550001111")
	s.Require().NoError(err)
	second, err := s.store.ChannelDeliveries.Claim(s.ctx, account.ID, message, "+15550001111")
	s.Require().NoError(err)

	s.True(first)
	s.False(second)
}

// A STOP whose opt-out could not be written is not taken, so its next delivery records it.
func (s *StoreSuite) TestAStopWhoseOptOutFailedIsNotTaken() {
	account := s.line(newID(), "+1555"+newID()[:7])
	message, person := "wamid."+newID(), "+1555"+newID()[:7]

	_, err := s.store.ChannelDeliveries.ClaimStop(s.ctx, account.ID, message, person,
		&OptOut{CustomerID: account.CustomerID, Channel: "whatsapp", Source: "keyword"})
	s.Require().Error(err, "an opt-out naming nobody is refused")

	fresh, err := s.store.ChannelDeliveries.ClaimStop(s.ctx, account.ID, message, person,
		&OptOut{CustomerID: account.CustomerID, Recipient: person, Channel: "whatsapp", Source: "keyword"})
	s.Require().NoError(err)
	s.True(fresh, "the failed write gave the message back")
	optedOut, err := s.store.OptOuts.OptedOut(s.ctx, account.CustomerID, person, "whatsapp")
	s.Require().NoError(err)
	s.True(optedOut)

	again, err := s.store.ChannelDeliveries.ClaimStart(s.ctx, account.ID, message, person, account.CustomerID, person, "whatsapp")
	s.Require().NoError(err)
	s.False(again, "a message already taken is not taken again")
	optedOut, err = s.store.OptOuts.OptedOut(s.ctx, account.CustomerID, person, "whatsapp")
	s.Require().NoError(err)
	s.True(optedOut, "a message already taken writes nothing")
}

func (s *StoreSuite) TestTheLastTimeSomebodyWroteIsWhenTheyLastWrote() {
	account := s.line(newID(), "+1555"+newID()[:7])

	never, err := s.store.ChannelDeliveries.LastAt(s.ctx, account.ID, "+15550001111")
	s.Require().NoError(err)
	s.True(never.IsZero(), "somebody who never wrote has no last message")

	before := time.Now().Add(-time.Second)
	_, err = s.store.ChannelDeliveries.Claim(s.ctx, account.ID, "wamid."+newID(), "+15550001111")
	s.Require().NoError(err)
	_, err = s.store.ChannelDeliveries.Claim(s.ctx, account.ID, "wamid."+newID(), "+15550002222")
	s.Require().NoError(err)

	last, err := s.store.ChannelDeliveries.LastAt(s.ctx, account.ID, "+15550001111")
	s.Require().NoError(err)
	s.True(last.After(before), "the last message of the person who wrote is theirs, not another's")
}

func (s *StoreSuite) TestChannelDeliveryQueryBudgets() {
	account := s.line(newID(), "+1555"+newID()[:7])

	s.Equal(1, s.countQueries(func() {
		claimed, err := s.store.ChannelDeliveries.Claim(s.ctx, account.ID, "wamid."+newID(), "+15550001111")
		s.Require().NoError(err)
		s.True(claimed)
	}), "Claim")
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.ChannelDeliveries.LastAt(s.ctx, account.ID, "+15550001111")
		s.Require().NoError(err)
	}), "LastAt")
}
