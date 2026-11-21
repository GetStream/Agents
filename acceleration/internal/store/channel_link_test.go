//go:build integration

package store

import "time"

// A code somebody saw go by must not claim a second number.
func (s *StoreSuite) TestACodeIsSpentOnce() {
	customerID := newID()
	link := ChannelLink{
		Code: newID()[:6], CustomerID: customerID, ConfigID: newID(),
		UserID: "on-call-engineer", ExpiresAt: time.Now().UTC().Add(time.Minute),
	}
	s.Require().NoError(s.store.ChannelLinks.Insert(s.ctx, &link))

	claimed, err := s.store.ChannelLinks.Claim(s.ctx, customerID, link.Code)
	s.Require().NoError(err)
	s.Equal("on-call-engineer", claimed.UserID)

	_, err = s.store.ChannelLinks.Claim(s.ctx, customerID, link.Code)
	s.ErrorIs(err, ErrUnknownChannelLink)
}

func (s *StoreSuite) TestACodePastItsTimeIsNoCodeAtAll() {
	customerID := newID()
	link := ChannelLink{
		Code: newID()[:6], CustomerID: customerID, ConfigID: newID(),
		UserID: "on-call-engineer", ExpiresAt: time.Now().UTC().Add(-time.Minute),
	}
	s.Require().NoError(s.store.ChannelLinks.Insert(s.ctx, &link))

	_, err := s.store.ChannelLinks.Claim(s.ctx, customerID, link.Code)

	s.ErrorIs(err, ErrUnknownChannelLink)
}

func (s *StoreSuite) TestAnotherAppsCodeCannotBeSpentHere() {
	link := ChannelLink{
		Code: newID()[:6], CustomerID: newID(), ConfigID: newID(),
		UserID: "on-call-engineer", ExpiresAt: time.Now().UTC().Add(time.Minute),
	}
	s.Require().NoError(s.store.ChannelLinks.Insert(s.ctx, &link))

	_, err := s.store.ChannelLinks.Claim(s.ctx, newID(), link.Code)

	s.ErrorIs(err, ErrUnknownChannelLink)
}

func (s *StoreSuite) TestChannelLinkQueryBudgets() {
	link := ChannelLink{Code: newID()[:6], CustomerID: newID(), ConfigID: newID(), UserID: "user-one", ExpiresAt: time.Now().Add(time.Minute)}

	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.ChannelLinks.Insert(s.ctx, &link)) }), "Insert")
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.ChannelLinks.Claim(s.ctx, link.CustomerID, link.Code)
		s.Require().NoError(err)
	}), "Claim")
}
