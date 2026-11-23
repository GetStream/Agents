//go:build integration

package store

import (
	"errors"
	"time"
)

// minted is a code of one app, for one of its agents, that may still be claimed.
func minted(customerID, configID, userID string) ChannelLink {
	return ChannelLink{
		Code: newID()[:8], CustomerID: customerID, ConfigID: configID,
		UserID: userID, ExpiresAt: time.Now().UTC().Add(time.Minute),
	}
}

// draws hands out codes in turn, the last one again once they run out, and counts the draws.
func draws(codes ...string) (func() (string, error), *int) {
	drawn := 0
	return func() (string, error) {
		code := codes[min(drawn, len(codes)-1)]
		drawn++
		return code, nil
	}, &drawn
}

// insertCode stores the link under the code it already has.
func (s *StoreSuite) insertCode(link *ChannelLink) {
	draw, _ := draws(link.Code)
	s.Require().NoError(s.store.ChannelLinks.Insert(s.ctx, link, draw))
}

// lapseCode writes a code past its time as it was before a later write deleted it.
func (s *StoreSuite) lapseCode(link *ChannelLink) {
	link.CreatedAt = time.Now().UTC().Add(-time.Hour)
	link.ExpiresAt = time.Now().UTC().Add(-time.Minute)
	_, err := s.store.global.NewInsert().Model(link).Exec(s.ctx)
	s.Require().NoError(err)
}

// codesHeld is how many rows the app holds under the code.
func (s *StoreSuite) codesHeld(customerID, code string) int {
	count, err := s.store.global.NewSelect().Model((*ChannelLink)(nil)).
		Where("customer_id = ? AND code = ?", customerID, code).Count(s.ctx)
	s.Require().NoError(err)
	return count
}

// A code somebody saw go by must not claim a second number, and the row it was is gone.
func (s *StoreSuite) TestACodeIsSpentOnce() {
	link := minted(newID(), newID(), "on-call-engineer")
	s.insertCode(&link)

	claimed, err := s.store.ChannelLinks.Claim(s.ctx, link.CustomerID, link.ConfigID, link.Code)
	s.Require().NoError(err)
	s.Equal("on-call-engineer", claimed.UserID)
	s.Equal(link.ConfigID, claimed.ConfigID)
	s.Zero(s.codesHeld(link.CustomerID, link.Code), "a claimed code is deleted, not kept as used")

	_, err = s.store.ChannelLinks.Claim(s.ctx, link.CustomerID, link.ConfigID, link.Code)
	s.ErrorIs(err, ErrUnknownChannelLink)
}

func (s *StoreSuite) TestACodePastItsTimeIsNoCodeAtAll() {
	link := minted(newID(), newID(), "on-call-engineer")
	s.lapseCode(&link)

	_, err := s.store.ChannelLinks.Claim(s.ctx, link.CustomerID, link.ConfigID, link.Code)

	s.ErrorIs(err, ErrUnknownChannelLink)
}

func (s *StoreSuite) TestAnotherAppsCodeCannotBeSpentHere() {
	link := minted(newID(), newID(), "on-call-engineer")
	s.insertCode(&link)

	_, err := s.store.ChannelLinks.Claim(s.ctx, newID(), link.ConfigID, link.Code)

	s.ErrorIs(err, ErrUnknownChannelLink)
	s.Equal(1, s.codesHeld(link.CustomerID, link.Code), "the code is still its own app's")
}

// Codes are drawn per app, so one app holding a code does not keep another from holding it.
func (s *StoreSuite) TestTwoAppsHoldOneCodeAtOnce() {
	first := minted(newID(), newID(), "first-app-user")
	second := minted(newID(), newID(), "second-app-user")
	second.Code = first.Code
	s.insertCode(&first)
	s.insertCode(&second)

	claimed, err := s.store.ChannelLinks.Claim(s.ctx, second.CustomerID, second.ConfigID, second.Code)
	s.Require().NoError(err)
	s.Equal("second-app-user", claimed.UserID)
	claimed, err = s.store.ChannelLinks.Claim(s.ctx, first.CustomerID, first.ConfigID, first.Code)
	s.Require().NoError(err)
	s.Equal("first-app-user", claimed.UserID)
}

// Texting a code to another agent's line of the app says so and leaves the code to claim.
func (s *StoreSuite) TestACodeForAnotherAgentIsNotSpent() {
	link := minted(newID(), newID(), "on-call-engineer")
	s.insertCode(&link)

	_, err := s.store.ChannelLinks.Claim(s.ctx, link.CustomerID, newID(), link.Code)
	s.ErrorIs(err, ErrChannelLinkElsewhere)

	claimed, err := s.store.ChannelLinks.Claim(s.ctx, link.CustomerID, link.ConfigID, link.Code)
	s.Require().NoError(err)
	s.Equal("on-call-engineer", claimed.UserID)
}

// Another agent's code that could not be claimed there either is no code at all, not one to
// send to the other line.
func (s *StoreSuite) TestAnotherAgentsSpentOrLapsedCodeIsNoCode() {
	used := minted(newID(), newID(), "on-call-engineer")
	at := time.Now().UTC()
	used.UsedAt = &at
	s.insertCode(&used)
	lapsed := minted(newID(), newID(), "on-call-engineer")
	s.lapseCode(&lapsed)

	for name, link := range map[string]ChannelLink{"past its time": lapsed, "marked used by an older release": used} {
		_, err := s.store.ChannelLinks.Claim(s.ctx, link.CustomerID, newID(), link.Code)
		s.ErrorIs(err, ErrUnknownChannelLink, name)
		_, err = s.store.ChannelLinks.Claim(s.ctx, link.CustomerID, link.ConfigID, link.Code)
		s.ErrorIs(err, ErrUnknownChannelLink, name)
	}
}

func (s *StoreSuite) TestAClaimNamingNoAgentClaimsNothing() {
	link := minted(newID(), newID(), "on-call-engineer")
	s.insertCode(&link)

	_, err := s.store.ChannelLinks.Claim(s.ctx, link.CustomerID, "", link.Code)

	s.ErrorIs(err, ErrUnknownChannelLink)
	s.Equal(1, s.codesHeld(link.CustomerID, link.Code))
}

// A code the app holds is drawn again, so two people linking never get one code.
func (s *StoreSuite) TestACodeTheAppHoldsIsDrawnAgain() {
	held := minted(newID(), newID(), "first-user")
	s.insertCode(&held)
	link := minted(held.CustomerID, held.ConfigID, "second-user")
	fresh := link.Code
	draw, drawn := draws(held.Code, fresh)

	s.Require().NoError(s.store.ChannelLinks.Insert(s.ctx, &link, draw))

	s.Equal(fresh, link.Code)
	s.Equal(2, *drawn)
	claimed, err := s.store.ChannelLinks.Claim(s.ctx, held.CustomerID, held.ConfigID, held.Code)
	s.Require().NoError(err)
	s.Equal("first-user", claimed.UserID, "the code held is not overwritten")
	claimed, err = s.store.ChannelLinks.Claim(s.ctx, link.CustomerID, link.ConfigID, fresh)
	s.Require().NoError(err)
	s.Equal("second-user", claimed.UserID)
}

// A draw that only ever gives a taken code ends, so linking fails rather than hangs.
func (s *StoreSuite) TestADrawThatIsAlwaysTakenGivesUp() {
	held := minted(newID(), newID(), "first-user")
	s.insertCode(&held)
	link := minted(held.CustomerID, held.ConfigID, "second-user")
	draw, drawn := draws(held.Code)

	err := s.store.ChannelLinks.Insert(s.ctx, &link, draw)

	s.Error(err)
	s.Equal(channelLinkDraws, *drawn)
	s.Equal(4, channelLinkDraws)
	s.Equal(1, s.codesHeld(held.CustomerID, held.Code))
}

// A code that cannot be drawn, or a link that ties a number to no one, writes nothing.
func (s *StoreSuite) TestALinkThatCannotBeWrittenWritesNothing() {
	broken := errors.New("no randomness")
	link := minted(newID(), newID(), "on-call-engineer")
	err := s.store.ChannelLinks.Insert(s.ctx, &link, func() (string, error) { return "", broken })
	s.ErrorIs(err, broken)

	nobody := minted(newID(), newID(), "")
	draw, _ := draws(nobody.Code)
	var invalid *ValidationError
	s.ErrorAs(s.store.ChannelLinks.Insert(s.ctx, &nobody, draw), &invalid)
	s.Equal("user_id", invalid.Field)
	s.Zero(s.codesHeld(nobody.CustomerID, nobody.Code))
}

// A code claimed or past its time is free to be drawn again.
func (s *StoreSuite) TestACodeIsIssuedAgainAfterItWasClaimedOrLapsed() {
	claimed := minted(newID(), newID(), "first-user")
	s.insertCode(&claimed)
	_, err := s.store.ChannelLinks.Claim(s.ctx, claimed.CustomerID, claimed.ConfigID, claimed.Code)
	s.Require().NoError(err)
	lapsed := minted(newID(), newID(), "first-user")

	for name, old := range map[string]ChannelLink{"claimed": claimed, "past its time": lapsed} {
		if name == "past its time" {
			s.lapseCode(&lapsed)
			old = lapsed
		}
		again := minted(old.CustomerID, old.ConfigID, "second-user")
		again.Code = old.Code
		s.insertCode(&again)

		link, err := s.store.ChannelLinks.Claim(s.ctx, old.CustomerID, old.ConfigID, old.Code)
		s.Require().NoError(err, name)
		s.Equal("second-user", link.UserID, name)
	}
}

// Writing a code deletes every app's codes past their time, so the table keeps no more than
// the codes that may still be claimed.
func (s *StoreSuite) TestWritingACodeDeletesTheCodesPastTheirTime() {
	live := minted(newID(), newID(), "first-user")
	s.insertCode(&live)
	lapsed := minted(newID(), newID(), "first-user")
	s.lapseCode(&lapsed)
	s.Require().Equal(1, s.codesHeld(lapsed.CustomerID, lapsed.Code))

	other := minted(newID(), newID(), "second-user")
	s.insertCode(&other)

	s.Zero(s.codesHeld(lapsed.CustomerID, lapsed.Code), "another app's code past its time")
	s.Equal(1, s.codesHeld(live.CustomerID, live.Code), "a code still to claim stays")
}

func (s *StoreSuite) TestChannelLinkQueryBudgets() {
	link := minted(newID(), newID(), "user-one")
	draw, _ := draws(link.Code)

	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.ChannelLinks.Insert(s.ctx, &link, draw)) }), "Insert")
	s.Equal(2, s.countQueries(func() {
		_, err := s.store.ChannelLinks.Claim(s.ctx, link.CustomerID, newID(), link.Code)
		s.Require().ErrorIs(err, ErrChannelLinkElsewhere)
	}), "Claim of another agent's code")
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.ChannelLinks.Claim(s.ctx, link.CustomerID, link.ConfigID, link.Code)
		s.Require().NoError(err)
	}), "Claim")
}
