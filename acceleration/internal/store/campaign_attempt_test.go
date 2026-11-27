//go:build integration

package store

import "time"

func (s *StoreSuite) TestAnotherCustomersEndOfADialIdChangesNothing() {
	campaign := s.runningCampaign(5, "+15550001")
	contact := s.claimDue("a", s.base, 10)[0].Contact
	attempt := CampaignAttempt{CampaignID: campaign.ID, ContactID: contact.ID, StepID: "step_1", Attempt: 1, Dial: 1, StartedAt: s.base}
	s.Require().NoError(s.store.CampaignAttempts.Insert(s.ctx, &attempt))
	before, _, err := s.store.CampaignAttempts.Latest(s.ctx, contact.ID)
	s.Require().NoError(err)

	s.Require().NoError(s.store.CampaignAttempts.End(s.ctx, "somebody-else", attempt.ID, "", "completed", "their-call", "", s.base.Add(time.Minute), nil))

	after, found, err := s.store.CampaignAttempts.Latest(s.ctx, contact.ID)
	s.Require().NoError(err)
	s.Require().True(found)
	s.Equal(before, after, "the campaign's dial is still being placed")
}

// Rang leaves a dial already ended as it ended, and touches no dial of another customer.
func (s *StoreSuite) TestRangLeavesAnEndedDialAndAnotherCustomersDialAsTheyAre() {
	campaign := s.runningCampaign(5, "+15550001")
	contact := s.claimDue("a", s.base, 10)[0].Contact
	ended := CampaignAttempt{CampaignID: campaign.ID, ContactID: contact.ID, StepID: "step_1", Attempt: 1, Dial: 1, StartedAt: s.base}
	s.Require().NoError(s.store.CampaignAttempts.Insert(s.ctx, &ended))
	s.Require().NoError(s.store.CampaignAttempts.End(s.ctx, campaign.CustomerID, ended.ID, "", "no_answer", "call-1", "vendor-1", s.base.Add(time.Minute), nil))

	s.Require().NoError(s.store.CampaignAttempts.Rang(s.ctx, campaign.CustomerID, ended.ID, "voicemail", "vendor-2"))
	latest, _, err := s.store.CampaignAttempts.Latest(s.ctx, contact.ID)
	s.Require().NoError(err)
	s.Empty(latest.Result, "an ended dial keeps its result")
	s.Equal("vendor-1", latest.VendorCallID)

	open := CampaignAttempt{CampaignID: campaign.ID, ContactID: contact.ID, StepID: "step_1", Attempt: 1, Dial: 2, StartedAt: s.base.Add(2 * time.Minute)}
	s.Require().NoError(s.store.CampaignAttempts.Insert(s.ctx, &open))
	s.Require().NoError(s.store.CampaignAttempts.Rang(s.ctx, "somebody-else", open.ID, "voicemail", "vendor-3"))
	latest, _, err = s.store.CampaignAttempts.Latest(s.ctx, contact.ID)
	s.Require().NoError(err)
	s.Require().Equal(open.ID, latest.ID)
	s.Empty(latest.Result, "another customer's Rang writes nothing")
	s.Empty(latest.VendorCallID)
}

func (s *StoreSuite) TestCampaignAttemptQueryBudgets() {
	campaigns := make([]Campaign, 3)
	var held []Claimed
	for i := range campaigns {
		campaigns[i] = s.runningCampaign(5, "+15550001")
	}
	held = s.claimDue("a", s.base, 10)
	s.Require().Len(held, 3)
	attempts := make([]CampaignAttempt, len(held))
	for i, claim := range held {
		attempts[i] = CampaignAttempt{CampaignID: claim.Campaign.ID, ContactID: claim.Contact.ID, StepID: "step_1", Attempt: 1, StartedAt: s.base}
		s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.CampaignAttempts.Insert(s.ctx, &attempts[i])) }), "Insert")
		classifyAfter := s.base.Add(time.Minute)
		s.Equal(1, s.countQueries(func() {
			s.Require().NoError(s.store.CampaignAttempts.End(s.ctx, claim.Campaign.CustomerID, attempts[i].ID, "", "completed", "call-"+attempts[i].ID, "", s.base, &classifyAfter))
		}), "End")
		s.Require().NoError(s.store.CampaignContacts.Settle(s.ctx, "a", claim.Contact.ID, Settle{Stage: AwaitingOutcome, Attempt: 1}))
	}
	s.Equal(1, s.countQueries(func() {
		latest, found, err := s.store.CampaignAttempts.Latest(s.ctx, held[0].Contact.ID)
		s.Require().NoError(err)
		s.True(found)
		s.Equal(attempts[0].ID, latest.ID)
	}), "Latest")
	contactIDs := []string{held[0].Contact.ID, held[1].Contact.ID, held[2].Contact.ID}
	s.Equal(1, s.countQueries(func() {
		made, err := s.store.CampaignAttempts.Made(s.ctx, contactIDs)
		s.Require().NoError(err)
		s.Len(made, 3)
	}), "Made")

	var classifying []Classifying
	s.Equal(3+2, s.countQueries(func() {
		var err error
		classifying, err = s.store.CampaignAttempts.ClaimClassifications(s.ctx, "b", s.base.Add(time.Minute), time.Minute, 10)
		s.Require().NoError(err)
	}), "ClaimClassifications claims the dials, leases their contacts and reads their campaigns, in one query each")
	s.Require().Len(classifying, 3)
	s.Equal(2+2, s.countQueries(func() {
		s.Require().NoError(s.store.CampaignAttempts.PutOff(s.ctx, "b", classifying[0].Attempt.ID, s.base.Add(time.Hour)))
	}), "PutOff releases the contact and puts the dial off")
}

// A router whose call outlived its lease ends its dial after the router that took the contact
// over ended it, and must not write over what that router read.
func (s *StoreSuite) TestADialEndsOnce() {
	s.runningCampaign(5, "+15550001")
	claim := s.claimDue("a", s.base, 10)[0]
	attempt := CampaignAttempt{CampaignID: claim.Campaign.ID, ContactID: claim.Contact.ID, StepID: "step_1", Attempt: 1, StartedAt: s.base}
	s.Require().NoError(s.store.CampaignAttempts.Insert(s.ctx, &attempt))

	s.Require().NoError(s.store.CampaignAttempts.End(s.ctx, claim.Campaign.CustomerID, attempt.ID, "", "busy", "", "", s.base.Add(time.Minute), nil))
	s.Require().NoError(s.store.CampaignAttempts.End(s.ctx, claim.Campaign.CustomerID, attempt.ID, "", "completed", "", "", s.base.Add(2*time.Minute), &s.base))

	ended, found, err := s.store.CampaignAttempts.Latest(s.ctx, claim.Contact.ID)
	s.Require().NoError(err)
	s.Require().True(found)
	s.Equal("busy", ended.CallOutcome)
	s.Equal(s.base.Add(time.Minute), ended.EndedAt.UTC())
	s.Empty(ended.ClassifyState)
}
