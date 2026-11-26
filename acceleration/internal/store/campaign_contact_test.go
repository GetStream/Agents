//go:build integration

package store

import (
	"fmt"
	"sync"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/shardmap"
)

// runningCampaign launches a campaign that may hold concurrency calls at once, with one contact
// per number, all due at s.base.
func (s *StoreSuite) runningCampaign(concurrency int, numbers ...string) Campaign {
	campaign := s.draftCampaign(newID(), numbers...)
	_, err := s.store.Campaigns.Update(s.ctx, campaign.CustomerID, campaign.ID, CampaignChanges{Concurrency: &concurrency})
	s.Require().NoError(err)
	return s.launch(campaign, s.base)
}

func (s *StoreSuite) claimDue(owner string, now time.Time, max int) []Claimed {
	claimed, err := s.store.CampaignContacts.Claim(s.ctx, owner, ShardClaim{}, now, time.Minute, max)
	s.Require().NoError(err)
	return claimed
}

func (s *StoreSuite) contactByNumber(campaign Campaign, number string) Contact {
	for _, contact := range s.contactsOf(campaign) {
		if contact.ToNumber == number {
			return contact
		}
	}
	s.FailNow("no contact " + number)
	return Contact{}
}

func (s *StoreSuite) dueAt(contact Contact, at time.Time) {
	_, err := s.store.GlobalDB().NewUpdate().Model((*Contact)(nil)).Set("next_attempt_at = ?", at).Where("id = ?", contact.ID).Exec(s.ctx)
	s.Require().NoError(err)
}

func (s *StoreSuite) campaignNow(campaign Campaign) Campaign {
	found, err := s.store.Campaigns.Get(s.ctx, campaign.CustomerID, campaign.ID)
	s.Require().NoError(err)
	return found
}

func (s *StoreSuite) TestAClaimNeverExceedsTheCampaignsConcurrency() {
	// Several campaigns raced at once widen the window a check-then-claim would slip through.
	campaigns := make([]Campaign, 6)
	for i := range campaigns {
		campaigns[i] = s.runningCampaign(2, "+15550001", "+15550002", "+15550003", "+15550004", "+15550005")
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	results := map[string][]Claimed{}
	var mu sync.Mutex
	for _, owner := range []string{"a", "b"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			claimed, err := s.store.CampaignContacts.Claim(s.ctx, owner, ShardClaim{}, s.base, time.Minute, 100)
			s.NoError(err)
			mu.Lock()
			results[owner] = claimed
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()

	perCampaign := map[string]int{}
	seen := map[string]bool{}
	for owner, claimed := range results {
		for _, one := range claimed {
			s.False(seen[one.Contact.ID], "contact %s claimed twice", one.Contact.ID)
			seen[one.Contact.ID] = true
			perCampaign[one.Campaign.ID]++
			s.Equal(owner, one.Contact.LeasedBy)
			s.Equal(InProgress, one.Contact.Stage)
		}
	}
	for _, campaign := range campaigns {
		s.Equal(2, perCampaign[campaign.ID], "campaign %s", campaign.ID)
		inProgress := 0
		for _, contact := range s.contactsOf(campaign) {
			if contact.Stage == InProgress {
				inProgress++
			}
		}
		s.Equal(2, inProgress, "campaign %s", campaign.ID)
	}
	s.Empty(s.claimDue("c", s.base, 100), "every campaign is full")
}

func (s *StoreSuite) TestAClaimTakesTheEarliestDueFirstAndStopsAtMax() {
	campaign := s.runningCampaign(5, "+15550001", "+15550002", "+15550003")
	s.dueAt(s.contactByNumber(campaign, "+15550001"), s.base.Add(-time.Minute))
	s.dueAt(s.contactByNumber(campaign, "+15550002"), s.base.Add(-2*time.Minute))

	claimed := s.claimDue("a", s.base, 2)

	s.Require().Len(claimed, 2)
	numbers := []string{claimed[0].Contact.ToNumber, claimed[1].Contact.ToNumber}
	s.ElementsMatch([]string{"+15550002", "+15550001"}, numbers)
	s.Equal(campaign.ID, claimed[0].Campaign.ID)
	s.Require().NotNil(claimed[0].Contact.LeaseUntil)
	s.InDelta(time.Minute, s.leftOnTheDatabasesClock(*claimed[0].Contact.LeaseUntil), float64(time.Second), "a minute on Postgres's clock")
}

func (s *StoreSuite) TestAContactNotYetDueIsNotClaimed() {
	campaign := s.runningCampaign(5, "+15550001", "+15550002")
	s.dueAt(s.contactByNumber(campaign, "+15550002"), s.base.Add(time.Hour))

	claimed := s.claimDue("a", s.base, 10)

	s.Require().Len(claimed, 1)
	s.Equal("+15550001", claimed[0].Contact.ToNumber)
	later := s.claimDue("a", s.base.Add(time.Hour), 10)
	s.Require().Len(later, 1)
	s.Equal("+15550002", later[0].Contact.ToNumber)
}

func (s *StoreSuite) TestAPausedCampaignsContactsAreNotClaimed() {
	campaign := s.runningCampaign(5, "+15550001")
	s.Require().NoError(s.store.Campaigns.Pause(s.ctx, campaign.CustomerID, campaign.ID, ""))

	s.Empty(s.claimDue("a", s.base, 10))

	s.Require().NoError(s.store.Campaigns.Resume(s.ctx, campaign.CustomerID, campaign.ID))
	s.Len(s.claimDue("a", s.base, 10), 1)
}

func (s *StoreSuite) TestAScheduledCampaignStartsWhenItsTimeComes() {
	campaign := s.draftCampaign(newID(), "+15550001")
	start := s.base.Add(time.Hour)
	s.launch(campaign, start)

	s.Require().NoError(s.store.Campaigns.StartDue(s.ctx, start.Add(-time.Second)))
	s.Equal(Scheduled, s.campaignNow(campaign).State)
	s.Empty(s.claimDue("a", start, 10), "a scheduled campaign rings nobody")

	s.Require().NoError(s.store.Campaigns.StartDue(s.ctx, start))
	started := s.campaignNow(campaign)
	s.Equal(Running, started.State)
	s.Require().NotNil(started.StartedAt)
	s.Equal(start, started.StartedAt.UTC())
	s.Len(s.claimDue("a", start, 10), 1)
}

func (s *StoreSuite) TestAnExpiredLeaseIsTakenOverAndALiveOneIsNot() {
	campaign := s.runningCampaign(5, "+15550001", "+15550002")
	s.dueAt(s.contactByNumber(campaign, "+15550002"), s.base.Add(5*time.Minute))
	_, err := s.store.CampaignContacts.Claim(s.ctx, "a", ShardClaim{}, s.base, 10*time.Minute, 10)
	s.Require().NoError(err)
	_, err = s.store.CampaignContacts.Claim(s.ctx, "a", ShardClaim{}, s.base.Add(5*time.Minute), time.Minute, 10)
	s.Require().NoError(err)
	s.leasesPass("campaign_contacts", 2*time.Minute)

	taken, err := s.store.CampaignContacts.TakeOverExpired(s.ctx, "b", ShardClaim{}, time.Minute, 10)

	s.Require().NoError(err)
	s.Require().Len(taken, 1)
	s.Equal("+15550002", taken[0].Contact.ToNumber)
	s.Equal("b", taken[0].Contact.LeasedBy)
	s.Require().NotNil(taken[0].Contact.LeaseUntil)
	s.InDelta(time.Minute, s.leftOnTheDatabasesClock(*taken[0].Contact.LeaseUntil), float64(time.Second), "a minute on Postgres's clock")
	s.Equal(campaign.ID, taken[0].Campaign.ID)
	s.Equal("a", s.contactByNumber(campaign, "+15550001").LeasedBy, "a live lease stays")
	again, err := s.store.CampaignContacts.TakeOverExpired(s.ctx, "c", ShardClaim{}, time.Minute, 10)
	s.Require().NoError(err)
	s.Empty(again, "b's new lease is live")
}

func (s *StoreSuite) TestOnlyTheLeaseHolderCanSettle() {
	campaign := s.runningCampaign(5, "+15550001")
	contact := s.claimDue("a", s.base, 10)[0].Contact
	next := s.base.Add(time.Hour)

	err := s.store.CampaignContacts.Settle(s.ctx, "b", contact.ID, Settle{Stage: Enrolled, Attempt: 1, NextAttemptAt: &next})
	s.ErrorIs(err, ErrNotLeaseHolder)
	s.Equal(InProgress, s.contactByNumber(campaign, "+15550001").Stage)

	s.Require().NoError(s.store.CampaignContacts.Settle(s.ctx, "a", contact.ID, Settle{Stage: Enrolled, Attempt: 1, NextAttemptAt: &next, CallID: "call-1"}))
	settled := s.contactByNumber(campaign, "+15550001")
	s.Equal(Enrolled, settled.Stage)
	s.Equal(1, settled.Attempt)
	s.Equal(next, settled.NextAttemptAt.UTC())
	s.Equal("call-1", settled.CallID)
	s.Empty(settled.LeasedBy)
	s.Nil(settled.LeaseUntil)

	err = s.store.CampaignContacts.Settle(s.ctx, "a", contact.ID, Settle{Stage: ContactCompleted})
	s.ErrorIs(err, ErrNotLeaseHolder, "a released lease settles nothing")
	s.NoError(s.store.CampaignContacts.Settle(s.ctx, "a", "deleted-contact", Settle{Stage: ContactCompleted}), "a deleted contact is nothing to settle")
}

func (s *StoreSuite) TestSettlingAContactOfAStoppedCampaignEndsItStopped() {
	campaign := s.runningCampaign(5, "+15550001", "+15550002")
	claimed := s.claimDue("a", s.base, 10)
	s.Require().Len(claimed, 2)
	s.Require().NoError(s.store.Campaigns.Stop(s.ctx, campaign.CustomerID, campaign.ID, s.base))
	next := s.base.Add(time.Hour)

	for _, one := range claimed {
		settle := Settle{Stage: Enrolled, Attempt: 1, NextAttemptAt: &next}
		if one.Contact.ToNumber == "+15550002" {
			settle = Settle{Stage: ContactCompleted, Attempt: 1, Outcome: "success", Converted: true}
		}
		s.Require().NoError(s.store.CampaignContacts.Settle(s.ctx, "a", one.Contact.ID, settle))
	}

	waiting := s.contactByNumber(campaign, "+15550001")
	s.Equal(ContactStopped, waiting.Stage)
	s.Nil(waiting.NextAttemptAt)
	done := s.contactByNumber(campaign, "+15550002")
	s.Equal(ContactCompleted, done.Stage)
	s.Equal("success", done.Outcome)
	s.True(done.Converted)
}

func (s *StoreSuite) TestSettlingAContactStoppedMidCallEndsItStopped() {
	campaign := s.runningCampaign(5, "+15550001")
	contact := s.claimDue("a", s.base, 10)[0].Contact
	s.Require().NoError(s.store.CampaignContacts.Stop(s.ctx, campaign.ID, contact.ID))
	next := s.base.Add(time.Hour)

	s.Require().NoError(s.store.CampaignContacts.Settle(s.ctx, "a", contact.ID, Settle{Stage: Enrolled, Attempt: 1, NextAttemptAt: &next}))

	settled := s.contactByNumber(campaign, "+15550001")
	s.Equal(ContactStopped, settled.Stage)
	s.Equal("stopped", settled.Reason)
	s.Nil(settled.NextAttemptAt)
	s.Equal(Running, s.campaignNow(campaign).State, "stopping one contact leaves the campaign running")
}

func (s *StoreSuite) TestAnAnsweredCallOfAStoppedCampaignStillAwaitsItsOutcome() {
	campaign := s.runningCampaign(5, "+15550001")
	contact := s.claimDue("a", s.base, 10)[0].Contact
	s.Require().NoError(s.store.Campaigns.Stop(s.ctx, campaign.CustomerID, campaign.ID, s.base))

	s.Require().NoError(s.store.CampaignContacts.Settle(s.ctx, "a", contact.ID, Settle{Stage: AwaitingOutcome, Attempt: 1}))

	s.Equal(AwaitingOutcome, s.contactByNumber(campaign, "+15550001").Stage)
}

func (s *StoreSuite) TestSettlingAContactOfAPausedCampaignKeepsItEnrolled() {
	campaign := s.runningCampaign(5, "+15550001")
	contact := s.claimDue("a", s.base, 10)[0].Contact
	s.Require().NoError(s.store.Campaigns.Pause(s.ctx, campaign.CustomerID, campaign.ID, ""))
	next := s.base.Add(time.Hour)

	s.Require().NoError(s.store.CampaignContacts.Settle(s.ctx, "a", contact.ID, Settle{Stage: Enrolled, Attempt: 1, NextAttemptAt: &next}))

	settled := s.contactByNumber(campaign, "+15550001")
	s.Equal(Enrolled, settled.Stage)
	s.Equal(next, settled.NextAttemptAt.UTC())
}

func (s *StoreSuite) TestAnAnsweredDialWaitsToBeClassifiedAndIsClaimedOnce() {
	campaign := s.runningCampaign(5, "+15550001")
	claimed := s.claimDue("a", s.base, 10)[0]
	attempt := CampaignAttempt{CampaignID: campaign.ID, ContactID: claimed.Contact.ID, StepID: "step_1", Attempt: 1, Dial: 1, StartedAt: s.base}
	s.Require().NoError(s.store.CampaignAttempts.Insert(s.ctx, &attempt))
	s.NotEmpty(attempt.ID)
	classifyAfter := s.base.Add(2 * time.Minute)
	s.Require().NoError(s.store.CampaignAttempts.End(s.ctx, campaign.CustomerID, attempt.ID, "", "completed", "call-1", "vendor-1", s.base.Add(time.Minute), &classifyAfter))
	s.Require().NoError(s.store.CampaignContacts.Settle(s.ctx, "a", claimed.Contact.ID, Settle{Stage: AwaitingOutcome, Attempt: 1, CallID: "call-1"}))

	latest, found, err := s.store.CampaignAttempts.Latest(s.ctx, claimed.Contact.ID)
	s.Require().NoError(err)
	s.Require().True(found)
	s.Empty(latest.Result)
	s.Equal("completed", latest.CallOutcome, "the phone's outcome is kept as is")
	s.Equal("call-1", latest.CallID)
	s.Equal("vendor-1", latest.VendorCallID)
	s.Equal("pending", latest.ClassifyState)
	early, err := s.store.CampaignAttempts.ClaimClassifications(s.ctx, "a", s.base.Add(time.Minute), time.Minute, 10)
	s.Require().NoError(err)
	s.Empty(early, "the transcript is not written yet")

	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make([][]Classifying, 2)
	for i, owner := range []string{"a", "b"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			found, err := s.store.CampaignAttempts.ClaimClassifications(s.ctx, owner, classifyAfter, time.Minute, 10)
			s.NoError(err)
			results[i] = found
		}()
	}
	close(start)
	wg.Wait()

	s.Equal(1, len(results[0])+len(results[1]), "one router classifies it")
	winner, loser := "a", "b"
	won := results[0]
	if len(won) == 0 {
		winner, loser, won = "b", "a", results[1]
	}
	s.Require().Len(won, 1)
	s.Equal(attempt.ID, won[0].Attempt.ID)
	s.Equal(1, won[0].Attempt.ClassifyTries)
	s.Equal(classifyAfter.Add(time.Minute), won[0].Attempt.ClassifyAfter.UTC())
	s.Equal(winner, won[0].Contact.LeasedBy)
	s.Require().NotNil(won[0].Contact.LeaseUntil)
	s.InDelta(time.Minute, s.leftOnTheDatabasesClock(*won[0].Contact.LeaseUntil), float64(time.Second), "the contact's lease, a minute on Postgres's clock")
	s.Equal(AwaitingOutcome, won[0].Contact.Stage)
	s.Equal(campaign.ID, won[0].Campaign.ID)

	s.ErrorIs(s.store.CampaignContacts.Settle(s.ctx, loser, claimed.Contact.ID, Settle{Stage: ContactCompleted}), ErrNotLeaseHolder)
	s.Require().NoError(s.store.CampaignContacts.ClassifyAndSettle(s.ctx, winner, Classification{AttemptID: attempt.ID, Outcome: "success"},
		claimed.Contact.ID, Settle{Stage: ContactCompleted, Attempt: 1, Outcome: "success", Converted: true}))
	latest, _, err = s.store.CampaignAttempts.Latest(s.ctx, claimed.Contact.ID)
	s.Require().NoError(err)
	s.Equal("done", latest.ClassifyState)
	s.Equal("success", latest.Outcome)
	s.Equal(ContactCompleted, s.contactByNumber(campaign, "+15550001").Stage)
	later, err := s.store.CampaignAttempts.ClaimClassifications(s.ctx, "a", classifyAfter.Add(time.Hour), time.Minute, 10)
	s.Require().NoError(err)
	s.Empty(later, "a classified dial is not classified again")
}

func (s *StoreSuite) TestTheLatestDialIsTheSecondOfADoubleDial() {
	campaign := s.runningCampaign(5, "+15550001")
	contact := s.claimDue("a", s.base, 10)[0].Contact
	_, found, err := s.store.CampaignAttempts.Latest(s.ctx, contact.ID)
	s.Require().NoError(err)
	s.False(found)

	for dial := 1; dial <= 2; dial++ {
		attempt := CampaignAttempt{CampaignID: campaign.ID, ContactID: contact.ID, StepID: "step_1", Attempt: 1, Dial: dial, StartedAt: s.base}
		s.Require().NoError(s.store.CampaignAttempts.Insert(s.ctx, &attempt))
		s.Require().NoError(s.store.CampaignAttempts.End(s.ctx, campaign.CustomerID, attempt.ID, "", "no_answer", fmt.Sprintf("call-%d", dial), "", s.base, nil))
	}

	latest, found, err := s.store.CampaignAttempts.Latest(s.ctx, contact.ID)
	s.Require().NoError(err)
	s.Require().True(found)
	s.Equal(2, latest.Dial)
	s.Equal("call-2", latest.CallID)
	s.Empty(latest.ClassifyState, "an unanswered dial has nothing to classify")
}

func (s *StoreSuite) TestEndingADialOfADeletedCampaignIsNothing() {
	s.NoError(s.store.CampaignAttempts.End(s.ctx, "app", "deleted-attempt", "", "completed", "call-1", "", s.base, nil))
}

func (s *StoreSuite) TestACampaignCompletesWhenNobodyIsLeft() {
	campaign := s.runningCampaign(5, "+15550001", "+15550002")
	autoEnroll := Campaign{CustomerID: newID(), Name: "Inbound leads", ConfigID: "config-1", FromNumber: "+15550100", Type: AutoEnroll}
	s.Require().NoError(s.store.Campaigns.Insert(s.ctx, &autoEnroll))
	s.launch(autoEnroll, s.base)
	for _, one := range s.claimDue("a", s.base, 10) {
		s.Require().NoError(s.store.CampaignContacts.Settle(s.ctx, "a", one.Contact.ID, Settle{Stage: ContactCompleted, Attempt: 1}))
	}

	s.Require().NoError(s.store.Campaigns.Complete(s.ctx, s.base.Add(time.Hour)))

	completed := s.campaignNow(campaign)
	s.Equal(Completed, completed.State)
	s.Require().NotNil(completed.FinishedAt)
	s.Equal(s.base.Add(time.Hour), completed.FinishedAt.UTC())
	s.Equal(Running, s.campaignNow(autoEnroll).State, "an auto-enroll campaign waits for more people")
}

func (s *StoreSuite) TestACampaignWithACallAwaitingAnOutcomeIsNotComplete() {
	campaign := s.runningCampaign(5, "+15550001", "+15550002")
	waiting := s.runningCampaign(5, "+15550003")
	s.dueAt(s.contactByNumber(waiting, "+15550003"), s.base.Add(time.Hour))
	inCall := s.runningCampaign(5, "+15550004")
	for _, one := range s.claimDue("a", s.base, 10) {
		stage := ContactCompleted
		if one.Contact.ToNumber == "+15550002" {
			stage = AwaitingOutcome
		}
		if one.Campaign.ID == inCall.ID {
			continue
		}
		s.Require().NoError(s.store.CampaignContacts.Settle(s.ctx, "a", one.Contact.ID, Settle{Stage: stage, Attempt: 1}))
	}

	s.Require().NoError(s.store.Campaigns.Complete(s.ctx, s.base))

	s.Equal(Running, s.campaignNow(campaign).State)
	s.Equal(Running, s.campaignNow(waiting).State, "somebody is still to be rung")
	s.Equal(Running, s.campaignNow(inCall).State, "a call is in progress")
}

func (s *StoreSuite) TestOneClaimServesEveryCampaignWithContactsDue() {
	numbers := []string{"+15550001", "+15550002", "+15550003", "+15550004", "+15550005", "+15550006"}
	first := s.runningCampaign(5, numbers...)
	second := s.runningCampaign(5, numbers...)

	perCampaign := map[string]int{}
	for _, one := range s.claimDue("a", s.base, 4) {
		perCampaign[one.Campaign.ID]++
	}

	s.Equal(2, perCampaign[first.ID])
	s.Equal(2, perCampaign[second.ID])
}

func (s *StoreSuite) TestADialMustSayWhenItStarted() {
	campaign := s.runningCampaign(5, "+15550001")
	contact := s.claimDue("a", s.base, 10)[0].Contact

	err := s.store.CampaignAttempts.Insert(s.ctx, &CampaignAttempt{CampaignID: campaign.ID, ContactID: contact.ID, StepID: "step_1", Attempt: 1, Dial: 1})

	s.Error(err)
}

// answered dials a contact, ends the dial as answered with its outcome to read at classifyAfter,
// and settles the contact to wait for it.
func (s *StoreSuite) answered(owner string, campaign Campaign, contact Contact, startedAt, classifyAfter time.Time) CampaignAttempt {
	attempt := CampaignAttempt{CampaignID: campaign.ID, ContactID: contact.ID, StepID: "step_1", Attempt: 1, Dial: 1, StartedAt: startedAt}
	s.Require().NoError(s.store.CampaignAttempts.Insert(s.ctx, &attempt))
	s.Require().NoError(s.store.CampaignAttempts.End(s.ctx, campaign.CustomerID, attempt.ID, "", "completed", "call-"+attempt.ID, "", startedAt, &classifyAfter))
	return attempt
}

func (s *StoreSuite) TestAContactStoppedMidCallStaysStoppedOnceItsOutcomeIsKnown() {
	campaign := s.runningCampaign(5, "+15550001")
	contact := s.claimDue("a", s.base, 10)[0].Contact
	s.answered("a", campaign, contact, s.base, s.base.Add(time.Minute))
	s.Require().NoError(s.store.CampaignContacts.Stop(s.ctx, campaign.ID, contact.ID))
	s.Require().NoError(s.store.CampaignContacts.Settle(s.ctx, "a", contact.ID, Settle{Stage: AwaitingOutcome, Attempt: 1}))
	s.Equal("stopped", s.contactByNumber(campaign, "+15550001").Reason, "waiting for the outcome keeps the stop")
	claimed, err := s.store.CampaignAttempts.ClaimClassifications(s.ctx, "b", s.base.Add(time.Minute), time.Minute, 10)
	s.Require().NoError(err)
	s.Require().Len(claimed, 1)
	next := s.base.Add(time.Hour)

	s.Require().NoError(s.store.CampaignContacts.Settle(s.ctx, "b", contact.ID, Settle{Stage: Enrolled, StepID: "step_2", Attempt: 0, NextAttemptAt: &next, Outcome: "callback"}))

	settled := s.contactByNumber(campaign, "+15550001")
	s.Equal(ContactStopped, settled.Stage)
	s.Equal("stopped", settled.Reason)
	s.Nil(settled.NextAttemptAt)
}

func (s *StoreSuite) TestOnlyTheLatestAnsweredDialOfAContactIsClassified() {
	campaign := s.runningCampaign(5, "+15550001")
	contact := s.claimDue("a", s.base, 10)[0].Contact
	classifyAfter := s.base.Add(5 * time.Minute)
	older := s.answered("a", campaign, contact, s.base, classifyAfter)
	latest := s.answered("a", campaign, contact, s.base.Add(time.Minute), classifyAfter)
	s.Require().NoError(s.store.CampaignContacts.Settle(s.ctx, "a", contact.ID, Settle{Stage: AwaitingOutcome, Attempt: 1}))

	claimed, err := s.store.CampaignAttempts.ClaimClassifications(s.ctx, "a", classifyAfter, time.Minute, 10)

	s.Require().NoError(err)
	s.Require().Len(claimed, 1)
	s.Equal(latest.ID, claimed[0].Attempt.ID)
	s.Require().NoError(s.store.CampaignContacts.ClassifyAndSettle(s.ctx, "a", Classification{AttemptID: latest.ID, Outcome: "success"},
		contact.ID, Settle{Stage: ContactCompleted, Attempt: 1, Outcome: "success"}))
	_, attempts, err := s.store.CampaignContacts.Get(s.ctx, campaign.ID, contact.ID)
	s.Require().NoError(err)
	for _, attempt := range attempts {
		if attempt.ID == older.ID {
			s.NotEqual("pending", attempt.ClassifyState, "settling the contact leaves no dial of it to classify")
		}
	}
	later, err := s.store.CampaignAttempts.ClaimClassifications(s.ctx, "b", classifyAfter.Add(time.Hour), time.Minute, 10)
	s.Require().NoError(err)
	s.Empty(later)
}

func (s *StoreSuite) TestOnlyADialOfTheContactsCampaignIsClassified() {
	campaign := s.runningCampaign(5, "+15550001", "+15550002")
	due := s.claimDue("a", s.base, 10)
	s.Require().Len(due, 2)
	answered, stray := due[0].Contact, due[1].Contact
	dial := s.answered("a", campaign, answered, s.base, s.base)
	for _, contact := range []Contact{answered, stray} {
		s.strayDial(contact, s.base.Add(time.Minute))
		s.Require().NoError(s.store.CampaignContacts.Settle(s.ctx, "a", contact.ID, Settle{Stage: AwaitingOutcome, Attempt: 1}))
	}

	claimed, err := s.store.CampaignAttempts.ClaimClassifications(s.ctx, "a", s.base.Add(time.Hour), time.Minute, 10)

	s.Require().NoError(err)
	s.Require().Len(claimed, 1)
	s.Equal(dial.ID, claimed[0].Attempt.ID)
}

func (s *StoreSuite) TestTheLatestDialIsOfTheContactsCampaign() {
	contact := s.contactsOf(s.draftCampaign(newID(), "+15550001"))[0]
	s.strayDial(contact, s.base)

	_, found, err := s.store.CampaignAttempts.Latest(s.ctx, contact.ID)

	s.Require().NoError(err)
	s.False(found)
}

func (s *StoreSuite) TestDialsMadeAreOfTheContactsCampaign() {
	contact := s.contactsOf(s.draftCampaign(newID(), "+15550001"))[0]
	s.strayDial(contact, s.base)

	made, err := s.store.CampaignAttempts.Made(s.ctx, []string{contact.ID})

	s.Require().NoError(err)
	s.Empty(made)
}

func (s *StoreSuite) TestDialsCountedByResultAreOfTheContactsCampaign() {
	contact := s.contactsOf(s.draftCampaign(newID(), "+15550001"))[0]
	s.strayDial(contact, s.base)

	count, err := s.store.CampaignAttempts.CountResult(s.ctx, contact.ID, 1, "")

	s.Require().NoError(err)
	s.Zero(count)
}

func (s *StoreSuite) TestSettlingAContactLeavesAnotherCampaignsDial() {
	s.runningCampaign(5, "+15550001")
	contact := s.claimDue("a", s.base, 10)[0].Contact
	s.strayDial(contact, s.base)

	s.Require().NoError(s.store.CampaignContacts.Settle(s.ctx, "a", contact.ID, Settle{Stage: ContactCompleted, Attempt: 1}))

	var states []string
	s.Require().NoError(s.store.GlobalDB().NewSelect().Table("campaign_attempts").Column("classify_state").Scan(s.ctx, &states))
	s.Equal([]string{"pending"}, states)
}

func (s *StoreSuite) TestADialWhoseContactAnotherRunnerHoldsIsNotClassified() {
	campaign := s.runningCampaign(5, "+15550001")
	contact := s.claimDue("a", s.base, 10)[0].Contact
	attempt := s.answered("a", campaign, contact, s.base, s.base.Add(time.Minute))
	s.Require().NoError(s.store.CampaignContacts.Settle(s.ctx, "a", contact.ID, Settle{Stage: AwaitingOutcome, Attempt: 1}))
	held, err := s.store.CampaignAttempts.ClaimClassifications(s.ctx, "a", s.base.Add(time.Minute), 10*time.Minute, 10)
	s.Require().NoError(err)
	s.Require().Len(held, 1)
	_, err = s.store.GlobalDB().NewUpdate().Model((*CampaignAttempt)(nil)).Set("classify_after = ?", s.base.Add(time.Minute)).Where("id = ?", attempt.ID).Exec(s.ctx)
	s.Require().NoError(err)

	s.leasesPass("campaign_contacts", 9*time.Minute)
	taken, err := s.store.CampaignAttempts.ClaimClassifications(s.ctx, "b", s.base.Add(2*time.Minute), time.Minute, 10)
	s.Require().NoError(err)
	s.Empty(taken, "a still holds the contact")

	s.leasesPass("campaign_contacts", 2*time.Minute)
	taken, err = s.store.CampaignAttempts.ClaimClassifications(s.ctx, "b", s.base.Add(12*time.Minute), time.Minute, 10)
	s.Require().NoError(err)
	s.Len(taken, 1, "a's lease ran out")
}

func (s *StoreSuite) TestAContactStoppedMidCallThatFailsKeepsWhyItFailed() {
	campaign := s.runningCampaign(5, "+15550001")
	contact := s.claimDue("a", s.base, 10)[0].Contact
	s.answered("a", campaign, contact, s.base, s.base.Add(time.Minute))
	s.Require().NoError(s.store.CampaignContacts.Stop(s.ctx, campaign.ID, contact.ID))
	s.Require().NoError(s.store.CampaignContacts.Settle(s.ctx, "a", contact.ID, Settle{Stage: AwaitingOutcome, Attempt: 1}))
	_, err := s.store.CampaignAttempts.ClaimClassifications(s.ctx, "a", s.base.Add(time.Minute), time.Minute, 10)
	s.Require().NoError(err)

	s.Require().NoError(s.store.CampaignContacts.Settle(s.ctx, "a", contact.ID, Settle{Stage: Failed, Attempt: 1, Reason: "opted_out", Outcome: "do_not_call"}))

	settled := s.contactByNumber(campaign, "+15550001")
	s.Equal(Failed, settled.Stage)
	s.Equal("opted_out", settled.Reason)
}

// The contact settles each time, so only the dial's own state keeps a late answer off it.
func (s *StoreSuite) TestALateClassificationChangesNothing() {
	campaign := s.runningCampaign(5, "+15550001")
	contact := s.claimDue("a", s.base, 10)[0].Contact
	older := s.answered("a", campaign, contact, s.base, s.base.Add(time.Minute))
	latest := s.answered("a", campaign, contact, s.base.Add(time.Minute), s.base.Add(time.Minute))
	s.Require().NoError(s.store.CampaignContacts.Settle(s.ctx, "a", contact.ID, Settle{Stage: AwaitingOutcome, Attempt: 1}))
	_, err := s.store.CampaignAttempts.ClaimClassifications(s.ctx, "a", s.base.Add(time.Minute), time.Minute, 10)
	s.Require().NoError(err)
	again := s.base.Add(time.Hour)
	s.Require().NoError(s.store.CampaignContacts.ClassifyAndSettle(s.ctx, "a", Classification{AttemptID: latest.ID, Outcome: "no_response"},
		contact.ID, Settle{Stage: Enrolled, Attempt: 1, NextAttemptAt: &again}))

	for _, late := range []CampaignAttempt{latest, older} {
		s.Require().Len(s.claimDue("a", again, 10), 1)
		again = again.Add(time.Hour)
		s.Require().NoError(s.store.CampaignContacts.ClassifyAndSettle(s.ctx, "a", Classification{AttemptID: late.ID, Outcome: "other"},
			contact.ID, Settle{Stage: Enrolled, Attempt: 1, NextAttemptAt: &again}))
	}

	_, attempts, err := s.store.CampaignContacts.Get(s.ctx, campaign.ID, contact.ID)
	s.Require().NoError(err)
	byID := map[string]CampaignAttempt{}
	for _, attempt := range attempts {
		byID[attempt.ID] = attempt
	}
	s.Equal("no_response", byID[latest.ID].Outcome, "a done dial")
	s.Equal("abandoned", byID[older.ID].ClassifyState, "an abandoned dial")
	s.Empty(byID[older.ID].Outcome)
}

func (s *StoreSuite) TestAClassificationPutOffIsReadAgainAtItsTimeByAnyRunner() {
	campaign := s.runningCampaign(1, "+15550001")
	claimed := s.claimDue("a", s.base, 10)[0]
	attempt := s.answered("a", campaign, claimed.Contact, s.base, s.base)
	s.Require().NoError(s.store.CampaignContacts.Settle(s.ctx, "a", claimed.Contact.ID, Settle{Stage: AwaitingOutcome, Attempt: 1}))
	read, err := s.store.CampaignAttempts.ClaimClassifications(s.ctx, "a", s.base, 20*time.Minute, 10)
	s.Require().NoError(err)
	s.Require().Len(read, 1)

	s.Require().NoError(s.store.CampaignAttempts.PutOff(s.ctx, "a", attempt.ID, s.base.Add(30*time.Second)))

	early, err := s.store.CampaignAttempts.ClaimClassifications(s.ctx, "b", s.base.Add(29*time.Second), 20*time.Minute, 10)
	s.Require().NoError(err)
	s.Empty(early)
	again, err := s.store.CampaignAttempts.ClaimClassifications(s.ctx, "b", s.base.Add(30*time.Second), 20*time.Minute, 10)
	s.Require().NoError(err)
	s.Require().Len(again, 1, "not held off for the rest of the first read's lease")
	s.Equal(2, again[0].Attempt.ClassifyTries)
	s.Equal("b", again[0].Contact.LeasedBy)
	s.Equal(AwaitingOutcome, again[0].Contact.Stage)
}

func (s *StoreSuite) TestOnlyTheRunnerReadingAClassificationPutsItOff() {
	campaign := s.runningCampaign(1, "+15550001")
	claimed := s.claimDue("a", s.base, 10)[0]
	attempt := s.answered("a", campaign, claimed.Contact, s.base, s.base)
	s.Require().NoError(s.store.CampaignContacts.Settle(s.ctx, "a", claimed.Contact.ID, Settle{Stage: AwaitingOutcome, Attempt: 1}))
	_, err := s.store.CampaignAttempts.ClaimClassifications(s.ctx, "a", s.base, 20*time.Minute, 10)
	s.Require().NoError(err)

	s.ErrorIs(s.store.CampaignAttempts.PutOff(s.ctx, "b", attempt.ID, s.base.Add(30*time.Second)), ErrNotLeaseHolder)

	early, err := s.store.CampaignAttempts.ClaimClassifications(s.ctx, "b", s.base.Add(time.Minute), 20*time.Minute, 10)
	s.Require().NoError(err)
	s.Empty(early, "a still reads it")
}

func (s *StoreSuite) TestAClassificationWhoseContactCannotSettleStaysPending() {
	campaign := s.runningCampaign(1, "+15550001")
	claimed := s.claimDue("a", s.base, 10)[0]
	attempt := s.answered("a", campaign, claimed.Contact, s.base, s.base)
	s.Require().NoError(s.store.CampaignContacts.Settle(s.ctx, "a", claimed.Contact.ID, Settle{Stage: AwaitingOutcome, Attempt: 1}))
	_, err := s.store.CampaignAttempts.ClaimClassifications(s.ctx, "b", s.base, time.Minute, 10)
	s.Require().NoError(err)

	err = s.store.CampaignContacts.ClassifyAndSettle(s.ctx, "a", Classification{AttemptID: attempt.ID, Outcome: "success"},
		claimed.Contact.ID, Settle{Stage: ContactCompleted, Attempt: 1, Outcome: "success", Converted: true})

	s.ErrorIs(err, ErrNotLeaseHolder)
	latest, _, err := s.store.CampaignAttempts.Latest(s.ctx, claimed.Contact.ID)
	s.Require().NoError(err)
	s.Equal("pending", latest.ClassifyState, "read again rather than left done with the contact still waiting")
	s.Empty(latest.Outcome)
	s.Equal(AwaitingOutcome, s.contactByNumber(campaign, "+15550001").Stage)
}

func (s *StoreSuite) TestAClassificationAndItsContactSettleTogether() {
	campaign := s.runningCampaign(1, "+15550001")
	claimed := s.claimDue("a", s.base, 10)[0]
	attempt := s.answered("a", campaign, claimed.Contact, s.base, s.base)
	s.Require().NoError(s.store.CampaignContacts.Settle(s.ctx, "a", claimed.Contact.ID, Settle{Stage: AwaitingOutcome, Attempt: 1}))
	_, err := s.store.CampaignAttempts.ClaimClassifications(s.ctx, "a", s.base, time.Minute, 10)
	s.Require().NoError(err)

	s.Require().NoError(s.store.CampaignContacts.ClassifyAndSettle(s.ctx, "a", Classification{AttemptID: attempt.ID, Outcome: "success"},
		claimed.Contact.ID, Settle{Stage: ContactCompleted, Attempt: 1, Outcome: "success", Converted: true}))

	latest, _, err := s.store.CampaignAttempts.Latest(s.ctx, claimed.Contact.ID)
	s.Require().NoError(err)
	s.Equal("done", latest.ClassifyState)
	s.Equal("success", latest.Outcome)
	s.Equal(ContactCompleted, s.contactByNumber(campaign, "+15550001").Stage)
}

func (s *StoreSuite) TestASilentCallThatCannotSettleStaysAnswered() {
	campaign := s.runningCampaign(1, "+15550001")
	claimed := s.claimDue("a", s.base, 10)[0]
	attempt := s.answered("a", campaign, claimed.Contact, s.base, s.base)
	s.Require().NoError(s.store.CampaignContacts.Settle(s.ctx, "a", claimed.Contact.ID, Settle{Stage: AwaitingOutcome, Attempt: 1}))
	_, err := s.store.CampaignAttempts.ClaimClassifications(s.ctx, "b", s.base, time.Minute, 10)
	s.Require().NoError(err)
	next := s.base.Add(time.Hour)

	err = s.store.CampaignContacts.ClassifyAndSettle(s.ctx, "a", Classification{AttemptID: attempt.ID, Outcome: "no_response"},
		claimed.Contact.ID, Settle{Stage: Enrolled, Attempt: 2, NextAttemptAt: &next})

	s.ErrorIs(err, ErrNotLeaseHolder)
	latest, _, err := s.store.CampaignAttempts.Latest(s.ctx, claimed.Contact.ID)
	s.Require().NoError(err)
	s.Empty(latest.Outcome)
	s.Equal("pending", latest.ClassifyState)
}

func (s *StoreSuite) TestASilentCallSettlesAsUnanswered() {
	campaign := s.runningCampaign(1, "+15550001")
	claimed := s.claimDue("a", s.base, 10)[0]
	attempt := s.answered("a", campaign, claimed.Contact, s.base, s.base)
	s.Require().NoError(s.store.CampaignContacts.Settle(s.ctx, "a", claimed.Contact.ID, Settle{Stage: AwaitingOutcome, Attempt: 1}))
	_, err := s.store.CampaignAttempts.ClaimClassifications(s.ctx, "a", s.base, time.Minute, 10)
	s.Require().NoError(err)
	next := s.base.Add(time.Hour)

	s.Require().NoError(s.store.CampaignContacts.ClassifyAndSettle(s.ctx, "a", Classification{AttemptID: attempt.ID, Outcome: "no_response"},
		claimed.Contact.ID, Settle{Stage: Enrolled, Attempt: 2, NextAttemptAt: &next}))

	latest, _, err := s.store.CampaignAttempts.Latest(s.ctx, claimed.Contact.ID)
	s.Require().NoError(err)
	s.Equal("completed", latest.CallOutcome, "the phone joined, whatever was said")
	s.Equal("no_response", latest.Outcome)
	s.Equal("done", latest.ClassifyState)
	s.Equal(Enrolled, s.contactByNumber(campaign, "+15550001").Stage)
}

func (s *StoreSuite) TestCampaignContactQueryBudgets() {
	draft := s.draftCampaign(newID())
	contacts := make([]Contact, 2*insertChunk+1)
	for i := range contacts {
		contacts[i] = Contact{ToNumber: fmt.Sprintf("+1555%07d", i)}
	}
	s.Equal(5+2, s.countQueries(func() {
		_, _, err := s.store.CampaignContacts.InsertMany(s.ctx, draft.ID, contacts)
		s.Require().NoError(err)
	}), "InsertMany locks the draft, inserts a chunk at a time and counts")
	s.Equal(2+2, s.countQueries(func() { s.Require().NoError(s.store.CampaignContacts.DeleteAll(s.ctx, draft.ID)) }),
		"DeleteAll locks the draft and deletes")

	running := make([]Campaign, 3)
	for i := range running {
		running[i] = s.runningCampaign(2, "+15550001", "+15550002", "+15550003")
	}
	var claimed []Claimed
	s.Equal(2+1+len(running), s.countQueries(func() { claimed = s.claimDue("a", s.base, 10) }),
		"Claim locks the due campaigns, then claims each one's share in one statement")
	s.Len(claimed, 6, "two of each campaign's three, its concurrency")
	s.leasesPass("campaign_contacts", time.Hour)
	s.Equal(2, s.countQueries(func() {
		taken, err := s.store.CampaignContacts.TakeOverExpired(s.ctx, "b", ShardClaim{}, time.Minute, 10)
		s.Require().NoError(err)
		s.Len(taken, 6)
	}), "TakeOverExpired takes every lapsed contact, then reads their campaigns, in one query each")

	contact := claimed[0].Contact
	s.Equal(1, s.countQueries(func() {
		listed, err := s.store.CampaignContacts.Select(s.ctx, running[0].ID, ContactFilter{Limit: 10})
		s.Require().NoError(err)
		s.Len(listed, 3)
	}), "Select")
	s.Equal(2, s.countQueries(func() {
		_, _, err := s.store.CampaignContacts.Get(s.ctx, contact.CampaignID, contact.ID)
		s.Require().NoError(err)
	}), "Get reads the contact, then its attempts")
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.CampaignContacts.Stop(s.ctx, contact.CampaignID, contact.ID)) }), "Stop")
	next := s.base.Add(time.Hour)
	s.Equal(2+2, s.countQueries(func() {
		s.Require().NoError(s.store.CampaignContacts.Settle(s.ctx, "b", contact.ID, Settle{Stage: Enrolled, Attempt: 1, NextAttemptAt: &next}))
	}), "Settle settles the contact and abandons its pending classifications")
	other := claimed[1].Contact
	attempt := s.answered("b", running[0], other, s.base, s.base.Add(time.Minute))
	s.Equal(3+2, s.countQueries(func() {
		s.Require().NoError(s.store.CampaignContacts.ClassifyAndSettle(s.ctx, "b",
			Classification{AttemptID: attempt.ID, Outcome: "callback"}, other.ID, Settle{Stage: Completed, Attempt: 1}))
	}), "ClassifyAndSettle classifies the dial, then settles as Settle does")
	ids := make([]string, len(running))
	for i, campaign := range running {
		ids[i] = campaign.ID
	}
	s.Equal(1, s.countQueries(func() {
		stages, err := s.store.CampaignContacts.Stages(s.ctx, ids)
		s.Require().NoError(err)
		s.Len(stages, 3)
	}), "Stages")
}

// An owner is one process, and a call that outlives its lease can find the contact taken over
// by that same process. The settle of the first claim must not land on the second.
func (s *StoreSuite) TestASettleUnderALeaseTakenOverSinceSettlesNothing() {
	campaign := s.runningCampaign(5, "+15550001")
	first := s.claimDue("a", s.base, 10)[0].Contact
	s.leasesPass("campaign_contacts", 2*time.Minute)
	taken, err := s.store.CampaignContacts.TakeOverExpired(s.ctx, "a", ShardClaim{}, time.Minute, 10)
	s.Require().NoError(err)
	s.Require().Len(taken, 1)
	second := taken[0].Contact
	next := s.base.Add(time.Hour)

	err = s.store.CampaignContacts.Settle(s.ctx, "a", first.ID, Settle{Stage: Enrolled, Attempt: 1, NextAttemptAt: &next, LeaseUntil: first.LeaseUntil})
	s.ErrorIs(err, ErrNotLeaseHolder)
	s.Equal(InProgress, s.contactByNumber(campaign, "+15550001").Stage)

	s.Require().NoError(s.store.CampaignContacts.Settle(s.ctx, "a", second.ID, Settle{Stage: ContactCompleted, Attempt: 1, LeaseUntil: second.LeaseUntil}))
	s.Equal(ContactCompleted, s.contactByNumber(campaign, "+15550001").Stage)
}

// autoEnrollCampaign is a draft auto-enroll campaign for a fresh customer, launched to start at
// startAt.
func (s *StoreSuite) autoEnrollCampaign(startAt time.Time) Campaign {
	campaign := Campaign{CustomerID: newID(), Name: "Inbound leads", ConfigID: "config-1", FromNumber: "+15550100", Type: AutoEnroll}
	s.Require().NoError(s.store.Campaigns.Insert(s.ctx, &campaign))
	return s.launch(campaign, startAt)
}

func (s *StoreSuite) addOne(campaign Campaign, number string) (Contact, error) {
	added, _, err := s.store.CampaignContacts.InsertMany(s.ctx, campaign.ID, []Contact{{CampaignID: campaign.ID, ToNumber: number}})
	if err != nil {
		return Contact{}, err
	}
	s.Require().Len(added, 1)
	return s.contactByNumber(campaign, number), nil
}

// replaceWith is a change that makes the contact what changes holds.
func replaceWith(changes Contact) func(Contact) (Contact, error) {
	return func(Contact) (Contact, error) { return changes, nil }
}

func (s *StoreSuite) claimedNumbers(now time.Time) []string {
	var numbers []string
	for _, claimed := range s.claimDue("a", now, 10) {
		numbers = append(numbers, claimed.Contact.ToNumber)
	}
	return numbers
}

func (s *StoreSuite) TestARunningAutoEnrollCampaignRingsAContactAddedToItFromTheFirstStep() {
	campaign := s.autoEnrollCampaign(s.base)
	before := time.Now().UTC()

	added, err := s.addOne(campaign, "+15550001")

	s.Require().NoError(err)
	s.Equal(Enrolled, added.Stage)
	s.Equal("step_1", added.StepID)
	s.Equal(0, added.Attempt)
	s.Require().NotNil(added.NextAttemptAt, "a contact with no next attempt is never rung")
	s.False(added.NextAttemptAt.Before(before.Truncate(time.Microsecond)), "due now, not at the start long past")
	s.Equal([]string{"+15550001"}, s.claimedNumbers(time.Now().Add(time.Minute)))
}

func (s *StoreSuite) TestAContactAddedToAPausedAutoEnrollCampaignWaitsForTheResume() {
	campaign := s.autoEnrollCampaign(s.base)
	s.Require().NoError(s.store.Campaigns.Pause(s.ctx, campaign.CustomerID, campaign.ID, ""))

	_, err := s.addOne(campaign, "+15550001")

	s.Require().NoError(err)
	s.Empty(s.claimedNumbers(time.Now().Add(time.Minute)))
	s.Require().NoError(s.store.Campaigns.Resume(s.ctx, campaign.CustomerID, campaign.ID))
	s.Equal([]string{"+15550001"}, s.claimedNumbers(time.Now().Add(time.Minute)))
}

func (s *StoreSuite) TestAContactAddedBeforeAScheduledStartIsDueAtTheStart() {
	start := time.Now().UTC().Add(24 * time.Hour).Truncate(time.Second)
	campaign := s.autoEnrollCampaign(start)
	s.Require().Equal(Scheduled, campaign.State)

	added, err := s.addOne(campaign, "+15550001")

	s.Require().NoError(err)
	s.Require().NotNil(added.NextAttemptAt)
	s.Equal(start, added.NextAttemptAt.UTC())
}

func (s *StoreSuite) TestAnOptedOutContactAddedAfterLaunchIsRejectedWithoutACall() {
	campaign := s.autoEnrollCampaign(s.base)
	s.Require().NoError(s.store.OptOuts.Insert(s.ctx, &OptOut{CustomerID: campaign.CustomerID, Recipient: "+15550001", Channel: "voice", Source: "test"}))

	added, err := s.addOne(campaign, "+15550001")

	s.Require().NoError(err)
	s.Equal(Rejected, added.Stage)
	s.Equal("opted_out", added.Reason)
	s.Nil(added.NextAttemptAt)
	s.Empty(s.claimedNumbers(time.Now().Add(time.Minute)))
}

func (s *StoreSuite) TestAStoppedAutoEnrollCampaignTakesNoMoreContacts() {
	campaign := s.autoEnrollCampaign(s.base)
	s.Require().NoError(s.store.Campaigns.Stop(s.ctx, campaign.CustomerID, campaign.ID, s.base))

	_, err := s.addOne(campaign, "+15550001")

	s.ErrorIs(err, ErrCampaignLocked)
	s.Empty(s.contactsOf(campaign))
}

func (s *StoreSuite) TestADraftsContactCanBeChanged() {
	campaign := s.draftCampaign(newID(), "+15550001")
	contact := s.contactByNumber(campaign, "+15550001")

	changed, err := s.store.CampaignContacts.Update(s.ctx, campaign.ID, contact.ID, replaceWith(Contact{
		ToNumber: "+15550009", Variables: map[string]string{"first_name": "Anna"}, TimeZone: "Europe/Amsterdam", Instructions: "Speak Dutch",
	}))

	s.Require().NoError(err)
	s.Equal(contact.ID, changed.ID)
	s.Equal(contact.Seq, changed.Seq, "a change keeps the contact's place in the queue")
	stored := s.contactsOf(campaign)
	s.Require().Len(stored, 1)
	s.Equal("+15550009", stored[0].ToNumber)
	s.Equal(map[string]string{"first_name": "Anna"}, stored[0].Variables)
	s.Equal("Europe/Amsterdam", stored[0].TimeZone)
	s.Equal("Speak Dutch", stored[0].Instructions)
}

func (s *StoreSuite) TestAnAddLeavesAContactMidSequenceAlone() {
	campaign := s.autoEnrollCampaign(s.base)
	first, err := s.addOne(campaign, "+15550001")
	s.Require().NoError(err)
	later := time.Now().UTC().Add(48 * time.Hour).Truncate(time.Second)
	_, err = s.store.GlobalDB().NewUpdate().Model((*Contact)(nil)).
		Set("attempt = 2").Set("step_id = 'step_2'").Set("next_attempt_at = ?", later).
		Where("id = ?", first.ID).Exec(s.ctx)
	s.Require().NoError(err)

	_, err = s.addOne(campaign, "+15550002")

	s.Require().NoError(err)
	kept := s.contactByNumber(campaign, "+15550001")
	s.Equal(2, kept.Attempt)
	s.Equal("step_2", kept.StepID)
	s.Require().NotNil(kept.NextAttemptAt)
	s.Equal(later, kept.NextAttemptAt.UTC())
}

func (s *StoreSuite) TestADraftsContactHasNoNextAttempt() {
	campaign := s.draftCampaign(newID(), "+15550001")
	s.Nil(s.contactByNumber(campaign, "+15550001").NextAttemptAt)
}

func (s *StoreSuite) TestOverlappingChangesToOneContactBothPersist() {
	campaign := s.draftCampaign(newID(), "+15550001")
	contact := s.contactByNumber(campaign, "+15550001")
	firstRead := make(chan struct{})
	release := make(chan struct{})
	var wait sync.WaitGroup
	wait.Add(2)
	go func() {
		defer wait.Done()
		_, err := s.store.CampaignContacts.Update(s.ctx, campaign.ID, contact.ID, func(found Contact) (Contact, error) {
			close(firstRead)
			<-release
			found.TimeZone = "Europe/Amsterdam"
			return found, nil
		})
		s.NoError(err)
	}()
	<-firstRead
	go func() {
		defer wait.Done()
		_, err := s.store.CampaignContacts.Update(s.ctx, campaign.ID, contact.ID, func(found Contact) (Contact, error) {
			found.Variables = map[string]string{"first_name": "Anna"}
			return found, nil
		})
		s.NoError(err)
	}()
	// The first change writes only once the second is queued on its lock, so they overlap.
	s.Require().Eventually(func() bool {
		var waiting int
		err := s.store.GlobalDB().QueryRowContext(s.ctx, `SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database() AND wait_event_type = 'Lock'`).Scan(&waiting)
		return err == nil && waiting > 0
	}, 5*time.Second, 10*time.Millisecond, "the second change never waited on the first's lock")
	close(release)
	wait.Wait()

	stored := s.contactsOf(campaign)
	s.Require().Len(stored, 1)
	s.Equal("Europe/Amsterdam", stored[0].TimeZone)
	s.Equal(map[string]string{"first_name": "Anna"}, stored[0].Variables)
}

func (s *StoreSuite) TestAContactCannotTakeANumberAnotherContactHolds() {
	campaign := s.draftCampaign(newID(), "+15550001", "+15550002")
	contact := s.contactByNumber(campaign, "+15550001")

	_, err := s.store.CampaignContacts.Update(s.ctx, campaign.ID, contact.ID, replaceWith(Contact{ToNumber: "+15550002"}))

	s.ErrorIs(err, ErrDuplicateContact)
	s.Equal("+15550001", s.contactByNumber(campaign, "+15550001").ToNumber)
}

func (s *StoreSuite) TestOneContactCanBeRemovedFromADraft() {
	campaign := s.draftCampaign(newID(), "+15550001", "+15550002")

	s.Require().NoError(s.store.CampaignContacts.Delete(s.ctx, campaign.ID, s.contactByNumber(campaign, "+15550001").ID))

	remaining := s.contactsOf(campaign)
	s.Require().Len(remaining, 1)
	s.Equal("+15550002", remaining[0].ToNumber)
}

func (s *StoreSuite) TestALaunchedCampaignsContactsCannotBeChangedOrRemoved() {
	campaign := s.autoEnrollCampaign(s.base)
	contact, err := s.addOne(campaign, "+15550001")
	s.Require().NoError(err)

	_, err = s.store.CampaignContacts.Update(s.ctx, campaign.ID, contact.ID, replaceWith(Contact{ToNumber: "+15550002"}))
	s.ErrorIs(err, ErrCampaignLocked)
	s.ErrorIs(s.store.CampaignContacts.Delete(s.ctx, campaign.ID, contact.ID), ErrCampaignLocked)
	s.Equal([]string{"+15550001"}, []string{s.contactsOf(campaign)[0].ToNumber})
}

func (s *StoreSuite) TestChangingOrRemovingAContactOfAnotherCampaignFindsNone() {
	campaign := s.draftCampaign(newID(), "+15550001")
	other := s.draftCampaign(newID(), "+15550001")
	contact := s.contactByNumber(other, "+15550001")

	_, err := s.store.CampaignContacts.Update(s.ctx, campaign.ID, contact.ID, replaceWith(Contact{ToNumber: "+15550002"}))
	s.ErrorIs(err, ErrNoCampaignContact)
	s.ErrorIs(s.store.CampaignContacts.Delete(s.ctx, campaign.ID, contact.ID), ErrNoCampaignContact)
	s.Equal("+15550001", s.contactByNumber(other, "+15550001").ToNumber)
}

// campaignFrom launches a campaign of one contact due at s.base, ringing from a number of
// country.
func (s *StoreSuite) campaignFrom(number, country string) Campaign {
	campaign := s.runningCampaign(5, "+15550001")
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &PhoneNumber{
		E164: number, Vendor: "telnyx", Country: country, CustomerID: campaign.CustomerID,
	}))
	_, err := s.store.GlobalDB().NewUpdate().Model((*Campaign)(nil)).Set("from_number = ?", number).
		Where("id = ?", campaign.ID).Exec(s.ctx)
	s.Require().NoError(err)
	return campaign
}

// campaignsClaimed is the campaigns of the contacts claimed in claim's shard at now.
func (s *StoreSuite) campaignsClaimed(claim ShardClaim, now time.Time) []string {
	claimed, err := s.store.CampaignContacts.Claim(s.ctx, "router-"+claim.Shard, claim, now, time.Minute, 10)
	s.Require().NoError(err)
	var campaigns []string
	for _, one := range claimed {
		campaigns = append(campaigns, one.Campaign.ID)
	}
	return campaigns
}

func (s *StoreSuite) TestACampaignIsDialledInTheShardOfItsNumbersCountry() {
	german := s.campaignFrom("+4930123456", "DE")
	american := s.campaignFrom("+12125550100", "US")

	s.Equal([]string{german.ID}, s.campaignsClaimed(ShardClaim{Shard: "europe-west4", Routing: regional}, s.base))
	s.Equal([]string{american.ID}, s.campaignsClaimed(ShardClaim{Shard: "us-east4", Routing: regional}, s.base))
}

func (s *StoreSuite) TestACampaignLeftUndialledInItsShardPastTheOverdueTimeIsDialledByAnother() {
	german := s.campaignFrom("+4930123456", "DE")
	us := ShardClaim{Shard: "us-east4", Routing: regional}
	s.Require().Empty(s.campaignsClaimed(us, s.base.Add(claimOverdue-time.Minute)), "not overdue yet")

	s.Equal([]string{german.ID}, s.campaignsClaimed(us, s.base.Add(claimOverdue+time.Minute)))
}

func (s *StoreSuite) TestAnExpiredLeaseIsTakenOverInTheShardOfTheCampaignsNumber() {
	german := s.campaignFrom("+4930123456", "DE")
	europe, us := ShardClaim{Shard: "europe-west4", Routing: regional}, ShardClaim{Shard: "us-east4", Routing: regional}
	s.Require().Len(s.campaignsClaimed(europe, s.base), 1)
	s.leasesPass("campaign_contacts", 2*time.Minute)

	taken, err := s.store.CampaignContacts.TakeOverExpired(s.ctx, "router-us", us, time.Minute, 10)
	s.Require().NoError(err)
	s.Empty(taken)
	taken, err = s.store.CampaignContacts.TakeOverExpired(s.ctx, "router-eu", europe, time.Minute, 10)
	s.Require().NoError(err)
	s.Require().Len(taken, 1)
	s.Equal(german.ID, taken[0].Campaign.ID)
}

func (s *StoreSuite) TestALeaseExpiredPastTheOverdueTimeIsTakenOverByAnyShard() {
	s.campaignFrom("+4930123456", "DE")
	s.Require().Len(s.campaignsClaimed(ShardClaim{Shard: "europe-west4", Routing: regional}, s.base), 1)
	us := ShardClaim{Shard: "us-east4", Routing: regional}
	s.leasesPass("campaign_contacts", time.Minute+claimOverdue-10*time.Second)

	taken, err := s.store.CampaignContacts.TakeOverExpired(s.ctx, "router-us", us, time.Minute, 10)
	s.Require().NoError(err)
	s.Require().Empty(taken, "not overdue yet")
	s.leasesPass("campaign_contacts", 20*time.Second)
	taken, err = s.store.CampaignContacts.TakeOverExpired(s.ctx, "router-us", us, time.Minute, 10)
	s.Require().NoError(err)
	s.Len(taken, 1)
}

func (s *StoreSuite) TestACampaignsNumberIsMatchedToItsCountryWhateverItsCase() {
	german := s.campaignFrom("+4930123456", "de")

	s.Empty(s.campaignsClaimed(ShardClaim{Shard: "us-east4", Routing: regional}, s.base))
	s.Equal([]string{german.ID}, s.campaignsClaimed(ShardClaim{Shard: "europe-west4", Routing: regional}, s.base))
}

func (s *StoreSuite) TestACampaignOfAnEUOnlyAppRingingFromAUSNumberIsDialledInEurope() {
	american := s.campaignFrom("+12125550100", "US")
	s.appUnder(american.CustomerID, shardmap.PolicyEUOnly)

	s.Empty(s.campaignsClaimed(placedIn("us-east4"), s.base))
	s.Equal([]string{american.ID}, s.campaignsClaimed(placedIn("europe-west4"), s.base))
}

func (s *StoreSuite) TestAnOverdueCampaignOfAnEUOnlyAppIsDialledByAnotherEuropeanShardAndNeverInTheUS() {
	german := s.campaignFrom("+4930123456", "DE")
	s.appUnder(german.CustomerID, shardmap.PolicyEUOnly)
	s.Require().Empty(s.campaignsClaimed(placedIn("europe-west1"), s.base.Add(claimOverdue-time.Minute)), "not overdue yet")

	s.Empty(s.campaignsClaimed(placedIn("us-east4"), s.base.Add(24*time.Hour)))
	s.Equal([]string{german.ID}, s.campaignsClaimed(placedIn("europe-west1"), s.base.Add(claimOverdue+time.Minute)))
}

func (s *StoreSuite) TestAnExpiredLeaseOfAnEUOnlyCampaignIsNeverTakenOverOutsideEurope() {
	german := s.campaignFrom("+4930123456", "DE")
	s.appUnder(german.CustomerID, shardmap.PolicyEUOnly)
	s.Require().Len(s.campaignsClaimed(placedIn("europe-west4"), s.base), 1)
	s.leasesPass("campaign_contacts", 24*time.Hour)

	taken, err := s.store.CampaignContacts.TakeOverExpired(s.ctx, "router-us", placedIn("us-east4"), time.Minute, 10)
	s.Require().NoError(err)
	s.Empty(taken)
	taken, err = s.store.CampaignContacts.TakeOverExpired(s.ctx, "router-eu", placedIn("europe-west1"), time.Minute, 10)
	s.Require().NoError(err)
	s.Len(taken, 1)
}

func (s *StoreSuite) TestACampaignOfACanadaOnlyAppIsDialledNowhereInADeploymentWithNoShardInCanada() {
	american := s.campaignFrom("+12125550100", "US")
	s.appUnder(american.CustomerID, shardmap.PolicyCanadaOnly)

	for _, shard := range []string{"us-east4", "europe-west4", "europe-west1"} {
		s.Empty(s.campaignsClaimed(placedIn(shard), s.base.Add(24*time.Hour)), shard)
	}
}

func (s *StoreSuite) TestACampaignOfAUSEastAppRingingFromAGermanNumberIsDialledInTheHomeShard() {
	german := s.campaignFrom("+4930123456", "DE")
	s.appUnder(german.CustomerID, shardmap.PolicyUSEast)

	s.Empty(s.campaignsClaimed(placedIn("europe-west4"), s.base))
	s.Equal([]string{german.ID}, s.campaignsClaimed(placedIn("us-east4"), s.base))
}

func (s *StoreSuite) TestACampaignsAgentsPolicyWinsOverItsApps() {
	german := s.campaignFrom("+4930123456", "DE")
	s.appUnder(german.CustomerID, shardmap.PolicyEUOnly)
	s.agentUnder(german.CustomerID, shardmap.PolicyUSEast)

	s.Empty(s.campaignsClaimed(placedIn("europe-west4"), s.base))
	s.Equal([]string{german.ID}, s.campaignsClaimed(placedIn("us-east4"), s.base))
}

func (s *StoreSuite) TestACampaignOfAClosestAppIsDialledInTheShardOfItsNumbersCountry() {
	german := s.campaignFrom("+4930123456", "DE")
	s.appUnder(german.CustomerID, shardmap.PolicyClosest)

	s.Empty(s.campaignsClaimed(placedIn("us-east4"), s.base))
	s.Equal([]string{german.ID}, s.campaignsClaimed(placedIn("europe-west4"), s.base))
}
