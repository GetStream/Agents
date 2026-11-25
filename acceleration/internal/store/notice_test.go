//go:build integration

package store

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

func (s *StoreSuite) limitNotice(organization string) LimitNotice {
	return LimitNotice{
		OrganizationID: organization, Kind: LimitNoCredit, AppID: "app-1", Tier: "free",
		Enforced: true, Message: "the organization has no credit left",
	}
}

// note writes a notice as the senders do, returning whether this call wrote it.
func (s *StoreSuite) note(notice Notice, err error) bool {
	s.Require().NoError(err)
	written, err := s.store.Notices.Insert(s.ctx, &notice)
	s.Require().NoError(err)
	return written
}

// claimed are the notices a sweep claims now, stale after an hour.
func (s *StoreSuite) claimed() []Notice {
	claimed, err := s.store.Notices.Claim(s.ctx, time.Hour, 100)
	s.Require().NoError(err)
	return claimed
}

// backdate moves the claims of organization's notices two hours back, past a sweep's staleness.
func (s *StoreSuite) backdate(organization string) {
	_, err := s.store.GlobalDB().NewUpdate().Model((*Notice)(nil)).Set("claimed_at = now() - interval '2 hours'").
		Where("organization_id = ?", organization).Exec(s.ctx)
	s.Require().NoError(err)
}

func keysOf(notices []Notice, organization string) []string {
	var keys []string
	for _, notice := range notices {
		if notice.OrganizationID == organization {
			keys = append(keys, notice.Kind+"/"+notice.DedupeKey)
		}
	}
	return keys
}

func (s *StoreSuite) TestALimitNoticeIsWrittenOncePerOrganizationAndKind() {
	organization := "org-" + uuid.NewString()
	notice := s.limitNotice(organization)

	s.True(s.note(notice.Notice()), "the first")
	s.False(s.note(notice.Notice()), "the same organization and kind again")
	notice.Kind = LimitSpendHardLimit
	s.True(s.note(notice.Notice()), "another kind")
	s.True(s.note(s.limitNotice("org-"+uuid.NewString()).Notice()), "another organization")

	notice.Kind = "suspended"
	row, err := notice.Notice()
	s.Require().NoError(err)
	_, err = s.store.Notices.Insert(s.ctx, &row)
	var invalid *ValidationError
	s.Require().True(errors.As(err, &invalid), "not a kind of notice: %v", err)
	s.Equal("kind", invalid.Field)
}

func (s *StoreSuite) TestACostAnomalyIsWrittenOncePerOrganizationAndDay() {
	day := time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC)
	organization := "org-" + uuid.NewString()
	anomaly := CostAnomaly{OrganizationID: organization, Day: day, SpentMicros: 500, BaselineMicros: 100}

	s.True(s.note(anomaly.Notice()))
	s.False(s.note(anomaly.Notice()), "the same day again")
	next := anomaly
	next.Day = day.AddDate(0, 0, 1)
	s.True(s.note(next.Notice()), "the next day is its own")
	other := anomaly
	other.OrganizationID = "org-" + uuid.NewString()
	s.True(s.note(other.Notice()), "another organization the same day is its own")

	anomaly.OrganizationID = ""
	row, err := anomaly.Notice()
	s.Require().NoError(err)
	_, err = s.store.Notices.Insert(s.ctx, &row)
	var invalid *ValidationError
	s.Require().True(errors.As(err, &invalid))
	s.Equal("organization_id", invalid.Field)
}

func (s *StoreSuite) TestAnUnpostedNoticeIsClaimedOnceItsClaimIsStaleWhateverItsKind() {
	day := time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC)
	organization := "org-" + uuid.NewString()
	s.note(s.limitNotice(organization).Notice())
	s.note(CostAnomaly{OrganizationID: organization, Day: day, SpentMicros: 500, BaselineMicros: 100}.Notice())

	s.Empty(keysOf(s.claimed(), organization), "its writer's claim stands")
	s.backdate(organization)
	claimed := s.claimed()
	s.ElementsMatch([]string{"no_credit/", "cost_anomaly/2026-03-10"}, keysOf(claimed, organization))
	for _, notice := range claimed {
		switch {
		case notice.OrganizationID != organization:
		case notice.Kind == LimitNoCredit:
			limit, err := notice.LimitNotice()
			s.Require().NoError(err)
			s.Equal(s.limitNotice(organization), limit, "read back in its own shape")
		default:
			anomaly, err := notice.CostAnomaly()
			s.Require().NoError(err)
			s.Equal(CostAnomaly{OrganizationID: organization, Day: day, SpentMicros: 500, BaselineMicros: 100}, anomaly)
		}
	}
	s.Empty(keysOf(s.claimed(), organization), "claimed again just now")

	s.note(CostAnomaly{OrganizationID: organization, Day: day.AddDate(0, 0, -1), SpentMicros: 1, BaselineMicros: 1}.Notice())
	for _, notice := range claimed {
		if notice.OrganizationID == organization {
			s.Require().NoError(s.store.Notices.MarkPosted(s.ctx, notice))
		}
	}
	s.backdate(organization)
	s.Equal([]string{"cost_anomaly/2026-03-09"}, keysOf(s.claimed(), organization), "the posted are not claimed, the day before is")
}

func (s *StoreSuite) TestABalanceWarningIsQueuedOnceToItsOrganizationsStreamDestinations() {
	organization, app, stranger := "org-"+uuid.NewString(), "app-"+uuid.NewString(), "app-"+uuid.NewString()
	s.Require().NoError(s.store.AppOrganizations.Upsert(s.ctx, &AppOrganization{AppID: app, OrganizationID: organization}))
	s.Require().NoError(s.store.AppOrganizations.Upsert(s.ctx, &AppOrganization{AppID: stranger, OrganizationID: "org-other"}))
	s.eventDestination(app, StreamSource, "dest-"+uuid.NewString(), ForwardAll)
	s.eventDestination(app, "github", "dest-"+uuid.NewString(), ForwardAll)
	s.eventDestination(stranger, StreamSource, "dest-"+uuid.NewString(), ForwardAll)
	warning, err := BalanceNotice{OrganizationID: organization, Kind: NoticeBalanceLow, GrantedMicros: 10, BalanceMicros: 4, ThresholdMicros: 5}.Notice()
	s.Require().NoError(err)
	delivery := EventDelivery{ID: "balance:" + organization, NextAttemptAt: time.Now()}
	var bodies []string
	body := func(appID string) ([]byte, error) {
		bodies = append(bodies, appID)
		return []byte(`{"app":"` + appID + `"}`), nil
	}

	sent, queued, err := s.store.Notices.InsertQueued(s.ctx, &warning, delivery, body)
	s.Require().NoError(err)
	again, queuedAgain, err := s.store.Notices.InsertQueued(s.ctx, &warning, delivery, body)
	s.Require().NoError(err)

	s.True(sent)
	s.Equal(1, queued, "only the stream destination of the organization's own app")
	s.Len(s.queued(delivery.ID), 1)
	s.Equal([]string{app}, bodies, "each body made for its app, and none the second time")
	s.False(again)
	s.Zero(queuedAgain)
	s.backdate(organization)
	s.Empty(keysOf(s.claimed(), organization), "posted as it was queued")
}

func (s *StoreSuite) TestNoticeQueryBudgets() {
	organization := "org-" + uuid.NewString()
	limit, err := s.limitNotice(organization).Notice()
	s.Require().NoError(err)
	warning, err := BalanceNotice{OrganizationID: organization, Kind: NoticeBalanceExhausted, GrantedMicros: 1, ThresholdMicros: 1}.Notice()
	s.Require().NoError(err)
	s.Require().NoError(s.store.AppOrganizations.Upsert(s.ctx, &AppOrganization{AppID: "app-budget", OrganizationID: organization}))
	for range 3 {
		s.eventDestination("app-budget", StreamSource, "dest-"+uuid.NewString(), ForwardAll)
	}

	s.Equal(1, s.countQueries(func() {
		_, err := s.store.Notices.Insert(s.ctx, &limit)
		s.Require().NoError(err)
	}))
	s.Equal(5, s.countQueries(func() {
		_, queued, err := s.store.Notices.InsertQueued(s.ctx, &warning, EventDelivery{ID: "budget", NextAttemptAt: time.Now()},
			func(string) ([]byte, error) { return []byte(`{}`), nil })
		s.Require().NoError(err)
		s.Equal(3, queued)
	}), "begin, the notice, its destinations, every delivery in one insert, commit")
	s.backdate(organization)
	s.Equal(1, s.countQueries(func() { s.Len(keysOf(s.claimed(), organization), 1) }))
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.Notices.MarkPosted(s.ctx, limit)) }))
}
