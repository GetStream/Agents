//go:build integration

package store

import (
	"time"

	"github.com/google/uuid"

	"github.com/GetStream/Vision-Agents/acceleration/internal/usage"
)

func (s *StoreSuite) TestResetsAreReadFromTheWindowsAsked() {
	today := time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC)
	audit := LimitAudit{CustomerID: "app-" + uuid.NewString(), Source: AuditSourceAPI}
	s.Require().NoError(s.store.UsageResets.InsertMany(s.ctx, []UsageReset{
		{OrganizationID: "org-1", Subject: usage.SubjectApp, AppID: "app-1", Interval: usage.Daily, WindowStart: today, BaselineMicros: 5, Reason: "r", Actor: "a"},
		{OrganizationID: "org-1", Subject: usage.SubjectApp, AppID: "app-1", Interval: usage.Daily, WindowStart: today.AddDate(0, 0, -40), BaselineMicros: 5, Reason: "r", Actor: "a"},
	}, audit))

	resets, err := s.store.UsageResets.Select(s.ctx, []string{"org-1"}, today.AddDate(0, 0, -1))

	s.Require().NoError(err)
	s.Require().Len(resets, 1)
	s.True(resets[0].WindowStart.Equal(today))
	entries, err := s.store.AuditEntries.Select(s.ctx, audit.CustomerID, AuditEntryFilter{ResourceType: AuditSpendLimit})
	s.Require().NoError(err)
	s.Len(entries, 2)
	s.Contains(entries[0].ResourceID, "reset:")
}

func (s *StoreSuite) TestUsageResetQueryBudgets() {
	organization := "org-" + uuid.NewString()
	today := time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC)
	reset := func(app string) UsageReset {
		return UsageReset{OrganizationID: organization, Subject: usage.SubjectApp, AppID: app, Interval: usage.Daily, WindowStart: today, Reason: "r", Actor: "a"}
	}
	audit := LimitAudit{CustomerID: "app-" + uuid.NewString(), Source: AuditSourceAPI}
	const insertMany = 4
	s.Equal(insertMany, s.countQueries(func() {
		s.Require().NoError(s.store.UsageResets.InsertMany(s.ctx, []UsageReset{reset("app-1"), reset("app-2"), reset("app-3")}, audit))
	}), "begin, every reset in one insert, every audit row in another, commit")
	s.Equal(1, s.countQueries(func() {
		resets, err := s.store.UsageResets.Select(s.ctx, []string{organization}, today)
		s.Require().NoError(err)
		s.Len(resets, 3)
	}))
}
