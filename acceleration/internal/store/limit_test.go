//go:build integration

package store

import (
	"errors"

	"github.com/google/uuid"

	"github.com/GetStream/Vision-Agents/acceleration/internal/usage"
)

var limitAudit = LimitAudit{CustomerID: "app-1", Source: AuditSourceAPI, ActorID: "someone"}

func (s *StoreSuite) newLimit(limit Limit) Limit {
	if limit.Interval == "" {
		limit.Interval = usage.Daily
	}
	if limit.OrganizationID == "" {
		limit.OrganizationID = "org-1"
	}
	limit.UpdatedBy = "someone"
	s.Require().NoError(s.store.Limits.Insert(s.ctx, &limit, limitAudit))
	return limit
}

func (s *StoreSuite) TestOneRowPerLevelAndInterval() {
	first := s.newLimit(Limit{Subject: usage.SubjectAPIKey, Level: LevelApp, AppID: "app-1", LimitMicros: 5})
	s.newLimit(Limit{Subject: usage.SubjectAPIKey, Level: LevelApp, AppID: "app-1", LimitMicros: 5, Interval: usage.Weekly})

	again := Limit{Subject: usage.SubjectAPIKey, Level: LevelApp, OrganizationID: "org-1", AppID: "app-1", Interval: usage.Daily, LimitMicros: 9, UpdatedBy: "x"}
	err := s.store.Limits.Insert(s.ctx, &again, limitAudit)

	var exists *ErrLimitExists
	s.Require().True(errors.As(err, &exists), "%v", err)
	s.Equal(first.ID, exists.ID)
}

func (s *StoreSuite) TestALimitsWritesAreAudited() {
	audit := LimitAudit{CustomerID: "app-" + uuid.NewString(), Source: AuditSourceManage, ActorID: "staff"}
	limit := Limit{Subject: usage.SubjectApp, Level: LevelApp, OrganizationID: "org-1", AppID: "app-1", Interval: usage.Daily, LimitMicros: 5, UpdatedBy: "x"}
	s.Require().NoError(s.store.Limits.Insert(s.ctx, &limit, audit))
	more, note := int64(7), "raised"
	updated, err := s.store.Limits.Update(s.ctx, "org-1", limit.ID, LimitChange{LimitMicros: &more, Note: &note, UpdatedBy: "other"}, audit)
	s.Require().NoError(err)
	s.EqualValues(7, updated.LimitMicros)
	s.Equal("other", updated.UpdatedBy)
	_, err = s.store.Limits.Update(s.ctx, "org-2", limit.ID, LimitChange{LimitMicros: &more}, audit)
	s.ErrorIs(err, ErrNoLimit, "another organization's limit is not there")
	_, err = s.store.Limits.Delete(s.ctx, "", limit.ID, audit)
	s.Require().NoError(err, "with no organization named, any organization's")
	_, err = s.store.Limits.Get(s.ctx, "org-1", limit.ID)
	s.ErrorIs(err, ErrNoLimit)

	entries, err := s.store.AuditEntries.Select(s.ctx, audit.CustomerID, AuditEntryFilter{ResourceType: AuditSpendLimit})
	s.Require().NoError(err)
	s.Require().Len(entries, 3)
	s.Equal([]string{AuditDeleted, AuditUpdated, AuditCreated}, []string{entries[0].Action, entries[1].Action, entries[2].Action})
	s.Equal([]AuditChange{{Field: "limit_micros", Before: float64(5), After: float64(7)}, {Field: "note", After: "raised"}}, entries[1].Changes)
}

func (s *StoreSuite) TestLimitsAreListedByOrganizationAndFilter() {
	a := s.newLimit(Limit{Subject: usage.SubjectCategory, Level: LevelOrganization, Category: "team:finance", LimitMicros: 1})
	b := s.newLimit(Limit{Subject: usage.SubjectCategory, Level: LevelOrganization, Category: "project:x", LimitMicros: 1})
	s.newLimit(Limit{Subject: usage.SubjectCategory, Level: LevelOrganization, OrganizationID: "org-2", Category: "team:finance", LimitMicros: 1})

	teams, err := s.store.Limits.Select(s.ctx, LimitFilter{OrganizationID: "org-1", Subject: usage.SubjectCategory, CategoryPrefix: "team:", Limit: 10})
	s.Require().NoError(err)
	s.Require().Len(teams, 1)
	s.Equal(a.ID, teams[0].ID)
	others, err := s.store.Limits.Select(s.ctx, LimitFilter{OrganizationID: "org-1", Subject: usage.SubjectCategory, NotCategoryPrefix: "team:", Limit: 10})
	s.Require().NoError(err)
	s.Require().Len(others, 1)
	s.Equal(b.ID, others[0].ID)
	after, err := s.store.Limits.Select(s.ctx, LimitFilter{OrganizationID: "org-1", After: a.ID, Limit: 10})
	s.Require().NoError(err)
	s.Require().Len(after, 1, "the page after the first row")
	s.Equal(b.ID, after[0].ID)

	both, err := s.store.Limits.SelectOf(s.ctx, []string{"org-1", "org-2"})
	s.Require().NoError(err)
	s.Len(both, 3)
}

func (s *StoreSuite) TestLimitQueryBudgets() {
	organization := "org-" + uuid.NewString()
	limit := Limit{OrganizationID: organization, Subject: usage.SubjectOrganization, Level: LevelOrganization, Interval: usage.Daily, LimitMicros: 5, UpdatedBy: "someone"}
	s.Equal(4, s.countQueries(func() { s.Require().NoError(s.store.Limits.Insert(s.ctx, &limit, limitAudit)) }),
		"begin, the limit, its audit row, commit")
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.Limits.Get(s.ctx, organization, limit.ID)
		s.Require().NoError(err)
	}))
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.Limits.SelectOf(s.ctx, []string{organization, "org-other"})
		s.Require().NoError(err)
	}))
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.Limits.Select(s.ctx, LimitFilter{OrganizationID: organization, Subject: usage.SubjectOrganization, Limit: 10})
		s.Require().NoError(err)
	}))
	more := int64(9)
	s.Equal(5, s.countQueries(func() {
		_, err := s.store.Limits.Update(s.ctx, organization, limit.ID, LimitChange{LimitMicros: &more, UpdatedBy: "someone"}, limitAudit)
		s.Require().NoError(err)
	}), "begin, the locked row, the write, its audit row, commit")
	s.Equal(5, s.countQueries(func() {
		_, err := s.store.Limits.Delete(s.ctx, organization, limit.ID, limitAudit)
		s.Require().NoError(err)
	}), "begin, the locked row, the delete, its audit row, commit")
}
