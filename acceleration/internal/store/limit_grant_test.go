//go:build integration

package store

import (
	"time"

	"github.com/google/uuid"

	"github.com/GetStream/Vision-Agents/acceleration/internal/usage"
)

func (s *StoreSuite) grant(grant LimitGrant) LimitGrant {
	if grant.OrganizationID == "" {
		grant.OrganizationID = "org-1"
	}
	if grant.Subject == "" {
		grant.Subject, grant.Category = usage.SubjectCategory, "team:finance"
	}
	if grant.AmountMicros == 0 {
		grant.AmountMicros = 5
	}
	grant.Reason, grant.Actor = "support", "someone"
	s.Require().NoError(s.store.LimitGrants.Insert(s.ctx, &grant, LimitAudit{CustomerID: "app-" + uuid.NewString(), Source: AuditSourceAPI}))
	return grant
}

func (s *StoreSuite) TestAGrantNamesOneSubject() {
	audit := LimitAudit{CustomerID: "app-1", Source: AuditSourceAPI}
	for name, grant := range map[string]LimitGrant{
		"a user grant with no user":           {Subject: usage.SubjectUser, AppID: "app-1"},
		"a key grant with a category":         {Subject: usage.SubjectAPIKey, AppID: "app-1", APIKey: "k", Category: "x"},
		"an organization grant naming an app": {Subject: usage.SubjectOrganization, AppID: "app-1"},
	} {
		s.Run(name, func() {
			grant.OrganizationID, grant.AmountMicros, grant.Reason, grant.Actor = "org-1", 5, "r", "a"
			s.Error(s.store.LimitGrants.Insert(s.ctx, &grant, audit))
		})
	}
	past := time.Now().Add(-time.Hour)
	expired := LimitGrant{OrganizationID: "org-1", Subject: usage.SubjectOrganization, AmountMicros: 5, ExpiresAt: &past, Reason: "r", Actor: "a"}
	s.Error(s.store.LimitGrants.Insert(s.ctx, &expired, audit), "limit_grants_expiry_check")
}

func (s *StoreSuite) TestLiveGrantsLeaveOutRevokedAndExpired() {
	// An hour ahead, so a slow insert is still before it (limit_grants_expiry_check), and
	// LimitGrants.SelectLive is asked at instants pinned to it: how long the setup takes does not matter.
	// Microseconds, as Postgres keeps them.
	expires := time.Now().Add(time.Hour).Truncate(time.Microsecond)
	live := s.grant(LimitGrant{})
	expiring := s.grant(LimitGrant{ExpiresAt: &expires})
	revoked := s.grant(LimitGrant{})
	_, err := s.store.LimitGrants.Revoke(s.ctx, "org-1", revoked.ID, "someone", "no longer needed", LimitAudit{CustomerID: "app-1", Source: AuditSourceAPI})
	s.Require().NoError(err)

	grants, err := s.store.LimitGrants.SelectLive(s.ctx, []string{"org-1"}, expires.Add(-time.Microsecond))
	s.Require().NoError(err)
	s.Equal([]int64{live.ID, expiring.ID}, idsOf(grants), "just before expires_at it is live")
	grants, err = s.store.LimitGrants.SelectLive(s.ctx, []string{"org-1"}, expires)
	s.Require().NoError(err)
	s.Equal([]int64{live.ID}, idsOf(grants), "at expires_at it is expired")
}

func (s *StoreSuite) TestRevokingTwiceIsANoOp() {
	grant := s.grant(LimitGrant{})
	audit := LimitAudit{CustomerID: "app-" + uuid.NewString(), Source: AuditSourceAPI}

	first, err := s.store.LimitGrants.Revoke(s.ctx, "org-1", grant.ID, "someone", "done", audit)
	s.Require().NoError(err)
	second, err := s.store.LimitGrants.Revoke(s.ctx, "org-1", grant.ID, "someone else", "again", audit)
	s.Require().NoError(err)

	s.Equal(first.RevokedAt, second.RevokedAt)
	s.Equal("someone", second.RevokedBy)
	entries, err := s.store.AuditEntries.Select(s.ctx, audit.CustomerID, AuditEntryFilter{ResourceType: AuditSpendLimit})
	s.Require().NoError(err)
	s.Len(entries, 1, "the second records nothing")
	_, err = s.store.LimitGrants.Revoke(s.ctx, "org-2", grant.ID, "x", "x", audit)
	s.ErrorIs(err, ErrNoGrant, "another organization's grant is not there")
}

// revoked is a grant of the organization, revoked.
func (s *StoreSuite) revoked(organization string) LimitGrant {
	grant := s.grant(LimitGrant{OrganizationID: organization})
	revoked, err := s.store.LimitGrants.Revoke(s.ctx, organization, grant.ID, "someone", "done", LimitAudit{CustomerID: "app-1", Source: AuditSourceAPI})
	s.Require().NoError(err)
	return revoked
}

// changeOf is the change to field an audit entry records.
func changeOf(entry AuditEntry, field string) (AuditChange, bool) {
	for _, change := range entry.Changes {
		if change.Field == field {
			return change, true
		}
	}
	return AuditChange{}, false
}

func (s *StoreSuite) TestAnUnrevokedGrantIsLiveAgainAndAudited() {
	grant := s.revoked("org-1")
	audit := LimitAudit{CustomerID: "app-" + uuid.NewString(), Source: AuditSourceAPI}

	unrevoked, err := s.store.LimitGrants.Unrevoke(s.ctx, "org-1", grant.ID, "revoked by mistake", audit)

	s.Require().NoError(err)
	s.Nil(unrevoked.RevokedAt)
	s.Empty(unrevoked.RevokedBy)
	live, err := s.store.LimitGrants.SelectLive(s.ctx, []string{"org-1"}, time.Now())
	s.Require().NoError(err)
	s.Equal([]int64{grant.ID}, idsOf(live))
	entries, err := s.store.AuditEntries.Select(s.ctx, audit.CustomerID, AuditEntryFilter{ResourceType: AuditSpendLimit})
	s.Require().NoError(err)
	s.Require().Len(entries, 1)
	s.Equal(AuditUpdated, entries[0].Action)
	reason, _ := changeOf(entries[0], "unrevoke_reason")
	s.Equal("revoked by mistake", reason.After)
	revokedAt, _ := changeOf(entries[0], "revoked_at")
	s.NotNil(revokedAt.Before)
	s.Nil(revokedAt.After)
}

func (s *StoreSuite) TestUnrevokingAGrantNotRevokedIsANoOp() {
	grant := s.grant(LimitGrant{})
	audit := LimitAudit{CustomerID: "app-" + uuid.NewString(), Source: AuditSourceAPI}

	unrevoked, err := s.store.LimitGrants.Unrevoke(s.ctx, "org-1", grant.ID, "why not", audit)

	s.Require().NoError(err)
	s.Nil(unrevoked.RevokedAt)
	entries, err := s.store.AuditEntries.Select(s.ctx, audit.CustomerID, AuditEntryFilter{ResourceType: AuditSpendLimit})
	s.Require().NoError(err)
	s.Empty(entries, "it records nothing")
	_, err = s.store.LimitGrants.Unrevoke(s.ctx, "org-2", s.revoked("org-1").ID, "x", audit)
	s.ErrorIs(err, ErrNoGrant, "another organization's grant is not there")
}

func (s *StoreSuite) TestAnyOrganizationsGrantIsUnrevokedWithNoOrganization() {
	grant := s.revoked("org-1")

	unrevoked, err := s.store.LimitGrants.Unrevoke(s.ctx, "", grant.ID, "support", LimitAudit{Source: AuditSourceManage, ActorID: "manage:staff"})

	s.Require().NoError(err)
	s.Nil(unrevoked.RevokedAt)
	entries, err := s.store.AuditEntries.Select(s.ctx, "org-1", AuditEntryFilter{ResourceType: AuditSpendLimit})
	s.Require().NoError(err)
	s.Require().Len(entries, 1, "audited under the grant's organization")
}

func (s *StoreSuite) TestAnExpiredGrantCannotBeUnrevoked() {
	grant := s.revoked("org-1")
	_, err := s.store.GlobalDB().ExecContext(s.ctx, "UPDATE limit_grants SET created_at = now() - interval '2 hours', "+
		"expires_at = now() - interval '1 hour' WHERE id = ?", grant.ID)
	s.Require().NoError(err)

	_, err = s.store.LimitGrants.Unrevoke(s.ctx, "org-1", grant.ID, "x", LimitAudit{CustomerID: "app-1", Source: AuditSourceAPI})

	s.ErrorIs(err, ErrGrantExpired)
	still, err := s.store.LimitGrants.Get(s.ctx, "org-1", grant.ID)
	s.Require().NoError(err)
	s.NotNil(still.RevokedAt)
}

func (s *StoreSuite) TestUnrevokingPastTheLiveCapIsRefused() {
	grant := s.revoked("org-full")
	for range MaxLiveGrants {
		s.grant(LimitGrant{OrganizationID: "org-full"})
	}

	_, err := s.store.LimitGrants.Unrevoke(s.ctx, "org-full", grant.ID, "x", LimitAudit{CustomerID: "app-1", Source: AuditSourceAPI})

	s.ErrorIs(err, ErrTooManyGrants)
	still, err := s.store.LimitGrants.Get(s.ctx, "org-full", grant.ID)
	s.Require().NoError(err)
	s.NotNil(still.RevokedAt)
}

func (s *StoreSuite) TestAUsedUpGrantIsUnrevokedPastTheLiveCap() {
	used := s.grant(LimitGrant{OrganizationID: "org-full", AmountMicros: 300})
	s.grantRow(s.store, []int64{used.ID}, 300, true)
	_, err := s.store.LimitGrants.Revoke(s.ctx, "org-full", used.ID, "someone", "done", LimitAudit{CustomerID: "app-1", Source: AuditSourceAPI})
	s.Require().NoError(err)
	for range MaxLiveGrants {
		s.grant(LimitGrant{OrganizationID: "org-full"})
	}

	_, err = s.store.LimitGrants.Unrevoke(s.ctx, "org-full", used.ID, "x", LimitAudit{CustomerID: "app-1", Source: AuditSourceAPI})

	s.NoError(err, "used up, it does not count toward the cap")
}

func (s *StoreSuite) TestAnOrganizationHasAtMostSoManyLiveGrants() {
	for range MaxLiveGrants {
		s.grant(LimitGrant{OrganizationID: "org-full"})
	}
	extra := LimitGrant{OrganizationID: "org-full", Subject: usage.SubjectOrganization, AmountMicros: 5, Reason: "r", Actor: "a"}

	s.ErrorIs(s.store.LimitGrants.Insert(s.ctx, &extra, LimitAudit{CustomerID: "app-1", Source: AuditSourceAPI}), ErrTooManyGrants)
	s.grant(LimitGrant{OrganizationID: "org-other"})
}

func (s *StoreSuite) TestGrantsAreListedNewestFirst() {
	a, b, c := s.grant(LimitGrant{}), s.grant(LimitGrant{}), s.grant(LimitGrant{})

	page, err := s.store.LimitGrants.Select(s.ctx, GrantFilter{OrganizationID: "org-1", Limit: 2})
	s.Require().NoError(err)
	s.Equal([]int64{c.ID, b.ID, a.ID}, idsOf(page), "two and one more")
	next, err := s.store.LimitGrants.Select(s.ctx, GrantFilter{OrganizationID: "org-1", Before: b.ID, Limit: 2})
	s.Require().NoError(err)
	s.Equal([]int64{a.ID}, idsOf(next))
}

func (s *StoreSuite) TestAUsedUpGrantIsNotLiveAndLeavesRoomForAnother() {
	for range MaxLiveGrants - 1 {
		s.grant(LimitGrant{OrganizationID: "org-used"})
	}
	used := s.grant(LimitGrant{OrganizationID: "org-used", AmountMicros: 300})
	extra := LimitGrant{OrganizationID: "org-used", Subject: usage.SubjectOrganization, AmountMicros: 5, Reason: "r", Actor: "a"}
	s.Require().ErrorIs(s.store.LimitGrants.Insert(s.ctx, &extra, LimitAudit{CustomerID: "app-1", Source: AuditSourceAPI}), ErrTooManyGrants)

	s.grantRow(s.store, []int64{used.ID}, 300, true)

	live, err := s.store.LimitGrants.SelectLive(s.ctx, []string{"org-used"}, time.Now())
	s.Require().NoError(err)
	s.NotContains(idsOf(live), used.ID, "used up, it is not read")
	s.Len(live, MaxLiveGrants-1)
	s.Require().NoError(s.store.LimitGrants.Insert(s.ctx, &extra, LimitAudit{CustomerID: "app-1", Source: AuditSourceAPI}),
		"nor counted toward the cap")
	yes := true
	listed, err := s.store.LimitGrants.Select(s.ctx, GrantFilter{OrganizationID: "org-used", Used: &yes, Limit: 50})
	s.Require().NoError(err)
	s.Equal([]int64{used.ID}, idsOf(listed))
}

func idsOf(grants []LimitGrant) []int64 {
	ids := make([]int64, len(grants))
	for i, grant := range grants {
		ids[i] = grant.ID
	}
	return ids
}

func (s *StoreSuite) TestLimitGrantQueryBudgets() {
	organization := "org-" + uuid.NewString()
	audit := LimitAudit{CustomerID: "app-" + uuid.NewString(), Source: AuditSourceAPI}
	grant := LimitGrant{OrganizationID: organization, Subject: usage.SubjectOrganization, AmountMicros: 5, Reason: "support", Actor: "someone"}
	s.Equal(6, s.countQueries(func() { s.Require().NoError(s.store.LimitGrants.Insert(s.ctx, &grant, audit)) }),
		"begin, the organization's lock, the live count, the grant, its audit row, commit")
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.LimitGrants.Get(s.ctx, organization, grant.ID)
		s.Require().NoError(err)
	}))
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.LimitGrants.SelectLive(s.ctx, []string{organization}, time.Now())
		s.Require().NoError(err)
	}))
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.LimitGrants.Select(s.ctx, GrantFilter{OrganizationID: organization, Limit: 10})
		s.Require().NoError(err)
	}))
	s.Equal(5, s.countQueries(func() {
		_, err := s.store.LimitGrants.Revoke(s.ctx, organization, grant.ID, "someone", "mistake", audit)
		s.Require().NoError(err)
	}), "begin, the locked grant, the write, its audit row, commit")
	s.Equal(8, s.countQueries(func() {
		_, err := s.store.LimitGrants.Unrevoke(s.ctx, organization, grant.ID, "not a mistake", audit)
		s.Require().NoError(err)
	}), "begin, the grant's organization, its lock, the locked grant, the live count, the write, its audit row, commit")
}
