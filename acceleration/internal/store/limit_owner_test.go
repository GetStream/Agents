//go:build integration

package store

import (
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/GetStream/Vision-Agents/acceleration/internal/usage"
)

// before makes a row as the release before rows said whose they are wrote it, an hour ago.
func (s *StoreSuite) before(table string, id int64) {
	_, err := s.store.global.NewRaw("UPDATE "+table+" SET billed = NULL, created_at = now() - interval '1 hour' WHERE id = ?", id).Exec(s.ctx)
	s.Require().NoError(err)
}

// wrote is a limit of id's that creator wrote before rows said whose they are.
func (s *StoreSuite) wrote(id, creator, source string) int64 {
	limit := Limit{Subject: usage.SubjectOrganization, Level: LevelOrganization, OrganizationID: id, Interval: usage.Daily, LimitMicros: 5, UpdatedBy: "x"}
	s.Require().NoError(s.store.Limits.Insert(s.ctx, &limit, LimitAudit{CustomerID: creator, Source: source}))
	s.before("limits", limit.ID)
	return limit.ID
}

func (s *StoreSuite) attributed(id int64) *bool {
	_, err := s.store.AttributeLimits(s.ctx)
	s.Require().NoError(err)
	limit, err := s.store.Limits.Get(s.ctx, "", id)
	s.Require().NoError(err)
	return limit.Billed
}

func (s *StoreSuite) TestALimitAnotherAppWroteIsTheOrganizations() {
	id := uuid.NewString()
	s.Equal(new(true), s.attributed(s.wrote(id, uuid.NewString(), AuditSourceAPI)))
}

func (s *StoreSuite) TestALimitAnAppInNoOrganizationWroteIsItsOwn() {
	id := uuid.NewString()
	s.Equal(new(false), s.attributed(s.wrote(id, id, AuditSourceAPI)))
}

func (s *StoreSuite) TestALimitAnAppOfTheOrganizationOfItsIDWroteIsTheOrganizations() {
	seen := uuid.NewString()
	s.member(seen, seen)
	created := Organization{Name: "o"}
	s.Require().NoError(s.store.Organizations.Insert(s.ctx, &created))
	registered := created.ID
	s.Require().NoError(s.store.Apps.Insert(s.ctx, &App{ID: registered, OrganizationID: registered, Name: "a"}))

	s.Equal(new(true), s.attributed(s.wrote(seen, seen, AuditSourceAPI)))
	s.Equal(new(true), s.attributed(s.wrote(registered, registered, AuditSourceAPI)))
}

func (s *StoreSuite) TestALimitOfAnAppRecordedElsewhereIsLeft() {
	id := uuid.NewString()
	s.member(id, uuid.NewString())
	s.Nil(s.attributed(s.wrote(id, id, AuditSourceAPI)))
}

func (s *StoreSuite) TestALimitJustWrittenByAnAppIsLeftAMinute() {
	id := uuid.NewString()
	limit := s.wrote(id, id, AuditSourceAPI)
	_, err := s.store.global.NewRaw("UPDATE limits SET created_at = now() - interval '30 seconds' WHERE id = ?", limit).Exec(s.ctx)
	s.Require().NoError(err)
	s.Nil(s.attributed(limit), "the organization its request named may not be recorded yet")
	_, err = s.store.global.NewRaw("UPDATE limits SET created_at = now() - interval '90 seconds' WHERE id = ?", limit).Exec(s.ctx)
	s.Require().NoError(err)
	s.Equal(new(false), s.attributed(limit))
}

func (s *StoreSuite) TestALimitStaffWroteIsAnOrganizationsTheRouterKnows() {
	seen, unknown := uuid.NewString(), uuid.NewString()
	s.member(uuid.NewString(), seen)
	created := Organization{Name: "o"}
	s.Require().NoError(s.store.Organizations.Insert(s.ctx, &created))

	s.Equal(new(true), s.attributed(s.wrote(seen, seen, AuditSourceManage)))
	s.Equal(new(true), s.attributed(s.wrote(created.ID, created.ID, AuditSourceManage)))
	s.Nil(s.attributed(s.wrote(unknown, unknown, AuditSourceManage)))
}

func (s *StoreSuite) TestALimitNoAuditNamesIsLeft() {
	id := uuid.NewString()
	limit := s.wrote(id, id, AuditSourceAPI)
	_, err := s.store.global.NewRaw("DELETE FROM audit_log WHERE customer_id = ?", id).Exec(s.ctx)
	s.Require().NoError(err)
	s.Nil(s.attributed(limit))
}

func (s *StoreSuite) TestAChangeOfALimitIsNotWhoWroteIt() {
	id := uuid.NewString()
	limit := s.wrote(id, uuid.NewString(), AuditSourceAPI)
	_, err := s.store.global.NewRaw("UPDATE audit_log SET action = 'updated' WHERE resource_type = 'limit' AND changes::text LIKE ?", "%"+id+"%").Exec(s.ctx)
	s.Require().NoError(err)
	s.Nil(s.attributed(limit))
}

// A limit's id may be one the audit log names another organization's limit by, as after a copy.
func (s *StoreSuite) TestAnAuditOfAnotherOrganizationsLimitIsNotWhoWroteIt() {
	id := uuid.NewString()
	limit := s.wrote(id, uuid.NewString(), AuditSourceAPI)
	_, err := s.store.global.NewRaw("UPDATE limits SET organization_id = ? WHERE id = ?", uuid.NewString(), limit).Exec(s.ctx)
	s.Require().NoError(err)
	s.Nil(s.attributed(limit))
}

func (s *StoreSuite) TestAnAttributedRowIsNotAttributedAgain() {
	id := uuid.NewString()
	limit := Limit{Subject: usage.SubjectOrganization, Level: LevelOrganization, OrganizationID: id, Billed: new(false), Interval: usage.Daily, UpdatedBy: "x"}
	s.Require().NoError(s.store.Limits.Insert(s.ctx, &limit, LimitAudit{CustomerID: uuid.NewString(), Source: AuditSourceAPI}))
	s.Equal(new(false), s.attributed(limit.ID))
}

func (s *StoreSuite) TestGrantsAndResetsAreAttributedAsLimitsAre() {
	id := uuid.NewString()
	audit := LimitAudit{CustomerID: id, Source: AuditSourceAPI}
	grant := LimitGrant{OrganizationID: id, Subject: usage.SubjectOrganization, AmountMicros: 5, Reason: "x", Actor: "x"}
	s.Require().NoError(s.store.LimitGrants.Insert(s.ctx, &grant, audit))
	resets := []UsageReset{{OrganizationID: id, Subject: usage.SubjectOrganization, Interval: usage.Daily, WindowStart: s.base, Reason: "x", Actor: "x"}}
	s.Require().NoError(s.store.UsageResets.InsertMany(s.ctx, resets, audit))
	s.before("limit_grants", grant.ID)
	s.before("usage_resets", resets[0].ID)

	n, err := s.store.AttributeLimits(s.ctx)

	s.Require().NoError(err)
	s.GreaterOrEqual(n, int64(2))
	read, err := s.store.LimitGrants.Get(s.ctx, "", grant.ID)
	s.Require().NoError(err)
	s.Equal(new(false), read.Billed)
	found, err := s.store.UsageResets.Select(s.ctx, []string{id}, s.base.Add(-time.Hour))
	s.Require().NoError(err)
	s.Equal(new(false), found[0].Billed)
}

// The release before inserts with no owner: it is refused a place either owner holds, and holds a
// place against both, as the index it knew refused it.
func (s *StoreSuite) TestARowOfNobodyKnownTakesThePlaceOfBothOwners() {
	insert := func(id string, billed *bool) error {
		_, err := s.store.global.NewRaw(`INSERT INTO limits (subject, level, organization_id, billed, "interval", limit_micros, updated_by)
VALUES ('organization', 'organization', ?, ?, 'daily', 5, 'x')`, id, billed).Exec(s.ctx)
		return err
	}
	for _, held := range []*bool{new(true), new(false), nil} {
		id := uuid.NewString()
		s.Require().NoError(insert(id, held))
		s.True(isViolation(insert(id, nil), uniqueViolation))
	}
	id := uuid.NewString()
	s.Require().NoError(insert(id, nil))
	for _, billed := range []bool{true, false} {
		limit := Limit{Subject: usage.SubjectOrganization, Level: LevelOrganization, OrganizationID: id, Billed: &billed, Interval: usage.Daily, UpdatedBy: "x"}
		var exists *ErrLimitExists
		s.Require().True(errors.As(s.store.Limits.Insert(s.ctx, &limit, limitAudit), &exists))
		s.Zero(exists.ID)
	}
	both := uuid.NewString()
	s.Require().NoError(insert(both, new(true)))
	s.Require().NoError(insert(both, new(false)))
	s.True(isViolation(insert(both, new(true)), uniqueViolation))
	s.True(isViolation(insert(both, new(false)), uniqueViolation))
}

func (s *StoreSuite) TestAWrittenRowCountsTowardItsOwnersGrantAlone() {
	id, member := uuid.NewString(), uuid.NewString()
	s.member(member, id)
	organizations := s.grant(LimitGrant{OrganizationID: id, AmountMicros: 10_000})
	apps := s.grant(LimitGrant{OrganizationID: id, Billed: new(false), AmountMicros: 10_000})
	unknown := s.grant(LimitGrant{OrganizationID: id, AmountMicros: 10_000})
	s.before("limit_grants", unknown.ID)
	row := func(app, organization string, cost int64) Request {
		written := s.unresolved(app, cost, 0)
		written.OrganizationID, written.LimitGrants = organization, []int64{organizations.ID, apps.ID, unknown.ID}
		return written
	}

	s.writeAsTheWriter(s.store, row(id, "", 1), row(member, id, 10), row(member, "", 100), row(uuid.NewString(), id, 1_000))

	s.EqualValues(1_110, s.used(organizations.ID))
	s.EqualValues(1, s.used(apps.ID))
	s.EqualValues(1_111, s.used(unknown.ID), "as it counted before rows said whose they are")
}

// An app recorded in an organization since draws on no grant it has as an app in none.
func (s *StoreSuite) TestARowOfAnAppInAnOrganizationCountsTowardNoGrantOfItsOwn() {
	app := uuid.NewString()
	grant := s.grant(LimitGrant{OrganizationID: app, Billed: new(false), AmountMicros: 10_000})
	s.member(app, uuid.NewString())

	s.writeAsTheWriter(s.store, s.unresolved(app, 100, grant.ID))

	s.Zero(s.used(grant.ID))
}

func (s *StoreSuite) TestGrantsOfNobodyKnownAreListedOnlyUnattributed() {
	id := uuid.NewString()
	grant := s.grant(LimitGrant{OrganizationID: id})
	s.before("limit_grants", grant.ID)

	owned, err := s.store.LimitGrants.Select(s.ctx, GrantFilter{Owner: id})
	s.Require().NoError(err)
	s.Empty(owned)
	all, err := s.store.LimitGrants.Select(s.ctx, GrantFilter{Owner: id, Unattributed: true})
	s.Require().NoError(err)
	s.Len(all, 1)
}
