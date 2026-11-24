//go:build integration

package policy

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/usage"
)

// sameID makes the app in no organization whose id is s.org's, beside s.app, which is in it.
func (s *PolicySuite) sameID() string { return s.org }

func (s *PolicySuite) zeroLimit(billed bool) store.Limit {
	return s.limit(store.Limit{Subject: usage.SubjectOrganization, Level: store.LevelOrganization, OrganizationID: s.org, Billed: &billed})
}

func (s *PolicySuite) TestAnAppsLimitRefusesNoOrganizationOfItsID() {
	alone := s.sameID()
	s.zeroLimit(false)
	s.spendRefused(s.enforcer, alone, ErrBudgetSpent)
	s.never(func() bool { return s.admit(s.enforcer, s.app).Spend != nil }, 300*time.Millisecond, 10*time.Millisecond)
}

func (s *PolicySuite) TestAnOrganizationsLimitRefusesNoAppOfItsID() {
	alone := s.sameID()
	s.zeroLimit(true)
	s.spendRefused(s.enforcer, s.app, ErrBudgetSpent)
	s.never(func() bool { return s.admit(s.enforcer, alone).Spend != nil }, 300*time.Millisecond, 10*time.Millisecond)
}

func (s *PolicySuite) TestBothOwnersOfOneIDAreReadTogether() {
	alone := s.sameID()
	s.zeroLimit(true)
	s.limit(store.Limit{Subject: usage.SubjectOrganization, Level: store.LevelOrganization, OrganizationID: s.org, Billed: new(false), LimitMicros: 0})

	s.spendRefused(s.enforcer, s.app, ErrBudgetSpent)
	s.spendRefused(s.enforcer, alone, ErrBudgetSpent)
}

func (s *PolicySuite) TestALimitOfNobodyKnownHoldsBothOwners() {
	alone := s.sameID()
	limit := s.zeroLimit(true)
	_, err := s.store.ShardDB().NewRaw("UPDATE limits SET billed = NULL WHERE id = ?", limit.ID).Exec(s.ctx)
	s.Require().NoError(err)

	s.spendRefused(s.enforcer, s.app, ErrBudgetSpent)
	s.spendRefused(s.enforcer, alone, ErrBudgetSpent)
}

func (s *PolicySuite) TestALimitWrittenHereHoldsItsOwnersNextAdmission() {
	alone := s.sameID()
	enforcer := s.newEnforcer(s.store, nil, func(settings *Settings) { settings.Fresh = time.Hour })
	s.Require().NoError(s.admit(enforcer, s.app).Spend)
	s.Require().NoError(s.admit(enforcer, alone).Spend)
	s.zeroLimit(false)
	s.zeroLimit(true)

	enforcer.LimitsChanged(s.ctx, store.AppOwner(alone))
	s.NoError(s.admit(enforcer, s.app).Spend, "the other owner's are read in their own time")
	s.spendRefused(enforcer, alone, ErrBudgetSpent)

	enforcer.LimitsChanged(s.ctx, s.org)
	s.spendRefused(enforcer, s.app, ErrBudgetSpent)
}

func (s *PolicySuite) TestAGrantAdmitsItsOwnersWorkAlone() {
	alone := s.sameID()
	for _, billed := range []bool{true, false} {
		s.zeroLimit(billed)
	}
	for _, billed := range []bool{true, false} {
		grant := store.LimitGrant{OrganizationID: s.org, Billed: &billed, Subject: usage.SubjectOrganization, AmountMicros: 1_000_000, Reason: "launch", Actor: "test"}
		s.Require().NoError(s.store.LimitGrants.Insert(s.ctx, &grant, store.LimitAudit{CustomerID: s.app, Source: store.AuditSourceAPI}))
		drawer, other := s.app, alone
		if !billed {
			drawer, other = alone, s.app
		}
		s.eventually(func() bool {
			admitted, err := s.enforcer.Admit(s.ctx, ownerOf(drawer))
			return err == nil && admitted.Spend == nil && len(admitted.Grants) == 1 && admitted.Grants[0] == grant.ID
		}, 5*time.Second, 10*time.Millisecond, "its owner draws on it")
		s.never(func() bool {
			admitted, err := s.enforcer.Admit(s.ctx, ownerOf(other))
			return err == nil && !errors.Is(admitted.Spend, ErrBudgetSpent)
		}, 300*time.Millisecond, 10*time.Millisecond)
		_, err := s.store.LimitGrants.Revoke(s.ctx, "", grant.ID, "test", "done", store.LimitAudit{CustomerID: s.app, Source: store.AuditSourceAPI})
		s.Require().NoError(err)
		s.spendRefused(s.enforcer, drawer, ErrBudgetSpent)
	}
}

func (s *PolicySuite) TestAResetCountsForItsOwnerAlone() {
	alone := s.sameID()
	one := int64(1_000_000)
	for _, billed := range []bool{true, false} {
		s.limit(store.Limit{Subject: usage.SubjectOrganization, Level: store.LevelOrganization, OrganizationID: s.org, Billed: &billed, LimitMicros: one})
	}
	s.spend(s.app, one)
	s.spend(alone, one)
	s.spendRefused(s.enforcer, s.app, ErrBudgetSpent)
	s.spendRefused(s.enforcer, alone, ErrBudgetSpent)
	start, _ := usage.Window(usage.Daily, time.Now())
	reset := func(billed bool) {
		s.Require().NoError(s.store.UsageResets.InsertMany(s.ctx, []store.UsageReset{{OrganizationID: s.org, Billed: &billed,
			Subject: usage.SubjectOrganization, Interval: usage.Daily, WindowStart: start, BaselineMicros: one, Reason: "x", Actor: "test"}},
			store.LimitAudit{CustomerID: s.app, Source: store.AuditSourceAPI}))
	}

	reset(false)
	s.eventually(func() bool { return s.admit(s.enforcer, alone).Spend == nil }, 5*time.Second, 10*time.Millisecond)
	s.never(func() bool { return s.admit(s.enforcer, s.app).Spend == nil }, 300*time.Millisecond, 10*time.Millisecond)
	reset(true)
	s.eventually(func() bool { return s.admit(s.enforcer, s.app).Spend == nil }, 5*time.Second, 10*time.Millisecond)
}

// Two routers share holds: what one holds for an app in no organization is held against that
// app's limits in the other, and not against the credit of the organization of its id.
func (s *PolicySuite) TestAnAppsHoldsAreSharedWithItsOwnAccountAlone() {
	_, redis := s.redis()
	a, b := s.billed(sharing(redis)), s.billed(sharing(redis))
	b.Join(s.ctx, s.app, s.org, openTier)
	alone := appAccount(s.sameID())
	remoteAlone := func() int64 {
		_, _ = b.Admit(context.Background(), ownerOf(s.sameID()))
		b.money.mu.Lock()
		defer b.money.mu.Unlock()
		if acct := b.money.accounts[alone]; acct != nil {
			return acct.remoteOrganization(alone)
		}
		return 0
	}
	s.Require().NoError(a.Reserve(ownerOf(s.sameID()), uuid.Must(uuid.NewV7()), 1_000, time.Minute))

	s.eventually(func() bool { return remoteAlone() == 1_000 }, 5*time.Second, 5*time.Millisecond)
	s.never(func() bool { return remoteHeld(b.Enforcer, s.org) != 0 }, 300*time.Millisecond, 10*time.Millisecond)

	s.usage().Credit(s.T(), s.org, 100_000_000)
	a.Join(s.ctx, s.app, s.org, openTier)
	s.read(a.Enforcer, s.org)
	s.Require().NoError(a.Reserve(s.owner(), uuid.Must(uuid.NewV7()), 7, time.Minute))
	s.eventually(func() bool { return remoteHeld(b.Enforcer, s.org) == 7 }, 5*time.Second, 5*time.Millisecond)
	s.EqualValues(1_000, remoteAlone())
}

func (s *PolicySuite) TestAnAppsGrantHoldsAreSharedWithItsOwnAccount() {
	_, redis := s.redis()
	a, b := s.newEnforcer(s.store, nil, sharing(redis)), s.newEnforcer(s.store, nil, sharing(redis))
	alone := s.sameID()
	s.zeroLimit(false)
	grant := store.LimitGrant{OrganizationID: alone, Billed: new(false), Subject: usage.SubjectOrganization, AmountMicros: 1_000, Reason: "launch", Actor: "test"}
	s.Require().NoError(s.store.LimitGrants.Insert(s.ctx, &grant, store.LimitAudit{CustomerID: alone, Source: store.AuditSourceAPI}))
	for _, enforcer := range []*Enforcer{a, b} {
		s.eventually(func() bool {
			admitted := s.admit(enforcer, alone)
			return admitted.Spend == nil && len(admitted.Grants) == 1
		},
			5*time.Second, 10*time.Millisecond)
	}

	s.Require().NoError(a.Reserve(ownerOf(alone), uuid.Must(uuid.NewV7()), 1_000, time.Minute))

	s.eventually(func() bool { return errors.Is(s.admit(b, alone).Spend, ErrBudgetSpent) }, 5*time.Second, 10*time.Millisecond,
		"the grant is held whole in the other router")
}
