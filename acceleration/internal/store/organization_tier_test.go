//go:build integration

package store

import (
	"time"

	"github.com/google/uuid"
)

func (s *StoreSuite) TestAnOrganizationHasTheTierItWasLastSeenWith() {
	organization := "org-" + uuid.NewString()
	unknown, err := s.store.OrganizationTiers.Get(s.ctx, organization)
	s.Require().NoError(err)
	s.Empty(unknown.Tier)

	s.Require().NoError(s.store.OrganizationTiers.Upsert(s.ctx, &OrganizationTier{OrganizationID: organization, Tier: "tier-a", SeenAt: s.base}))
	s.Require().NoError(s.store.OrganizationTiers.Upsert(s.ctx, &OrganizationTier{OrganizationID: organization, Tier: "tier-b", SeenAt: s.base.Add(time.Second)}))

	tier, err := s.store.OrganizationTiers.Get(s.ctx, organization)
	s.Require().NoError(err)
	s.Equal("tier-b", tier.Tier)
}

func (s *StoreSuite) TestATierSeenEarlierThanTheRecordedOneChangesNothing() {
	// Routers write what they saw in the background, so the older write may land last.
	organization := "org-" + uuid.NewString()
	s.Require().NoError(s.store.OrganizationTiers.Upsert(s.ctx, &OrganizationTier{OrganizationID: organization, Tier: "tier-b", SeenAt: s.base.Add(time.Second)}))
	s.Require().NoError(s.store.OrganizationTiers.Upsert(s.ctx, &OrganizationTier{OrganizationID: organization, Tier: "tier-a", SeenAt: s.base}))

	tier, err := s.store.OrganizationTiers.Get(s.ctx, organization)
	s.Require().NoError(err)
	s.Equal("tier-b", tier.Tier)
}

func (s *StoreSuite) TestOrganizationTierQueryBudgets() {
	organization := "org-" + uuid.NewString()
	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.OrganizationTiers.Upsert(s.ctx, &OrganizationTier{OrganizationID: organization, Tier: "pro", SeenAt: time.Now()}))
	}))
	s.Equal(1, s.countQueries(func() {
		tier, err := s.store.OrganizationTiers.Get(s.ctx, organization)
		s.Require().NoError(err)
		s.Equal("pro", tier.Tier)
	}))
}
