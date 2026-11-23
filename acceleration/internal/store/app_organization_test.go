//go:build integration

package store

import (
	"slices"
	"time"

	"github.com/google/uuid"
)

func (s *StoreSuite) TestAnAppIsInTheOrganizationItIsRegisteredOrSeenUnder() {
	org := Organization{Name: "registering"}
	s.Require().NoError(s.store.Organizations.Insert(s.ctx, &org))
	registered := App{OrganizationID: org.ID, Name: "registered"}
	s.Require().NoError(s.store.Apps.Insert(s.ctx, &registered))
	s.Require().NoError(s.store.AppOrganizations.Upsert(s.ctx, &AppOrganization{AppID: "app-seen", OrganizationID: org.ID}))

	for app, want := range map[string]bool{registered.ID: true, "app-seen": true, "app-never": false} {
		in, err := s.store.AppOrganizations.Holds(s.ctx, org.ID, app)
		s.Require().NoError(err)
		s.Equal(want, in, app)
		other, err := s.store.AppOrganizations.Holds(s.ctx, "org-other", app)
		s.Require().NoError(err)
		s.False(other, app)
	}
}

func (s *StoreSuite) TestAnOrganizationsAppsAreTheOnesSeenOrRegisteredUnderItAndItsOwn() {
	org := Organization{Name: "registering"}
	s.Require().NoError(s.store.Organizations.Insert(s.ctx, &org))
	registered := App{OrganizationID: org.ID, Name: "registered"}
	s.Require().NoError(s.store.Apps.Insert(s.ctx, &registered))
	moved := App{OrganizationID: org.ID, Name: "moved"}
	s.Require().NoError(s.store.Apps.Insert(s.ctx, &moved))
	s.Require().NoError(s.store.AppOrganizations.Upsert(s.ctx, &AppOrganization{AppID: moved.ID, OrganizationID: "org-other"}))
	s.Require().NoError(s.store.AppOrganizations.Upsert(s.ctx, &AppOrganization{AppID: "app-seen-b", OrganizationID: org.ID}))
	s.Require().NoError(s.store.AppOrganizations.Upsert(s.ctx, &AppOrganization{AppID: "app-seen-a", OrganizationID: org.ID}))
	s.Require().NoError(s.store.AppOrganizations.Upsert(s.ctx, &AppOrganization{AppID: "app-elsewhere", OrganizationID: "org-other"}))

	apps, err := s.store.AppOrganizations.Apps(s.ctx, org.ID)

	s.Require().NoError(err)
	s.Require().NotEmpty(apps)
	s.Equal(org.ID, apps[0], "its own id first")
	s.ElementsMatch([]string{org.ID, registered.ID, "app-seen-a", "app-seen-b"}, apps,
		"not the app a request named another organization for, nor another organization's")
	s.Less(slices.Index(apps, "app-seen-a"), slices.Index(apps, "app-seen-b"), "in order")
}

func (s *StoreSuite) TestAnAppBelongsToTheOrganizationItWasLastSeenUnder() {
	unknown, err := s.store.AppOrganizations.Get(s.ctx, "app-1")
	s.Require().NoError(err)
	s.Empty(unknown.OrganizationID)

	s.Require().NoError(s.store.AppOrganizations.Upsert(s.ctx, &AppOrganization{AppID: "app-1", OrganizationID: "org-1"}))
	s.Require().NoError(s.store.AppOrganizations.Upsert(s.ctx, &AppOrganization{AppID: "app-1", OrganizationID: "org-2"}))

	membership, err := s.store.AppOrganizations.Get(s.ctx, "app-1")
	s.Require().NoError(err)
	s.Equal("org-2", membership.OrganizationID)
}

func (s *StoreSuite) TestAnAppMovedToAnotherOrganizationIsAnsweredUnderTheNewOneOnEveryReplica() {
	replica := s.replica()
	app := "app-" + newID()
	s.Require().NoError(s.store.AppOrganizations.Upsert(s.ctx, &AppOrganization{AppID: app, OrganizationID: "org-first"}))
	under, err := replica.AppOrganizations.Get(s.ctx, app)
	s.Require().NoError(err)
	s.Equal("org-first", under.OrganizationID)

	s.Require().NoError(s.store.AppOrganizations.Upsert(s.ctx, &AppOrganization{AppID: app, OrganizationID: "org-second"}))

	s.Eventually(func() bool {
		under, err := replica.AppOrganizations.Get(s.ctx, app)
		return err == nil && under.OrganizationID == "org-second"
	}, 2*time.Second, 10*time.Millisecond, "the other replica still has the old organization")
}

func (s *StoreSuite) TestAppOrganizationQueryBudgets() {
	registered := s.app()
	seen := "app-" + uuid.NewString()

	s.Equal(1, s.countQueries(func() {
		s.Equal(s.redisTrips(1), s.countRedis(func() {
			s.Require().NoError(s.store.AppOrganizations.Upsert(s.ctx, &AppOrganization{AppID: seen, OrganizationID: "org-1"}))
		}))
	}))
	get := func() {
		_, err := s.store.AppOrganizations.Get(s.ctx, seen)
		s.Require().NoError(err)
	}
	s.Equal(1, s.countQueries(get))
	if s.store.cache != nil {
		get()
		s.Equal(0, s.countQueries(func() { s.Equal(0, s.countRedis(get)) }), "then the local cache answers")
		s.Equal(0, s.countQueries(func() {
			held, err := s.store.AppOrganizations.Holds(s.ctx, "org-1", seen)
			s.Require().NoError(err)
			s.True(held)
		}), "an app seen under an organization is answered from the cache")
	}
	s.Equal(2, s.countQueries(func() {
		held, err := s.store.AppOrganizations.Holds(s.ctx, registered.OrganizationID, registered.ID)
		s.Require().NoError(err)
		s.True(held)
	}), "an app never seen under one is looked up where it was registered")
}

func (s *StoreSuite) TestAnOrganizationsAppsAreReadInOneStatement() {
	app := s.app()
	s.Equal(1, s.countQueries(func() {
		apps, err := s.store.AppOrganizations.Apps(s.ctx, app.OrganizationID)
		s.Require().NoError(err)
		s.Equal([]string{app.OrganizationID, app.ID}, apps)
	}))
	s.Equal(2, s.countQueries(func() {
		_, err := s.store.AppOrganizations.ActivityApps(s.ctx, app.OrganizationID)
		s.Require().NoError(err)
	}), "its apps, then whether the app of its own id is its")
}

func (s *StoreSuite) TestAnAppMovedByACommitThatLandsAfterItGaveUpIsForgottenAgain() {
	proxy, dsn := s.pgProxy(true)
	slow, err := OpenShards(Options{DSN: dsn, ShardDSN: s.shardDSN, Redis: s.redis(), writeTimeout: 300 * time.Millisecond})
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.NoError(slow.Close()) })
	app := "app-" + newID()
	s.Require().NoError(s.store.AppOrganizations.Upsert(s.ctx, &AppOrganization{AppID: app, OrganizationID: "org-before"}))

	s.Require().Error(slow.AppOrganizations.Upsert(s.ctx, &AppOrganization{AppID: app, OrganizationID: "org-after"}),
		"the COMMIT is held past the read timeout")
	seen, err := s.store.AppOrganizations.Get(s.ctx, app)
	s.Require().NoError(err)
	s.Require().Equal("org-before", seen.OrganizationID, "the app has not moved while the COMMIT is held")
	proxy.let()
	s.Require().Eventually(func() bool {
		moved, err := s.store.global.NewSelect().Model((*AppOrganization)(nil)).
			Where("app_id = ? AND organization_id = ?", app, "org-after").Exists(s.ctx)
		s.Require().NoError(err)
		return moved
	}, 5*time.Second, 10*time.Millisecond, "the held COMMIT lands")

	s.Eventually(func() bool {
		seen, err := s.store.AppOrganizations.Get(s.ctx, app)
		s.Require().NoError(err)
		return seen.OrganizationID == "org-after"
	}, 5*time.Second, 10*time.Millisecond, "the copy cached before the COMMIT landed is dropped again")
}
