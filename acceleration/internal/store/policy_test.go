//go:build integration

package store

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

func (s *StoreSuite) TestAScopeNobodyWroteAPolicyForHasTheEmptyOne() {
	document, err := s.store.Policies.Get(s.ctx, ScopeApp, "app-1")

	s.Require().NoError(err)
	s.Equal(PolicyDocument{}, document)
}

func (s *StoreSuite) TestSavingAPolicyReplacesWhatWasThere() {
	no, yes := false, true
	s.Require().NoError(s.store.Policies.Upsert(s.ctx, &Policy{Scope: ScopeOrganization, ScopeID: "org-1", Document: PolicyDocument{
		Tags:            map[string]string{"team": "a"},
		DataPolicy:      PolicyDataPolicy{AllowTraining: &no, Retention: "30d"},
		PromptInjection: &yes,
	}}))
	s.Require().NoError(s.store.Policies.Upsert(s.ctx, &Policy{Scope: ScopeOrganization, ScopeID: "org-1", Document: PolicyDocument{
		PromptInjection: &no,
	}}))

	document, err := s.store.Policies.Get(s.ctx, ScopeOrganization, "org-1")
	s.Require().NoError(err)
	s.Nil(document.Tags)
	s.False(document.DataPolicy.Floor().Asks())
	s.Require().NotNil(document.PromptInjection)
	s.False(*document.PromptInjection)

	app, err := s.store.Policies.Get(s.ctx, ScopeApp, "org-1")
	s.Require().NoError(err)
	s.Nil(app.PromptInjection, "an app and an organization sharing an id are different scopes")
}

func (s *StoreSuite) TestSavingAnOrganizationsPolicyWithoutTheRequirementKeepsTheStoredOne() {
	// A save over HTTP leaves the requirement out, whatever the document it read said, so
	// one landing after the operator's write keeps what the operator set.
	yes, no := true, false
	s.Require().NoError(s.store.Policies.Upsert(s.ctx, &Policy{Scope: ScopeOrganization, ScopeID: "org-1", Document: PolicyDocument{RequireOwnStreamApp: &yes}}))
	s.Require().NoError(s.store.Policies.Upsert(s.ctx, &Policy{Scope: ScopeOrganization, ScopeID: "org-1", Document: PolicyDocument{PromptInjection: &yes}}))

	document, err := s.store.Policies.Get(s.ctx, ScopeOrganization, "org-1")
	s.Require().NoError(err)
	s.Require().NotNil(document.PromptInjection)
	s.True(*document.PromptInjection)
	s.Require().NotNil(document.RequireOwnStreamApp, "a save that leaves it out keeps the operator's")
	s.True(*document.RequireOwnStreamApp)

	s.Require().NoError(s.store.Policies.Upsert(s.ctx, &Policy{Scope: ScopeOrganization, ScopeID: "org-1", Document: PolicyDocument{RequireOwnStreamApp: &no}}))
	document, err = s.store.Policies.Get(s.ctx, ScopeOrganization, "org-1")
	s.Require().NoError(err)
	s.Require().NotNil(document.RequireOwnStreamApp)
	s.False(*document.RequireOwnStreamApp, "the operator still turns it off")
}

func (s *StoreSuite) TestAUsersPolicyIsKeptUnderTheirApp() {
	messages := int64(3)
	s.Require().NoError(s.store.Policies.Upsert(s.ctx, &Policy{Scope: ScopeUser, ScopeID: UserScopeID("app-1", "ann"), Document: PolicyDocument{
		Quota: &Quota{MessagesPerUserPerDay: &messages},
	}}))

	own, err := s.store.Policies.Get(s.ctx, ScopeUser, UserScopeID("app-1", "ann"))
	s.Require().NoError(err)
	s.Require().NotNil(own.Quota)
	s.Equal(&messages, own.Quota.MessagesPerUserPerDay)
	other, err := s.store.Policies.Get(s.ctx, ScopeUser, UserScopeID("app-2", "ann"))
	s.Require().NoError(err)
	s.Nil(other.Quota, "the same user id in another app is another user")
}

func (s *StoreSuite) TestAnAppsUsersAreListedByIDAndOnlyTheirs() {
	for _, id := range []string{
		UserScopeID("app_1", "bea"), UserScopeID("app_1", "ann"), UserScopeID("app_1", "Cal"),
		UserScopeID("app_1x", "dan"), UserScopeID("appa1", "eve"),
	} {
		s.Require().NoError(s.store.Policies.Upsert(s.ctx, &Policy{Scope: ScopeUser, ScopeID: id, Document: PolicyDocument{}}))
	}
	s.Require().NoError(s.store.Policies.Upsert(s.ctx, &Policy{Scope: ScopeApp, ScopeID: "app_1", Document: PolicyDocument{}}))

	first, err := s.store.Policies.SelectUsers(s.ctx, "app_1", "", 2)
	s.Require().NoError(err)
	rest, err := s.store.Policies.SelectUsers(s.ctx, "app_1", "Cal", 2)
	s.Require().NoError(err)

	s.Equal([]string{"app_1/Cal", "app_1/ann", "app_1/bea"}, scopeIDs(first), "byte order, and one past the page")
	s.Equal([]string{"app_1/ann", "app_1/bea"}, scopeIDs(rest))
	has, err := s.store.Policies.HasUsers(s.ctx, "app_1")
	s.Require().NoError(err)
	s.True(has)
	none, err := s.store.Policies.HasUsers(s.ctx, "app")
	s.Require().NoError(err)
	s.False(none, "an _ in an app id is matched as itself")
}

func (s *StoreSuite) TestDeletingAPolicySaysWhetherThereWasOne() {
	s.Require().NoError(s.store.Policies.Upsert(s.ctx, &Policy{Scope: ScopeUser, ScopeID: UserScopeID("app-1", "ann"), Document: PolicyDocument{}}))

	s.Require().NoError(s.store.Policies.Delete(s.ctx, ScopeUser, UserScopeID("app-1", "ann")))
	s.ErrorIs(s.store.Policies.Delete(s.ctx, ScopeUser, UserScopeID("app-1", "ann")), ErrNoPolicy)

	has, err := s.store.Policies.HasUsers(s.ctx, "app-1")
	s.Require().NoError(err)
	s.False(has)
}

func (s *StoreSuite) TestAUserScopeIDSplitsAtItsLastSlash() {
	app, user, ok := SplitUserScopeID(UserScopeID("app-1", "ann lee"))
	s.True(ok)
	s.Equal("app-1", app)
	s.Equal("ann lee", user)
	for _, bad := range []string{"", "app-1", "/ann", "app-1/"} {
		_, _, ok := SplitUserScopeID(bad)
		s.False(ok, bad)
	}
}

func scopeIDs(policies []Policy) []string {
	ids := make([]string, len(policies))
	for i, policy := range policies {
		ids[i] = policy.ScopeID
	}
	return ids
}

func (s *StoreSuite) TestAPolicySavedOnOneReplicaIsReadByTheOther() {
	replica := s.replica()
	app := "app-" + uuid.NewString()
	was, err := replica.Policies.Get(s.ctx, ScopeApp, app)
	s.Require().NoError(err)
	s.Nil(was.AllowedModels)

	allowed := []string{"openai/gpt-5"}
	s.Require().NoError(s.store.Policies.Upsert(s.ctx, &Policy{Scope: ScopeApp, ScopeID: app, Document: PolicyDocument{AllowedModels: &allowed}}))

	s.Eventually(func() bool {
		now, err := replica.Policies.Get(s.ctx, ScopeApp, app)
		return err == nil && now.AllowedModels != nil && slices.Equal(*now.AllowedModels, allowed)
	}, 2*time.Second, 10*time.Millisecond, "the other replica still routes on the old policy")
}

func (s *StoreSuite) TestAUserPolicyDeletedOnOneReplicaIsGoneOnTheOther() {
	replica := s.replica()
	app := "app-" + uuid.NewString()
	limit := int64(3)
	s.Require().NoError(s.store.Policies.Upsert(s.ctx, &Policy{Scope: ScopeUser, ScopeID: UserScopeID(app, "ann"), Document: PolicyDocument{Quota: &Quota{MessagesPerUserPerDay: &limit}}}))
	has, err := replica.Policies.HasUsers(s.ctx, app)
	s.Require().NoError(err)
	s.True(has)

	s.Require().NoError(s.store.Policies.Delete(s.ctx, ScopeUser, UserScopeID(app, "ann")))

	s.Eventually(func() bool {
		has, err := replica.Policies.HasUsers(s.ctx, app)
		return err == nil && !has
	}, 2*time.Second, 10*time.Millisecond, "the other replica still looks up the app's users")
}

// The replica that saved answers its own next read with the new policy at once. Its old copy
// is in its local cache before every save, and the race with its invalidation is lost
// rarely, so many saves run side by side, as a busy replica's do.
func (s *StoreSuite) TestAPolicySavedOnAReplicaIsWhatThatReplicaReadsNext() {
	if s.store.cache == nil {
		s.T().Skipf("%s not set", redisAddressVar)
	}
	s.readsThePolicyItSaved(s.store)
}

// On a cluster the policy and the app's list of policies sit in different slots, and each
// node sends its own invalidations.
func (s *StoreSuite) TestAPolicySavedOnAReplicaOnAClusterIsWhatThatReplicaReadsNext() {
	s.readsThePolicyItSaved(s.clusterReplica())
}

// readsThePolicyItSaved saves many policies on store side by side, each read twice before,
// and fails for every read after a save that fails or answers with the policy from before.
func (s *StoreSuite) readsThePolicyItSaved(store *Store) {
	limit := int64(7)
	var stale atomic.Int64
	var saves sync.WaitGroup
	prefix := uuid.NewString()
	for writer := range 8 {
		saves.Go(func() {
			for attempt := range 200 {
				scopeID := fmt.Sprintf("app-%s-%d-%d", prefix, writer, attempt)
				for range 2 {
					if _, err := store.Policies.Get(s.ctx, ScopeApp, scopeID); err != nil {
						stale.Add(1)
					}
				}
				policy := Policy{Scope: ScopeApp, ScopeID: scopeID, Document: PolicyDocument{Quota: &Quota{MessagesPerUserPerDay: &limit}}}
				if err := store.Policies.Upsert(s.ctx, &policy); err != nil {
					stale.Add(1)
				}
				if now, err := store.Policies.Get(s.ctx, ScopeApp, scopeID); err != nil || now.Quota == nil {
					stale.Add(1)
				}
			}
		})
	}
	saves.Wait()
	s.Zero(stale.Load(), "reads that failed or answered with the policy from before the save")
}

func (s *StoreSuite) TestAnAppsPoliciesAreItsOwnAndThoseOfTheOrganizationItWasLastSeenUnder() {
	yes, no := true, false
	s.Require().NoError(s.store.Policies.Upsert(s.ctx, &Policy{Scope: ScopeApp, ScopeID: "app-1", Document: PolicyDocument{PromptInjection: &yes}}))
	s.Require().NoError(s.store.Policies.Upsert(s.ctx, &Policy{Scope: ScopeOrganization, ScopeID: "org-1", Document: PolicyDocument{PromptInjection: &no}}))
	s.Require().NoError(s.store.Policies.Upsert(s.ctx, &Policy{Scope: ScopeOrganization, ScopeID: "org-2", Document: PolicyDocument{Tags: map[string]string{"org": "2"}}}))
	s.Require().NoError(s.store.AppOrganizations.Upsert(s.ctx, &AppOrganization{AppID: "app-1", OrganizationID: "org-2"}))

	read, err := s.store.Policies.OfApp(s.ctx, "app-1")

	s.Require().NoError(err)
	s.Equal(AppPolicies{
		App:            PolicyDocument{PromptInjection: &yes},
		OrganizationID: "org-2",
		Organization:   PolicyDocument{Tags: map[string]string{"org": "2"}},
	}, read)
	alone, err := s.store.Policies.OfApp(s.ctx, "app-2")
	s.Require().NoError(err)
	s.Equal(AppPolicies{}, alone, "an app in no organization with no policy of its own")
}

// appPoliciesOn is what replica reads of the app's policies once its read is what want says.
func (s *StoreSuite) appPoliciesOn(replica *Store, app string, want func(AppPolicies) bool, msg string) {
	s.Eventually(func() bool {
		read, err := replica.Policies.OfApp(s.ctx, app)
		return err == nil && want(read)
	}, 2*time.Second, 10*time.Millisecond, msg)
}

func (s *StoreSuite) TestAnOrganizationsPolicySavedOnOneReplicaIsReadForEachOfItsAppsOnTheOther() {
	replica := s.replica()
	org := "org-" + uuid.NewString()
	apps := []string{"app-" + uuid.NewString(), "app-" + uuid.NewString(), "app-" + uuid.NewString()}
	for _, app := range apps {
		s.Require().NoError(s.store.AppOrganizations.Upsert(s.ctx, &AppOrganization{AppID: app, OrganizationID: org}))
		read, err := replica.Policies.OfApp(s.ctx, app)
		s.Require().NoError(err)
		s.Nil(read.Organization.Tags)
	}

	s.Require().NoError(s.store.Policies.Upsert(s.ctx, &Policy{Scope: ScopeOrganization, ScopeID: org, Document: PolicyDocument{Tags: map[string]string{"team": "a"}}}))

	for _, app := range apps {
		s.appPoliciesOn(replica, app, func(read AppPolicies) bool { return read.Organization.Tags["team"] == "a" },
			"the other replica still decides the app on its organization's old policy")
	}
	s.Require().NoError(s.store.Policies.Delete(s.ctx, ScopeOrganization, org))
	s.appPoliciesOn(replica, apps[0], func(read AppPolicies) bool { return read.Organization.Tags == nil },
		"the other replica still decides the app on its organization's deleted policy")
}

func (s *StoreSuite) TestAnAppsPoliciesFollowItsOwnPolicyAndItsMoveToAnotherOrganizationOnEveryReplica() {
	replica := s.replica()
	app := "app-" + uuid.NewString()
	s.Require().NoError(s.store.Policies.Upsert(s.ctx, &Policy{Scope: ScopeOrganization, ScopeID: app + "-second", Document: PolicyDocument{Tags: map[string]string{"org": "second"}}}))
	s.Require().NoError(s.store.AppOrganizations.Upsert(s.ctx, &AppOrganization{AppID: app, OrganizationID: app + "-first"}))
	read, err := replica.Policies.OfApp(s.ctx, app)
	s.Require().NoError(err)
	s.Equal(app+"-first", read.OrganizationID)

	s.Require().NoError(s.store.AppOrganizations.Upsert(s.ctx, &AppOrganization{AppID: app, OrganizationID: app + "-second"}))
	s.appPoliciesOn(replica, app, func(read AppPolicies) bool {
		return read.OrganizationID == app+"-second" && read.Organization.Tags["org"] == "second"
	}, "the other replica still decides the app under its old organization")

	yes := true
	s.Require().NoError(s.store.Policies.Upsert(s.ctx, &Policy{Scope: ScopeApp, ScopeID: app, Document: PolicyDocument{PromptInjection: &yes}}))
	s.appPoliciesOn(replica, app, func(read AppPolicies) bool { return read.App.PromptInjection != nil },
		"the other replica still decides the app on its old policy")
}

func (s *StoreSuite) TestPolicyQueryBudgets() {
	app := "app-" + uuid.NewString()
	limit := int64(3)
	user := Policy{Scope: ScopeUser, ScopeID: UserScopeID(app, "ann"), Document: PolicyDocument{Quota: &Quota{MessagesPerUserPerDay: &limit}}}

	s.Equal(1, s.countQueries(func() {
		s.Equal(s.redisTrips(1), s.countRedis(func() { s.Require().NoError(s.store.Policies.Upsert(s.ctx, &user)) }),
			"the document and the app's user flag are forgotten in one round trip")
	}))
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.Policies.SelectUsers(s.ctx, app, "", 10)
		s.Require().NoError(err)
	}))
	for name, read := range map[string]func(){
		"get": func() {
			_, err := s.store.Policies.Get(s.ctx, ScopeUser, user.ScopeID)
			s.Require().NoError(err)
		},
		"has users": func() {
			_, err := s.store.Policies.HasUsers(s.ctx, app)
			s.Require().NoError(err)
		},
	} {
		s.Equal(1, s.countQueries(read), name)
		if s.store.cache != nil {
			read()
			s.Equal(0, s.countQueries(func() { s.Equal(0, s.countRedis(read)) }), name+" is then answered locally")
		}
	}
	s.Equal(1, s.countQueries(func() {
		s.Equal(s.redisTrips(1), s.countRedis(func() { s.Require().NoError(s.store.Policies.Delete(s.ctx, ScopeUser, user.ScopeID)) }))
	}))
}

func (s *StoreSuite) TestAppPoliciesQueryBudgets() {
	org := "org-" + uuid.NewString()
	apps := []string{"app-" + uuid.NewString(), "app-" + uuid.NewString(), "app-" + uuid.NewString()}
	yes := true
	for _, app := range apps {
		s.Require().NoError(s.store.AppOrganizations.Upsert(s.ctx, &AppOrganization{AppID: app, OrganizationID: org}))
		s.Require().NoError(s.store.Policies.Upsert(s.ctx, &Policy{Scope: ScopeApp, ScopeID: app, Document: PolicyDocument{PromptInjection: &yes}}))
	}
	organization := Policy{Scope: ScopeOrganization, ScopeID: org, Document: PolicyDocument{Tags: map[string]string{"team": "a"}}}
	s.Require().NoError(s.store.Policies.Upsert(s.ctx, &organization))

	read := func() {
		read, err := s.store.Policies.OfApp(s.ctx, apps[0])
		s.Require().NoError(err)
		s.Equal("a", read.Organization.Tags["team"])
		s.NotNil(read.App.PromptInjection)
	}
	// The app's document, its organization and the organization's document.
	s.Equal(1, s.countQueries(read), "a cold read")
	if s.store.cache != nil {
		read()
		s.Equal(0, s.countQueries(func() { s.Equal(0, s.countRedis(read)) }), "then answered locally")
	}
	// The write, then the apps whose cached policies it changed, all forgotten in one round trip.
	s.Equal(s.redisTrips(1)+1, s.countQueries(func() {
		s.Equal(s.redisTrips(1), s.countRedis(func() { s.Require().NoError(s.store.Policies.Upsert(s.ctx, &organization)) }))
	}), "an organization's save, however many apps it has")
}
