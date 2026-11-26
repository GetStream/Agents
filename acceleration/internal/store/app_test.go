//go:build integration

package store

import (
	"errors"

	"github.com/GetStream/Vision-Agents/acceleration/internal/shardmap"
)

// app stores an organization and an app inside it, which is what a key needs to exist.
func (s *StoreSuite) app() App {
	org := Organization{Name: "acme"}
	s.Require().NoError(s.store.Organizations.Insert(s.ctx, &org))

	app := App{OrganizationID: org.ID, Name: "production"}
	s.Require().NoError(s.store.Apps.Insert(s.ctx, &app))
	return app
}

func (s *StoreSuite) TestAnAppNeedsAnOrganizationAndAName() {
	s.Error(s.store.Apps.Insert(s.ctx, &App{Name: "production"}))
	s.Error(s.store.Apps.Insert(s.ctx, &App{OrganizationID: "org-1"}))
}

func (s *StoreSuite) TestAnAppKeepsTheIDItWasGivenAndIsReadBack() {
	org := Organization{Name: "acme"}
	s.Require().NoError(s.store.Organizations.Insert(s.ctx, &org))
	app := App{ID: "moved-app", OrganizationID: org.ID, Name: "production"}
	s.Require().NoError(s.store.Apps.Insert(s.ctx, &app))

	read, err := s.store.Apps.Get(s.ctx, "moved-app")
	s.Require().NoError(err)
	s.Equal(org.ID, read.OrganizationID)
	_, err = s.store.Apps.Get(s.ctx, "never-made")
	s.ErrorIs(err, ErrNoApp)
}

func (s *StoreSuite) TestAnAppsSettingsAreItsOwnAndAnUnknownAppAdmitsEverybody() {
	app, other := s.app(), s.app()
	_, err := s.store.GlobalDB().NewUpdate().Model((*App)(nil)).
		Set(`settings = '{"allow_anonymous": false, "agent_teams": true}'`).Where("id = ?", app.ID).Exec(s.ctx)
	s.Require().NoError(err)

	settings, err := s.store.Apps.GetSettings(s.ctx, app.ID)
	s.Require().NoError(err)
	s.False(settings.AnonymousAllowed())
	s.True(settings.GuestAllowed())
	s.True(settings.AgentTeamsChecked())
	for _, id := range []string{other.ID, "never-made"} {
		settings, err := s.store.Apps.GetSettings(s.ctx, id)
		s.Require().NoError(err)
		s.Equal(AppSettings{}, settings, id)
	}
	_, err = s.store.Apps.GetSettings(s.ctx, "")
	s.Error(err)
}

func (s *StoreSuite) TestAnAppIsKnownWhenRegisteredOrSeenWithAnOrganization() {
	registered := s.app()
	s.Require().NoError(s.store.AppOrganizations.Upsert(s.ctx, &AppOrganization{AppID: "seen-app", OrganizationID: "org-1"}))

	known, err := s.store.Apps.Known(s.ctx, []string{registered.ID, "seen-app", "elsewhere"})
	s.Require().NoError(err)
	s.Equal(map[string]bool{registered.ID: true, "seen-app": true}, known)
}

func (s *StoreSuite) TestAppQueryBudgets() {
	org := Organization{Name: "acme"}
	s.Require().NoError(s.store.Organizations.Insert(s.ctx, &org))
	app := App{OrganizationID: org.ID, Name: "production"}
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.Apps.Insert(s.ctx, &app)) }))
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.Apps.Get(s.ctx, app.ID)
		s.Require().NoError(err)
	}))
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.Apps.Known(s.ctx, []string{app.ID, "a", "b"})
		s.Require().NoError(err)
	}), "registered and seen apps in one union")
	settings := func() {
		_, err := s.store.Apps.GetSettings(s.ctx, app.ID)
		s.Require().NoError(err)
	}
	s.Equal(1, s.countQueries(settings))
	if s.store.cache != nil {
		settings()
		s.Equal(0, s.countQueries(func() { s.Equal(0, s.countRedis(settings)) }), "then the local cache answers")
	}
}

func (s *StoreSuite) TestAnAppsRoutingPolicyIsWrittenWithoutTouchingItsOtherSettings() {
	app := s.app()
	_, err := s.store.GlobalDB().NewUpdate().Model((*App)(nil)).
		Set(`settings = '{"allow_anonymous": false}'`).Where("id = ?", app.ID).Exec(s.ctx)
	s.Require().NoError(err)

	s.Require().NoError(s.store.Apps.SetRoutingPolicy(s.ctx, app.ID, shardmap.PolicyEUOnly))

	read, err := s.store.Apps.Get(s.ctx, app.ID)
	s.Require().NoError(err)
	s.Equal("eu_only", read.Settings.RoutingPolicy)
	s.False(read.Settings.AnonymousAllowed())
}

func (s *StoreSuite) TestAnAppsRoutingPolicyIsReadBackAtOnceAfterACachedRead() {
	app := s.app()
	before, err := s.store.Apps.RoutingPolicy(s.ctx, app.ID, "")
	s.Require().NoError(err)
	s.Equal(shardmap.PolicyClosest, before)

	s.Require().NoError(s.store.Apps.SetRoutingPolicy(s.ctx, app.ID, shardmap.PolicyUSEast))

	after, err := s.store.Apps.RoutingPolicy(s.ctx, app.ID, "")
	s.Require().NoError(err)
	s.Equal(shardmap.PolicyUSEast, after)
}

func (s *StoreSuite) TestAnAppsRoutingPolicyWhoseCommitAnswerIsLostIsReadBackAtOnce() {
	if s.store.cache == nil {
		s.T().Skipf("%s not set", redisAddressVar)
	}
	app := s.app()
	_, err := s.store.Apps.RoutingPolicy(s.ctx, app.ID, "")
	s.Require().NoError(err)
	_, lossy := s.lossyStore(false)

	s.Require().Error(lossy.Apps.SetRoutingPolicy(s.ctx, app.ID, shardmap.PolicyEUOnly), "the COMMIT's answer never comes")

	policy, err := s.store.Apps.RoutingPolicy(s.ctx, app.ID, "")
	s.Require().NoError(err)
	s.Equal(shardmap.PolicyEUOnly, policy, "the COMMIT landed, and the cached settings were dropped with no answer to it")
}

func (s *StoreSuite) TestClearingAnAppsRoutingPolicyMakesItClosest() {
	app := s.app()
	s.Require().NoError(s.store.Apps.SetRoutingPolicy(s.ctx, app.ID, shardmap.PolicyUSOnly))

	s.Require().NoError(s.store.Apps.SetRoutingPolicy(s.ctx, app.ID, ""))

	read, err := s.store.Apps.Get(s.ctx, app.ID)
	s.Require().NoError(err)
	s.Empty(read.Settings.RoutingPolicy)
	policy, err := s.store.Apps.RoutingPolicy(s.ctx, app.ID, "")
	s.Require().NoError(err)
	s.Equal(shardmap.PolicyClosest, policy)
}

func (s *StoreSuite) TestARoutingPolicyThatIsNoneIsRefused() {
	app := s.app()

	err := s.store.Apps.SetRoutingPolicy(s.ctx, app.ID, "eu")

	invalid, ok := errors.AsType[*ValidationError](err)
	s.Require().True(ok, "%v", err)
	s.Equal("routing_policy", invalid.Field)
}

func (s *StoreSuite) TestARoutingPolicyForAnAppWithNoRowIsRefused() {
	s.ErrorIs(s.store.Apps.SetRoutingPolicy(s.ctx, "never-made", shardmap.PolicyEUOnly), ErrNoApp)
}

func (s *StoreSuite) TestAnAgentsRoutingPolicyWinsOverItsAppsAndAnAgentWithNoneFollowsTheApp() {
	app := s.app()
	s.Require().NoError(s.store.Apps.SetRoutingPolicy(s.ctx, app.ID, shardmap.PolicyUSEast))
	kept := &AgentConfig{CustomerID: app.ID, Name: "kept", RoutingPolicy: "eu_only"}
	s.Require().NoError(s.store.AgentConfigs.Insert(s.ctx, kept))
	following := &AgentConfig{CustomerID: app.ID, Name: "following"}
	s.Require().NoError(s.store.AgentConfigs.Insert(s.ctx, following))

	policy, err := s.store.Apps.RoutingPolicy(s.ctx, app.ID, kept.ID)
	s.Require().NoError(err)
	s.Equal(shardmap.PolicyEUOnly, policy)
	policy, err = s.store.Apps.RoutingPolicy(s.ctx, app.ID, following.ID)
	s.Require().NoError(err)
	s.Equal(shardmap.PolicyUSEast, policy)
}

func (s *StoreSuite) TestAnAgentsRoutingPolicyIsKeptByAnUpdateAndReadBackAfterIt() {
	app := s.app()
	config := &AgentConfig{CustomerID: app.ID, Name: "kept", RoutingPolicy: "canada_only"}
	s.Require().NoError(s.store.AgentConfigs.Insert(s.ctx, config))
	_, err := s.store.Apps.RoutingPolicy(s.ctx, app.ID, config.ID)
	s.Require().NoError(err)

	config.RoutingPolicy = "us_only"
	s.Require().NoError(s.store.AgentConfigs.Update(s.ctx, config))

	policy, err := s.store.Apps.RoutingPolicy(s.ctx, app.ID, config.ID)
	s.Require().NoError(err)
	s.Equal(shardmap.PolicyUSOnly, policy)
}

func (s *StoreSuite) TestTheRoutingPolicyOfAConfigTheAppDoesNotHoldIsAnError() {
	app := s.app()

	_, err := s.store.Apps.RoutingPolicy(s.ctx, app.ID, "never-made")
	s.ErrorIs(err, ErrNoAgentConfig)
}

func (s *StoreSuite) TestAWarmRoutingPolicyReadSendsNoQuery() {
	if s.store.cache == nil {
		s.T().Skip("the store has no Redis to cache in")
	}
	app := s.app()
	config := &AgentConfig{CustomerID: app.ID, Name: "warm"}
	s.Require().NoError(s.store.AgentConfigs.Insert(s.ctx, config))
	read := func() {
		_, err := s.store.Apps.RoutingPolicy(s.ctx, app.ID, config.ID)
		s.Require().NoError(err)
	}
	read()

	s.Equal(0, s.countQueries(read))
}
