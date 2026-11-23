//go:build integration

package store

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
