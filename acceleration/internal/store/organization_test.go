//go:build integration

package store

func (s *StoreSuite) TestAnOrganizationNeedsANameAndIsGivenAnID() {
	s.Error(s.store.Organizations.Insert(s.ctx, &Organization{}))

	first, second := Organization{Name: "acme"}, Organization{Name: "acme"}
	s.Require().NoError(s.store.Organizations.Insert(s.ctx, &first))
	s.Require().NoError(s.store.Organizations.Insert(s.ctx, &second))
	s.NotEmpty(first.ID)
	s.NotEqual(first.ID, second.ID, "a name is not an organization's identity")
}

func (s *StoreSuite) TestOrganizationQueryBudgets() {
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.Organizations.Insert(s.ctx, &Organization{Name: "acme"})) }))
}
