//go:build integration

package store

func (s *StoreSuite) TestAKnowledgeBaseIsKeptWhereItWasFirstClaimed() {
	eu := KnowledgePlace{Region: "eu", Account: KnowledgeAccountStream}
	_, recorded, err := s.store.KnowledgeNamespaces.GetPlace(s.ctx, "acme-app", "docs")
	s.Require().NoError(err)
	s.False(recorded)

	kept, err := s.store.KnowledgeNamespaces.Claim(s.ctx, "acme-app", "docs", eu)
	s.Require().NoError(err)
	s.Equal(eu, kept)
	kept, err = s.store.KnowledgeNamespaces.Claim(s.ctx, "acme-app", "docs", KnowledgePlace{Region: "us", Account: KnowledgeAccountCustomer})
	s.Require().NoError(err)
	s.Equal(eu, kept, "a second claim answers where the base already is")
	place, recorded, err := s.store.KnowledgeNamespaces.GetPlace(s.ctx, "acme-app", "docs")
	s.Require().NoError(err)
	s.True(recorded)
	s.Equal(eu, place)
	_, recorded, err = s.store.KnowledgeNamespaces.GetPlace(s.ctx, "other-app", "docs")
	s.Require().NoError(err)
	s.False(recorded, "another customer's base of the same name is its own")

	s.Require().NoError(s.store.KnowledgeNamespaces.Delete(s.ctx, "acme-app", "docs"))
	us := KnowledgePlace{Region: "us", Account: KnowledgeAccountStream}
	kept, err = s.store.KnowledgeNamespaces.Claim(s.ctx, "acme-app", "docs", us)
	s.Require().NoError(err)
	s.Equal(us, kept, "a base made again is kept where its first write goes")
}

func (s *StoreSuite) TestAnInvalidKnowledgePlaceIsNotClaimed() {
	var invalid *ValidationError
	_, err := s.store.KnowledgeNamespaces.Claim(s.ctx, "acme-app", "docs", KnowledgePlace{Region: "EU West", Account: KnowledgeAccountStream})
	s.Require().ErrorAs(err, &invalid)
	s.Equal("region", invalid.Field)
	_, err = s.store.KnowledgeNamespaces.Claim(s.ctx, "acme-app", "docs", KnowledgePlace{Region: "eu", Account: "theirs"})
	s.Require().ErrorAs(err, &invalid)
	s.Equal("account", invalid.Field)

	_, recorded, err := s.store.KnowledgeNamespaces.GetPlace(s.ctx, "acme-app", "docs")
	s.Require().NoError(err)
	s.False(recorded)
}

func (s *StoreSuite) TestKnowledgeNamespaceQueryBudgets() {
	for range 20 {
		_, err := s.store.KnowledgeNamespaces.Claim(s.ctx, "acme-app", newID(), KnowledgePlace{Region: "eu", Account: KnowledgeAccountCustomer})
		s.Require().NoError(err)
	}
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.KnowledgeNamespaces.Claim(s.ctx, "acme-app", "docs", KnowledgePlace{Region: "us", Account: KnowledgeAccountStream})
		s.Require().NoError(err)
	}))
	s.Equal(1, s.countQueries(func() {
		_, _, err := s.store.KnowledgeNamespaces.GetPlace(s.ctx, "acme-app", "docs")
		s.Require().NoError(err)
	}))
	s.Equal(1, s.countQueries(func() {
		kept, err := s.store.KnowledgeNamespaces.SelectInCustomerAccounts(s.ctx)
		s.Require().NoError(err)
		s.Len(kept, 20, "only the bases in customers' own accounts")
	}))
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.KnowledgeNamespaces.Delete(s.ctx, "acme-app", "docs")) }))
}
