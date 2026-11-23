//go:build integration

package store

// The brand is the vendor's: saving the profile again must not forget it was registered.
func (s *StoreSuite) TestSavingABusinessProfileAgainKeepsItsBrand() {
	customerID := newID()
	profile := BusinessProfile{CustomerID: customerID, LegalBusinessName: "Acme Inc", Address: PostalAddress{Country: "US"}}
	s.Require().NoError(s.store.BusinessProfiles.Upsert(s.ctx, &profile))
	s.Require().NoError(s.store.BusinessProfiles.SetBrand(s.ctx, customerID, "brand-1", "VERIFIED"))

	profile = BusinessProfile{CustomerID: customerID, LegalBusinessName: "Acme Corp"}
	s.Require().NoError(s.store.BusinessProfiles.Upsert(s.ctx, &profile))

	held, err := s.store.BusinessProfiles.Get(s.ctx, customerID)
	s.Require().NoError(err)
	s.Equal("Acme Corp", held.LegalBusinessName)
	s.Equal("brand-1", held.VendorBrandID)
	s.Equal("brand-1", profile.VendorBrandID, "the saved row is handed back")
}

func (s *StoreSuite) TestBusinessProfileQueryBudgets() {
	customerID := newID()
	profile := BusinessProfile{CustomerID: customerID, LegalBusinessName: "Acme Inc"}
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.BusinessProfiles.Upsert(s.ctx, &profile)) }), "Upsert")
	s.Equal(1, s.countQueries(func() {
		requested, err := s.store.BusinessProfiles.RequestBrand(s.ctx, customerID, s.base)
		s.Require().NoError(err)
		s.True(requested)
	}), "RequestBrand")
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.BusinessProfiles.ForgetBrandRequest(s.ctx, customerID)) }),
		"ForgetBrandRequest")
	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.BusinessProfiles.SetBrand(s.ctx, customerID, "brand-1", "VERIFIED"))
	}), "SetBrand")
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.BusinessProfiles.Get(s.ctx, customerID)
		s.Require().NoError(err)
	}), "Get")
}
