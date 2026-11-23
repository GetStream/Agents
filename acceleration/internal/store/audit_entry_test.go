//go:build integration

package store

func (s *StoreSuite) TestAPriceListChangeIsRecordedWithNoCustomer() {
	entry := AuditEntry{
		ResourceType: AuditResourcePriceList, ResourceID: "change-" + newID(), Action: AuditCreated,
		Source: AuditSourceManage, ActorID: "staff:" + newID(), ActorName: "Łukasz",
		Changes: []AuditChange{{Field: "unit_price_micros", After: 1}},
	}

	s.Require().NoError(s.store.AuditEntries.Insert(s.ctx, &entry))

	var kept AuditEntry
	s.Require().NoError(s.store.GlobalDB().NewSelect().Model(&kept).Where("id = ?", entry.ID).Scan(s.ctx))
	s.Empty(kept.CustomerID)
	s.Equal(AuditSourceManage, kept.Source)
	s.Equal("Łukasz", kept.ActorName)
}

func (s *StoreSuite) TestAPriceListChangeNamingACustomerIsRefused() {
	entry := AuditEntry{
		CustomerID: "acme-app", ResourceType: AuditResourcePriceList, ResourceID: "change-" + newID(),
		Action: AuditCreated, Changes: []AuditChange{{Field: "unit_price_micros", After: 1}},
	}

	s.Error(s.store.AuditEntries.Insert(s.ctx, &entry))
}

func (s *StoreSuite) TestAnAppsChangeWithNoCustomerIsRefused() {
	entry := AuditEntry{
		ResourceType: AuditPolicy, ResourceID: newID(), Action: AuditCreated,
		Changes: []AuditChange{{Field: "budget", After: 1}},
	}

	s.Error(s.store.AuditEntries.Insert(s.ctx, &entry))
}

func (s *StoreSuite) TestNoClientCanNameItselfTheManagementAPI() {
	s.Equal(AuditSourceAPI, AuditSource(AuditSourceManage))
	s.Equal(AuditSourceCLI, AuditSource(AuditSourceCLI))
}

func (s *StoreSuite) TestAuditEntryQueryBudgets() {
	entry := func() *AuditEntry {
		return &AuditEntry{CustomerID: "acme", ResourceType: AuditPolicy, ResourceID: "policy-1", Action: AuditUpdated,
			Changes: []AuditChange{{Field: "budget", Before: 1, After: 2}}}
	}
	for range 5 {
		s.Require().NoError(s.store.AuditEntries.Insert(s.ctx, entry()))
	}
	last := entry()

	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.AuditEntries.Insert(s.ctx, last)) }))
	s.Equal(1, s.countQueries(func() {
		listed, err := s.store.AuditEntries.Select(s.ctx, "acme", AuditEntryFilter{ResourceType: AuditPolicy, Limit: 3})
		s.Require().NoError(err)
		s.Len(listed, 4)
	}))
	s.Equal(1, s.countQueries(func() {
		latest, found, err := s.store.AuditEntries.Latest(s.ctx, "acme", AuditEntryFilter{ResourceType: AuditPolicy, ResourceID: "policy-1"})
		s.Require().NoError(err)
		s.True(found)
		s.Equal(last.ID, latest.ID)
	}))
	s.Equal(1, s.countQueries(func() {
		_, found, err := s.store.AuditEntries.Get(s.ctx, "acme", last.ID)
		s.Require().NoError(err)
		s.True(found)
	}))
}
