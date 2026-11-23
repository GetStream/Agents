//go:build integration

package store

func (s *StoreSuite) TestAnAppsTelephonyIsSuspendedUntilLifted() {
	_, err := s.store.TelephonySuspensions.Get(s.ctx, "app")
	s.ErrorIs(err, ErrNotSuspended)

	s.Require().NoError(s.store.TelephonySuspensions.Upsert(s.ctx, &TelephonySuspension{CustomerID: "app", Reason: "fraud", SuspendedBy: "ann"}))
	s.Require().NoError(s.store.TelephonySuspensions.Upsert(s.ctx, &TelephonySuspension{CustomerID: "app", Reason: "unpaid", SuspendedBy: "bob"}))
	suspension, err := s.store.TelephonySuspensions.Get(s.ctx, "app")
	s.Require().NoError(err)
	s.Equal("unpaid", suspension.Reason, "a second suspension replaces the reason")
	s.Equal("bob", suspension.SuspendedBy)
	_, err = s.store.TelephonySuspensions.Get(s.ctx, "other-app")
	s.ErrorIs(err, ErrNotSuspended, "another app is not suspended with it")

	s.Require().NoError(s.store.TelephonySuspensions.Delete(s.ctx, "app"))
	_, err = s.store.TelephonySuspensions.Get(s.ctx, "app")
	s.ErrorIs(err, ErrNotSuspended)
	s.ErrorIs(s.store.TelephonySuspensions.Delete(s.ctx, "app"), ErrNotSuspended)
}

func (s *StoreSuite) TestTelephonySuspensionQueryBudgets() {
	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.TelephonySuspensions.Upsert(s.ctx, &TelephonySuspension{CustomerID: "app", Reason: "fraud", SuspendedBy: "ann"}))
	}))
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.TelephonySuspensions.Get(s.ctx, "app")
		s.Require().NoError(err)
	}))
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.TelephonySuspensions.Delete(s.ctx, "app")) }))
}
