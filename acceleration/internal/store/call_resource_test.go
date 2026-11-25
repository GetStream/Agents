//go:build integration

package store

import "time"

func (s *StoreSuite) TestReleasingACallReturnsItsResourceAndIsIdempotent() {
	s.Require().NoError(s.store.CallResources.Insert(s.ctx, &CallResource{
		TrunkID: "trunk-1", RouteID: "route-1",
		CallType: "default", CallID: "call-1", CustomerID: "acme",
	}))

	released, err := s.store.CallResources.Release(s.ctx, AppScope{Unpinned: true}, "default", "call-1")
	s.Require().NoError(err)
	s.Require().Len(released, 1)
	s.Equal("trunk-1", released[0].TrunkID)
	s.Equal("route-1", released[0].RouteID)

	again, err := s.store.CallResources.Release(s.ctx, AppScope{Unpinned: true}, "default", "call-1")
	s.Require().NoError(err)
	s.Empty(again, "a retried delivery finds nothing left to release")
}

func (s *StoreSuite) TestReleasingAnUnknownCallReturnsNothing() {
	released, err := s.store.CallResources.Release(s.ctx, AppScope{Unpinned: true}, "default", "nobody-called")
	s.Require().NoError(err)
	s.Empty(released)
}

func (s *StoreSuite) TestATransferAddsASecondTrunkToTheSameCall() {
	s.Require().NoError(s.store.CallResources.Insert(s.ctx, &CallResource{
		TrunkID: "call-trunk", RouteID: "call-route",
		CallType: "default", CallID: "call-1", CustomerID: "acme",
	}))
	s.Require().NoError(s.store.CallResources.Insert(s.ctx, &CallResource{
		TrunkID: "transfer-trunk", RouteID: "transfer-route",
		CallType: "default", CallID: "call-1", CustomerID: "acme",
	}))

	released, err := s.store.CallResources.Release(s.ctx, AppScope{Unpinned: true}, "default", "call-1")
	s.Require().NoError(err)
	s.Require().Len(released, 2, "both the call's own trunk and the transfer's trunk are released")

	trunkIDs := []string{released[0].TrunkID, released[1].TrunkID}
	s.Contains(trunkIDs, "call-trunk")
	s.Contains(trunkIDs, "transfer-trunk")
}

func (s *StoreSuite) TestRecordCallResourceRequiresATrunk() {
	err := s.store.CallResources.Insert(s.ctx, &CallResource{
		RouteID: "route-1", CallType: "default", CallID: "call-1", CustomerID: "acme",
	})
	s.ErrorContains(err, "trunk")
}

func (s *StoreSuite) TestRecordCallResourceRequiresARoute() {
	err := s.store.CallResources.Insert(s.ctx, &CallResource{
		TrunkID: "trunk-1", CallType: "default", CallID: "call-1", CustomerID: "acme",
	})
	s.ErrorContains(err, "route")
}

func (s *StoreSuite) TestRecordCallResourceRequiresACall() {
	err := s.store.CallResources.Insert(s.ctx, &CallResource{
		TrunkID: "trunk-1", RouteID: "route-1", CallType: "default", CustomerID: "acme",
	})
	s.ErrorContains(err, "call")
}

func (s *StoreSuite) TestReadingACallsResourcesDeletesNothing() {
	for _, trunk := range []string{"read-trunk-1", "read-trunk-2"} {
		s.Require().NoError(s.store.CallResources.Insert(s.ctx, &CallResource{
			TrunkID: trunk, RouteID: "route-" + trunk,
			CallType: "default", CallID: "read-call", CustomerID: "acme",
		}))
	}

	read, err := s.store.CallResources.Select(s.ctx, AppScope{Unpinned: true}, "default", "read-call")
	s.Require().NoError(err)
	s.Len(read, 2)

	released, err := s.store.CallResources.Release(s.ctx, AppScope{Unpinned: true}, "default", "read-call")
	s.Require().NoError(err)
	s.Len(released, 2, "reading left both rows for the release")
}

func (s *StoreSuite) TestCallResourceQueryBudgets() {
	shards := len(s.store.shards())
	var first *CallResource
	for i := range 3 {
		resource := &CallResource{TrunkID: "trunk-1", RouteID: "route-1", CallType: "default", CallID: "call-1", CustomerID: "acme", Shared: true}
		s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.CallResources.Insert(s.ctx, resource)) }), "Insert")
		if i == 0 {
			first = resource
		}
	}
	s.Equal(shards, s.countSettledReads(func() {
		resources, err := s.store.CallResources.Select(s.ctx, AppScope{Unpinned: true}, "default", "call-1")
		s.Require().NoError(err)
		s.Len(resources, 3)
	}), "Select")
	s.Equal(shards, s.countQueries(func() {
		_, found, err := s.store.CallResources.Pin(s.ctx, "acme", "default", "call-1")
		s.Require().NoError(err)
		s.True(found)
	}), "Pin finds the call's lines on the shards")
	s.Equal(shards+1, s.countQueries(func() {
		_, found, err := s.store.CallResources.Pin(s.ctx, "acme", "default", "no-lines")
		s.Require().NoError(err)
		s.False(found)
	}), "Pin then looks for a number attached to the call")
	s.Equal(shards, s.countQueries(func() {
		stale, err := s.store.CallResources.SelectStale(s.ctx, time.Now().Add(time.Minute))
		s.Require().NoError(err)
		s.Len(stale, 3)
	}), "SelectStale")
	s.Equal(shards, s.countQueries(func() { s.Require().NoError(s.store.CallResources.Forget(s.ctx, *first)) }), "Forget")
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.CallResources.Delete(s.ctx, first.ID+1)) }), "Delete")
	s.Equal(shards, s.countQueries(func() {
		released, err := s.store.CallResources.Release(s.ctx, AppScope{Unpinned: true}, "default", "call-1")
		s.Require().NoError(err)
		s.Len(released, 1)
	}), "Release")
}
