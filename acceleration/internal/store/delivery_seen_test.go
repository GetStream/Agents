//go:build integration

package store

import (
	"time"
)

func (s *StoreSuite) TestADeliveryIsFirstOncePerScope() {
	first, err := s.store.DeliveriesSeen.First(s.ctx, DeliveryScopeHook, "key-1")
	s.Require().NoError(err)
	again, err := s.store.DeliveriesSeen.First(s.ctx, DeliveryScopeHook, "key-1")
	s.Require().NoError(err)
	otherScope, err := s.store.DeliveriesSeen.First(s.ctx, DeliveryScopeConnectionEvent, "key-1")
	s.Require().NoError(err)

	s.True(first)
	s.False(again)
	s.True(otherScope)
}

func (s *StoreSuite) TestAnEventIsFirstOncePerSubscription() {
	first, err := s.store.DeliveriesSeen.First(s.ctx, DeliveryScopeConnectionEvent, ConnectionEventKey("sub-1", "evt-1"))
	s.Require().NoError(err)
	again, err := s.store.DeliveriesSeen.First(s.ctx, DeliveryScopeConnectionEvent, ConnectionEventKey("sub-1", "evt-1"))
	s.Require().NoError(err)
	otherSubscription, err := s.store.DeliveriesSeen.First(s.ctx, DeliveryScopeConnectionEvent, ConnectionEventKey("sub-2", "evt-1"))
	s.Require().NoError(err)

	s.True(first)
	s.False(again)
	s.True(otherSubscription)
}

func (s *StoreSuite) TestADeliveryWithoutAKeyIsRefused() {
	_, err := s.store.DeliveriesSeen.First(s.ctx, DeliveryScopeHook, "")

	var invalid *ValidationError
	s.Require().ErrorAs(err, &invalid)
	s.Equal("key", invalid.Field)
}

func (s *StoreSuite) TestADeliveryOfADayAgoIsForgottenAndFirstAgain() {
	_, err := s.store.GlobalDB().ExecContext(s.ctx, "INSERT INTO deliveries_seen (scope, key, seen_at) VALUES (?, ?, ?), (?, ?, ?)",
		DeliveryScopeHook, "old", time.Now().Add(-deliveryKeptFor-time.Hour),
		DeliveryScopeConnectionEvent, "recent", time.Now().Add(-time.Hour))
	s.Require().NoError(err)
	s.store.DeliveriesSeen.pruned.Store(0)

	old, err := s.store.DeliveriesSeen.First(s.ctx, DeliveryScopeHook, "old")
	s.Require().NoError(err)
	recent, err := s.store.DeliveriesSeen.First(s.ctx, DeliveryScopeConnectionEvent, "recent")
	s.Require().NoError(err)

	s.True(old)
	s.False(recent)
}

func (s *StoreSuite) TestDeliverySeenQueryBudgets() {
	s.store.DeliveriesSeen.pruned.Store(0)
	s.Equal(2, s.countQueries(func() {
		first, err := s.store.DeliveriesSeen.First(s.ctx, DeliveryScopeHook, "key-1")
		s.Require().NoError(err)
		s.True(first)
	}), "First, pruning")
	s.Equal(1, s.countQueries(func() {
		first, err := s.store.DeliveriesSeen.First(s.ctx, DeliveryScopeConnectionEvent, ConnectionEventKey("sub", "evt"))
		s.Require().NoError(err)
		s.True(first)
	}), "First")
}
