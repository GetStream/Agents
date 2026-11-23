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

func (s *StoreSuite) TestMergingTheDeliveryTablesKeepsWhatWasSeen() {
	merged := s.migratedTo("seen", 20270113000001-1)
	defer merged.Close()
	seen := time.Now().UTC()
	_, err := merged.global.ExecContext(s.ctx, `
INSERT INTO hook_deliveries (key, seen_at) VALUES ('hook-key', ?);
INSERT INTO connection_event_subscriptions (id, customer_id, connection_id, config_id, binding, event, key, token, secret_sealed, kek_version, status, created_at, updated_at)
VALUES ('sub', 'app', 'conn', 'config', 'acme', 'issue', 'k', 'token', '\x00', 1, 'active', ?, ?);
INSERT INTO connection_event_deliveries (subscription_id, event_id, received_at) VALUES ('sub', 'evt', ?);
`, seen, seen, seen, seen)
	s.Require().NoError(err)

	s.Require().NoError(merged.Migrate(s.ctx))

	for _, delivery := range []struct{ scope, key string }{
		{DeliveryScopeHook, "hook-key"},
		{DeliveryScopeConnectionEvent, ConnectionEventKey("sub", "evt")},
	} {
		first, err := merged.DeliveriesSeen.First(s.ctx, delivery.scope, delivery.key)
		s.Require().NoError(err)
		s.False(first, delivery.key)
	}
	fresh, err := merged.DeliveriesSeen.First(s.ctx, DeliveryScopeConnectionEvent, ConnectionEventKey("sub", "other"))
	s.Require().NoError(err)
	s.True(fresh)
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
