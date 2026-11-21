//go:build integration

package store

import (
	"strconv"
	"time"

	"github.com/uptrace/bun"
)

// eventSubscription stores a subscription of customer's connection, due at s.base, for config
// and binding crm, keyed by key.
func (s *StoreSuite) eventSubscription(customerID, connectionID, configID, key string) ConnectionEventSubscription {
	due := s.base
	sub := ConnectionEventSubscription{
		CustomerID: customerID, ConnectionID: connectionID, ConfigID: configID, Binding: "crm",
		Event: "issue.created", Key: key, Token: "token-" + connectionID + "-" + configID + "-" + key,
		SecretSealed: []byte("sealed"), KEKVersion: 1, Status: ConnectionEventPending, NextAttemptAt: &due,
	}
	added, err := s.store.ConnectionEventSubscriptions.Insert(s.ctx, &sub)
	s.Require().NoError(err)
	s.Require().True(added)
	return sub
}

// connectionEventSubscriptions are a customer's connection's subscriptions, oldest first.
func (s *StoreSuite) connectionEventSubscriptions(customerID, connectionID string) ([]ConnectionEventSubscription, error) {
	subs := []ConnectionEventSubscription{}
	err := s.store.GlobalDB().NewSelect().Model(&subs).Where("customer_id = ?", customerID).
		Where("connection_id = ?", connectionID).Order("created_at", "id").Scan(s.ctx)
	return subs, err
}

func (s *StoreSuite) TestTheSameSubscriptionAddedTwiceIsStoredOnce() {
	s.eventSubscription("acme-app", "conn-1", "config-1", "key-1")
	again := ConnectionEventSubscription{
		CustomerID: "acme-app", ConnectionID: "conn-1", ConfigID: "config-1", Binding: "crm",
		Event: "issue.created", Key: "key-1", Token: "another-token", SecretSealed: []byte("sealed"), KEKVersion: 1,
		Status: ConnectionEventPending,
	}

	added, err := s.store.ConnectionEventSubscriptions.Insert(s.ctx, &again)

	s.Require().NoError(err)
	s.False(added)
	held, err := s.connectionEventSubscriptions("acme-app", "conn-1")
	s.Require().NoError(err)
	s.Require().Len(held, 1)
	s.Equal("token-conn-1-config-1-key-1", held[0].Token)
}

func (s *StoreSuite) TestAConnectionsSubscriptionsAreOnlyItsCustomers() {
	s.eventSubscription("acme-app", "conn-1", "config-1", "key-1")

	held, err := s.connectionEventSubscriptions("other-app", "conn-1")
	s.Require().NoError(err)
	s.Empty(held)
	s.Require().NoError(s.store.ConnectionEventSubscriptions.DeleteConnection(s.ctx, "other-app", "conn-1"))
	held, err = s.connectionEventSubscriptions("acme-app", "conn-1")
	s.Require().NoError(err)
	s.Len(held, 1, "another customer's delete leaves it")
}

func (s *StoreSuite) TestAClaimLeasesWhatIsDueAndNotWhatIsNot() {
	due := s.eventSubscription("acme-app", "conn-1", "config-1", "key-1")
	later := s.eventSubscription("acme-app", "conn-1", "config-1", "key-2")
	future := s.base.Add(time.Hour)
	later.NextAttemptAt = &future
	s.Require().NoError(s.store.ConnectionEventSubscriptions.Update(s.ctx, &later))

	claimed, err := s.store.ConnectionEventSubscriptions.Claim(s.ctx, s.base, 10, s.base.Add(time.Minute))
	s.Require().NoError(err)
	again, err := s.store.ConnectionEventSubscriptions.Claim(s.ctx, s.base, 10, s.base.Add(time.Minute))
	s.Require().NoError(err)

	s.Require().Len(claimed, 1)
	s.Equal(due.ID, claimed[0].ID)
	s.True(claimed[0].NextAttemptAt.Equal(s.base.Add(time.Minute)), "pushed to the lease's end")
	s.Empty(again, "a leased one is not taken again before its lease ends")
}

func (s *StoreSuite) TestAClaimWhoseLeaseRanOutCannotOverwriteTheClaimAfterIt() {
	s.eventSubscription("acme-app", "conn-1", "config-1", "key-1")
	slow, err := s.store.ConnectionEventSubscriptions.Claim(s.ctx, s.base, 10, s.base.Add(time.Minute))
	s.Require().NoError(err)
	s.Require().Len(slow, 1)
	taken, err := s.store.ConnectionEventSubscriptions.Claim(s.ctx, s.base.Add(2*time.Minute), 10, s.base.Add(3*time.Minute))
	s.Require().NoError(err)
	s.Require().Len(taken, 1)

	slow[0].Status, slow[0].Error = ConnectionEventFailed, "too late"
	err = s.store.ConnectionEventSubscriptions.Update(s.ctx, &slow[0])
	taken[0].Status = ConnectionEventActive
	s.Require().NoError(s.store.ConnectionEventSubscriptions.Update(s.ctx, &taken[0]))

	s.ErrorIs(err, ErrConnectionEventSubscriptionTaken)
	s.Equal([]int{1, 2}, []int{slow[0].Attempts, taken[0].Attempts}, "each claim counts its attempt")
	stored, err := s.store.ConnectionEventSubscriptions.GetByToken(s.ctx, taken[0].Token)
	s.Require().NoError(err)
	s.Equal(ConnectionEventActive, stored.Status)
	s.Empty(stored.Error)
}

// TestTwoRoutersClaimingAtOnceTakeEachSubscriptionOnce: router A's claim pauses after it read
// the due rows; router B claims them meanwhile. A then takes none of them, so the server is
// never asked for one subscription by two routers at once.
func (s *StoreSuite) TestTwoRoutersClaimingAtOnceTakeEachSubscriptionOnce() {
	for _, key := range []string{"one", "two", "three"} {
		s.eventSubscription("acme-app", "conn-1", "config-1", key)
	}
	routerA, err := s.openRouter()
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.Require().NoError(routerA.Close()) })
	paused := func(due *bun.SelectQuery) *bun.SelectQuery {
		return due.Where("(SELECT 1 FROM pg_sleep(0.5)) = 1")
	}
	type claimed struct {
		claimed []ConnectionEventSubscription
		err     error
	}
	byA := make(chan claimed, 1)

	go func() {
		subs, err := claim[ConnectionEventSubscription](s.ctx, routerA.global, connectionEventSubscriptions, s.base, s.base.Add(time.Minute), 10, paused, "ces.*")
		byA <- claimed{subs, err}
	}()
	s.Require().Eventually(func() bool {
		var sleeping int
		s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx,
			"SELECT count(*) FROM pg_stat_activity WHERE wait_event = 'PgSleep' AND datname = current_database()").Scan(&sleeping))
		return sleeping == 1
	}, 5*time.Second, 10*time.Millisecond, "router A's claim is in its pause")
	byB, err := s.store.ConnectionEventSubscriptions.Claim(s.ctx, s.base, 10, s.base.Add(time.Minute))
	s.Require().NoError(err)
	a := <-byA
	s.Require().NoError(a.err)

	s.Len(byB, 3)
	s.Empty(a.claimed, "router A takes none of what router B leased")
}

func (s *StoreSuite) TestTheNextSubscriptionDueIsTheFirstAndNoneWhenNoneIsEverDue() {
	_, found, err := s.store.ConnectionEventSubscriptions.NextDue(s.ctx)
	s.Require().NoError(err)
	s.False(found, "no subscriptions")
	sub := s.eventSubscription("acme-app", "conn-1", "config-1", "key-1")
	sub.NextAttemptAt = nil
	s.Require().NoError(s.store.ConnectionEventSubscriptions.Update(s.ctx, &sub))

	_, found, err = s.store.ConnectionEventSubscriptions.NextDue(s.ctx)
	s.Require().NoError(err)
	s.False(found, "a grant that does not expire is never due")
	s.eventSubscription("acme-app", "conn-1", "config-1", "key-2")
	next, found, err := s.store.ConnectionEventSubscriptions.NextDue(s.ctx)
	s.Require().NoError(err)
	s.True(found)
	s.True(next.Equal(s.base))
}

func (s *StoreSuite) TestMakingAConnectionDueMakesEveryOneOfItsSubscriptionsDue() {
	sub := s.eventSubscription("acme-app", "conn-1", "config-1", "key-1")
	other := s.eventSubscription("acme-app", "conn-2", "config-1", "key-1")
	for _, held := range []*ConnectionEventSubscription{&sub, &other} {
		held.NextAttemptAt = nil
		s.Require().NoError(s.store.ConnectionEventSubscriptions.Update(s.ctx, held))
	}

	s.Require().NoError(s.store.ConnectionEventSubscriptions.DueConnection(s.ctx, "acme-app", "conn-1", s.base))

	claimed, err := s.store.ConnectionEventSubscriptions.Claim(s.ctx, s.base, 10, s.base.Add(time.Minute))
	s.Require().NoError(err)
	s.Require().Len(claimed, 1)
	s.Equal(sub.ID, claimed[0].ID)
}

func (s *StoreSuite) TestConnectionEventSubscriptionQueryBudgets() {
	for i := range 30 {
		s.eventSubscription("acme-app", "conn-1", "config-1", "key-"+strconv.Itoa(i))
	}
	var sub ConnectionEventSubscription
	s.Equal(1, s.countQueries(func() { sub = s.eventSubscription("acme-app", "conn-1", "config-1", "key-last") }), "Insert")

	s.Equal(1, s.countQueries(func() {
		_, err := s.store.ConnectionEventSubscriptions.GetByToken(s.ctx, sub.Token)
		s.Require().NoError(err)
	}), "GetByToken")
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.ConnectionEventSubscriptions.GetByBinding(s.ctx, "conn-1", "config-1", "crm", "key-last")
		s.Require().NoError(err)
	}), "GetByBinding")
	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.ConnectionEventSubscriptions.DueOthers(s.ctx, "conn-1", "key-last", sub.ID, s.base))
	}), "DueOthers")
	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.ConnectionEventSubscriptions.Due(s.ctx, sub.ID, s.base))
	}), "Due")
	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.ConnectionEventSubscriptions.DueConnection(s.ctx, "acme-app", "conn-1", s.base))
	}), "DueConnection")
	var claimed []ConnectionEventSubscription
	s.Equal(1, s.countQueries(func() {
		var err error
		claimed, err = s.store.ConnectionEventSubscriptions.Claim(s.ctx, s.base.Add(time.Second), 20, s.base.Add(time.Minute))
		s.Require().NoError(err)
		s.Len(claimed, 20)
	}), "Claim")
	s.Equal(1, s.countQueries(func() {
		_, _, err := s.store.ConnectionEventSubscriptions.NextDue(s.ctx)
		s.Require().NoError(err)
	}), "NextDue")
	claimed[0].Status = ConnectionEventActive
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.ConnectionEventSubscriptions.Update(s.ctx, &claimed[0])) }), "Update")
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.ConnectionEventSubscriptions.Delete(s.ctx, sub.ID)) }), "Delete")
	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.ConnectionEventSubscriptions.DeleteConnection(s.ctx, "acme-app", "conn-1"))
	}), "DeleteConnection")
}
