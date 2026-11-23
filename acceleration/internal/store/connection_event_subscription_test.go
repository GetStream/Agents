//go:build integration

package store

import (
	"crypto/rand"
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
	connection := s.connection("acme-app", nil).ID
	s.eventSubscription("acme-app", connection, "config-1", "key-1")
	again := ConnectionEventSubscription{
		CustomerID: "acme-app", ConnectionID: connection, ConfigID: "config-1", Binding: "crm",
		Event: "issue.created", Key: "key-1", Token: "another-token", SecretSealed: []byte("sealed"), KEKVersion: 1,
		Status: ConnectionEventPending,
	}

	added, err := s.store.ConnectionEventSubscriptions.Insert(s.ctx, &again)

	s.Require().NoError(err)
	s.False(added)
	held, err := s.connectionEventSubscriptions("acme-app", connection)
	s.Require().NoError(err)
	s.Require().Len(held, 1)
	s.Equal("token-"+connection+"-config-1-key-1", held[0].Token)
}

func (s *StoreSuite) TestAConnectionsSubscriptionsAreOnlyItsCustomers() {
	connection := s.connection("acme-app", nil).ID
	s.eventSubscription("acme-app", connection, "config-1", "key-1")

	held, err := s.connectionEventSubscriptions("other-app", connection)
	s.Require().NoError(err)
	s.Empty(held)
	s.ErrorIs(s.store.ConnectorConnections.Delete(s.ctx, "other-app", connection), ErrNoConnectorConnection)
	held, err = s.connectionEventSubscriptions("acme-app", connection)
	s.Require().NoError(err)
	s.Len(held, 1, "another customer's delete leaves it")
}

func (s *StoreSuite) TestAClaimLeasesWhatIsDueAndNotWhatIsNot() {
	connection := s.connection("acme-app", nil).ID
	due := s.eventSubscription("acme-app", connection, "config-1", "key-1")
	later := s.eventSubscription("acme-app", connection, "config-1", "key-2")
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
	connection := s.connection("acme-app", nil).ID
	s.eventSubscription("acme-app", connection, "config-1", "key-1")
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
	connection := s.connection("acme-app", nil).ID
	for _, key := range []string{"one", "two", "three"} {
		s.eventSubscription("acme-app", connection, "config-1", key)
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
	connection := s.connection("acme-app", nil).ID
	_, found, err := s.store.ConnectionEventSubscriptions.NextDue(s.ctx)
	s.Require().NoError(err)
	s.False(found, "no subscriptions")
	sub := s.eventSubscription("acme-app", connection, "config-1", "key-1")
	sub.NextAttemptAt = nil
	s.Require().NoError(s.store.ConnectionEventSubscriptions.Update(s.ctx, &sub))

	_, found, err = s.store.ConnectionEventSubscriptions.NextDue(s.ctx)
	s.Require().NoError(err)
	s.False(found, "a grant that does not expire is never due")
	s.eventSubscription("acme-app", connection, "config-1", "key-2")
	next, found, err := s.store.ConnectionEventSubscriptions.NextDue(s.ctx)
	s.Require().NoError(err)
	s.True(found)
	s.True(next.Equal(s.base))
}

func (s *StoreSuite) TestMakingAConnectionDueMakesEveryOneOfItsSubscriptionsDue() {
	first := s.connection("acme-app", nil).ID
	second := s.connection("acme-app", nil).ID
	sub := s.eventSubscription("acme-app", first, "config-1", "key-1")
	other := s.eventSubscription("acme-app", second, "config-1", "key-1")
	for _, held := range []*ConnectionEventSubscription{&sub, &other} {
		held.NextAttemptAt = nil
		s.Require().NoError(s.store.ConnectionEventSubscriptions.Update(s.ctx, held))
	}

	s.Require().NoError(s.store.ConnectionEventSubscriptions.DueConnection(s.ctx, "acme-app", first, s.base))

	claimed, err := s.store.ConnectionEventSubscriptions.Claim(s.ctx, s.base, 10, s.base.Add(time.Minute))
	s.Require().NoError(err)
	s.Require().Len(claimed, 1)
	s.Equal(sub.ID, claimed[0].ID)
}

func (s *StoreSuite) TestConnectionEventSubscriptionQueryBudgets() {
	connection := s.connection("acme-app", nil).ID
	for i := range 30 {
		s.eventSubscription("acme-app", connection, "config-1", "key-"+strconv.Itoa(i))
	}
	var sub ConnectionEventSubscription
	// BEGIN, the connection's lock, the insert, COMMIT.
	s.Equal(4, s.countQueries(func() { sub = s.eventSubscription("acme-app", connection, "config-1", "key-last") }), "Insert")

	s.Equal(1, s.countQueries(func() {
		_, err := s.store.ConnectionEventSubscriptions.GetByToken(s.ctx, sub.Token)
		s.Require().NoError(err)
	}), "GetByToken")
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.ConnectionEventSubscriptions.GetByBinding(s.ctx, connection, "config-1", "crm", "key-last")
		s.Require().NoError(err)
	}), "GetByBinding")
	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.ConnectionEventSubscriptions.DueOthers(s.ctx, connection, "key-last", sub.ID, s.base))
	}), "DueOthers")
	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.ConnectionEventSubscriptions.Due(s.ctx, sub.ID, s.base))
	}), "Due")
	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.ConnectionEventSubscriptions.DueConnection(s.ctx, "acme-app", connection, s.base))
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
}

// TestASubscriptionOfAConnectionNotLiveIsRefused: a deleted connection, another customer's and
// one nobody made take no subscription, so no sealed secret or token is stored for one.
func (s *StoreSuite) TestASubscriptionOfAConnectionNotLiveIsRefused() {
	deleted := s.connection("acme-app", nil).ID
	s.Require().NoError(s.store.ConnectorConnections.Delete(s.ctx, "acme-app", deleted))
	elsewhere := s.connection("other-app", nil).ID

	for _, connectionID := range []string{deleted, elsewhere, "missing"} {
		sub := s.subscriptionOf("acme-app", connectionID)
		added, err := s.store.ConnectionEventSubscriptions.Insert(s.ctx, &sub)

		s.ErrorIs(err, ErrNoConnectorConnection, connectionID)
		s.False(added)
		held, err := s.connectionEventSubscriptions("acme-app", connectionID)
		s.Require().NoError(err)
		s.Empty(held, connectionID)
	}
}

// TestAnInsertDuringAConnectionDeleteWritesNothingOnceTheDeleteCommits: the delete's transaction
// holds the connection, so the insert waits, then finds the connection gone and stores nothing.
func (s *StoreSuite) TestAnInsertDuringAConnectionDeleteWritesNothingOnceTheDeleteCommits() {
	connection := s.connection("acme-app", nil).ID
	deleting := s.begin()
	affected, err := softDeleteConnections(s.ctx, deleting, "acme-app", false, connection)
	s.Require().NoError(err)
	s.Require().EqualValues(1, affected)
	type inserted struct {
		added bool
		err   error
	}
	done := make(chan inserted, 1)

	go func() {
		sub := s.subscriptionOf("acme-app", connection)
		added, err := s.store.ConnectionEventSubscriptions.Insert(s.ctx, &sub)
		done <- inserted{added, err}
	}()
	s.Require().Eventually(func() bool { return s.waitingForALock() == 1 }, 5*time.Second, 10*time.Millisecond,
		"the insert waits for the delete")
	s.Require().NoError(deleting.Commit())
	var got inserted
	select {
	case got = <-done:
	case <-time.After(5 * time.Second):
		s.FailNow("the insert never returned")
	}

	s.ErrorIs(got.err, ErrNoConnectorConnection)
	s.False(got.added)
	held, err := s.connectionEventSubscriptions("acme-app", connection)
	s.Require().NoError(err)
	s.Empty(held)
}

// TestADeleteThatWaitsForAnInsertDropsWhatTheInsertCommitted: an insert holds the connection
// FOR SHARE, as Insert does, while the delete waits for it; the delete then drops the row the
// insert committed during the wait.
func (s *StoreSuite) TestADeleteThatWaitsForAnInsertDropsWhatTheInsertCommitted() {
	connection := s.connection("acme-app", nil).ID
	inserting := s.begin()
	_, err := inserting.ExecContext(s.ctx, "SELECT 1 FROM connector_connections WHERE id = ? FOR SHARE", connection)
	s.Require().NoError(err)
	sub := s.subscriptionOf("acme-app", connection)
	sub.ID, sub.Arguments, sub.CreatedAt, sub.UpdatedAt = newID(), map[string]any{}, s.base, s.base
	_, err = inserting.NewInsert().Model(&sub).Exec(s.ctx)
	s.Require().NoError(err)
	deleted := make(chan error, 1)

	go func() { deleted <- s.store.ConnectorConnections.Delete(s.ctx, "acme-app", connection) }()
	s.Require().Eventually(func() bool { return s.waitingForALock() == 1 }, 5*time.Second, 10*time.Millisecond,
		"the delete waits for the insert")
	s.Require().NoError(inserting.Commit())
	select {
	case err = <-deleted:
	case <-time.After(5 * time.Second):
		s.FailNow("the delete never returned")
	}

	s.Require().NoError(err)
	held, err := s.connectionEventSubscriptions("acme-app", connection)
	s.Require().NoError(err)
	s.Empty(held)
}

// Insert holds the connection lock until its insert commits, so a delete that
// comes while the insert is between its lock and its INSERT waits for it and drops its row.
func (s *StoreSuite) TestADeleteDuringAnInsertWaitsForItAndDropsItsRow() {
	connection := s.connection("acme-app", nil).ID
	// Another transaction holds the subscription's key, so Insert waits at its INSERT, after its lock.
	holder := s.begin()
	held := s.subscriptionOf("acme-app", connection)
	held.ID, held.Arguments, held.CreatedAt, held.UpdatedAt = newID(), map[string]any{}, s.base, s.base
	_, err := holder.NewInsert().Model(&held).Exec(s.ctx)
	s.Require().NoError(err)
	type inserted struct {
		added bool
		err   error
	}
	done := make(chan inserted, 1)
	go func() {
		sub := s.subscriptionOf("acme-app", connection)
		added, err := s.store.ConnectionEventSubscriptions.Insert(s.ctx, &sub)
		done <- inserted{added, err}
	}()
	s.Require().Eventually(func() bool { return s.waitingForALock() == 1 }, 5*time.Second, 10*time.Millisecond,
		"the insert waits for the held key")
	deleted := make(chan error, 1)
	go func() { deleted <- s.store.ConnectorConnections.Delete(s.ctx, "acme-app", connection) }()
	s.Require().Eventually(func() bool { return s.waitingForALock() == 2 }, 5*time.Second, 10*time.Millisecond,
		"the delete waits for the insert's lock")
	s.Require().NoError(holder.Rollback())
	var got inserted
	select {
	case got = <-done:
	case <-time.After(5 * time.Second):
		s.FailNow("the insert never returned")
	}
	s.Require().NoError(got.err)
	s.True(got.added)
	select {
	case err = <-deleted:
	case <-time.After(5 * time.Second):
		s.FailNow("the delete never returned")
	}
	s.Require().NoError(err)
	rows, err := s.connectionEventSubscriptions("acme-app", connection)
	s.Require().NoError(err)
	s.Empty(rows)
}

// subscriptionOf is a subscription of customer's connection, not stored, with a token of its own.
func (s *StoreSuite) subscriptionOf(customerID, connectionID string) ConnectionEventSubscription {
	return ConnectionEventSubscription{
		CustomerID: customerID, ConnectionID: connectionID, ConfigID: "config-1", Binding: "crm",
		Event: "issue.created", Key: "key-1", Token: "token-" + rand.Text(), SecretSealed: []byte("sealed"),
		KEKVersion: 1, Status: ConnectionEventPending,
	}
}
