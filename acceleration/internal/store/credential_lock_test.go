//go:build integration

package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// router is another router's store: a pool of its own on the suite's database, connected
// before the test starts, so a concurrent caller contends in Postgres, not in one pool.
func (s *StoreSuite) router() *Store {
	router, err := s.openRouter()
	s.Require().NoError(err)
	s.T().Cleanup(func() { router.Close() })
	s.Require().NoError(router.Ping(s.ctx))
	return router
}

// credentialLocks counts the advisory locks in this database under the credential namespace, held
// (granted) or waited for, as pg_locks shows a two-integer key: classid the first key and
// objsubid 2 (https://www.postgresql.org/docs/current/view-pg-locks.html).
func (s *StoreSuite) credentialLocks(granted bool) int {
	var count int
	s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx, `
SELECT count(*) FROM pg_locks
WHERE locktype = 'advisory' AND classid = ? AND objsubid = 2 AND granted = ?
  AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`,
		credentialLockNamespace, granted).Scan(&count))
	return count
}

// holding locks the connection's credential state from another router until the returned release is
// called, and returns once the lock is held.
func (s *StoreSuite) holding(connection ConnectorConnection) (release func()) {
	held, done, finished := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	holder := s.router()
	go func() {
		finished <- holder.ConnectorConnections.WithLocked(s.ctx, connection.CustomerID, connection.ID,
			func(*ConnectorConnection, func() error) (bool, error) {
				close(held)
				<-done
				return false, nil
			})

	}()
	<-held
	return func() {
		close(done)
		s.Require().NoError(<-finished)
	}
}

func (s *StoreSuite) TestASaveAtTheStoredRevisionAdvancesIt() {
	connection := s.connection("acme-app", nil)
	expires := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)

	connection.Revision = 2
	connection.Status = ConnectionConnected
	connection.CredentialsSealed = []byte("sealed for revision 2")
	connection.CredentialsKEKVersion = 1
	connection.ExpiresAt = &expires
	connection.AccountID = "acct-1"
	connection.GrantedScopes = []string{"read"}
	connection.Metadata = map[string]string{"instance": "https://eu.acme.example"}
	s.Require().NoError(s.store.ConnectorConnections.SaveAtRevision(s.ctx, &connection, 1))

	stored, err := s.store.ConnectorConnections.Get(s.ctx, "acme-app", connection.ID)
	s.Require().NoError(err)
	s.Equal(2, stored.Revision)
	s.Equal(ConnectionConnected, stored.Status)
	s.Equal([]byte("sealed for revision 2"), stored.CredentialsSealed)
	s.Equal(1, stored.CredentialsKEKVersion)
	s.Require().NotNil(stored.ExpiresAt)
	s.True(expires.Equal(*stored.ExpiresAt))
	s.Equal("acct-1", stored.AccountID)
	s.Equal([]string{"read"}, stored.GrantedScopes)
	s.Equal(map[string]string{"instance": "https://eu.acme.example"}, stored.Metadata)
}

func (s *StoreSuite) TestASaveAtAStaleRevisionIsRefused() {
	connection := s.connection("acme-app", nil)
	stale := connection

	connection.Revision = 2
	connection.LastError = "the first writer"
	s.Require().NoError(s.store.ConnectorConnections.SaveAtRevision(s.ctx, &connection, 1))

	stale.Revision = 2
	stale.LastError = "a writer that read revision 1 too"
	err := s.store.ConnectorConnections.SaveAtRevision(s.ctx, &stale, 1)
	s.ErrorIs(err, ErrConnectorConnectionChanged)

	stored, err := s.store.ConnectorConnections.Get(s.ctx, "acme-app", connection.ID)
	s.Require().NoError(err)
	s.Equal("the first writer", stored.LastError)
}

func (s *StoreSuite) TestASaveIsNewCredentialsAtTheNextRevision() {
	connection := s.connection("acme-app", nil)

	connection.Revision = 1
	s.ErrorContains(s.store.ConnectorConnections.SaveAtRevision(s.ctx, &connection, 1), "new credentials at revision 2")
	connection.Revision = 3
	s.ErrorContains(s.store.ConnectorConnections.SaveAtRevision(s.ctx, &connection, 1), "new credentials at revision 2")
	s.Equal(1, s.storedConnection(connection.ID).Revision)
}

func (s *StoreSuite) TestASaveCannotPutBackCredentialsACheckpointRetired() {
	connection := s.connection("acme-app", nil)
	snapshot := connection

	// A refresh checkpoints needs_reauthorization at the same revision before it spends the
	// refresh token, and never learns what became of it.
	err := s.store.ConnectorConnections.WithLocked(s.ctx, "acme-app", connection.ID,
		func(locked *ConnectorConnection, checkpoint func() error) (bool, error) {
			locked.Status = ConnectionNeedsReauthorization
			locked.LastError = "refresh in flight"
			return false, checkpoint()
		})

	s.Require().NoError(err)

	// A save from a snapshot read before it, putting the old credentials back as connected.
	snapshot.Status = ConnectionConnected
	s.Error(s.store.ConnectorConnections.SaveAtRevision(s.ctx, &snapshot, 1))

	stored := s.storedConnection(connection.ID)
	s.Equal(ConnectionNeedsReauthorization, stored.Status)
	s.Equal("refresh in flight", stored.LastError)
}

func (s *StoreSuite) TestALockedCallbackCannotMoveTheRevisionBack() {
	connection := s.connection("acme-app", nil)

	err := s.store.ConnectorConnections.WithLocked(s.ctx, "acme-app", connection.ID,
		func(locked *ConnectorConnection, _ func() error) (bool, error) {
			locked.Revision = 0
			return true, nil
		})

	s.ErrorContains(err, "behind")
	s.Equal(1, s.storedConnection(connection.ID).Revision)
}

func (s *StoreSuite) TestACommitAfterTheCallersDeadlineStillLands() {
	connection := s.connection("acme-app", nil)

	// The provider answers after the caller's deadline, with tokens it already rotated.
	ctx, cancel := context.WithTimeout(s.ctx, 100*time.Millisecond)
	defer cancel()
	err := s.store.ConnectorConnections.WithLocked(ctx, "acme-app", connection.ID,
		func(locked *ConnectorConnection, _ func() error) (bool, error) {
			<-ctx.Done()
			locked.Revision++
			locked.Status = ConnectionConnected
			locked.CredentialsSealed = []byte("rotated")
			locked.CredentialsKEKVersion = 1
			return true, nil
		})

	s.Require().NoError(err)

	stored := s.storedConnection(connection.ID)
	s.Equal(2, stored.Revision, "the rotation is kept")
	s.Equal(ConnectionConnected, stored.Status)
}

func (s *StoreSuite) TestAWaiterOutlastsTheDriversReadTimeout() {
	connection := s.connection("acme-app", nil)
	release := s.holding(connection)
	waiter := s.router()

	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	defer cancel()
	var ran atomic.Bool
	waited := make(chan error, 1)
	go func() {
		waited <- waiter.ConnectorConnections.WithLocked(ctx, "acme-app", connection.ID,
			func(*ConnectorConnection, func() error) (bool, error) {
				ran.Store(true)
				return false, nil
			})

	}()
	// Past pgdriver's default ReadTimeout of 10s (newDefaultConfig in
	// github.com/uptrace/bun/driver/pgdriver v1.2.18), which bounds any one blocking read.
	time.Sleep(12 * time.Second)
	s.Len(waited, 0, "the waiter is still waiting, not failed")
	release()

	s.Require().NoError(<-waited, "a waiter waits as long as its own context allows")
	s.True(ran.Load())
	s.assertNothingIsHeld(waiter)
}

// storedConnection is the connection as stored now.
func (s *StoreSuite) storedConnection(id string) ConnectorConnection {
	stored, err := s.store.ConnectorConnections.Get(s.ctx, "acme-app", id)
	s.Require().NoError(err)
	return stored
}

func (s *StoreSuite) TestASaveOntoADeletedConnectionIsRefused() {
	connection := s.connection("acme-app", nil)
	s.Require().NoError(s.store.ConnectorConnections.Delete(s.ctx, "acme-app", connection.ID))

	connection.Revision = 2
	connection.CredentialsSealed = []byte("sealed after the delete")
	s.ErrorIs(s.store.ConnectorConnections.SaveAtRevision(s.ctx, &connection, 1), ErrConnectorConnectionChanged)
}

func (s *StoreSuite) TestASaveIsOnlyItsOwnCustomers() {
	theirs := s.connection("other-app", nil)
	theirs.CustomerID = "acme-app"
	theirs.Revision = 2

	s.ErrorIs(s.store.ConnectorConnections.SaveAtRevision(s.ctx, &theirs, 1), ErrConnectorConnectionChanged)
	_, err := s.store.ConnectorConnections.Get(s.ctx, "acme-app", theirs.ID)
	s.ErrorIs(err, ErrNoConnectorConnection)
}

func (s *StoreSuite) TestLockingAnotherCustomersConnectionFindsNone() {
	theirs := s.connection("other-app", nil)

	err := s.store.ConnectorConnections.WithLocked(s.ctx, "acme-app", theirs.ID,
		func(*ConnectorConnection, func() error) (bool, error) {
			s.Fail("the callback runs only for the customer's own connection")
			return false, nil
		})

	s.ErrorIs(err, ErrNoConnectorConnection)
}

func (s *StoreSuite) TestLockingADeletedConnectionFindsNone() {
	connection := s.connection("acme-app", nil)
	s.Require().NoError(s.store.ConnectorConnections.Delete(s.ctx, "acme-app", connection.ID))

	err := s.store.ConnectorConnections.WithLocked(s.ctx, "acme-app", connection.ID,
		func(*ConnectorConnection, func() error) (bool, error) { return true, nil })

	s.ErrorIs(err, ErrNoConnectorConnection)
}

func (s *StoreSuite) TestLockedCallbacksOnOneConnectionNeverOverlapAcrossRouters() {
	connection := s.connection("acme-app", nil)

	const routers = 8
	stores := make([]*Store, routers)
	for i := range stores {
		stores[i] = s.router()
	}
	errs := make([]error, routers)
	start := make(chan struct{})
	var active atomic.Int32
	var overlapped atomic.Bool
	var wg sync.WaitGroup
	for i, router := range stores {
		wg.Go(func() {
			<-start
			errs[i] = router.ConnectorConnections.WithLocked(s.ctx, "acme-app", connection.ID,
				func(locked *ConnectorConnection, _ func() error) (bool, error) {
					if active.Add(1) != 1 {
						overlapped.Store(true)
					}
					time.Sleep(10 * time.Millisecond)
					locked.Revision++
					locked.LastError = fmt.Sprintf("revision %d", locked.Revision)
					active.Add(-1)
					return true, nil
				})

		})
	}
	close(start)
	wg.Wait()

	for _, err := range errs {
		s.NoError(err)
	}
	s.False(overlapped.Load(), "two routers ran the callback for one connection at once")
	stored, err := s.store.ConnectorConnections.Get(s.ctx, "acme-app", connection.ID)
	s.Require().NoError(err)
	s.Equal(1+routers, stored.Revision, "every router saw the revision the one before it committed")
	s.Zero(s.credentialLocks(true), "every lock is released")
}

func (s *StoreSuite) TestACheckpointIsCommittedWhileTheLockIsStillHeld() {
	connection := s.connection("acme-app", nil)
	other := s.router()

	err := s.store.ConnectorConnections.WithLocked(s.ctx, "acme-app", connection.ID,
		func(locked *ConnectorConnection, checkpoint func() error) (bool, error) {
			locked.Status = ConnectionNeedsReauthorization
			locked.LastError = "refresh in flight"
			s.Require().NoError(checkpoint())

			seen, err := other.ConnectorConnections.Get(s.ctx, "acme-app", connection.ID)
			s.Require().NoError(err)
			s.Equal(ConnectionNeedsReauthorization, seen.Status, "another router reads the checkpoint at once")
			s.Equal(1, s.credentialLocks(true), "and the lock is still held")

			locked.Revision++
			locked.Status = ConnectionConnected
			locked.LastError = ""
			return true, nil
		})

	s.Require().NoError(err)

	stored, err := s.store.ConnectorConnections.Get(s.ctx, "acme-app", connection.ID)
	s.Require().NoError(err)
	s.Equal(2, stored.Revision, "the final commit follows the checkpoint's revision")
	s.Equal(ConnectionConnected, stored.Status)
}

func (s *StoreSuite) TestACallbackThatFailsCommitsNothingMore() {
	connection := s.connection("acme-app", nil)
	failed := errors.New("the callback failed")

	err := s.store.ConnectorConnections.WithLocked(s.ctx, "acme-app", connection.ID,
		func(locked *ConnectorConnection, _ func() error) (bool, error) {
			locked.Revision++
			return true, failed
		})

	s.ErrorIs(err, failed)

	stored, err := s.store.ConnectorConnections.Get(s.ctx, "acme-app", connection.ID)
	s.Require().NoError(err)
	s.Equal(1, stored.Revision)
}

func (s *StoreSuite) TestAWriteThatSkippedTheLockCannotBeOverwrittenByTheLockedCallback() {
	connection := s.connection("acme-app", nil)

	err := s.store.ConnectorConnections.WithLocked(s.ctx, "acme-app", connection.ID,
		func(locked *ConnectorConnection, _ func() error) (bool, error) {

			_, err := s.store.GlobalDB().ExecContext(s.ctx,
				"UPDATE connector_connections SET revision = 2 WHERE id = ?", connection.ID)
			s.Require().NoError(err)
			locked.Revision++
			return true, nil
		})

	s.ErrorIs(err, ErrConnectorConnectionChanged)
}

func (s *StoreSuite) TestAnotherConnectionsLockDoesNotWait() {
	held := s.connection("acme-app", nil)
	free := s.connection("acme-app", nil)
	release := s.holding(held)
	defer release()

	ctx, cancel := context.WithTimeout(s.ctx, 2*time.Second)
	defer cancel()
	err := s.store.ConnectorConnections.WithLocked(ctx, "acme-app", free.ID,
		func(*ConnectorConnection, func() error) (bool, error) { return false, nil })

	s.NoError(err)
}

func (s *StoreSuite) TestAWaiterPastItsDeadlineReturnsAndLeavesNoLockOrConnection() {
	connection := s.connection("acme-app", nil)
	release := s.holding(connection)
	waiter := s.router()

	ctx, cancel := context.WithTimeout(s.ctx, 200*time.Millisecond)
	defer cancel()
	began := time.Now()
	err := waiter.ConnectorConnections.WithLocked(ctx, "acme-app", connection.ID,
		func(*ConnectorConnection, func() error) (bool, error) {
			s.Fail("a waiter that gave up never runs the callback")
			return false, nil
		})

	s.ErrorIs(err, context.DeadlineExceeded)
	s.Less(time.Since(began), time.Second, "the waiter returns at its deadline, not when the lock frees")
	s.assertTheWaitEnded(waiter)
	release()
	s.assertNothingIsHeld(waiter)
}

func (s *StoreSuite) TestAWaiterThatIsCanceledReturnsAndLeavesNoLockOrConnection() {
	connection := s.connection("acme-app", nil)
	release := s.holding(connection)
	waiter := s.router()

	// No deadline: a plain cancel, which the driver does not see while a query runs.
	ctx, cancel := context.WithCancel(s.ctx)
	time.AfterFunc(200*time.Millisecond, cancel)
	err := waiter.ConnectorConnections.WithLocked(ctx, "acme-app", connection.ID,
		func(*ConnectorConnection, func() error) (bool, error) {
			s.Fail("a waiter that gave up never runs the callback")
			return false, nil
		})

	s.ErrorIs(err, context.Canceled)
	s.assertTheWaitEnded(waiter)
	release()
	s.assertNothingIsHeld(waiter)

	err = waiter.ConnectorConnections.WithLocked(s.ctx, "acme-app", connection.ID,
		func(*ConnectorConnection, func() error) (bool, error) { return false, nil })

	s.NoError(err, "the waiter's router locks the connection again once it is free")
}

// assertTheWaitEnded checks that a waiter that gave up is no longer queued for the lock in
// Postgres and gave its connection up, while the holder still holds it.
func (s *StoreSuite) assertTheWaitEnded(waiter *Store) {
	s.Eventually(func() bool { return s.credentialLocks(false) == 0 }, 2*time.Second, 10*time.Millisecond,
		"the server stops waiting for the lock on behalf of a caller that gave up")
	s.Equal(1, s.credentialLocks(true), "the holder keeps the lock")
	s.Eventually(func() bool { return waiter.locks.Stats().InUse == 0 }, 2*time.Second, 10*time.Millisecond,
		"the waiter's connection goes back")
}

// assertNothingIsHeld checks that once the holder is done no credential lock is left, so an
// abandoned wait did not end up holding the lock it was canceled out of.
func (s *StoreSuite) assertNothingIsHeld(waiter *Store) {
	s.Eventually(func() bool { return s.credentialLocks(true) == 0 && s.credentialLocks(false) == 0 },
		2*time.Second, 10*time.Millisecond, "no credential lock is left behind")
	s.Zero(waiter.locks.Stats().InUse)
	s.Zero(waiter.lockSlots.callersOf("acme-app"), "the waiter gave its customer's lock slot back")
}

func (s *StoreSuite) TestCredentialLocksHeldAtOnceFillTheirOwnPoolAndLeaveTheServingOneFree() {
	router := s.router()
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	var inside atomic.Int32
	finished := make(chan error, maxConns)
	// As many lockers as the serving pool has connections, each reading through the store
	// inside its lock, as the resolver does: holding their locks on the serving pool, they
	// would take all of it and wait for one more. Spread over 10 customers, 4 each, so no
	// customer's cap (lockConnsPerCustomer) holds one back.
	for i := range maxConns {
		customerID := fmt.Sprintf("acme-app-%d", i%10)
		connection := s.connection(customerID, nil)
		go func() {
			finished <- router.ConnectorConnections.WithLocked(s.ctx, customerID, connection.ID,
				func(locked *ConnectorConnection, _ func() error) (bool, error) {
					if _, err := router.ConnectorConnections.Get(s.ctx, customerID, locked.ID); err != nil {
						return false, err
					}
					inside.Add(1)
					<-release
					return false, nil
				})
		}()
	}
	// The lock pool's size, written out so a change to it is a change to this test too.
	const heldAtOnce = 16
	s.Require().Eventually(func() bool {
		return inside.Load() == heldAtOnce && router.locks.Stats().WaitCount == maxConns-heldAtOnce
	}, 5*time.Second, 10*time.Millisecond, "16 locks are held and the lockers past them wait for a lock connection")
	s.Equal(heldAtOnce, s.credentialLocks(true))

	ctx, cancel := context.WithTimeout(s.ctx, 2*time.Second)
	defer cancel()
	_, _, err := router.Server(ctx)
	s.Require().NoError(err, "a query outside the locks gets a serving connection while every lock connection is held")

	unblock()
	for range maxConns {
		s.Require().NoError(<-finished)
	}
	s.Equal(int32(maxConns), inside.Load(), "every locker past the pool waited its turn and ran")
	s.Zero(router.locks.Stats().InUse)
}

func (s *StoreSuite) TestClosingTheStoreClosesItsLockPool() {
	router, err := s.openRouter()
	s.Require().NoError(err)
	connection := s.connection("acme-app", nil)
	s.Require().NoError(router.ConnectorConnections.WithLocked(s.ctx, "acme-app", connection.ID,
		func(*ConnectorConnection, func() error) (bool, error) { return false, nil }))
	s.Require().Equal(1, router.locks.Stats().OpenConnections, "the lock's connection stays open in the lock pool")

	s.Require().NoError(router.Close())

	s.Zero(router.locks.Stats().OpenConnections)
}

func (s *StoreSuite) TestAProviderAppLockIsHeldOnTheLockPool() {
	router := s.router()
	var locks, serving int
	s.Require().NoError(router.ConnectorConfigTokens.WithProviderAppLock(s.ctx, "acme-app", "slack", func() error {
		locks, serving = router.locks.Stats().InUse, router.GlobalDB().Stats().InUse
		return nil
	}))
	s.Equal(1, locks, "the provider app lock keeps a lock pool connection")
	s.Zero(serving, "and no serving one")
}

// callersOf is how many of the customer's locks hold a lock slot or wait for one.
func (l *lockSlots) callersOf(customerID string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	if slots := l.customers[customerID]; slots != nil {
		return slots.callers
	}
	return 0
}

// customersLocking is how many customers have an entry in the lock slot map.
func (l *lockSlots) customersLocking() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.customers)
}

// parked starts n locks of the customer's own connections on router, each stalled inside
// until release is called, as a refresh stalls at a token endpoint the customer runs. inside
// counts the ones that got in; release returns once every one has.
func (s *StoreSuite) parked(router *Store, customerID string, n int) (inside *atomic.Int32, release func()) {
	inside = new(atomic.Int32)
	stalled := make(chan struct{})
	finished := make(chan error, n)
	for range n {
		connection := s.connection(customerID, nil)
		go func() {
			finished <- router.ConnectorConnections.WithLocked(s.ctx, customerID, connection.ID,
				func(*ConnectorConnection, func() error) (bool, error) {
					inside.Add(1)
					<-stalled
					return false, nil
				})
		}()
	}
	return inside, sync.OnceFunc(func() {
		close(stalled)
		for range n {
			s.Require().NoError(<-finished)
		}
	})
}

func (s *StoreSuite) TestOneCustomersStalledLocksLeaveTheLockPoolToOtherCustomers() {
	router := s.router()
	// As many of one customer's locks as the lock pool has connections, all stalled.
	inside, release := s.parked(router, "evil-app", lockConns)
	defer release()
	// One customer's cap, written out so a change to it is a change to this test too.
	const perCustomer = 4
	s.Require().Eventually(func() bool {
		return inside.Load() == perCustomer && router.lockSlots.callersOf("evil-app") == lockConns
	}, 5*time.Second, 10*time.Millisecond, "4 of the customer's locks are held and the rest wait for a slot")
	s.Equal(perCustomer, router.locks.Stats().InUse, "a lock waiting for its customer's slot holds no connection")

	ctx, cancel := context.WithTimeout(s.ctx, 3*time.Second)
	defer cancel()
	other := s.connection("other-app", nil)
	ran := 0
	s.Require().NoError(router.ConnectorConnections.WithLocked(ctx, "other-app", other.ID,
		func(*ConnectorConnection, func() error) (bool, error) { ran++; return false, nil }))
	s.Require().NoError(router.ConnectorConfigTokens.WithProviderAppLock(ctx, "other-app", "slack",
		func() error { ran++; return nil }))
	s.Equal(2, ran, "another customer's credential and provider app locks run")

	short, stop := context.WithTimeout(s.ctx, 200*time.Millisecond)
	defer stop()
	err := router.ConnectorConfigTokens.WithProviderAppLock(short, "evil-app", "slack", func() error {
		s.Fail("a provider app lock past its customer's cap waits for a slot")
		return nil
	})
	s.ErrorIs(err, context.DeadlineExceeded, "a provider app lock counts against the same cap and gives up with its context")

	release()
	s.Equal(int32(lockConns), inside.Load(), "every one of the customer's locks ran in its turn")
	s.Zero(router.lockSlots.callersOf("evil-app"))
	s.Zero(router.locks.Stats().InUse)
}

func (s *StoreSuite) TestACustomersLockSlotIsGivenBackHoweverItsLockEnds() {
	router := s.router()
	connection := s.connection("acme-app", nil)

	s.Panics(func() {
		_ = router.ConnectorConnections.WithLocked(s.ctx, "acme-app", connection.ID,
			func(*ConnectorConnection, func() error) (bool, error) { panic("callback") })
	})
	s.Zero(router.lockSlots.callersOf("acme-app"), "a callback that panics gives its slot back")

	inside, release := s.parked(router, "acme-app", lockConnsPerCustomer)
	s.Require().Eventually(func() bool { return inside.Load() == lockConnsPerCustomer }, 5*time.Second, 10*time.Millisecond)
	ctx, cancel := context.WithTimeout(s.ctx, 100*time.Millisecond)
	defer cancel()
	err := router.ConnectorConnections.WithLocked(ctx, "acme-app", connection.ID,
		func(*ConnectorConnection, func() error) (bool, error) { return false, nil })
	s.ErrorIs(err, context.DeadlineExceeded)
	release()
	s.Zero(router.lockSlots.callersOf("acme-app"), "a lock that gave up waiting for a slot leaves none counted")
	s.Zero(router.lockSlots.customersLocking(), "a customer whose locks all ended leaves no entry in the map")

	closed, err := s.openRouter()
	s.Require().NoError(err)
	s.Require().NoError(closed.Close())
	err = closed.ConnectorConnections.WithLocked(s.ctx, "acme-app", connection.ID,
		func(*ConnectorConnection, func() error) (bool, error) { return false, nil })
	s.ErrorContains(err, "credential lock connection")
	s.Zero(closed.lockSlots.callersOf("acme-app"), "a lock that got no connection gives its slot back")
}

// panicsOnValue is a context whose Value panics once armed, as a driver or query hook that
// reads a context value can: the seam for a panic inside the lock's own statements.
type panicsOnValue struct {
	context.Context
	armed *atomic.Bool
}

func (c panicsOnValue) Value(key any) any {
	if c.armed.Load() {
		panic("context value")
	}
	return c.Context.Value(key)
}

func (s *StoreSuite) TestACustomersLockSlotIsGivenBackWhenTheLockStatementPanics() {
	router := s.router()
	connection := s.connection("acme-app", nil)
	armed := new(atomic.Bool)
	armed.Store(true)

	s.Panics(func() {
		_ = router.ConnectorConnections.WithLocked(panicsOnValue{s.ctx, armed}, "acme-app", connection.ID,
			func(*ConnectorConnection, func() error) (bool, error) {
				s.Fail("a lock that panicked never runs the callback")
				return false, nil
			})
	})
	s.Zero(router.lockSlots.callersOf("acme-app"), "a panic while taking the lock gives the slot back")
	s.Zero(router.lockSlots.customersLocking())
}

func (s *StoreSuite) TestACustomersLockSlotIsGivenBackWhenTheUnlockStatementPanics() {
	router := s.router()
	connection := s.connection("acme-app", nil)
	armed := new(atomic.Bool)

	s.Panics(func() {
		_ = router.ConnectorConnections.WithLocked(panicsOnValue{s.ctx, armed}, "acme-app", connection.ID,
			func(*ConnectorConnection, func() error) (bool, error) {
				armed.Store(true)
				return false, nil
			})
	})
	s.Zero(router.lockSlots.callersOf("acme-app"), "a panic while unlocking gives the slot back")
	s.Zero(router.lockSlots.customersLocking())
}

// deadlinePassedUnsaid is a context whose deadline has passed but which does not say so yet:
// a context.WithDeadline whose timer has not run, as on a busy machine.
type deadlinePassedUnsaid struct{ context.Context }

func (deadlinePassedUnsaid) Deadline() (time.Time, bool) {
	return time.Now().Add(-time.Millisecond), true
}

func (s *StoreSuite) TestALockCutByItsDeadlineBeforeTheContextSaysSoFailsWithTheDeadline() {
	connection := s.connection("acme-app", nil)
	locked := func(router *Store) error {
		return router.ConnectorConnections.WithLocked(deadlinePassedUnsaid{s.ctx}, "acme-app", connection.ID,
			func(*ConnectorConnection, func() error) (bool, error) {
				s.Fail("a lock past its deadline never runs the callback")
				return false, nil
			})
	}

	warm := s.router()
	s.Require().NoError(warm.ConnectorConnections.WithLocked(s.ctx, "acme-app", connection.ID,
		func(*ConnectorConnection, func() error) (bool, error) { return false, nil }))
	s.Require().Equal(1, warm.locks.Stats().Idle, "the next lock reuses this connection")
	s.ErrorIs(locked(warm), context.DeadlineExceeded, "a pg_try_advisory_lock cut by the deadline")
	s.assertNothingIsHeld(warm)
}
