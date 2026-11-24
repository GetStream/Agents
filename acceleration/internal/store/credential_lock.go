package store

import (
	"context"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/uptrace/bun"
)

// credentialLockNamespace is the first key of the two-integer advisory lock a connection's
// credential state is held under; the second is credentialLockKey. Postgres keeps the one-bigint and the
// two-integer key spaces apart ("note that these two key spaces do not overlap",
// https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-ADVISORY-LOCKS),
// and the other advisory locks in this repository take one bigint
// (hashtextextended(..., 731) in logs.go and the agent_logs migrations,
// hashtextextended(..., definitionLockSeed) in connectors.go), so no key here can be one of
// theirs. 839 is this lock's issue, AI-839, as 833 is definitionLockSeed's; the value only
// has to differ from the other two-integer locks: providerAppLockNamespace's 872
// (config_tokens.go), and the change feed's, whose first key is a change number's high half
// (record_data_change), 0 and up, and reaches 839 only past 839 << 32 changes.
const credentialLockNamespace int32 = 839

// credentialDetachedTimeout bounds the statements that run on a context of their own because the
// caller's may already be done: a credentials write, so tokens a provider already rotated are not
// dropped by the caller's deadline, and the unlock. It is the prototype's unlock timeout
// (withConnectorConnectionLock in internal/store/connectors.go on codex/connector-support at
// cf62af0d); unverified, not measured. Each is one statement by primary key or lock key. An
// unlock that misses it closes the connection instead, and the session's end releases the
// lock.
const credentialDetachedTimeout = 2 * time.Second

// credentialRetryFirst and credentialRetryMost are the pause between two pg_try_advisory_lock attempts:
// the first pause, doubled after each miss up to the most. A miss means another router holds
// the lock, usually across a provider round trip, and the most bounds how long a waiter idles
// after the holder unlocks. Unverified, not measured.
const (
	credentialRetryFirst = 5 * time.Millisecond
	credentialRetryMost  = 100 * time.Millisecond
)

// lockConns is how many connections the pool credential and provider app locks are held on
// (Store.locks) keeps at most, so how many of those locks one router holds at once; one more
// waits for a connection until its context ends. A lock keeps its connection for as long as it
// is held, a refresh's provider round trip included (up to the resolver's retrieveTimeout,
// 30 s), and the statements its callback runs through the store borrow a connection of the
// serving pool (maxConns) for that one statement. On a pool of their own, locks stalled at a
// provider can never take the serving pool, and a holder that asks for a serving connection
// never waits on one another holder keeps. 16 lets that many refreshes stall at once before
// another connection's credential has to wait, and adds that many to the 40 a router holds on
// the shared database; idle ones close after 5 minutes (connect). A choice, not measured.
const lockConns = 16

// lockConnsPerCustomer is how many lock pool connections one customer's locks, credential and
// provider app ones together, hold at once; one more of that customer's waits, holding no
// connection, until one of them is given back or its own context ends. A customer decides
// how long its refreshes take: a custom connector names its own token endpoint
// (oauth2code/discovery.go), which may answer each token request just inside
// connectorHTTPTimeout (10 s, cmd/router/main.go) for as long as retrieveTimeout (30 s,
// resolver.go) lets a refresh run. Without a cap, one customer's 16 stalled refreshes would
// fill lockConns, and every other customer's locks would wait until their contexts ended. 4 of
// 16 leaves 12 to the others while one customer stalls, so it takes 4 customers to fill the
// pool, and still lets a customer refresh 4 connections at once. A choice, not measured. The
// count is kept per Store, so per router process, as the pool it divides is: another
// router's locks are on its own pool and count there.
const lockConnsPerCustomer = 4

// lockSlots counts the lock pool connections each customer's locks hold or wait for, so that
// none holds more than lockConnsPerCustomer. A customer's entry lives while one of its locks
// is held or wanted, so the map holds at most the customers locking at this moment.
type lockSlots struct {
	mu        sync.Mutex
	customers map[string]*customerLockSlots
}

type customerLockSlots struct {
	// held has one value per connection the customer's locks hold, up to its capacity,
	// lockConnsPerCustomer.
	held chan struct{}
	// callers is how many of the customer's locks hold a slot or wait for one.
	callers int
}

// take waits for one of the customer's slots until ctx ends, and returns what gives it back.
func (l *lockSlots) take(ctx context.Context, customerID string) (func(), error) {
	l.mu.Lock()
	if l.customers == nil {
		l.customers = map[string]*customerLockSlots{}
	}
	slots := l.customers[customerID]
	if slots == nil {
		slots = &customerLockSlots{held: make(chan struct{}, lockConnsPerCustomer)}
		l.customers[customerID] = slots
	}
	slots.callers++
	l.mu.Unlock()
	select {
	case slots.held <- struct{}{}:
		return func() {
			<-slots.held
			l.leave(customerID, slots)
		}, nil
	case <-ctx.Done():
		l.leave(customerID, slots)
		return nil, context.Cause(ctx)
	}
}

func (l *lockSlots) leave(customerID string, slots *customerLockSlots) {
	l.mu.Lock()
	defer l.mu.Unlock()
	slots.callers--
	if slots.callers == 0 {
		delete(l.customers, customerID)
	}
}

// withCredentialLock runs fn on one lock pool connection that holds the session-level advisory
// lock on the connection's credential state, and gives the lock and the connection back when
// fn returns.
//
// Session-level, not pg_advisory_xact_lock as connectors.go uses: checkpoint commits while
// the lock must stay held, and a transaction-level lock goes with the commit. "Once acquired
// at session level, an advisory lock is held until explicitly released or the session ends",
// and session-level requests "do not honor transaction semantics"
// (https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS). That is also
// why it lives on one dedicated connection: a lock taken on a pooled connection and not
// released would stay with whoever borrows it next. Statements fn runs on that connection
// (conn) take no second one; any other statement takes one of the serving pool.
func (s *Store) withCredentialLock(ctx context.Context, customerID, id string, fn func(bun.Conn) error) error {
	conn, unlock, err := s.acquireCredentialLock(ctx, customerID, credentialLockNamespace, credentialLockKey(customerID, id))
	if err != nil {
		return err
	}
	defer unlock()
	return fn(conn)
}

// acquireCredentialLock takes one of the customer's lock slots (lockSlots), then the lock
// (tryCredentialLock), and returns the connection holding it and what unlocks it and gives
// the slot back. namespace is the first key of the two (credentialLockNamespace,
// providerAppLockNamespace).
func (s *Store) acquireCredentialLock(ctx context.Context, customerID string, namespace, key int32) (bun.Conn, func(), error) {
	leave, err := s.lockSlots.take(ctx, customerID)
	if err != nil {
		return bun.Conn{}, nil, fmt.Errorf("store: wait for a customer's credential lock slot: %w", err)
	}
	locked := false
	defer func() {
		if !locked {
			leave()
		}
	}()
	conn, err := s.tryCredentialLock(ctx, namespace, key)
	if err != nil {
		return bun.Conn{}, nil, err
	}
	locked = true
	return conn, func() {
		defer leave()
		releaseCredentialLock(ctx, conn, namespace, key)
	}, nil
}

// tryCredentialLock tries the lock with pg_try_advisory_lock, which "will either obtain the
// lock immediately and return true, or return false without waiting"
// (https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-ADVISORY-LOCKS),
// and pauses between tries until it gets it or ctx is done. Nothing waits in Postgres, so a
// caller that gives up leaves no wait queued on the server, and a waiter holds no connection
// while it pauses. A blocking pg_advisory_lock could not promise that: the driver bounds
// every read by its ReadTimeout, 10s unless set (newDefaultConfig in
// github.com/uptrace/bun/driver/pgdriver v1.2.18, which store.Open does not change), or by
// ctx's deadline, and drops the socket when it passes, while the server "will detect the loss
// of the connection only at the next interaction with the socket"
// (https://www.postgresql.org/docs/current/runtime-config-connection.html#GUC-CLIENT-CONNECTION-CHECK-INTERVAL),
// so the wait would stay queued. The cost is that waiters are not served in order.
func (s *Store) tryCredentialLock(ctx context.Context, namespace, key int32) (bun.Conn, error) {
	pause := credentialRetryFirst
	for {
		conn, err := s.locks.Conn(ctx)
		if err != nil {
			return bun.Conn{}, fmt.Errorf("store: credential lock connection: %w", err)
		}
		locked, err := tryAdvisoryLock(ctx, conn, namespace, key)
		if err != nil {
			if cause := ended(ctx); cause != nil {
				return bun.Conn{}, fmt.Errorf("store: wait for credential lock: %w", cause)
			}
			return bun.Conn{}, fmt.Errorf("store: acquire credential lock: %w", err)
		}
		if locked {
			return conn, nil
		}
		_ = conn.Close()
		timer := time.NewTimer(pause)
		select {
		case <-ctx.Done():
			timer.Stop()
			return bun.Conn{}, fmt.Errorf("store: wait for credential lock: %w", context.Cause(ctx))
		case <-timer.C:
		}
		pause = min(2*pause, credentialRetryMost)
	}
}

// ended is why ctx is done, or context.DeadlineExceeded once its deadline has passed but
// before ctx says so. The driver sets a socket's deadline to ctx's (Conn.deadline in
// github.com/uptrace/bun/driver/pgdriver v1.2.18, driver.go), so a statement it cuts can fail
// before the timer that ends ctx has run, which a busy machine delays; that failure
// is ctx's deadline, not a broken connection.
func ended(ctx context.Context) error {
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}

// tryAdvisoryLock runs pg_try_advisory_lock on conn. A statement that fails or panics may have
// been granted the lock with the answer lost, so it discards conn: closing it ends the session,
// which releases the lock.
func tryAdvisoryLock(ctx context.Context, conn bun.Conn, namespace, key int32) (bool, error) {
	answered := false
	defer discardOnPanic(conn, &answered)
	var locked bool
	err := conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock(?::integer, ?::integer)", namespace, key).Scan(&locked)
	answered = true
	if err != nil {
		discard(conn)
	}
	return locked, err
}

// releaseCredentialLock unlocks and hands the connection back to the lock pool.
// pg_advisory_unlock returns false when the lock was not held
// (https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-ADVISORY-LOCKS);
// that, or an unlock that fails or panics, closes the connection rather than return a session
// in a state nobody knows. Neither returns it to the pool still holding the lock, nor leaves it
// out of the pool holding it, which would keep the credential locked for every router until
// this process exits.
func releaseCredentialLock(ctx context.Context, conn bun.Conn, namespace, key int32) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), credentialDetachedTimeout)
	defer cancel()
	answered := false
	defer discardOnPanic(conn, &answered)
	var released bool
	err := conn.QueryRowContext(ctx, "SELECT pg_advisory_unlock(?::integer, ?::integer)", namespace, key).Scan(&released)
	answered = true
	if err != nil || !released {
		discard(conn)
		return
	}
	_ = conn.Close()
}

// discardOnPanic discards conn when the statement on it panicked, which it tells by *answered
// still being false; the panic goes on. A query hook can panic (bunotel's BeforeQuery reads the
// context), and so can the driver. It discards on a goroutine of its own: a panic inside
// database/sql's query leaves the Conn's close lock read-held (DB.queryDC in database/sql/sql.go gives it
// back on an error or when the rows close, not on a panic), and closing it then blocks for good. That goroutine is
// the only one left blocked then; any other panic closes the connection at once.
func discardOnPanic(conn bun.Conn, answered *bool) {
	if !*answered {
		go discard(conn)
	}
}

// discard closes the connection instead of returning it to the pool: database/sql drops a
// connection whose Raw callback returns driver.ErrBadConn, and Postgres ends the session.
func discard(conn bun.Conn) {
	_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	_ = conn.Close()
}

// credentialLockKey is the second lock key: the first four bytes of SHA-256 over customer and
// connection id, joined by NUL, which no Postgres text value contains ("the character with
// code zero (sometimes called NUL) cannot be stored",
// https://www.postgresql.org/docs/current/datatype-character.html), so no two pairs join to
// the same input. It is computed here rather than by hashtext, so it does not depend on
// the server's hash function.
//
// 32 bits, because the two-integer form is what keeps credential locks apart from every bigint
// lock (credentialLockNamespace), and its other half is the namespace. So two connections can
// share a key. That costs contention, never correctness: the lock only orders callbacks, and
// each callback writes its own row under its own compare-and-swap, so a collision makes two
// connections take turns. With keys spread uniformly over 2^32 values, a lock taken while k
// others are held shares a key with one of them with probability about (k-1)/2^32, 2.3e-7
// for k = 1000; a table of a million connections has about 116 colliding pairs
// (n(n-1)/2 / 2^32), and a pair waits only when both are locked at once.
func credentialLockKey(customerID, id string) int32 {
	sum := sha256.Sum256([]byte(customerID + "\x00" + id))
	return int32(binary.BigEndian.Uint32(sum[:4]))
}
