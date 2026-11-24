package store

import (
	"context"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
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

// withCredentialLock runs fn on one pool connection that holds the session-level advisory lock on
// the connection's credential state, and gives the lock and the connection back when fn returns.
//
// Session-level, not pg_advisory_xact_lock as connectors.go uses: checkpoint commits while
// the lock must stay held, and a transaction-level lock goes with the commit. "Once acquired
// at session level, an advisory lock is held until explicitly released or the session ends",
// and session-level requests "do not honor transaction semantics"
// (https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS). That is also
// why it lives on one dedicated connection: a lock taken on a pooled connection and not
// released would stay with whoever borrows it next.
func (s *Store) withCredentialLock(ctx context.Context, customerID, id string, fn func(bun.Conn) error) error {
	key := credentialLockKey(customerID, id)
	conn, err := s.acquireCredentialLock(ctx, credentialLockNamespace, key)
	if err != nil {
		return err
	}
	defer releaseCredentialLock(ctx, conn, credentialLockNamespace, key)
	return fn(conn)
}

// acquireCredentialLock tries the lock with pg_try_advisory_lock, which "will either obtain the
// lock immediately and return true, or return false without waiting"
// (https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-ADVISORY-LOCKS),
// and pauses between tries until it gets it or ctx is done. namespace is the first key of the
// two (credentialLockNamespace, providerAppLockNamespace). Nothing waits in Postgres, so a
// caller that gives up leaves no wait queued on the server, and a waiter holds no connection
// while it pauses. A blocking pg_advisory_lock could not promise that: the driver bounds
// every read by its ReadTimeout, 10s unless set (newDefaultConfig in
// github.com/uptrace/bun/driver/pgdriver v1.2.18, which store.Open does not change), or by
// ctx's deadline, and drops the socket when it passes, while the server "will detect the loss
// of the connection only at the next interaction with the socket"
// (https://www.postgresql.org/docs/current/runtime-config-connection.html#GUC-CLIENT-CONNECTION-CHECK-INTERVAL),
// so the wait would stay queued. The cost is that waiters are not served in order.
func (s *Store) acquireCredentialLock(ctx context.Context, namespace, key int32) (bun.Conn, error) {
	pause := credentialRetryFirst
	for {
		conn, err := s.global.Conn(ctx)
		if err != nil {
			return bun.Conn{}, fmt.Errorf("store: credential lock connection: %w", err)
		}
		var locked bool
		err = conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock(?::integer, ?::integer)", namespace, key).Scan(&locked)
		if err != nil {
			// The lock may have been granted with the answer lost; closing the connection
			// ends the session, which releases it.
			discard(conn)
			if ctx.Err() != nil {
				return bun.Conn{}, fmt.Errorf("store: wait for credential lock: %w", context.Cause(ctx))
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

// releaseCredentialLock unlocks and hands the connection back to the pool. pg_advisory_unlock
// returns false when the lock was not held
// (https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-ADVISORY-LOCKS);
// that, or an unlock that fails, closes the connection rather than return a session in a
// state nobody knows.
func releaseCredentialLock(ctx context.Context, conn bun.Conn, namespace, key int32) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), credentialDetachedTimeout)
	defer cancel()
	var released bool
	err := conn.QueryRowContext(ctx, "SELECT pg_advisory_unlock(?::integer, ?::integer)", namespace, key).Scan(&released)
	if err != nil || !released {
		discard(conn)
		return
	}
	_ = conn.Close()
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
