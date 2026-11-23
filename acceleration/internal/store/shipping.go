package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// shipLockTimeout bounds giving the ship lock back as the router stops.
const shipLockTimeout = 5 * time.Second

// ShipLock is the session advisory lock that makes one router of this shard the one that
// sweeps its unshipped rows, held on a connection kept for it: a router that dies ends its
// session, and the lock with it. One goroutine uses it. CostAnomalyLock is the same lock under
// another key, for other work one router at a time does.
type ShipLock struct {
	db   *bun.DB
	key  string
	conn *sql.Conn
	held bool
}

// ShipLock is this shard's ship lock, not taken yet.
func (s *Store) ShipLock() *ShipLock {
	return &ShipLock{db: s.shard, key: "router.usage.ship"}
}

// CostAnomalyLock makes one router of the whole deployment the one that checks every
// organization's spend for a jump. It is in the shared database, since the check reads every
// shard: a lock per shard would have a router of each shard post the same notice.
func (s *Store) CostAnomalyLock() *ShipLock {
	return &ShipLock{db: s.global, key: "router.cost_anomaly"}
}

// shipLockKeepalives has Postgres probe the holder's connection, so a holder whose network
// went silent loses its session, and the lock, within about a minute rather than at the
// kernel's default of two hours. Ignored on a Unix socket.
const shipLockKeepalives = "SELECT set_config('tcp_keepalives_idle', '30', false), " +
	"set_config('tcp_keepalives_interval', '10', false), set_config('tcp_keepalives_count', '3', false)"

// Hold takes the lock when it is free and reports whether this router holds it. Once taken it
// is kept, and each call checks the session holding it is still there. A router that does not
// get it gives the connection back to the pool until the next try. Bound ctx: a connection that
// stops answering holds the call until it ends.
func (l *ShipLock) Hold(ctx context.Context) (bool, error) {
	if l.conn == nil {
		conn, err := l.db.DB.Conn(ctx)
		if err != nil {
			return false, stack.Wrap(fmt.Errorf("store: ship lock: %w", err))
		}
		l.conn = conn
	}
	if l.held {
		if _, err := l.conn.ExecContext(ctx, "SELECT 1"); err != nil {
			l.drop()
			return false, stack.Wrap(fmt.Errorf("store: ship lock: %w", err))
		}
		return true, nil
	}
	if err := l.conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock(hashtextextended($1, 0))", l.key).Scan(&l.held); err != nil {
		// The lock may have been granted with the answer lost; ending the session releases it.
		l.drop()
		return false, stack.Wrap(fmt.Errorf("store: ship lock: %w", err))
	}
	if !l.held {
		_ = l.conn.Close()
		l.conn = nil
		return false, nil
	}
	if _, err := l.conn.ExecContext(ctx, shipLockKeepalives); err != nil {
		l.drop()
		return false, stack.Wrap(fmt.Errorf("store: ship lock: keepalives: %w", err))
	}
	return true, nil
}

// Release gives the lock back, if it is held, and the connection with it.
func (l *ShipLock) Release() {
	if l.conn == nil {
		return
	}
	if l.held {
		ctx, cancel := context.WithTimeout(context.Background(), shipLockTimeout)
		defer cancel()
		if _, err := l.conn.ExecContext(ctx, "SELECT pg_advisory_unlock(hashtextextended($1, 0))", l.key); err != nil {
			l.drop()
			return
		}
	}
	_ = l.conn.Close()
	l.conn, l.held = nil, false
}

// drop ends the session rather than handing it back to the pool with the lock in it.
func (l *ShipLock) drop() {
	_ = l.conn.Raw(func(any) error { return driver.ErrBadConn })
	_ = l.conn.Close()
	l.conn, l.held = nil, false
}
