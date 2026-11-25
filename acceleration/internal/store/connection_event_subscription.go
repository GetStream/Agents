package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// ErrNoConnectionEventSubscription is a token or an id no subscription has.
var ErrNoConnectionEventSubscription = errors.New("store: no such connection event subscription")

// ErrConnectionEventSubscriptionTaken is an Update of a subscription another claim took after
// the lease it was claimed under ran out, or one since deleted.
var ErrConnectionEventSubscriptionTaken = errors.New("store: connection event subscription is no longer held")

// How far a connection event subscription has got.
const (
	// ConnectionEventPending is a subscription the server has not accepted yet.
	ConnectionEventPending = "pending"
	// ConnectionEventActive is one the server accepted, and delivers to.
	ConnectionEventActive = "active"
	// ConnectionEventFailed is one the server refused, Error saying why.
	ConnectionEventFailed = "failed"
)

// ConnectionEventSubscription is one MCP Events webhook subscription on a connection's server,
// for one event one binding of one agent config declares (internal/mcpevents).
type ConnectionEventSubscription struct {
	bun.BaseModel `bun:"table:connection_event_subscriptions,alias:ces"`

	ID           string `bun:"id,pk"`
	CustomerID   string `bun:"customer_id,notnull"`
	ConnectionID string `bun:"connection_id,notnull"`
	ConfigID     string `bun:"config_id,notnull"`
	// Binding is the alias of the config's binding that declares the event.
	Binding   string         `bun:"binding,notnull"`
	Event     string         `bun:"event,notnull"`
	Arguments map[string]any `bun:"arguments,type:jsonb,notnull"`
	// Key is the event and its arguments, hashed (mcpevents.Key).
	Key string `bun:"key,notnull"`
	// Token is the callback's path segment.
	Token string `bun:"token,notnull"`
	// SecretSealed is the subscription's own signing secret, sealed under KEKVersion.
	SecretSealed []byte `bun:"secret_sealed,notnull"`
	KEKVersion   int    `bun:"kek_version,notnull"`
	// RemoteID is the id the server gave the subscription.
	RemoteID string `bun:"remote_id,notnull"`
	// RefreshBefore is when the server stops delivering unless asked again. Nil is a grant that
	// does not expire, or none yet.
	RefreshBefore *time.Time `bun:"refresh_before"`
	Status        string     `bun:"status,notnull"`
	Error         string     `bun:"error,notnull"`
	// NextAttemptAt is when a worker next asks the server for it. Nil is never.
	NextAttemptAt *time.Time `bun:"next_attempt_at"`
	// Attempts is how many times a worker claimed it.
	Attempts int `bun:"attempts,notnull"`
	// Failures is how many times in a row the server refused it.
	Failures  int       `bun:"failures,notnull"`
	CreatedAt time.Time `bun:"created_at,notnull"`
	UpdatedAt time.Time `bun:"updated_at,notnull"`
}

// Validate refuses a subscription the table refuses, or one no callback could be matched to.
func (sub *ConnectionEventSubscription) Validate() error {
	return validate("connection event subscription").
		required("customer_id", sub.CustomerID).
		required("connection_id", sub.ConnectionID).
		required("config_id", sub.ConfigID).
		required("binding", sub.Binding).
		required("event", sub.Event).
		required("key", sub.Key).
		required("token", sub.Token).
		check(len(sub.SecretSealed) > 0, "secret_sealed", "is empty").
		check(sub.KEKVersion >= 1, "kek_version", "is below 1").
		oneOf("status", sub.Status, ConnectionEventPending, ConnectionEventActive, ConnectionEventFailed).
		check(sub.Failures >= 0, "failures", "is negative").
		check(sub.Attempts >= 0, "attempts", "is negative").
		done()
}

// ConnectionEventSubscriptions are the MCP Events subscriptions on connections' servers.
type ConnectionEventSubscriptions struct {
	store *Store
}

// Insert stores a new subscription unless one for the same connection,
// config, binding and key is already there, and reports whether it stored it. Two routers
// adding the same one at once store it once (connection_event_subscriptions_one). A connection
// not live is ErrNoConnectorConnection, and nothing is stored.
//
// The connection is locked FOR SHARE first, a lock that conflicts with the one a delete's UPDATE
// or DELETE of the row takes (https://www.postgresql.org/docs/current/explicit-locking.html#LOCKING-ROWS).
// An insert during a delete's transaction waits for its commit and then finds the connection
// gone; a delete that comes second waits for the insert and drops what it committed
// (softDeleteConnections, DeleteUser). Otherwise the row, with its sealed secret and callback
// token, would outlive the connection.
func (r *ConnectionEventSubscriptions) Insert(ctx context.Context, sub *ConnectionEventSubscription) (bool, error) {
	if err := sub.Validate(); err != nil {
		return false, stack.Wrap(err)
	}
	now := time.Now().UTC()
	sub.ID, sub.CreatedAt, sub.UpdatedAt = newID(), now, now
	if sub.Arguments == nil {
		sub.Arguments = map[string]any{}
	}
	var affected int64
	err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		var live string
		err := tx.NewSelect().Model((*ConnectorConnection)(nil)).Column("cc.id").
			Where("cc.customer_id = ? AND cc.id = ? AND cc.deleted_at IS NULL", sub.CustomerID, sub.ConnectionID).
			For("SHARE").
			Scan(ctx, &live)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: %s", ErrNoConnectorConnection, sub.ConnectionID)
		}
		if err != nil {
			return fmt.Errorf("store: lock an event subscription's connection: %w", err)
		}
		result, err := tx.NewInsert().Model(sub).
			On("CONFLICT (connection_id, config_id, binding, key) DO NOTHING").
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("store: add connection event subscription: %w", err)
		}
		affected, err = result.RowsAffected()
		if err != nil {
			return fmt.Errorf("store: add connection event subscription: %w", err)
		}
		return nil
	})
	if err != nil {
		return false, stack.Wrap(err)
	}
	return affected == 1, nil
}

// GetByToken is the subscription a callback's token names.
func (r *ConnectionEventSubscriptions) GetByToken(ctx context.Context, token string) (ConnectionEventSubscription, error) {
	if token == "" {
		return ConnectionEventSubscription{}, ErrNoConnectionEventSubscription
	}
	var sub ConnectionEventSubscription
	err := r.store.global.NewSelect().Model(&sub).Where("token = ?", token).Limit(1).Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return ConnectionEventSubscription{}, ErrNoConnectionEventSubscription
	}
	if err != nil {
		return ConnectionEventSubscription{}, stack.Wrap(fmt.Errorf("store: connection event subscription: %w", err))
	}
	return sub, nil
}

// GetByBinding is the subscription one binding of one config has for an event
// (its Key) on a connection.
func (r *ConnectionEventSubscriptions) GetByBinding(ctx context.Context, connectionID, configID, binding, key string) (ConnectionEventSubscription, error) {
	var sub ConnectionEventSubscription
	err := r.store.global.NewSelect().Model(&sub).
		Where("connection_id = ?", connectionID).
		Where("config_id = ?", configID).
		Where("binding = ?", binding).
		Where("key = ?", key).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return ConnectionEventSubscription{}, ErrNoConnectionEventSubscription
	}
	if err != nil {
		return ConnectionEventSubscription{}, stack.Wrap(fmt.Errorf("store: connection event subscription: %w", err))
	}
	return sub, nil
}

// DueOthers makes due at now every subscription of a connection for
// an event (its Key) but the one with id.
func (r *ConnectionEventSubscriptions) DueOthers(ctx context.Context, connectionID, key, id string, now time.Time) error {
	_, err := r.store.global.NewUpdate().Model((*ConnectionEventSubscription)(nil)).
		Set("next_attempt_at = ?", now.UTC()).
		Where("connection_id = ?", connectionID).
		Where("key = ?", key).
		Where("id != ?", id).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: due connection event subscriptions: %w", err))
	}
	return nil
}

// Due makes one subscription due at now, so the next worker to look
// asks the server for it again, or drops it.
func (r *ConnectionEventSubscriptions) Due(ctx context.Context, id string, now time.Time) error {
	_, err := r.store.global.NewUpdate().Model((*ConnectionEventSubscription)(nil)).
		Set("next_attempt_at = ?", now.UTC()).
		Where("id = ?", id).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: due connection event subscription: %w", err))
	}
	return nil
}

// DueConnection makes every subscription of a connection due at now, so the
// next worker to look asks the server for each again, or drops it.
func (r *ConnectionEventSubscriptions) DueConnection(ctx context.Context, customerID, connectionID string, now time.Time) error {
	_, err := r.store.global.NewUpdate().Model((*ConnectionEventSubscription)(nil)).
		Set("next_attempt_at = ?", now.UTC()).
		Where("customer_id = ?", customerID).
		Where("connection_id = ?", connectionID).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: due connection event subscriptions: %w", err))
	}
	return nil
}

// Claim takes at most limit subscriptions due now, each leased for lease.
func (r *ConnectionEventSubscriptions) Claim(ctx context.Context, limit int, lease time.Duration) ([]ConnectionEventSubscription, error) {
	return claim[ConnectionEventSubscription](ctx, r.store.global, connectionEventSubscriptions, lease, limit, nil, "ces.*")
}

var connectionEventSubscriptions = queue{name: "connection event subscriptions", alias: "ces", key: "id"}

// NextDue is when the subscription due first is due, any router's.
// found is false when none is ever due: there are no subscriptions, or none expires.
func (r *ConnectionEventSubscriptions) NextDue(ctx context.Context) (next time.Time, found bool, err error) {
	var at sql.NullTime
	if err := r.store.global.NewSelect().Model((*ConnectionEventSubscription)(nil)).ColumnExpr("min(next_attempt_at)").Scan(ctx, &at); err != nil {
		return time.Time{}, false, stack.Wrap(fmt.Errorf("store: next connection event subscription: %w", err))
	}
	return at.Time, at.Valid, nil
}

// Update writes what asking the server for a subscription came to, while the claim sub came
// from still holds it.
func (r *ConnectionEventSubscriptions) Update(ctx context.Context, sub *ConnectionEventSubscription) error {
	if err := sub.Validate(); err != nil {
		return stack.Wrap(err)
	}
	sub.UpdatedAt = time.Now().UTC()
	update := r.store.global.NewUpdate().Model(sub).
		Column("remote_id", "refresh_before", "status", "error", "next_attempt_at", "failures", "updated_at")
	result, err := connectionEventSubscriptions.heldBy(update, sub.ID, sub.Attempts).Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: save connection event subscription: %w", err))
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: save connection event subscription: %w", err))
	}
	if updated != 1 {
		return stack.Wrap(ErrConnectionEventSubscriptionTaken)
	}
	return nil
}

// Delete drops a subscription, so deliveries to its token are
// refused. One already gone is not an error.
func (r *ConnectionEventSubscriptions) Delete(ctx context.Context, id string) error {
	_, err := r.store.global.NewDelete().Model((*ConnectionEventSubscription)(nil)).Where("id = ?", id).Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: delete connection event subscription: %w", err))
	}
	return nil
}
