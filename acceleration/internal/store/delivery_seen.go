package store

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// What a delivery key is unique within (DeliverySeen.Scope).
const (
	// DeliveryScopeHook: a Stream webhook delivery, under the key api.deliveryKey gives it.
	DeliveryScopeHook = "hook"
	// DeliveryScopeConnectionEvent: an MCP event a connection's subscription got, under its
	// subscription id and event id.
	DeliveryScopeConnectionEvent = "connection_event"
)

// deliveryKeptFor is how long a delivery is remembered, which is longer than Stream retries.
const deliveryKeptFor = 24 * time.Hour

// deliveriesPrunedEvery is how often deliveries older than a day are forgotten, which is
// rarely enough that a busy hook does not pay for it on every delivery.
const deliveriesPrunedEvery = time.Minute

// DeliverySeen is a delivery already acted on, kept only so a retry of it is told apart.
type DeliverySeen struct {
	bun.BaseModel `bun:"table:deliveries_seen,alias:ds"`

	Scope  string    `bun:"scope,pk"`
	Key    string    `bun:"key,pk"`
	SeenAt time.Time `bun:"seen_at,notnull"`
}

// Validate refuses a delivery of no known scope or without a key.
func (d *DeliverySeen) Validate() error {
	return validate("delivery seen").
		oneOf("scope", d.Scope, DeliveryScopeHook, DeliveryScopeConnectionEvent).
		required("key", d.Key).
		done()
}

// ConnectionEventKey is what a subscription's event is recorded under.
func ConnectionEventKey(subscriptionID, eventID string) string {
	return subscriptionID + "/" + eventID
}

// DeliveriesSeen are the deliveries acted on in the last day.
type DeliveriesSeen struct {
	store *Store
	// pruned is when deliveries were last forgotten, in Unix nanoseconds.
	pruned atomic.Int64
}

// First records a delivery and reports whether it is the first with that scope and key.
func (r *DeliveriesSeen) First(ctx context.Context, scope, key string) (bool, error) {
	if err := r.prune(ctx); err != nil {
		return false, err
	}
	return firstSeen(ctx, r.store.global, scope, key)
}

// prune deletes the deliveries older than a day, at most once every deliveriesPrunedEvery.
func (r *DeliveriesSeen) prune(ctx context.Context) error {
	now := time.Now().UTC()
	if last := r.pruned.Load(); now.UnixNano()-last >= int64(deliveriesPrunedEvery) &&
		r.pruned.CompareAndSwap(last, now.UnixNano()) {
		_, err := r.store.global.NewDelete().Model((*DeliverySeen)(nil)).Where("seen_at < ?", now.Add(-deliveryKeptFor)).Exec(ctx)
		if err != nil {
			return stack.Wrap(fmt.Errorf("store: forget deliveries: %w", err))
		}
	}
	return nil
}

// firstSeen is First's record on db, the store's or a transaction's.
func firstSeen(ctx context.Context, db bun.IDB, scope, key string) (bool, error) {
	seen := &DeliverySeen{Scope: scope, Key: key, SeenAt: time.Now().UTC()}
	if err := seen.Validate(); err != nil {
		return false, stack.Wrap(err)
	}
	result, err := db.NewInsert().Model(seen).On("CONFLICT (scope, key) DO NOTHING").Exec(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: record delivery: %w", err))
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: record delivery: %w", err))
	}
	return inserted == 1, nil
}
