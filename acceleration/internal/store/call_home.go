package store

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// CallHome is the shard that placed or answered a Stream call: its legs, lines and
// participants are there alone, and an event about it that lands in another shard is handed
// to a router of this one.
type CallHome struct {
	bun.BaseModel `bun:"table:call_homes"`

	CallID   string `bun:"call_id,pk"`
	CallType string `bun:"call_type,pk"`
	// StreamAppPK is the app the call is in, zero for the deployment's own.
	StreamAppPK int64     `bun:"stream_app_pk,pk"`
	CustomerID  string    `bun:"customer_id,pk"`
	Shard       string    `bun:"shard,notnull"`
	OpenedAt    time.Time `bun:"opened_at,notnull"`
}

// CallHomes are where calls live, kept in the shared database so any shard can tell.
type CallHomes struct {
	store *Store
}

// Elsewhere is the shard a call of the scope's app lives in when that is not this one, and
// empty when it is this one's, or nothing here or elsewhere placed or answered it.
func (r *CallHomes) Elsewhere(ctx context.Context, scope AppScope, callType, callID string) (string, error) {
	if !r.store.homed {
		return "", nil
	}
	var shards []string
	err := homesOf(r.store.global, scope, callType, callID).Column("shard").
		OrderExpr("opened_at DESC").
		Scan(ctx, &shards)
	if err != nil {
		return "", stack.Wrap(fmt.Errorf("store: call home: %w", err))
	}
	if len(shards) == 0 || slices.Contains(shards, r.store.ShardName()) {
		return "", nil
	}
	return shards[0], nil
}

// homesOf are the homes of a call of the scope's app.
func homesOf(db bun.IDB, scope AppScope, callType, callID string) *bun.SelectQuery {
	return db.NewSelect().Model((*CallHome)(nil)).
		Where("call_id = ?", callID).
		Where("call_type = ?", callType).
		WhereGroup(" AND ", func(q *bun.SelectQuery) *bun.SelectQuery {
			q = q.Where("false")
			if scope.App != 0 {
				q = q.WhereOr("stream_app_pk = ?", scope.App)
			}
			if scope.Unpinned {
				q = q.WhereOr("stream_app_pk = 0")
			}
			return q
		})
}

// anotherElsewhere reports whether a call of the scope's app lives in another shard for a customer
// other than customerID, read with db.
func (r *CallHomes) anotherElsewhere(ctx context.Context, db bun.IDB, scope AppScope, callType, callID, customerID string) (bool, error) {
	if !r.store.homed {
		return false, nil
	}
	found, err := homesOf(db, scope, callType, callID).
		Where("customer_id <> ?", customerID).Where("shard <> ?", r.store.ShardName()).Exists(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: call homes elsewhere: %w", err))
	}
	return found, nil
}

// record makes this shard the home of a customer's call in app, unless it has one.
func (r *CallHomes) record(ctx context.Context, customerID string, app int64, callType, callID string) error {
	if !r.store.homed {
		return nil
	}
	home := CallHome{CallID: callID, CallType: callType, StreamAppPK: app, CustomerID: customerID,
		Shard: r.store.ShardName(), OpenedAt: time.Now().UTC()}
	if _, err := r.store.global.NewInsert().Model(&home).On("CONFLICT DO NOTHING").Exec(ctx); err != nil {
		return stack.Wrap(fmt.Errorf("store: record call home: %w", err))
	}
	return nil
}

// sweep forgets the homes of this shard's calls opened before before, which no call event
// can still be about.
func (r *CallHomes) sweep(ctx context.Context, before time.Time) error {
	if !r.store.homed {
		return nil
	}
	_, err := r.store.global.NewDelete().Model((*CallHome)(nil)).
		Where("shard = ?", r.store.ShardName()).
		Where("opened_at < ?", before.UTC()).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: sweep call homes: %w", err))
	}
	return nil
}
