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

// SessionHome is the shard a session was first opened in, which it stays in: its rows are
// written there alone, and a request about it that lands in another shard is handed to a router
// of this one.
type SessionHome struct {
	bun.BaseModel `bun:"table:session_homes"`

	SessionID  string    `bun:"session_id,pk"`
	CustomerID string    `bun:"customer_id,pk"`
	Shard      string    `bun:"shard,notnull"`
	OpenedAt   time.Time `bun:"opened_at,notnull"`
}

// SessionHomes are where sessions live, kept in the shared database so any shard can tell.
type SessionHomes struct {
	store *Store
}

// Of is the shard a customer's session lives in, empty for a session with no home: one of a
// deployment of one unnamed shard, or not shipped yet (Store.ShipIndex).
func (r *SessionHomes) Of(ctx context.Context, customerID, id string) (string, error) {
	if !r.store.homed {
		return "", nil
	}
	var shard string
	err := r.store.global.NewSelect().Model((*SessionHome)(nil)).Column("shard").
		Where("customer_id = ?", customerID).
		Where("session_id = ?", id).
		Scan(ctx, &shard)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", stack.Wrap(fmt.Errorf("store: session home: %w", err))
	}
	return shard, nil
}

// Elsewhere reports the shard a customer's session lives in when that is not this one, and
// empty when it is this one's or has no home.
func (r *SessionHomes) Elsewhere(ctx context.Context, customerID, id string) (string, error) {
	shard, err := r.Of(ctx, customerID, id)
	if err != nil || shard == r.store.ShardName() {
		return "", err
	}
	return shard, nil
}

// taken reports whether any customer's session that lives in another shard has id.
func (r *SessionHomes) taken(ctx context.Context, id string) (bool, error) {
	if !r.store.homed {
		return false, nil
	}
	taken, err := r.store.global.NewSelect().Model((*SessionHome)(nil)).
		Where("session_id = ?", id).
		Where("shard <> ?", r.store.ShardName()).
		Exists(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: session id taken: %w", err))
	}
	return taken, nil
}

// forget drops the home of a session deleted from this shard, so its id is free again.
func (r *SessionHomes) forget(ctx context.Context, customerID, id string) error {
	if !r.store.homed {
		return nil
	}
	_, err := r.store.global.NewDelete().Model((*SessionHome)(nil)).
		Where("customer_id = ?", customerID).
		Where("session_id = ?", id).
		Where("shard = ?", r.store.ShardName()).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: forget session home: %w", err))
	}
	return nil
}

// record makes this shard the home of homes that have none. A session that has one keeps it.
func (r *SessionHomes) record(ctx context.Context, homes []SessionHome) error {
	if !r.store.homed || len(homes) == 0 {
		return nil
	}
	for i := range homes {
		homes[i].Shard = r.store.ShardName()
	}
	if _, err := r.store.global.NewInsert().Model(&homes).On("CONFLICT DO NOTHING").Exec(ctx); err != nil {
		return stack.Wrap(fmt.Errorf("store: record session homes: %w", err))
	}
	return nil
}
