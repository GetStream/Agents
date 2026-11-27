package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// Shard is the shard that last served a request a user's own client sent, empty when none
// has. It is read through the row cache, which Served drops on every replica when it changes.
func (r *Users) Shard(ctx context.Context, customerID, userID string) (string, error) {
	if customerID == "" || userID == "" {
		return "", nil
	}
	return cached(ctx, r.store, userShardKey(customerID, userID), func(ctx context.Context) (string, error) {
		var user User
		err := r.store.global.NewSelect().Model(&user).
			Column("shard").
			Where("customer_id = ?", customerID).
			Where("id = ?", userID).
			Scan(ctx)
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		if err != nil {
			return "", stack.Wrap(fmt.Errorf("store: user shard: %w", err))
		}
		return user.Shard, nil
	})
}

// Served records that shard served a request the user's own client sent. Every such request
// says it, so the row is written only when the shard held for the user is another; a user
// with no row yet has nothing written, and is written once a row is there.
func (r *Users) Served(ctx context.Context, customerID, userID, shard string) error {
	if customerID == "" || userID == "" || shard == "" {
		return nil
	}
	held, err := r.Shard(ctx, customerID, userID)
	if err != nil {
		return err
	}
	if held == shard {
		return nil
	}
	written, err := r.store.global.NewUpdate().Model((*User)(nil)).
		Set("shard = ?", shard).
		Where("customer_id = ?", customerID).
		Where("id = ?", userID).
		Where("shard IS DISTINCT FROM ?", shard).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: record user shard: %w", err))
	}
	if changed, err := written.RowsAffected(); err == nil && changed > 0 {
		r.store.forget(ctx, userShardKey(customerID, userID))
	}
	return nil
}

func userShardKey(customerID, userID string) string {
	return cacheKey("user_shard", customerID, userID)
}
