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

// AppDataKey is one version of an app's data key, as Cloud KMS wrapped it. The plain key is
// never stored.
type AppDataKey struct {
	bun.BaseModel `bun:"table:app_data_keys,alias:adk"`

	CustomerID string    `bun:"customer_id,pk"`
	Version    int       `bun:"version,pk"`
	WrappedKey []byte    `bun:"wrapped_key,notnull"`
	CreatedAt  time.Time `bun:"created_at,notnull"`
}

// Validate refuses a data key without an app, a version or a wrapped key.
func (k *AppDataKey) Validate() error {
	return validate("app data key").
		required("customer_id", k.CustomerID).
		check(k.Version >= 1, "version", "must be 1 or more").
		check(len(k.WrappedKey) > 0, "wrapped_key", "is required").
		done()
}

// ErrNoAppDataKey says the app has no data key of that version.
var ErrNoAppDataKey = errors.New("store: no such app data key")

// AppDataKeys are the apps' wrapped data keys, in the shared database.
type AppDataKeys struct {
	store *Store
}

// InsertIfAbsent stores a new version of an app's data key unless the app has that version
// already, and says whether it stored it. Two replicas can make an app's first key at once,
// and only one of them may win, since every secret of the app must open under the same key.
func (r *AppDataKeys) InsertIfAbsent(ctx context.Context, key *AppDataKey) (bool, error) {
	if err := key.Validate(); err != nil {
		return false, stack.Wrap(err)
	}
	key.CreatedAt = time.Now().UTC().Truncate(time.Microsecond)
	result, err := r.store.global.NewInsert().Model(key).On("CONFLICT (customer_id, version) DO NOTHING").Exec(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: insert app data key: %w", err))
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: insert app data key: %w", err))
	}
	return inserted == 1, nil
}

// Get returns one version of the app's data key, or ErrNoAppDataKey.
func (r *AppDataKeys) Get(ctx context.Context, customerID string, version int) (AppDataKey, error) {
	var key AppDataKey
	err := r.store.global.NewSelect().Model(&key).
		Where("adk.customer_id = ?", customerID).
		Where("adk.version = ?", version).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return AppDataKey{}, stack.Wrap(ErrNoAppDataKey)
	}
	if err != nil {
		return AppDataKey{}, stack.Wrap(fmt.Errorf("store: get app data key: %w", err))
	}
	return key, nil
}
