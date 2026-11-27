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

// Insert stores a new version of an app's data key.
func (r *AppDataKeys) Insert(ctx context.Context, key *AppDataKey) error {
	if err := key.Validate(); err != nil {
		return stack.Wrap(err)
	}
	key.CreatedAt = time.Now().UTC().Truncate(time.Microsecond)
	if _, err := r.store.global.NewInsert().Model(key).Exec(ctx); err != nil {
		return stack.Wrap(fmt.Errorf("store: insert app data key: %w", err))
	}
	return nil
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
