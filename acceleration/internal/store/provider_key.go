package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// ProviderKey is a key a customer keeps with the router for their own account at a vendor, in
// one region. Work routed on it is billed to them by the vendor, and by the router as BYOK.
type ProviderKey struct {
	bun.BaseModel `bun:"table:provider_keys,alias:pk"`

	CustomerID string `bun:"customer_id,pk"`
	Provider   string `bun:"provider,pk"`
	Region     string `bun:"region,pk"`
	// APIKeySealed is the key, sealed under KEKVersion and bound to the customer, the provider
	// and the region, so it opens on no other row.
	APIKeySealed []byte `bun:"api_key_sealed,notnull"`
	KEKVersion   int    `bun:"kek_version,notnull"`
	// KeyHint is the key's last four characters, empty for one too short to show any of.
	KeyHint   string    `bun:"key_hint,notnull"`
	CreatedAt time.Time `bun:"created_at,notnull"`
	UpdatedAt time.Time `bun:"updated_at,notnull"`
}

// Validate refuses a key without a customer, a well-formed provider and region, or a sealed
// key.
func (k *ProviderKey) Validate() error {
	return validate("provider key").
		required("customer_id", k.CustomerID).
		check(providerName.MatchString(k.Provider), "provider", "is not a provider name").
		check(regionName.MatchString(k.Region), "region", "is not a region name").
		check(len(k.APIKeySealed) > 0, "api_key_sealed", "is required").
		done()
}

var (
	providerName = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)
	regionName   = regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}$`)
)

// ErrNoProviderKey says a customer keeps no key for that provider in that region.
var ErrNoProviderKey = errors.New("store: no such provider key")

const (
	defaultProviderKeyLimit = 25
	maxProviderKeyLimit     = 200
)

// ProviderKeyLimit is the page size a provider key list uses for the limit asked for.
// Select returns one row more than this.
func ProviderKeyLimit(asked int) int {
	return clampLimit(asked, defaultProviderKeyLimit, maxProviderKeyLimit)
}

// ProviderKeyPosition is where a page of a customer's provider keys ends: the provider and the
// region of its last key, which are the key's own and so a total order.
type ProviderKeyPosition struct {
	Provider string `json:"p"`
	Region   string `json:"r"`
}

// ProviderKeyFilter picks a page of a customer's provider keys.
type ProviderKeyFilter struct {
	Limit int
	After *ProviderKeyPosition
}

// ProviderKeys are the keys customers keep for their own accounts at the vendors, in the
// shared database. Every request of a customer's that is routed reads all of theirs, so they
// are cached by customer, and stay sealed in the cache as they are in Postgres.
type ProviderKeys struct {
	store *Store
}

// Upsert stores a key, replacing the one the customer kept for its provider and region, fills
// in its timestamps and drops what every replica had cached of the customer's keys, so each
// routes on it from its next request. It answers the row it replaced, and false when there
// was none.
func (r *ProviderKeys) Upsert(ctx context.Context, key *ProviderKey) (ProviderKey, bool, error) {
	if err := key.Validate(); err != nil {
		return ProviderKey{}, false, stack.Wrap(err)
	}

	var was ProviderKey
	var replaced bool
	err := r.store.detachedWrite(ctx, func(ctx context.Context, tx bun.Tx) error {
		err := tx.NewSelect().Model(&was).
			Where("customer_id = ? AND provider = ? AND region = ?", key.CustomerID, key.Provider, key.Region).
			For("UPDATE").
			Scan(ctx)
		switch {
		case err == nil:
			replaced = true
		case !errors.Is(err, sql.ErrNoRows):
			return err
		}
		now := time.Now().UTC().Truncate(time.Microsecond)
		key.CreatedAt, key.UpdatedAt = now, now
		if replaced {
			key.CreatedAt = was.CreatedAt
		}
		_, err = tx.NewInsert().Model(key).
			On("CONFLICT (customer_id, provider, region) DO UPDATE").
			Set("api_key_sealed = EXCLUDED.api_key_sealed").
			Set("kek_version = EXCLUDED.kek_version").
			Set("key_hint = EXCLUDED.key_hint").
			Set("updated_at = EXCLUDED.updated_at").
			Exec(ctx)
		return err
	}, func() []string { return []string{providerKeysKey(key.CustomerID)} })
	if err != nil {
		return ProviderKey{}, false, stack.Wrap(fmt.Errorf("store: put provider key: %w", err))
	}
	return was, replaced, nil
}

// SelectAll returns every key a customer keeps, by provider and then region, through the
// cache. A customer keeps at most one per provider and region, so there is no page.
func (r *ProviderKeys) SelectAll(ctx context.Context, customerID string) ([]ProviderKey, error) {
	if customerID == "" {
		return nil, stack.Wrap(errors.New("store: customer id is required"))
	}
	return cached(ctx, r.store, providerKeysKey(customerID), func(ctx context.Context) ([]ProviderKey, error) {
		keys := []ProviderKey{}
		err := r.store.global.NewSelect().Model(&keys).
			Where("customer_id = ?", customerID).
			Order("provider", "region").
			Scan(ctx)
		if err != nil {
			return nil, stack.Wrap(fmt.Errorf("store: provider keys: %w", err))
		}
		return keys, nil
	})
}

// Select returns a page of a customer's keys, by provider and then region, one more than the
// limit so a caller can tell whether there is another.
func (r *ProviderKeys) Select(ctx context.Context, customerID string, filter ProviderKeyFilter) ([]ProviderKey, error) {
	if customerID == "" {
		return nil, stack.Wrap(errors.New("store: customer id is required"))
	}
	var keys []ProviderKey
	query := r.store.global.NewSelect().Model(&keys).Where("customer_id = ?", customerID)
	if filter.After != nil {
		query = query.Where("(provider, region) > (?, ?)", filter.After.Provider, filter.After.Region)
	}
	err := query.Order("provider", "region").Limit(ProviderKeyLimit(filter.Limit) + 1).Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: provider key page: %w", err))
	}
	return keys, nil
}

// SelectDaytona returns every customer's Daytona keys, for a job that reaches each customer's
// own account there, by customer and then region. SandboxDaytona is also the name
// providerkeys gives the provider.
func (r *ProviderKeys) SelectDaytona(ctx context.Context) ([]ProviderKey, error) {
	keys := []ProviderKey{}
	err := r.store.global.NewSelect().Model(&keys).
		Where("provider = ?", SandboxDaytona).
		Order("customer_id", "region").
		Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: provider keys of %s: %w", SandboxDaytona, err))
	}
	return keys, nil
}

// Delete forgets a customer's key for a provider in a region, here and in every replica's
// cache, and answers the row it deleted. Work is routed on the deployment's own key from then
// on.
func (r *ProviderKeys) Delete(ctx context.Context, customerID, provider, region string) (ProviderKey, error) {
	if customerID == "" || provider == "" || region == "" {
		return ProviderKey{}, stack.Wrap(errors.New("store: a customer, a provider and a region are required"))
	}
	var deleted ProviderKey
	err := r.store.detachedWrite(ctx, func(ctx context.Context, tx bun.Tx) error {
		return tx.NewDelete().Model(&deleted).
			Where("customer_id = ? AND provider = ? AND region = ?", customerID, provider, region).
			Returning("*").
			Scan(ctx)
	}, func() []string {
		// Named once the write answered: a commit that fails can still have landed.
		if deleted.CustomerID == "" {
			return nil
		}
		return []string{providerKeysKey(customerID)}
	})
	if errors.Is(err, sql.ErrNoRows) {
		return ProviderKey{}, stack.Wrap(fmt.Errorf("%w: %s in %s", ErrNoProviderKey, provider, region))
	}
	if err != nil {
		return ProviderKey{}, stack.Wrap(fmt.Errorf("store: delete provider key: %w", err))
	}
	return deleted, nil
}

func providerKeysKey(customerID string) string { return cacheKey("provider-keys", customerID) }
