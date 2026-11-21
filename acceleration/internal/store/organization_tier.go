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

// OrganizationTier is the tier Stream last sent for an organization, an opaque string, kept for
// work no request carried.
type OrganizationTier struct {
	bun.BaseModel `bun:"table:organization_tiers,alias:ot"`

	OrganizationID string    `bun:"organization_id,pk"`
	Tier           string    `bun:"tier,notnull"`
	SeenAt         time.Time `bun:"seen_at,notnull"`
}

// Validate refuses a tier without an organization, a tier or when it was seen.
func (t *OrganizationTier) Validate() error {
	return validate("organization tier").
		required("organization_id", t.OrganizationID).
		required("tier", t.Tier).
		check(!t.SeenAt.IsZero(), "seen_at", "is required").
		done()
}

// OrganizationTiers are the tiers organizations were last seen with, in the shared database.
type OrganizationTiers struct {
	store *Store
}

// Upsert records the tier an organization was seen with at its SeenAt. A tier seen earlier
// than the one recorded changes nothing: routers write what they saw in the background, and
// two writes may land in either order. Earlier is by each writing router's clock, so routers
// whose clocks disagree by more than the gap between two tiers may keep the older.
func (r *OrganizationTiers) Upsert(ctx context.Context, seen *OrganizationTier) error {
	if err := seen.Validate(); err != nil {
		return stack.Wrap(err)
	}
	seen.SeenAt = seen.SeenAt.UTC()
	_, err := r.store.global.NewInsert().Model(seen).
		On("CONFLICT (organization_id) DO UPDATE").
		Set("tier = EXCLUDED.tier").
		Set("seen_at = EXCLUDED.seen_at").
		Where("ot.seen_at < EXCLUDED.seen_at").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: set organization tier: %w", err))
	}
	return nil
}

// Get returns the tier an organization was last seen with, or the zero row for one never seen
// with any.
func (r *OrganizationTiers) Get(ctx context.Context, organizationID string) (OrganizationTier, error) {
	var seen OrganizationTier
	err := r.store.global.NewSelect().Model(&seen).Where("organization_id = ?", organizationID).Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return OrganizationTier{}, nil
	}
	if err != nil {
		return OrganizationTier{}, stack.Wrap(fmt.Errorf("store: tier of: %w", err))
	}
	return seen, nil
}
