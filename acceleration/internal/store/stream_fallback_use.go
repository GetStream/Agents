package store

import (
	"context"
	"fmt"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// StreamFallbackUse is how often app mode wrote a customer with no app of its own into
// the deployment's app, and over what time.
type StreamFallbackUse struct {
	bun.BaseModel `bun:"table:stream_fallback_uses,alias:sfu"`

	CustomerID string    `bun:"customer_id,pk"`
	FirstAt    time.Time `bun:"first_at,notnull"`
	LastAt     time.Time `bun:"last_at,notnull"`
	Uses       int64     `bun:"uses,notnull"`
}

// Validate refuses a use without a customer, or of fewer than one.
func (u *StreamFallbackUse) Validate() error {
	return validate("stream fallback use").
		required("customer_id", u.CustomerID).
		check(u.Uses > 0, "uses", "is not positive").
		check(!u.LastAt.IsZero(), "last_at", "is required").
		done()
}

// StreamFallbackUses are the customers that wrote into the deployment's own Stream app, in
// the shared database.
type StreamFallbackUses struct {
	store *Store
}

// Add adds the use's uses to its customer's, the last at its LastAt.
func (r *StreamFallbackUses) Add(ctx context.Context, use *StreamFallbackUse) error {
	if err := use.Validate(); err != nil {
		return stack.Wrap(err)
	}
	use.LastAt = use.LastAt.UTC()
	use.FirstAt = use.LastAt
	_, err := r.store.global.NewInsert().Model(use).
		On("CONFLICT (customer_id) DO UPDATE").
		Set("last_at = GREATEST(sfu.last_at, EXCLUDED.last_at)").
		Set("uses = sfu.uses + EXCLUDED.uses").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: record stream fallback use: %w", err))
	}
	return nil
}

// Select returns the customers that used the fallback since the time given, the most recent
// first.
func (r *StreamFallbackUses) Select(ctx context.Context, since time.Time) ([]StreamFallbackUse, error) {
	var uses []StreamFallbackUse
	err := r.store.global.NewSelect().Model(&uses).Where("last_at >= ?", since.UTC()).Order("last_at DESC").Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: read stream fallback uses: %w", err))
	}
	return uses, nil
}
