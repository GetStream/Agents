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

// MoveCursor is where a copy of an app from another deployment stands here.
type MoveCursor struct {
	bun.BaseModel `bun:"table:data_move_cursors,alias:dmc"`

	CustomerID string `bun:"customer_id,pk"`
	// Source is the deployment the app is copied from.
	Source string `bun:"source,pk"`
	// Cursor is the source's cursor this deployment holds every change up to, nil while a copy
	// is underway.
	Cursor *int64 `bun:"cursor"`
	// ReadAt is when the source was read for it.
	ReadAt time.Time `bun:"read_at,notnull"`
}

// Validate refuses a cursor that names no app, no source or no time it was read at.
func (c *MoveCursor) Validate() error {
	return validate("move cursor").
		required("customer_id", c.CustomerID).
		required("source", c.Source).
		check(!c.ReadAt.IsZero(), "read_at", "is required").
		done()
}

// MoveCursors are where each copy of an app from another deployment stands, in the shared
// database.
type MoveCursors struct {
	store *Store
}

// Get answers where the copy of a customer from a source stands here, nil when none was ever
// started.
func (r *MoveCursors) Get(ctx context.Context, customerID, source string) (*MoveCursor, error) {
	var saved MoveCursor
	err := r.store.global.NewSelect().Model(&saved).Where("customer_id = ? AND source = ?", customerID, source).Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: move cursor: %w", err))
	}
	return &saved, nil
}

// Upsert records where the copy of a customer from a source stands here: the cursor the source
// was read up to at ReadAt, or nil for a copy that has started and not finished. A cursor never
// moves back: of two runs at once, the one that read less cannot undo the other.
func (r *MoveCursors) Upsert(ctx context.Context, cursor *MoveCursor) error {
	if err := cursor.Validate(); err != nil {
		return stack.Wrap(err)
	}
	_, err := r.store.global.NewInsert().Model(cursor).
		On("CONFLICT (customer_id, source) DO UPDATE").
		Set("cursor = CASE WHEN EXCLUDED.cursor IS NULL THEN NULL ELSE GREATEST(dmc.cursor, EXCLUDED.cursor) END").
		Set("read_at = CASE WHEN EXCLUDED.cursor IS NULL THEN EXCLUDED.read_at ELSE GREATEST(dmc.read_at, EXCLUDED.read_at) END").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: save move cursor: %w", err))
	}
	return nil
}
