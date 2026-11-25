package store

import (
	"context"
	"fmt"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// Organization owns apps. It exists so a bill and a rate limit have something to name that
// outlives any one app.
type Organization struct {
	bun.BaseModel `bun:"table:organizations,alias:o"`

	ID        string    `bun:"id,pk"`
	Name      string    `bun:"name,notnull"`
	CreatedAt time.Time `bun:"created_at,notnull"`
}

// Validate refuses an organization with no name.
func (o *Organization) Validate() error {
	return validate("organization").required("name", o.Name).done()
}

// Organizations are the organizations apps belong to, in the shared database.
type Organizations struct {
	store *Store
}

// Insert stores a new organization and fills in its id and timestamp.
func (r *Organizations) Insert(ctx context.Context, org *Organization) error {
	if err := org.Validate(); err != nil {
		return stack.Wrap(err)
	}

	org.ID = newID()
	org.CreatedAt = time.Now().UTC()

	if _, err := r.store.global.NewInsert().Model(org).Exec(ctx); err != nil {
		return stack.Wrap(fmt.Errorf("store: create organization: %w", err))
	}
	return nil
}
