package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// SetTimezone records the zone a user was explicitly known to be in. The
// row is created when there is none; on an existing row only the zone moves,
// never the kind the claim path reads, and a zone it already holds is not
// written again: every session opened with a zone comes through here.
func (r *Users) SetTimezone(ctx context.Context, customerID, userID, kind, zone string) error {
	if customerID == "" || userID == "" || zone == "" {
		return nil
	}
	if kind == "" {
		kind = UserKindAuthenticated
	}
	user := &User{CustomerID: customerID, ID: userID, Kind: kind, Timezone: zone, CreatedAt: time.Now().UTC()}
	_, err := r.store.global.NewInsert().Model(user).
		On("CONFLICT (customer_id, id) DO UPDATE").
		Set("timezone = EXCLUDED.timezone").
		Where("u.timezone IS DISTINCT FROM EXCLUDED.timezone").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: set user timezone: %w", err))
	}
	return nil
}

// GetTimezone returns the zone stored for a user, or empty.
func (r *Users) GetTimezone(ctx context.Context, customerID, userID string) (string, error) {
	if customerID == "" || userID == "" {
		return "", nil
	}
	var user User
	err := r.store.global.NewSelect().Model(&user).
		Column("timezone").
		Where("customer_id = ?", customerID).
		Where("id = ?", userID).
		Limit(1).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", stack.Wrap(fmt.Errorf("store: user timezone: %w", err))
	}
	return user.Timezone, nil
}
