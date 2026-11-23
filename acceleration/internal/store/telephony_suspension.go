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

// TelephonySuspension is an app Stream staff stopped from using the phone.
type TelephonySuspension struct {
	bun.BaseModel `bun:"table:telephony_suspensions,alias:ts"`

	CustomerID string `bun:"customer_id,pk"`
	// Reason is why, for the staff who read it later. The app is never shown it.
	Reason      string    `bun:"reason,notnull"`
	SuspendedBy string    `bun:"suspended_by,notnull"`
	SuspendedAt time.Time `bun:"suspended_at,notnull"`
}

// Validate refuses a suspension without a customer, a reason or who made it.
func (t *TelephonySuspension) Validate() error {
	return validate("telephony suspension").
		required("customer_id", t.CustomerID).
		required("reason", t.Reason).
		required("suspended_by", t.SuspendedBy).
		done()
}

// ErrNotSuspended is an app whose telephony is not suspended.
var ErrNotSuspended = errors.New("store: this app's telephony is not suspended")

// TelephonySuspensions are the apps Stream staff stopped from using the phone, in the shared
// database.
type TelephonySuspensions struct {
	store *Store
}

// Get returns why an app's telephony is suspended, or ErrNotSuspended.
func (r *TelephonySuspensions) Get(ctx context.Context, customerID string) (TelephonySuspension, error) {
	var suspension TelephonySuspension
	err := r.store.global.NewSelect().Model(&suspension).Where("customer_id = ?", customerID).Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return TelephonySuspension{}, stack.Wrap(ErrNotSuspended)
	}
	if err != nil {
		return TelephonySuspension{}, stack.Wrap(fmt.Errorf("store: telephony suspension: %w", err))
	}
	return suspension, nil
}

// Upsert suspends an app's telephony, replacing the reason of a suspension already in place.
func (r *TelephonySuspensions) Upsert(ctx context.Context, suspension *TelephonySuspension) error {
	if err := suspension.Validate(); err != nil {
		return stack.Wrap(err)
	}
	suspension.SuspendedAt = time.Now().UTC().Truncate(time.Microsecond)
	_, err := r.store.global.NewInsert().Model(suspension).
		On("CONFLICT (customer_id) DO UPDATE").
		Set("reason = EXCLUDED.reason").
		Set("suspended_by = EXCLUDED.suspended_by").
		Set("suspended_at = EXCLUDED.suspended_at").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: suspend telephony: %w", err))
	}
	return nil
}

// Delete lets an app use the phone again, or returns ErrNotSuspended.
func (r *TelephonySuspensions) Delete(ctx context.Context, customerID string) error {
	result, err := r.store.global.NewDelete().Model((*TelephonySuspension)(nil)).
		Where("customer_id = ?", customerID).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: lift telephony suspension: %w", err))
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: lift telephony suspension: %w", err))
	}
	if affected == 0 {
		return stack.Wrap(ErrNotSuspended)
	}
	return nil
}
