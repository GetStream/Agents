package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// What a PhoneNumberOrder asks a vendor to do.
const (
	OrderBuy     = "buy"
	OrderRelease = "release"
)

// PhoneNumberOrder is a purchase or a release of a number, kept from before the vendor is asked
// until the number's row says what the vendor did. One left behind is an outcome that was not
// recorded, which the vendor's own account settles.
type PhoneNumberOrder struct {
	bun.BaseModel `bun:"table:phone_number_orders,alias:pno"`

	ID         int64  `bun:"id,pk,autoincrement"`
	CustomerID string `bun:"customer_id,notnull"`
	Vendor     string `bun:"vendor,notnull"`
	E164       string `bun:"e164,notnull"`
	// Action is OrderBuy or OrderRelease.
	Action string `bun:"action,notnull"`
	// Country, MonthlyCostMicros and Tags are what a purchase's row is written with when the
	// vendor's answer was lost: the country it was offered from and the price it was quoted.
	Country           string            `bun:"country,notnull"`
	MonthlyCostMicros int64             `bun:"monthly_cost_micros,notnull"`
	Tags              map[string]string `bun:"tags,type:jsonb,nullzero"`
	// CreatedAt is when the number was bought or released, as its row says.
	CreatedAt time.Time `bun:"created_at,notnull"`
	// NextAttemptAt is when the sweep may settle the order, and Attempts how often a sweep
	// claimed it (phoneNumberOrders).
	Attempts      int       `bun:"attempts,notnull"`
	NextAttemptAt time.Time `bun:"next_attempt_at,notnull"`
}

// Validate refuses an order no vendor could be asked about.
func (o *PhoneNumberOrder) Validate() error {
	return validate("phone number order").
		required("customer_id", o.CustomerID).
		required("vendor", o.Vendor).
		phoneNumber("e164", o.E164).
		oneOf("action", o.Action, OrderBuy, OrderRelease).
		check(!o.CreatedAt.IsZero(), "created_at", "is required").
		check(!o.NextAttemptAt.IsZero(), "next_attempt_at", "is required").
		done()
}

// ErrNumberRecorded is Bought's error when a row already holds the number.
var ErrNumberRecorded = errors.New("store: a row already holds the number")

// PhoneNumberOrders are the purchases and releases of numbers whose outcome is not recorded
// yet, in the shared database.
type PhoneNumberOrders struct {
	store *Store
}

// Insert keeps an order, before the vendor is asked.
func (r *PhoneNumberOrders) Insert(ctx context.Context, order *PhoneNumberOrder) error {
	if err := order.Validate(); err != nil {
		return stack.Wrap(err)
	}
	order.CreatedAt = order.CreatedAt.UTC().Truncate(time.Microsecond)
	order.NextAttemptAt = order.NextAttemptAt.UTC().Truncate(time.Microsecond)
	if _, err := r.store.global.NewInsert().Model(order).Returning("id").Exec(ctx); err != nil {
		return stack.Wrap(fmt.Errorf("store: record number order: %w", err))
	}
	return nil
}

// Delete forgets an order whose outcome is recorded. One already gone is no error.
func (r *PhoneNumberOrders) Delete(ctx context.Context, id int64) error {
	if _, err := r.store.global.NewDelete().Model((*PhoneNumberOrder)(nil)).Where("id = ?", id).Exec(ctx); err != nil {
		return stack.Wrap(fmt.Errorf("store: forget number order: %w", err))
	}
	return nil
}

// phoneNumberOrders is the queue of orders the sweep settles.
var phoneNumberOrders = queue{name: "phone number orders", alias: "pno", key: "id"}

// Claim takes up to limit orders due now, oldest due first, and moves each one's next
// attempt to the end of lease: another router's claim skips them, and one the sweep could not settle
// is tried again behind the orders due before it.
func (r *PhoneNumberOrders) Claim(ctx context.Context, lease time.Duration, limit int) ([]PhoneNumberOrder, error) {
	return claim[PhoneNumberOrder](ctx, r.store.global, phoneNumberOrders, lease, limit, nil, "pno.*")
}

// Bought records the number an order bought and forgets the order, as one. It reports false,
// and records nothing, for an order that is gone, settled by another router. A row already
// holding the number is ErrNumberRecorded, and keeps the order.
func (r *PhoneNumberOrders) Bought(ctx context.Context, id int64, number *PhoneNumber) (bool, error) {
	var bought bool
	err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		result, err := tx.NewDelete().Model((*PhoneNumberOrder)(nil)).Where("id = ?", id).Exec(ctx)
		if err != nil {
			return fmt.Errorf("store: forget number order: %w", err)
		}
		if deleted, _ := result.RowsAffected(); deleted == 0 {
			return nil
		}
		err = insertNumber(ctx, tx, number)
		if isViolation(err, uniqueViolation) {
			return ErrNumberRecorded
		}
		bought = err == nil
		return err
	})
	if err != nil {
		return false, stack.Wrap(err)
	}
	return bought, nil
}
