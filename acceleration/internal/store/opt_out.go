package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// OptOut is somebody who asked not to be reached on a channel, or on any.
type OptOut struct {
	bun.BaseModel `bun:"table:opt_outs,alias:oo"`

	ID         string     `bun:"id,pk"`
	CustomerID string     `bun:"customer_id,notnull"`
	Recipient  string     `bun:"recipient,notnull"`
	Channel    string     `bun:"channel,notnull"`
	Source     string     `bun:"source,notnull"`
	CreatedAt  time.Time  `bun:"created_at,notnull"`
	RevokedAt  *time.Time `bun:"revoked_at"`
}

// OptOutAll is an opt-out from every channel at once.
const OptOutAll = "all"

// Validate refuses an opt-out that names nobody.
func (o *OptOut) Validate() error {
	return validate("opt-out").
		required("customer_id", o.CustomerID).
		required("recipient", o.Recipient).
		required("channel", o.Channel).
		done()
}

// OptOuts are the people who asked not to be reached, in the shared database.
type OptOuts struct {
	store *Store
}

// ErrUnknownOptOut is an opt-out id the app does not hold, or one already revoked.
var ErrUnknownOptOut = errors.New("store: there is no such opt-out")

// Insert records that somebody asked not to be reached. Asking twice keeps the first.
func (r *OptOuts) Insert(ctx context.Context, optOut *OptOut) error {
	return insertOptOut(ctx, r.store.global, optOut)
}

// insertOptOut is OptOuts.Insert on db, which may be the transaction a delivery is claimed
// in (ChannelDeliveries.ClaimStop).
func insertOptOut(ctx context.Context, db bun.IDB, optOut *OptOut) error {
	if err := optOut.Validate(); err != nil {
		return stack.Wrap(err)
	}
	optOut.ID = newID()
	optOut.CreatedAt = time.Now().UTC().Truncate(time.Microsecond)
	_, err := db.NewInsert().Model(optOut).
		On("CONFLICT (customer_id, recipient, channel) WHERE revoked_at IS NULL DO NOTHING").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: opt out: %w", err))
	}
	err = db.NewSelect().Model(optOut).
		Where("customer_id = ?", optOut.CustomerID).
		Where("recipient = ?", optOut.Recipient).
		Where("channel = ?", optOut.Channel).
		Where("revoked_at IS NULL").
		Scan(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: opt out: %w", err))
	}
	return nil
}

// Revoke lifts one opt-out, keeping the row.
func (r *OptOuts) Revoke(ctx context.Context, customerID, id string) error {
	result, err := r.store.global.NewUpdate().Model((*OptOut)(nil)).
		Set("revoked_at = ?", time.Now().UTC()).
		Where("customer_id = ?", customerID).
		Where("id = ?", id).
		Where("revoked_at IS NULL").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: revoke opt-out: %w", err))
	}
	if affected, _ := result.RowsAffected(); affected == 0 {
		return stack.Wrap(ErrUnknownOptOut)
	}
	return nil
}

// RevokeAll lifts every opt-out a recipient has on a channel and on all of them, as
// texting START does.
func (r *OptOuts) RevokeAll(ctx context.Context, customerID, recipient, channel string) error {
	return revokeAllOptOuts(ctx, r.store.global, customerID, recipient, channel)
}

// revokeAllOptOuts is OptOuts.RevokeAll on db, which may be the transaction a delivery is
// claimed in (ChannelDeliveries.ClaimStart).
func revokeAllOptOuts(ctx context.Context, db bun.IDB, customerID, recipient, channel string) error {
	_, err := db.NewUpdate().Model((*OptOut)(nil)).
		Set("revoked_at = ?", time.Now().UTC()).
		Where("customer_id = ?", customerID).
		Where("recipient = ?", recipient).
		Where("channel IN (?)", bun.In([]string{channel, OptOutAll})).
		Where("revoked_at IS NULL").
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("store: revoke opt-outs: %w", err)
	}
	return nil
}

// OptedOut reports whether a recipient asked not to be reached on a channel, or on any.
func (r *OptOuts) OptedOut(ctx context.Context, customerID, recipient, channel string) (bool, error) {
	found, err := r.store.global.NewSelect().Model((*OptOut)(nil)).
		Where("customer_id = ?", customerID).
		Where("recipient = ?", recipient).
		Where("channel IN (?)", bun.In([]string{channel, OptOutAll})).
		Where("revoked_at IS NULL").
		Exists(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: opted out: %w", err))
	}
	return found, nil
}

// Select pages through an app's live opt-outs, newest first.
func (r *OptOuts) Select(ctx context.Context, customerID string, limit int, after *CreatedPosition) ([]OptOut, error) {
	optOuts := []OptOut{}
	query := r.store.global.NewSelect().Model(&optOuts).
		Where("customer_id = ?", customerID).
		Where("revoked_at IS NULL")
	if after != nil {
		query = query.Where("(created_at, id) < (?, ?)", after.At, after.ID)
	}
	err := query.Order("created_at DESC", "id DESC").Limit(DLCLimit(limit) + 1).Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: opt-outs: %w", err))
	}
	return optOuts, nil
}
