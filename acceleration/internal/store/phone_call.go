package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// PhoneCall is one phone call the router placed, and how it ended. Which outcome a call has is
// decided in package outcome; the store only keeps it.
type PhoneCall struct {
	bun.BaseModel `bun:"table:phone_calls,alias:pc"`

	ID int64 `bun:"id,pk,autoincrement"`
	// Token is in the URL the vendor posts the call's status to, and the only thing proving the
	// post is about this call. Empty once swept.
	Token      string `bun:"token,nullzero"`
	CustomerID string `bun:"customer_id,notnull"`
	// StreamAppPK is the Stream app the call is in. Zero, stored as NULL, is the deployment's
	// own app.
	StreamAppPK  int64  `bun:"stream_app_pk,nullzero"`
	CallType     string `bun:"call_type,notnull"`
	CallID       string `bun:"call_id,notnull"`
	Vendor       string `bun:"vendor,notnull"`
	LocalNumber  string `bun:"local_number,notnull"`
	RemoteNumber string `bun:"remote_number,notnull"`
	// Reports is whether the vendor posts a final status for the call.
	Reports      bool       `bun:"reports,notnull"`
	VendorCallID string     `bun:"vendor_call_id,nullzero"`
	PlacedAt     time.Time  `bun:"placed_at,notnull"`
	AnsweredAt   *time.Time `bun:"answered_at"`
	EndedAt      *time.Time `bun:"ended_at"`
	// Outcome is empty until decided.
	Outcome      string     `bun:"outcome,nullzero"`
	VendorStatus string     `bun:"vendor_status,nullzero"`
	DecidedAt    *time.Time `bun:"decided_at"`
}

// Validate refuses a call no vendor post could be matched to.
func (c *PhoneCall) Validate() error {
	return validate("phone call").
		required("customer_id", c.CustomerID).
		required("call_type", c.CallType).
		required("call_id", c.CallID).
		required("vendor", c.Vendor).
		oneOf("outcome", c.Outcome, "", "completed", "busy", "no_answer", "failed", "canceled", "unknown").
		check(!c.PlacedAt.IsZero(), "placed_at", "is required").
		done()
}

// ErrNoPhoneCall is what a phone call no shard holds reads as.
var ErrNoPhoneCall = errors.New("store: no such phone call")

// PhoneCallWhere finds one phone call: by Token when it is set, else by the rest. The newest
// match is the one.
type PhoneCallWhere struct {
	Token        string
	CustomerID   string
	StreamAppPK  int64
	CallType     string
	CallID       string
	LocalNumber  string
	RemoteNumber string
}

func (w PhoneCallWhere) on(q *bun.SelectQuery) *bun.SelectQuery {
	if w.Token != "" {
		return q.Where("pc.token = ?", w.Token)
	}
	return q.Where("pc.customer_id = ?", w.CustomerID).
		Where("COALESCE(pc.stream_app_pk, 0) = ?", w.StreamAppPK).
		Where("pc.call_type = ?", w.CallType).
		Where("pc.call_id = ?", w.CallID).
		Where("pc.local_number = ?", w.LocalNumber).
		Where("pc.remote_number = ?", w.RemoteNumber)
}

// PhoneCalls are the calls the router placed, each on the shard that placed it.
type PhoneCalls struct {
	store *Store
}

// Insert saves a phone call this shard is placing.
func (r *PhoneCalls) Insert(ctx context.Context, call *PhoneCall) error {
	if err := call.Validate(); err != nil {
		return stack.Wrap(err)
	}
	if _, err := r.store.shard.NewInsert().Model(call).Exec(ctx); err != nil {
		return stack.Wrap(fmt.Errorf("store: insert phone call: %w", err))
	}
	return nil
}

// Delete removes the phone call with token, from whichever shard holds it.
func (r *PhoneCalls) Delete(ctx context.Context, token string) error {
	_, err := onEveryShard(ctx, r.store, func(ctx context.Context, db bun.IDB) (sql.Result, error) {
		return db.NewDelete().Model((*PhoneCall)(nil)).Where("token = ?", token).Exec(ctx)
	})
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: delete phone call: %w", err))
	}
	return nil
}

// Change locks the row where finds, in whichever shard holds it, hands it to change, and
// writes back what change left. change returning false writes nothing. It returns the row as
// it now stands, or ErrNoPhoneCall.
//
// The lock is what makes concurrent changes safe: a vendor's post and the call's own events
// can arrive at once, and each must see what the other wrote.
func (r *PhoneCalls) Change(ctx context.Context, where PhoneCallWhere, change func(*PhoneCall) bool) (PhoneCall, error) {
	// A vendor's post lands on whichever shard is nearest it, not the one that placed the call.
	for _, db := range r.store.shards() {
		var call PhoneCall
		err := db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
			err := where.on(tx.NewSelect().Model(&call)).OrderExpr("pc.id DESC").Limit(1).For("UPDATE").Scan(ctx)
			if err != nil {
				return err
			}
			changed := call
			if !change(&changed) {
				return nil
			}
			if _, err := tx.NewUpdate().Model(&changed).WherePK().Exec(ctx); err != nil {
				return err
			}
			call = changed
			return nil
		})
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return PhoneCall{}, stack.Wrap(fmt.Errorf("store: change phone call: %w", err))
		}
		return call, nil
	}
	return PhoneCall{}, ErrNoPhoneCall
}

// GetByToken reads the phone call token was made for, from whichever shard holds it, without
// locking it. It returns ErrNoPhoneCall when none does.
func (r *PhoneCalls) GetByToken(ctx context.Context, token string) (PhoneCall, error) {
	call, err := whicheverShard(ctx, r.store, func(ctx context.Context, db bun.IDB) (PhoneCall, error) {
		var call PhoneCall
		err := PhoneCallWhere{Token: token}.on(db.NewSelect().Model(&call)).OrderExpr("pc.id DESC").Limit(1).Scan(ctx)
		return call, err
	})
	if errors.Is(err, sql.ErrNoRows) {
		return PhoneCall{}, ErrNoPhoneCall
	}
	if err != nil {
		return PhoneCall{}, stack.Wrap(fmt.Errorf("store: phone call by token: %w", err))
	}
	return call, nil
}

// Select returns the customer's newest phone call for each of callIDs, from every shard. A
// call id with none is left out.
func (r *PhoneCalls) Select(ctx context.Context, customerID string, callIDs []string) ([]PhoneCall, error) {
	if len(callIDs) == 0 {
		return nil, nil
	}
	lists, err := everyShard(ctx, r.store, func(ctx context.Context, db bun.IDB) ([]PhoneCall, error) {
		var calls []PhoneCall
		err := db.NewSelect().Model(&calls).
			DistinctOn("pc.call_id").
			Where("pc.customer_id = ?", customerID).
			Where("pc.call_id IN (?)", bun.In(callIDs)).
			OrderExpr("pc.call_id, pc.id DESC").
			Scan(ctx)
		return calls, err
	})
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: phone calls: %w", err))
	}
	newest := map[string]PhoneCall{}
	for _, call := range slices.Concat(lists...) {
		if kept, ok := newest[call.CallID]; !ok || call.PlacedAt.After(kept.PlacedAt) {
			newest[call.CallID] = call
		}
	}
	return slices.Collect(maps.Values(newest)), nil
}

// ClearTokens clears the token of every call this shard placed before placedBefore, so a
// vendor's post for it is refused, and keeps the rows. Each shard clears its own.
func (r *PhoneCalls) ClearTokens(ctx context.Context, placedBefore time.Time) (int64, error) {
	result, err := r.store.shard.NewUpdate().Model((*PhoneCall)(nil)).
		Set("token = NULL").
		Where("token IS NOT NULL").
		Where("placed_at < ?", placedBefore).
		Exec(ctx)
	if err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: clear phone call tokens: %w", err))
	}
	cleared, err := result.RowsAffected()
	if err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: clear phone call tokens: %w", err))
	}
	return cleared, nil
}
