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

// queue is a table of work routers claim by next_attempt_at. A claim moves a row's next
// attempt to the end of its lease and counts the attempt in attempts, so a row whose router
// died holding it comes due again by itself. A write the claim makes afterwards names the
// attempts it saw (heldBy), so a slow claim and the one that took the row after its lease ran
// out never both write it.
//
// Every queue names these columns the same: attempts and next_attempt_at. A queue whose rows
// are held by a named router rather than by a lease's end adds lease_until and leased_by.
type queue struct {
	// name says which queue an error is about.
	name string
	// alias is the model's, as its bun tag gives it.
	alias string
	// key is the column a row is known by.
	key string
}

// claim leases up to limit rows due at now until leaseUntil and returns them with columns.
// pick narrows what is due, and may be nil. Rows are taken oldest due first, and a row another
// claim is taking is skipped rather than waited on.
func claim[T any](ctx context.Context, db bun.IDB, q queue, now, leaseUntil time.Time, limit int,
	pick func(*bun.SelectQuery) *bun.SelectQuery, columns string) ([]T, error) {
	var claimed []T
	if limit < 1 {
		return claimed, nil
	}
	due := db.NewSelect().Model((*T)(nil)).
		Column(q.alias+"."+q.key).
		Where("?.next_attempt_at <= ?", bun.Ident(q.alias), now.UTC())
	if pick != nil {
		due = pick(due)
	}
	due = due.OrderExpr("?.next_attempt_at, ?", bun.Ident(q.alias), bun.Ident(q.alias+"."+q.key)).
		Limit(limit).
		For("UPDATE OF ? SKIP LOCKED", bun.Ident(q.alias))
	err := db.NewUpdate().With("due", due).Model((*T)(nil)).
		TableExpr("due").
		Set("next_attempt_at = ?", leaseUntil.UTC()).
		Set("attempts = ?.attempts + 1", bun.Ident(q.alias)).
		Where("? = ?", bun.Ident(q.alias+"."+q.key), bun.Ident("due."+q.key)).
		Returning(columns).
		Scan(ctx, &claimed)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: claim %s: %w", q.name, err))
	}
	return claimed, nil
}

// nextDue is when the soonest row pick narrows to is due, and false when there is none.
func nextDue[T any](ctx context.Context, db bun.IDB, q queue, pick func(*bun.SelectQuery) *bun.SelectQuery) (time.Time, bool, error) {
	var next time.Time
	query := db.NewSelect().Model((*T)(nil)).ColumnExpr("?.next_attempt_at", bun.Ident(q.alias))
	if pick != nil {
		query = pick(query)
	}
	err := query.OrderExpr("?.next_attempt_at", bun.Ident(q.alias)).Limit(1).Scan(ctx, &next)
	if errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, stack.Wrap(fmt.Errorf("store: next %s: %w", q.name, err))
	}
	return next, true, nil
}

// heldBy narrows update to the row known by key while the claim that counted attempts still
// holds it.
func (q queue) heldBy(update *bun.UpdateQuery, key any, attempts int) *bun.UpdateQuery {
	return update.Where("? = ?", bun.Ident(q.key), key).Where("attempts = ?", attempts)
}
