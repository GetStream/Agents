package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
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

// claimSettle bounds a claim that runs on after its caller stopped (claim).
const claimSettle = 5 * time.Second

// claim leases up to limit rows due now for lease and returns them with columns. Both are
// counted on Postgres's clock: the lease is judged by the next claim, on any router, so a router
// whose clock is behind must not see a live lease as over. pick narrows what is due, and may be
// nil. Rows are taken oldest due first, and a row another claim is taking is skipped rather than
// waited on.
func claim[T any](ctx context.Context, db bun.IDB, q queue, lease time.Duration, limit int,
	pick func(*bun.SelectQuery) *bun.SelectQuery, columns string) ([]T, error) {
	var claimed []T
	if limit < 1 {
		return claimed, nil
	}
	due := db.NewSelect().Model((*T)(nil)).
		Column(q.alias+"."+q.key).
		Where("?.next_attempt_at <= now()", bun.Ident(q.alias))
	if pick != nil {
		due = pick(due)
	}
	due = due.OrderExpr("?.next_attempt_at, ?", bun.Ident(q.alias), bun.Ident(q.alias+"."+q.key)).
		Limit(limit).
		For("UPDATE OF ? SKIP LOCKED", bun.Ident(q.alias))
	// Why: a router stopping cancels ctx while the claim is in flight. Cancelled between its
	// commit and the read of its rows, the claim would lease rows no one works on until the lease
	// ran out. It runs to the end on a context of its own, and gives back what it took when its
	// caller has gone.
	running, done := context.WithTimeout(context.WithoutCancel(ctx), claimSettle)
	defer done()
	err := db.NewUpdate().With("due", due).Model((*T)(nil)).
		TableExpr("due").
		Set("next_attempt_at = now() + make_interval(secs => ?)", lease.Seconds()).
		Set("attempts = ?.attempts + 1", bun.Ident(q.alias)).
		Where("? = ?", bun.Ident(q.alias+"."+q.key), bun.Ident("due."+q.key)).
		Returning(columns).
		Scan(running, &claimed)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: claim %s: %w", q.name, err))
	}
	if ctx.Err() != nil {
		if err := unclaim(running, db, q, claimed); err != nil {
			return nil, stack.Wrap(errors.Join(ctx.Err(), err))
		}
		return nil, stack.Wrap(ctx.Err())
	}
	return claimed, nil
}

// unclaim makes the rows a claim took due again, each while that claim still holds it. The
// attempt it counted stays counted: attempts is what a later write names its claim by (heldBy),
// and giving the number back would let the claim before it write again.
func unclaim[T any](ctx context.Context, db bun.IDB, q queue, claimed []T) error {
	if len(claimed) == 0 {
		return nil
	}
	table := db.Dialect().Tables().Get(reflect.TypeFor[T]())
	key, attempts := table.FieldMap[q.key], table.FieldMap["attempts"]
	held := make([][]any, 0, len(claimed))
	for i := range claimed {
		row := reflect.ValueOf(&claimed[i]).Elem()
		held = append(held, []any{key.Value(row).Interface(), attempts.Value(row).Interface()})
	}
	_, err := db.NewUpdate().Model((*T)(nil)).
		Set("next_attempt_at = now()").
		Where("(?, attempts) IN (?)", bun.Ident(q.key), bun.In(held)).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("store: give back a claim of %s: %w", q.name, err)
	}
	return nil
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
