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

// GuestMemoryMove is a claimed guest's memories to move to the account that claimed them. It
// is kept until a pass after the scheduled ones finds nothing under the guest, or until it is
// given up.
type GuestMemoryMove struct {
	bun.BaseModel `bun:"table:guest_memory_moves,alias:gmm"`

	ID         int64  `bun:"id,pk,autoincrement"`
	CustomerID string `bun:"customer_id,notnull"`
	GuestID    string `bun:"guest_id,notnull"`
	UserID     string `bun:"user_id,notnull"`
	// APIKey and OrganizationID are the claim's attribution, which the copies are billed under.
	APIKey         string `bun:"api_key,notnull"`
	OrganizationID string `bun:"organization_id,notnull"`
	// Passes is how many passes ran to the end, and Failures how many in a row failed.
	Passes   int `bun:"passes,notnull"`
	Failures int `bun:"failures,notnull"`
	// Attempts is how many times the move was claimed: the claim a write holds the move by
	// (guestMemoryMoves).
	Attempts      int       `bun:"attempts,notnull"`
	NextAttemptAt time.Time `bun:"next_attempt_at,notnull"`
	CreatedAt     time.Time `bun:"created_at,notnull"`
}

// Validate refuses a move that does not say whose memories go where.
func (m *GuestMemoryMove) Validate() error {
	return validate("guest memory move").
		required("customer_id", m.CustomerID).
		required("guest_id", m.GuestID).
		required("user_id", m.UserID).
		check(m.GuestID != m.UserID, "user_id", "is the guest").
		done()
}

// GuestMemoryMoves are the claimed guests whose memories are still to move, in the shared
// database. Routers claim them as a queue.
type GuestMemoryMoves struct {
	store *Store
}

var guestMemoryMoves = queue{name: "guest memory moves", alias: "gmm", key: "id"}

// Queue asks for a guest's memories to be moved to the account that claimed them, due now. One
// already queued for the guest is kept, with its schedule started over: what made it queue
// again, a session of the guest's ending, leaves memories to extract.
func (r *GuestMemoryMoves) Queue(ctx context.Context, move GuestMemoryMove) error {
	if err := move.Validate(); err != nil {
		return stack.Wrap(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	move.ID, move.Passes, move.Failures, move.Attempts, move.NextAttemptAt, move.CreatedAt = 0, 0, 0, 0, now, now
	_, err := r.store.global.NewInsert().Model(&move).
		On("CONFLICT (customer_id, guest_id) DO UPDATE").Set("passes = 0").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: queue a guest memory move: %w", err))
	}
	return nil
}

// Claim takes up to limit moves due now but those of except, oldest due first, holding each
// until lease ends: another router's claim skips them, and one whose router died comes due
// again by itself.
func (r *GuestMemoryMoves) Claim(ctx context.Context, lease time.Duration, limit int, except []int64) ([]GuestMemoryMove, error) {
	var pick func(*bun.SelectQuery) *bun.SelectQuery
	if len(except) > 0 {
		pick = func(due *bun.SelectQuery) *bun.SelectQuery { return due.Where("gmm.id NOT IN (?)", bun.In(except)) }
	}
	return claim[GuestMemoryMove](ctx, r.store.global, guestMemoryMoves, lease, limit, pick, "gmm.*")
}

// Held reports whether the claim that read move still holds it: the row is there, and no later
// claim took it.
func (r *GuestMemoryMoves) Held(ctx context.Context, move GuestMemoryMove) (bool, error) {
	held, err := r.store.global.NewSelect().Model((*GuestMemoryMove)(nil)).
		Where("id = ?", move.ID).Where("attempts = ?", move.Attempts).
		Exists(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: read a guest memory move's claim: %w", err))
	}
	return held, nil
}

// Passed counts a pass of a move the claim still holds, and makes the move due again after
// wait, on Postgres's clock as a claim judges it. It reports false, writing nothing, for a move
// the claim no longer holds.
func (r *GuestMemoryMoves) Passed(ctx context.Context, move GuestMemoryMove, wait time.Duration) (bool, error) {
	update := r.store.global.NewUpdate().Model((*GuestMemoryMove)(nil)).
		Set("passes = passes + 1").
		Set("failures = 0").
		Set("next_attempt_at = now() + make_interval(secs => ?)", wait.Seconds())
	return r.write(ctx, guestMemoryMoves.heldBy(update, move.ID, move.Attempts).Exec, "count a pass of")
}

// Failed counts a failed pass of a move the claim still holds, and makes it due again after
// wait. It reports false, writing nothing, for a move the claim no longer holds.
func (r *GuestMemoryMoves) Failed(ctx context.Context, move GuestMemoryMove, wait time.Duration) (bool, error) {
	update := r.store.global.NewUpdate().Model((*GuestMemoryMove)(nil)).
		Set("failures = failures + 1").
		Set("next_attempt_at = now() + make_interval(secs => ?)", wait.Seconds())
	return r.write(ctx, guestMemoryMoves.heldBy(update, move.ID, move.Attempts).Exec, "count a failed pass of")
}

// Release makes a move the claim still holds due again now, counting no pass and no failure:
// its pass was stopped by the router stopping, not by the store.
func (r *GuestMemoryMoves) Release(ctx context.Context, move GuestMemoryMove) (bool, error) {
	update := r.store.global.NewUpdate().Model((*GuestMemoryMove)(nil)).Set("next_attempt_at = now()")
	return r.write(ctx, guestMemoryMoves.heldBy(update, move.ID, move.Attempts).Exec, "release")
}

// Finish forgets a move that is done or given up, while the claim still holds it. It reports
// false for a move the claim no longer holds.
func (r *GuestMemoryMoves) Finish(ctx context.Context, move GuestMemoryMove) (bool, error) {
	query := r.store.global.NewDelete().Model((*GuestMemoryMove)(nil)).
		Where("id = ?", move.ID).Where("attempts = ?", move.Attempts)
	return r.write(ctx, query.Exec, "finish")
}

func (r *GuestMemoryMoves) write(ctx context.Context, exec func(context.Context, ...any) (sql.Result, error), what string) (bool, error) {
	result, err := exec(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: %s a guest memory move: %w", what, err))
	}
	written, err := result.RowsAffected()
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: %s a guest memory move: %w", what, err))
	}
	return written > 0, nil
}

// DeleteFor deletes a customer's moves naming userID as the guest or as the account, and
// returns the guests they were of: the memories still under them are that user's too.
func (r *GuestMemoryMoves) DeleteFor(ctx context.Context, customerID, userID string) ([]string, error) {
	var guests []string
	err := r.store.global.NewDelete().Model((*GuestMemoryMove)(nil)).
		Where("customer_id = ?", customerID).
		Where("guest_id = ? OR user_id = ?", userID, userID).
		Returning("guest_id").
		Scan(ctx, &guests)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, stack.Wrap(fmt.Errorf("store: delete a user's guest memory moves: %w", err))
	}
	return guests, nil
}

// Pending is the move queued for a customer's guest, and false when there is none.
func (r *GuestMemoryMoves) Pending(ctx context.Context, customerID, guestID string) (GuestMemoryMove, bool, error) {
	var moves []GuestMemoryMove
	err := r.store.global.NewSelect().Model(&moves).
		Where("customer_id = ?", customerID).Where("guest_id = ?", guestID).
		Scan(ctx)
	if err != nil {
		return GuestMemoryMove{}, false, stack.Wrap(fmt.Errorf("store: read a guest memory move: %w", err))
	}
	if len(moves) == 0 {
		return GuestMemoryMove{}, false, nil
	}
	return moves[0], true, nil
}
