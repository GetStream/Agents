package store

import (
	"context"
	"fmt"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// GuestMemoryMove is a claimed guest's memories to move to the account that claimed them. It
// is kept until a pass leaves nothing under the guest after the last scheduled one, or until
// it is given up.
type GuestMemoryMove struct {
	bun.BaseModel `bun:"table:guest_memory_moves,alias:gmm"`

	ID         int64  `bun:"id,pk,autoincrement"`
	CustomerID string `bun:"customer_id,notnull"`
	GuestID    string `bun:"guest_id,notnull"`
	UserID     string `bun:"user_id,notnull"`
	// APIKey and OrganizationID are the claim's attribution, which the copies are billed under.
	APIKey         string `bun:"api_key,notnull"`
	OrganizationID string `bun:"organization_id,notnull"`
	// Passes is how many passes of the move ran to the end.
	Passes int `bun:"passes,notnull"`
	// Attempts is how many times the move was claimed, failed claims included: the claim a write
	// holds the move by (guestMemoryMoves).
	Attempts      int       `bun:"attempts,notnull"`
	NextAttemptAt time.Time `bun:"next_attempt_at,notnull"`
	CreatedAt     time.Time `bun:"created_at,notnull"`
}

// GuestMemoryMoves are the claimed guests whose memories are still to move, in the shared
// database. Routers claim them as a queue.
type GuestMemoryMoves struct {
	store *Store
}

var guestMemoryMoves = queue{name: "guest memory moves", alias: "gmm", key: "id"}

// Queue asks for a guest's memories to be moved to the account that claimed them, due now. A
// guest of the customer already queued is left as it is: one move per guest, however often the
// claim is sent.
func (r *GuestMemoryMoves) Queue(ctx context.Context, move GuestMemoryMove) error {
	err := validate("guest memory move").
		required("customer_id", move.CustomerID).
		required("guest_id", move.GuestID).
		required("user_id", move.UserID).
		done()
	if err != nil {
		return stack.Wrap(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	move.ID, move.Passes, move.Attempts, move.NextAttemptAt, move.CreatedAt = 0, 0, 0, now, now
	_, err = r.store.global.NewInsert().Model(&move).On("CONFLICT (customer_id, guest_id) DO NOTHING").Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: queue a guest memory move: %w", err))
	}
	return nil
}

// Claim takes up to limit moves due now, oldest due first, holding each until lease ends:
// another router's claim skips them, and one whose router died comes due again by itself.
func (r *GuestMemoryMoves) Claim(ctx context.Context, lease time.Duration, limit int) ([]GuestMemoryMove, error) {
	return claim[GuestMemoryMove](ctx, r.store.global, guestMemoryMoves, lease, limit, nil, "gmm.*")
}

// Passed counts a pass of a move the claim still holds, and makes the move due again after
// wait, on Postgres's clock as a claim judges it.
func (r *GuestMemoryMoves) Passed(ctx context.Context, move GuestMemoryMove, wait time.Duration) error {
	update := r.store.global.NewUpdate().Model((*GuestMemoryMove)(nil)).
		Set("passes = passes + 1").
		Set("next_attempt_at = now() + make_interval(secs => ?)", wait.Seconds())
	if _, err := guestMemoryMoves.heldBy(update, move.ID, move.Attempts).Exec(ctx); err != nil {
		return stack.Wrap(fmt.Errorf("store: count a guest memory move's pass: %w", err))
	}
	return nil
}

// Retry makes a move whose pass failed due again after wait, while the claim still holds it.
func (r *GuestMemoryMoves) Retry(ctx context.Context, move GuestMemoryMove, wait time.Duration) error {
	update := r.store.global.NewUpdate().Model((*GuestMemoryMove)(nil)).
		Set("next_attempt_at = now() + make_interval(secs => ?)", wait.Seconds())
	if _, err := guestMemoryMoves.heldBy(update, move.ID, move.Attempts).Exec(ctx); err != nil {
		return stack.Wrap(fmt.Errorf("store: retry a guest memory move: %w", err))
	}
	return nil
}

// Finish forgets a move that is done or given up, while the claim still holds it.
func (r *GuestMemoryMoves) Finish(ctx context.Context, move GuestMemoryMove) error {
	query := r.store.global.NewDelete().Model((*GuestMemoryMove)(nil)).
		Where("id = ?", move.ID).Where("attempts = ?", move.Attempts)
	if _, err := query.Exec(ctx); err != nil {
		return stack.Wrap(fmt.Errorf("store: finish a guest memory move: %w", err))
	}
	return nil
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
