package session

import (
	"context"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// MemoryMovePacing is when the passes of a claimed guest's memory move run.
type MemoryMovePacing struct {
	// After is how long after each pass the next one runs, the first pass running once the
	// claim queued the move. A memory store extracts what the guest said last after the claim,
	// so a pass that found nothing is not the end until these have run.
	After []time.Duration
	// Again is how long after a later pass that left memories behind the next one runs.
	Again time.Duration
	// Retry is how long after a pass that failed the next one runs, doubled for each failure.
	Retry time.Duration
	// Attempts is how many times a move is claimed before it is given up.
	Attempts int
}

// memoryMovePacing runs a pass at once, two minutes later, and fifteen minutes after the claim:
// mem0 has been seen to start an extraction ten minutes after it was queued.
var memoryMovePacing = MemoryMovePacing{
	After:    []time.Duration{2 * time.Minute, 13 * time.Minute},
	Again:    15 * time.Minute,
	Retry:    30 * time.Second,
	Attempts: 12,
}

const (
	// memoryMoveLease is how long a router holds a move it claimed, longer than a pass takes.
	memoryMoveLease = 10 * time.Minute
	// memoryMoveBatch is how many moves one sweep claims, and runs one after another.
	memoryMoveBatch = 10
)

// QueueMemoryMove asks for what was remembered about one of the customer's guests to be filed
// under the account that claimed them. The move runs off the claim's path (MoveGuestMemories).
func (m *Manager) QueueMemoryMove(ctx context.Context, customerID, guestID, userID string) error {
	if m.options.Memory == nil || m.options.Store == nil {
		return stack.Wrap(ErrNoMemory)
	}
	// The copies are billed as the claim would have been, under the key it was sent with.
	asked := routing.AttributionFrom(ctx)
	return m.options.Store.GuestMemoryMoves.Queue(ctx, store.GuestMemoryMove{
		CustomerID: customerID, GuestID: guestID, UserID: userID,
		APIKey: asked.APIKey, OrganizationID: asked.OrganizationID,
	})
}

// MoveGuestMemories runs a pass of every guest memory move due, and reports how many ran.
// Every router sweeps, and each move is claimed by one of them at a time.
func (m *Manager) MoveGuestMemories(ctx context.Context) (int, error) {
	if m.options.Memory == nil || m.options.Store == nil {
		return 0, nil
	}
	moves, err := m.options.Store.GuestMemoryMoves.Claim(ctx, memoryMoveLease, memoryMoveBatch)
	if err != nil {
		return 0, err
	}
	for _, move := range moves {
		m.moveGuestMemories(ctx, move)
	}
	return len(moves), nil
}

// moveGuestMemories runs one pass of a move and says when the next one is due, or finishes it.
// What it cannot write back is left to the lease, after which another sweep runs it again.
func (m *Manager) moveGuestMemories(ctx context.Context, move store.GuestMemoryMove) {
	pacing := m.memoryMovePacing()
	moves := m.options.Store.GuestMemoryMoves
	fields := []any{"app_id", move.CustomerID, "guest", move.GuestID, "user", move.UserID, "attempts", move.Attempts}
	owner := routing.Owner{CustomerID: move.CustomerID, Attribution: routing.Attribution{APIKey: move.APIKey, OrganizationID: move.OrganizationID}}
	left, err := m.options.Memory.For(owner).
		MoveUser(ctx, move.CustomerID, move.GuestID, move.UserID)
	var written error
	switch {
	case err != nil && move.Attempts >= pacing.Attempts:
		m.logger.Warn("gave up moving a claimed guest's memories to the account; what is left stays under the guest",
			append(fields, "error", err)...)
		written = moves.Finish(ctx, move)
	case err != nil:
		m.logger.Warn("could not move a claimed guest's memories to the account; trying again", append(fields, "error", err)...)
		written = moves.Retry(ctx, move, pacing.Retry<<min(move.Attempts-1, 5))
	case move.Passes < len(pacing.After):
		written = moves.Passed(ctx, move, pacing.After[move.Passes])
	case left == 0:
		written = moves.Finish(ctx, move)
	case move.Attempts >= pacing.Attempts:
		m.logger.Warn("gave up moving a claimed guest's memories to the account; what is left stays under the guest",
			append(fields, "left", left)...)
		written = moves.Finish(ctx, move)
	default:
		written = moves.Passed(ctx, move, pacing.Again)
	}
	if written != nil {
		m.logger.Error("could not record a pass of a guest memory move; it runs again once its claim runs out",
			append(fields, "error", written)...)
	}
}

// memoryMovePacing is the manager's pacing: its options', or memoryMovePacing when they name
// no attempts.
func (m *Manager) memoryMovePacing() MemoryMovePacing {
	if m.options.MemoryMoves.Attempts == 0 {
		return memoryMovePacing
	}
	return m.options.MemoryMoves
}
