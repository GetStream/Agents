package session

import (
	"context"
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/GetStream/Vision-Agents/acceleration/internal/memory"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// MemoryMovePacing is when the passes of a claimed guest's memory move run.
type MemoryMovePacing struct {
	// After is how long after each pass the next one runs, the first pass running once the
	// claim queued the move. A memory store extracts what the guest said last after the claim,
	// so the move does not end before these have run.
	After []time.Duration
	// Again is how long after a later pass that found memories the next one runs.
	Again time.Duration
	// Retry is how long after a failed pass the next one runs, doubled for each failure in a
	// row up to Park.
	Retry, Park time.Duration
	// GiveUp is how long after the claim a move still failing, or still finding memories, is
	// given up.
	GiveUp time.Duration
}

// memoryMovePacing runs a pass at once, two minutes later, and fifteen minutes after the claim:
// mem0 has been seen to start an extraction ten minutes after it was queued. A mem0 that is
// down is retried at most daily, for a week.
var memoryMovePacing = MemoryMovePacing{
	After:  []time.Duration{2 * time.Minute, 13 * time.Minute},
	Again:  15 * time.Minute,
	Retry:  30 * time.Second,
	Park:   24 * time.Hour,
	GiveUp: 7 * 24 * time.Hour,
}

const (
	// memoryMoveLease is how long a router holds a move it claimed, and memoryMovePass, well
	// under it, how long a pass may run: a pass outliving its claim would run beside the next
	// router's and copy what that one copies.
	memoryMoveLease = 10 * time.Minute
	memoryMovePass  = 5 * time.Minute
	// memoryMoveBatch is how many moves one sweep runs, each claimed as its pass starts.
	memoryMoveBatch = 10
	// memoryMoveWrite bounds writing a pass's outcome back, the router stopping or not.
	memoryMoveWrite = 5 * time.Second
)

// memoryMovesGivenUp counts the moves given up, by why: failing, a memory store that kept
// failing, or arriving, one that kept finding memories under the guest.
var memoryMovesGivenUp = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "router",
	Name:      "guest_memory_moves_given_up_total",
	Help:      "Claimed guests' memory moves given up a week after the claim, by why: failing (the memory store kept failing) or arriving (memories kept arriving under the guest)",
}, []string{"reason"})

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

// requeueClaimedGuest queues the move again when a session of a claimed guest ends: it went on
// remembering under the guest after the claim, and the move may have ended before mem0
// extracted it.
func (m *Manager) requeueClaimedGuest(ctx context.Context, spec Spec) {
	if m.options.Memory == nil || m.options.Store == nil || spec.Memory.UserID == "" {
		return
	}
	guest, err := m.options.Store.Users.GetGuest(ctx, spec.CustomerID, spec.Memory.UserID)
	if errors.Is(err, store.ErrNotAGuest) || err == nil && guest.ClaimedBy == "" {
		return
	}
	if err == nil {
		err = m.QueueMemoryMove(ctx, spec.CustomerID, guest.ID, guest.ClaimedBy)
	}
	if err != nil {
		m.logger.Error("could not queue the memory move of a claimed guest whose session ended; what it learned stays under the guest",
			"app_id", spec.CustomerID, "session", spec.ID, "error", err)
	}
}

// MoveGuestMemories runs a pass of every guest memory move due, and reports how many ran.
// Every router sweeps, and each move is claimed by one of them at a time.
func (m *Manager) MoveGuestMemories(ctx context.Context) (int, error) {
	if m.options.Memory == nil || m.options.Store == nil {
		return 0, nil
	}
	var ran []int64
	// A router stopping claims no more: it would only give them back.
	for len(ran) < memoryMoveBatch && ctx.Err() == nil {
		// One at a time, so each move's lease starts with its pass rather than with the batch,
		// and each once a sweep, however soon its next pass is due.
		moves, err := m.options.Store.GuestMemoryMoves.Claim(ctx, memoryMoveLease, 1, ran)
		if err != nil {
			return len(ran), err
		}
		if len(moves) == 0 {
			break
		}
		m.moveGuestMemories(ctx, moves[0])
		ran = append(ran, moves[0].ID)
	}
	return len(ran), nil
}

// moveGuestMemories runs one pass of a move and says when the next one is due, or finishes it.
// What it cannot write back is left to the lease, after which another sweep runs it again.
func (m *Manager) moveGuestMemories(ctx context.Context, move store.GuestMemoryMove) {
	pacing := m.memoryMovePacing()
	moves := m.options.Store.GuestMemoryMoves
	owned := m.options.Memory.For(routing.Owner{CustomerID: move.CustomerID,
		Attribution: routing.Attribution{APIKey: move.APIKey, OrganizationID: move.OrganizationID}})
	fields := []any{"app_id", move.CustomerID, "guest", move.GuestID, "user", move.UserID, "failures", move.Failures}

	// Why: a truncate or a forget of either user deletes the move mid-pass, and a lease that ran
	// out hands it to another router; neither may see this pass copy or delete on.
	held := func(ctx context.Context) error {
		ok, err := moves.Held(ctx, move)
		if err == nil && !ok {
			err = stack.Wrap(memory.ErrMoveLost)
		}
		return err
	}
	passing, stop := context.WithTimeout(ctx, memoryMovePass)
	moved, err := owned.MoveUser(passing, move.CustomerID, move.GuestID, move.UserID, held)
	stop()
	// Written back even once the router is stopping, so a deploy does not leave the move held
	// until its lease runs out.
	stopping := ctx.Err() != nil
	ctx, done := context.WithTimeout(context.WithoutCancel(ctx), memoryMoveWrite)
	defer done()

	age := time.Since(move.CreatedAt)
	var kept bool
	var written error
	switch {
	case errors.Is(err, memory.ErrMoveLost):
		m.dropCopies(ctx, move, owned, moved.Copies)
		return
	case err != nil && stopping:
		kept, written = moves.Release(ctx, move)
	case err != nil && age >= pacing.GiveUp:
		memoryMovesGivenUp.WithLabelValues("failing").Inc()
		m.logger.Warn("gave up moving a claimed guest's memories to the account; what is left stays under the guest",
			append(fields, "error", err)...)
		kept, written = moves.Finish(ctx, move)
	case err != nil:
		m.logger.Warn("could not move a claimed guest's memories to the account; trying again", append(fields, "error", err)...)
		kept, written = moves.Failed(ctx, move, min(pacing.Retry<<min(move.Failures, 20), pacing.Park))
	case move.Passes < len(pacing.After):
		kept, written = moves.Passed(ctx, move, pacing.After[move.Passes])
	case moved.Found == 0:
		kept, written = moves.Finish(ctx, move)
	case age >= pacing.GiveUp:
		memoryMovesGivenUp.WithLabelValues("arriving").Inc()
		m.logger.Warn("gave up moving a claimed guest's memories to the account; what is left stays under the guest",
			append(fields, "found", moved.Found, "left", moved.Left)...)
		kept, written = moves.Finish(ctx, move)
	default:
		kept, written = moves.Passed(ctx, move, pacing.Again)
	}
	if written != nil {
		m.logger.Error("could not record a pass of a guest memory move; it runs again once its claim runs out",
			append(fields, "error", written)...)
		return
	}
	if !kept {
		m.dropCopies(ctx, move, owned, moved.Copies)
	}
}

// dropCopies takes back the copies a pass made of a move it no longer holds, unless the same
// move is still there: a truncate or a forget deleted it, and the copies would put back what it
// erased, even if a guest's session queued a new move since. A move another router claimed
// since is that router's, and its pass counts these copies as made.
func (m *Manager) dropCopies(ctx context.Context, move store.GuestMemoryMove, owned interface {
	DeleteMemories(context.Context, string, []string) error
}, copies []string) {
	if len(copies) == 0 {
		return
	}
	held, pending, err := m.options.Store.GuestMemoryMoves.Pending(ctx, move.CustomerID, move.GuestID)
	if err == nil && pending && held.ID == move.ID {
		return
	}
	if err == nil {
		err = owned.DeleteMemories(ctx, move.CustomerID, copies)
	}
	if err != nil {
		m.logger.Error("could not take back the copies of a guest memory move that was deleted mid-pass",
			"app_id", move.CustomerID, "guest", move.GuestID, "user", move.UserID, "copies", len(copies), "error", err)
	}
}

// memoryMovePacing is the manager's pacing: its options', or memoryMovePacing when they name
// no time to give up after.
func (m *Manager) memoryMovePacing() MemoryMovePacing {
	if m.options.MemoryMoves.GiveUp == 0 {
		return memoryMovePacing
	}
	return m.options.MemoryMoves
}
