package phone

import (
	"context"
	"errors"
	"fmt"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// Move is what MoveToCallHook did, or would do, with one number.
type Move string

const (
	// Moved is a number now pointed at the router's call webhook.
	Moved Move = "moved"
	// WouldMove is a number a run with Apply points at the router's call webhook.
	WouldMove Move = "would move"
	// AlreadyMoved is a number already pointed at the router's call webhook, left as it is.
	AlreadyMoved Move = "already moved"
	// NoStoredBridge is a number whose trunk credentials were not kept, left as it is: the
	// webhook would hang up its calls. Detaching and attaching it again keeps them.
	NoStoredBridge Move = "no stored bridge, needs a re-attach"
)

// MovedNumber is one number MoveToCallHook went through.
type MovedNumber struct {
	E164 string
	// App is the Stream app the number is lined in. Zero is the deployment's own.
	App  int64
	Move Move
}

// CallHookMove picks the numbers MoveToCallHook points at the router's call webhook.
type CallHookMove struct {
	Vendor string
	// App, when set, keeps the numbers made in that Stream app. Zero is the deployment's own.
	App *int64
	// E164, when set, keeps that one number.
	E164 string
	// Apply writes at the vendor. Without it nothing is written there, and each number is
	// reported with what a run with it would do.
	Apply bool
}

// callHookMovePage is how many numbers MoveToCallHook reads from the store at a time.
const callHookMovePage = 100

// MoveToCallHook points the attached numbers of a vendor whose numbers ask the router's call
// webhook (CallHooked) at that webhook, as Attach does for a number it attaches, and reports
// each number as it goes. It refuses before anything is read at the vendor when the router
// could not answer the webhook. Running it again moves nothing already moved. It stops at the
// first number it could not read or move.
func (s *Service) MoveToCallHook(ctx context.Context, move CallHookMove, report func(MovedNumber)) error {
	if s.store == nil {
		return stack.Wrap(errors.New("phone: moving numbers needs a database to know which are attached"))
	}
	if s.sealer == nil {
		return stack.Wrap(fmt.Errorf("phone: %s numbers are answered with their stored trunk: %w", move.Vendor, ErrNoSealer))
	}
	provider, err := s.registry.Open(move.Vendor)
	if err != nil {
		return stack.Wrap(err)
	}
	hooked, ok := provider.(CallHooked)
	if !ok {
		return stack.Wrap(Invalid(fmt.Errorf("phone: %s numbers do not ask a webhook of the router's", move.Vendor)))
	}
	if err := hooked.CallHookReady(s.publicURL); err != nil {
		return err
	}

	filter := store.AttachedFilter{Vendor: move.Vendor, App: move.App, E164: move.E164, Limit: callHookMovePage}
	for {
		numbers, err := s.store.PhoneNumbers.Attached(ctx, filter)
		if err != nil {
			return err
		}
		for _, number := range numbers {
			moved, err := s.moveToCallHook(ctx, provider, hooked, number, move.Apply)
			if err != nil {
				return fmt.Errorf("phone: move %s: %w", number.E164, err)
			}
			report(MovedNumber{E164: number.E164, App: number.StreamAppPK, Move: moved})
		}
		if len(numbers) < filter.Limit {
			return nil
		}
		filter.After = numbers[len(numbers)-1].ID
	}
}

// moveToCallHook points one attached number at the router's call webhook, unless it is there
// already, or its trunk was not kept for the webhook to answer with, or apply is false.
func (s *Service) moveToCallHook(ctx context.Context, provider Provider, hooked CallHooked, number store.PhoneNumber,
	apply bool) (Move, error) {
	stored, err := s.store.PhoneNumbers.AttachedBridge(ctx, number.Vendor, number.E164)
	if errors.Is(err, store.ErrNoStoredBridge) {
		return NoStoredBridge, nil
	}
	if err != nil {
		return "", err
	}
	// Opened here as the webhook opens it, so a number is not moved to answer calls with a
	// password that does not open.
	bridge, err := s.openBridge(stored, number.E164)
	if err != nil {
		return "", err
	}
	on, err := hooked.OnCallHook(ctx, number.E164, s.publicURL)
	if err != nil {
		return "", err
	}
	if on {
		return AlreadyMoved, nil
	}
	if !apply {
		return WouldMove, nil
	}
	if err := provider.ConfigureInbound(ctx, Inbound{E164: number.E164, Bridge: bridge, PublicURL: s.publicURL}); err != nil {
		return "", stack.Wrap(err)
	}
	return Moved, nil
}
