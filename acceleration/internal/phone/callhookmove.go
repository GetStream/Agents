package phone

import (
	"context"
	"errors"
	"fmt"
	"slices"

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

// Allowlist is what MoveToCallHook did, or would do, with the addresses of a number's trunk.
type Allowlist string

const (
	// AllowlistSet is a trunk that now takes calls from the vendor's declared addresses only.
	AllowlistSet Allowlist = "allowlist set"
	// WouldSetAllowlist is a trunk a run with Apply gives the vendor's declared addresses.
	WouldSetAllowlist Allowlist = "would set allowlist"
	// AllowlistAlreadySet is a trunk that has exactly the vendor's declared addresses, left as it is.
	AllowlistAlreadySet Allowlist = "allowlist already set"
)

// MovedNumber is one number MoveToCallHook went through.
type MovedNumber struct {
	E164 string
	// App is the Stream app the number is lined in. Zero is the deployment's own.
	App  int64
	Move Move
	// Allowlist is what was done with its trunk's addresses, empty for a number left as it is.
	Allowlist Allowlist
}

// CallHookMove picks the numbers MoveToCallHook points at the router's call webhook.
type CallHookMove struct {
	Vendor string
	// App, when set, keeps the numbers made in that Stream app. Zero is the deployment's own.
	App *int64
	// E164, when set, keeps that one number.
	E164 string
	// Apply writes at the vendor and at Stream. Without it nothing is written at either, and
	// each number is reported with what a run with it would do.
	Apply bool
}

// callHookMovePage is how many numbers MoveToCallHook reads from the store at a time.
const callHookMovePage = 100

// MoveToCallHook points the attached numbers of a vendor whose numbers ask the router's call
// webhook (CallHooked) at that webhook, as Attach does for a number it attaches, gives the
// trunk each is on the vendor's declared signalling addresses, as a trunk made now has, and
// reports each number as it goes. It refuses before anything is read at the vendor when the router
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
			var allowed Allowlist
			if err == nil && moved != NoStoredBridge {
				allowed, err = s.allowTrunk(ctx, number, move.Apply)
			}
			if err != nil {
				return fmt.Errorf("phone: move %s: %w", number.E164, err)
			}
			report(MovedNumber{E164: number.E164, App: number.StreamAppPK, Move: moved, Allowlist: allowed})
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
	err = provider.ConfigureInbound(ctx, Inbound{E164: number.E164, Bridge: bridge, PublicURL: s.publicURL})
	if err != nil {
		return "", stack.Wrap(err)
	}
	return Moved, nil
}

// allowTrunk gives the trunk an attached number is on exactly the vendor's declared signalling
// addresses, keeping its name, numbers and password, unless it has them already or apply is
// false. A trunk shared by several numbers is written for the first of them only.
func (s *Service) allowTrunk(ctx context.Context, number store.PhoneNumber, apply bool) (Allowlist, error) {
	allowed, err := s.allowlistFor(number.Vendor)
	if err != nil {
		return "", err
	}
	stream, err := s.streamForApp(ctx, number.CustomerID, number.StreamAppPK)
	if err != nil {
		return "", err
	}
	trunk, err := stream.trunk(ctx, number.StreamTrunkID)
	if err != nil {
		return "", err
	}
	if slices.Equal(slices.Sorted(slices.Values(trunk.AllowedIPs)), slices.Sorted(slices.Values(allowed))) {
		return AllowlistAlreadySet, nil
	}
	if !apply {
		return WouldSetAllowlist, nil
	}
	trunk.AllowedIPs = allowed
	if err := stream.UpdateTrunk(ctx, number.StreamTrunkID, trunk); err != nil {
		return "", err
	}
	return AllowlistSet, nil
}
