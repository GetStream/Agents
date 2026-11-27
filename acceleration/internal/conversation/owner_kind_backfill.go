package conversation

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"time"

	getstream "github.com/GetStream/getstream-go/v5"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/streamapp"
)

// OwnerKindBackfill is what BackfillOwnerKinds did with a conversation's channel.
type OwnerKindBackfill string

const (
	// OwnerKindStamped is a channel that records its owner's kind now (with write, or would).
	OwnerKindStamped OwnerKindBackfill = "stamped"
	// OwnerKindRecorded is a channel that recorded its owner's kind already.
	OwnerKindRecorded OwnerKindBackfill = "already recorded"
	// OwnerKindNoOwner is a conversation with no channel of the customer's naming an owner: not
	// an agent channel, gone, another customer's, or a backend's for nobody.
	OwnerKindNoOwner OwnerKindBackfill = "no owner"
	// OwnerKindLeftAlone is a channel left as it is, for want of one kind its owner's next
	// token is sure to prove (backfillOwnerKind).
	OwnerKindLeftAlone OwnerKindBackfill = "left alone"
	// OwnerKindFailed is a channel that could not be read or written.
	OwnerKindFailed OwnerKindBackfill = "failed"
)

// backfillPage is how many conversations BackfillOwnerKinds reads from the store at once.
const backfillPage = 500

const (
	// maxLimitWait is the longest BackfillOwnerKinds waits on one answer of Stream's, so a reset
	// time far ahead holds it no longer than two of Stream's one-minute rate-limit windows.
	maxLimitWait = 2 * time.Minute
	// maxLimitWaits is how many times it waits out a rate limit on one call before it stops.
	maxLimitWaits = 5
	// defaultLimitWait is how long it waits when Stream refuses with a rate limit and says
	// neither Retry-After nor when the limit resets: five tries in a row would all be refused.
	defaultLimitWait = 5 * time.Second
)

// OwnerKindRun is what BackfillOwnerKinds walks, and how.
type OwnerKindRun struct {
	// Mode is how the deployment authenticates callers.
	Mode auth.Mode
	// Customer is the one customer whose conversations are walked, or every customer's when empty.
	Customer string
	// After is where the walk starts: the conversations after it, by customer and then
	// conversation. The zero value starts at the first. OwnerKindResult.Done is what to rerun with.
	After store.HeldConversation
	// Write records the kinds. Without it the walk only says what it would do.
	Write bool
	// Page is how many conversations are read from the store at once; zero is backfillPage.
	Page int
	// Line is told of every channel as it is done, when it is not nil.
	Line func(OwnerKindLine)
	// Sleep waits out a rate limit, and gives up when ctx is done. A nil one waits on a timer.
	Sleep func(ctx context.Context, wait time.Duration) error
}

// OwnerKindLine is what BackfillOwnerKinds did with one conversation's channel.
type OwnerKindLine struct {
	CustomerID     string
	ConversationID string
	// Kind is the kind recorded on the channel, or that would be, or that a failed write was to
	// record. It is empty for a channel left as it was.
	Kind    auth.Kind
	Outcome OwnerKindBackfill
	// Written is whether Outcome is a write that was made.
	Written bool
	// Err is why the channel failed.
	Err error
}

// Failure reasons a failed line names. The error's own text is left to the log: it can carry
// the request's URL, with the app's API key in its query, or a store's host and statement,
// and operators paste these lines.
const (
	failedRateLimited = "rate_limited"
	failedStream      = "stream_error"
	failedStore       = "store_error"
	failedOther       = "error"
)

// failure is an error that says which side of the backfill it came from.
type failure struct {
	reason string
	err    error
}

func (f *failure) Error() string { return f.err.Error() }
func (f *failure) Unwrap() error { return f.err }

// failedOn is err, marked as one of Stream's or the store's.
func failedOn(reason string, err error) error {
	return &failure{reason: reason, err: stack.Wrap(err)}
}

// failedReason is one word for why err failed a channel: rate_limited, stream_error, store_error
// or error.
func failedReason(err error) string {
	var marked *failure
	switch _, limited := rateLimitWait(err, time.Now()); {
	case limited:
		return failedRateLimited
	case errors.As(err, &marked):
		return marked.reason
	}
	return failedOther
}

// Action is the line's outcome in a word: would stamp, stamped, skipped:<why> or
// failed:<reason>, where the reason is one of a fixed few and never the error's text.
func (l OwnerKindLine) Action() string {
	switch {
	case l.Outcome == OwnerKindFailed:
		return "failed:" + failedReason(l.Err)
	case l.Outcome != OwnerKindStamped:
		return "skipped:" + string(l.Outcome)
	case l.Written:
		return "stamped"
	}
	return "would stamp"
}

// OwnerKindResult is how a BackfillOwnerKinds walk went.
type OwnerKindResult struct {
	// Counted is how many channels came to each outcome.
	Counted map[OwnerKindBackfill]int
	// Done is the last conversation done, which is where a walk that stopped early got to: a
	// rerun with it as OwnerKindRun.After carries on from the next. A channel that failed is done;
	// one the walk stopped at, for a rate limit that did not lift, is not.
	Done store.HeldConversation
	// Complete is whether the walk reached the last conversation.
	Complete bool
}

// BackfillOwnerKinds records the owner's kind on the channels of the conversations this shard's
// sessions held that were made before channels recorded it (ownerKindField), for a deployment
// authenticating callers in run.Mode. Without run.Write it only says what it would do. It tells
// run.Line of every channel and says how many came to each outcome. A channel that fails is
// logged and counted, and the walk goes on; the error then says how many did. Stream's rate
// limit is waited out for as long as it asks (maxLimitWait, maxLimitWaits times), and the walk
// stops at the channel it does not lift for, as it does when ctx is done.
func (s *Service) BackfillOwnerKinds(ctx context.Context, held *store.Store, run OwnerKindRun) (OwnerKindResult, error) {
	result := OwnerKindResult{Counted: map[OwnerKindBackfill]int{}, Done: run.After}
	after, size := run.After, cmp.Or(run.Page, backfillPage)
	for {
		page, err := held.Sessions.HeldConversations(ctx, run.Customer, after, size)
		if err != nil {
			return result, err
		}
		for _, conversation := range page {
			outcome, kind, err := s.backfillOwnerKind(ctx, held, run, conversation.CustomerID, conversation.ConversationID)
			if err != nil && ctx.Err() != nil {
				// Not a channel's failure: the walk was ended, and a rerun starts with this one.
				return result, stack.Wrap(ctx.Err())
			}
			if err != nil {
				s.logger.Warn("could not record the owner's kind on a conversation's channel",
					"app_id", conversation.CustomerID, "conversation", conversation.ConversationID, "error", err)
			}
			if run.Line != nil {
				run.Line(OwnerKindLine{
					CustomerID: conversation.CustomerID, ConversationID: conversation.ConversationID,
					Kind: kind, Outcome: outcome, Written: run.Write, Err: err,
				})
			}
			result.Counted[outcome]++
			if _, limited := rateLimitWait(err, time.Now()); limited {
				return result, stack.Wrap(fmt.Errorf("conversation: Stream still rate limits the channels: %w", err))
			}
			result.Done = conversation
		}
		if len(page) < size {
			break
		}
		after = page[len(page)-1]
	}
	result.Complete = true
	if failed := result.Counted[OwnerKindFailed]; failed > 0 {
		return result, stack.Wrap(fmt.Errorf("conversation: %d channels could not be read or written", failed))
	}
	return result, nil
}

// backfillOwnerKind records the owner's kind on a customer's conversation's channel, when it has
// none, as the kind of caller every one of the owner's sessions on it was, on any shard.
//
// A channel with no kind recorded is its owner's by id alone, so a guest and a signed-in user of
// one id each reach the other's. A recorded kind is one the owner's next token has to prove
// (auth.Kind.Reaches), so it is recorded only when the sessions leave no doubt of it and it is
// the kind a guest's token is refreshed to (api.seenAsGuest), and the channel is left as it is
// otherwise:
//   - sessions of more than one kind, or of none: one may be somebody else's going by the id,
//     and a deleted session leaves its channel behind;
//   - an anonymous caller's, whose name nobody checked;
//   - a guest's, unless the id is still a guest no router minted, and the mode is not proxy: a
//     minted guest's refreshed token, and any behind a proxy, says no role, and an id that is an
//     account's now went on as a signed-in user. One an import brought as minted is left out
//     wherever its customer acts: that may be the deployment's app, and its own without this knowing
//     it, where they are refreshed to no role;
//   - a signed-in user's of an id a guest whose token said the role held a session under
//     (Sessions.HeldAsGuest), or that is still a guest not known as minted, here or, for a
//     customer acting in its own Stream app, where an import brought it from: their token says
//     the role. One from before the router recorded which it minted is such a guest.
//
// A guest that went on as a signed-in user and whose sessions as a guest were all deleted leaves
// nothing that says so, and is stamped a signed-in user's.
func (s *Service) backfillOwnerKind(ctx context.Context, held *store.Store, run OwnerKindRun, customer, cid string) (OwnerKindBackfill, auth.Kind, error) {
	mode := run.Mode
	channelType, id := channelOf(cid)
	if channelType != streamapp.AgentChannelType {
		return OwnerKindNoOwner, "", nil
	}
	app, err := s.appOf(ctx, customer, cid, false)
	if err != nil {
		return OwnerKindFailed, "", failedOn(failedStore, err)
	}
	client, err := s.chats.ForApp(ctx, customer, app)
	if err != nil {
		return OwnerKindFailed, "", failedOn(failedStream, err)
	}
	one, none := 1, 0
	var found *getstream.StreamResponse[getstream.QueryChannelsResponse]
	err = s.waitOutLimit(ctx, run, func() error {
		var err error
		found, err = client.Chat().QueryChannels(ctx, &getstream.QueryChannelsRequest{
			FilterConditions: map[string]any{"cid": cid}, Limit: &one, MessageLimit: &none,
		})
		return err
	})
	if err != nil {
		return OwnerKindFailed, "", failedOn(failedStream, err)
	}
	if len(found.Data.Channels) == 0 || found.Data.Channels[0].Channel == nil {
		return OwnerKindNoOwner, "", nil
	}
	custom := found.Data.Channels[0].Channel.Custom
	owner, _ := custom["support_owner_id"].(string)
	// A row may name a conversation in an app another customer shares, as an import may bring
	// one: the channel says whose it is, as ownedBy reads it.
	if custom[CustomerField] != customer || owner == "" {
		return OwnerKindNoOwner, "", nil
	}
	if _, recorded := custom[ownerKindField]; recorded {
		return OwnerKindRecorded, "", nil
	}
	kinds, err := held.Sessions.CallerKinds(ctx, customer, cid, owner)
	if err != nil {
		return OwnerKindFailed, "", failedOn(failedStore, err)
	}
	if len(kinds) != 1 || !auth.Kind(kinds[0]).Verified() {
		return OwnerKindLeftAlone, "", nil
	}
	kind := auth.Kind(kinds[0])
	if kind != auth.KindServer {
		guest, err := held.Users.GetGuest(ctx, customer, owner)
		if err != nil && !errors.Is(err, store.ErrNotAGuest) {
			return OwnerKindFailed, "", failedOn(failedStore, err)
		}
		minted := guest.Minted
		if kind == auth.KindAuthenticated && mode != auth.Proxy && guest.MintedElsewhere && !minted {
			// Their token is refreshed in the app their customer acts in, wherever this is kept. A
			// customer that acts in none, or in none it may write to, acts in none of its own.
			acting, err := s.chats.Placed(ctx, customer)
			refused := errors.Is(err, streamapp.ErrNoIdentity) || parked(err)
			if err != nil && !refused {
				return OwnerKindFailed, "", failedOn(failedStream, err)
			}
			minted = !refused && streamapp.Own(customer, acting, s.chats.DeploymentApp())
		}
		keepsTheRole := err == nil && !minted && mode != auth.Proxy
		if kind == auth.KindGuest && (!keepsTheRole || guest.MintedElsewhere) {
			return OwnerKindLeftAlone, "", nil
		}
		if kind == auth.KindAuthenticated && keepsTheRole {
			return OwnerKindLeftAlone, "", nil
		}
	}
	if kind == auth.KindAuthenticated {
		guest, err := held.Sessions.HeldAsGuest(ctx, customer, owner)
		if err != nil {
			return OwnerKindFailed, "", failedOn(failedStore, err)
		}
		if guest {
			return OwnerKindLeftAlone, "", nil
		}
	}
	if run.Write {
		err := s.waitOutLimit(ctx, run, func() error {
			_, err := client.Chat().UpdateChannelPartial(ctx, channelType, id, &getstream.UpdateChannelPartialRequest{
				Set: map[string]any{ownerKindField: kind}, Unset: []string{},
			})
			return err
		})
		if err != nil {
			return OwnerKindFailed, kind, failedOn(failedStream, err)
		}
	}
	return OwnerKindStamped, kind, nil
}

// waitOutLimit runs call, and again after the wait Stream asked for while it answers with a rate
// limit, up to maxLimitWaits times. What it last answered is the error.
func (s *Service) waitOutLimit(ctx context.Context, run OwnerKindRun, call func() error) error {
	sleep := run.Sleep
	if sleep == nil {
		sleep = sleepFor
	}
	for waits := 0; ; waits++ {
		err := call()
		wait, limited := rateLimitWait(err, time.Now())
		if !limited || waits == maxLimitWaits {
			return err
		}
		wait = min(cmp.Or(wait, defaultLimitWait), maxLimitWait)
		s.logger.Info("Stream rate limits the owner-kind backfill; waiting", "wait", wait)
		if err := sleep(ctx, wait); err != nil {
			return err
		}
	}
}

// sleepFor waits for d, or until ctx is done.
func sleepFor(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
