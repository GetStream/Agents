package conversation

import (
	"context"
	"errors"
	"fmt"

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

// BackfillOwnerKinds records the owner's kind on the channels of the conversations this shard's
// sessions held that were made before channels recorded it (ownerKindField), only customer's when
// it is not empty, for a deployment authenticating callers in mode. Without write it only says
// what it would do. It says how many channels came to each outcome. A channel that fails is
// logged and counted, and the walk goes on; the error then says how many did.
func (s *Service) BackfillOwnerKinds(ctx context.Context, held *store.Store, mode auth.Mode, customer string, write bool) (map[OwnerKindBackfill]int, error) {
	counted := map[OwnerKindBackfill]int{}
	var after store.HeldConversation
	for {
		page, err := held.Sessions.HeldConversations(ctx, customer, after, backfillPage)
		if err != nil {
			return counted, err
		}
		for _, conversation := range page {
			outcome, err := s.backfillOwnerKind(ctx, held, mode, conversation.CustomerID, conversation.ConversationID, write)
			if err != nil {
				s.logger.Warn("could not record the owner's kind on a conversation's channel",
					"app_id", conversation.CustomerID, "conversation", conversation.ConversationID, "error", err)
			}
			counted[outcome]++
		}
		if len(page) < backfillPage {
			break
		}
		after = page[len(page)-1]
	}
	if failed := counted[OwnerKindFailed]; failed > 0 {
		return counted, stack.Wrap(fmt.Errorf("conversation: %d channels could not be read or written", failed))
	}
	return counted, nil
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
func (s *Service) backfillOwnerKind(ctx context.Context, held *store.Store, mode auth.Mode, customer, cid string, write bool) (OwnerKindBackfill, error) {
	channelType, id := channelOf(cid)
	if channelType != streamapp.AgentChannelType {
		return OwnerKindNoOwner, nil
	}
	client, _, err := s.clientFor(ctx, customer, cid)
	if err != nil {
		return OwnerKindFailed, stack.Wrap(err)
	}
	one, none := 1, 0
	found, err := client.Chat().QueryChannels(ctx, &getstream.QueryChannelsRequest{
		FilterConditions: map[string]any{"cid": cid}, Limit: &one, MessageLimit: &none,
	})
	if err != nil {
		return OwnerKindFailed, stack.Wrap(err)
	}
	if len(found.Data.Channels) == 0 || found.Data.Channels[0].Channel == nil {
		return OwnerKindNoOwner, nil
	}
	custom := found.Data.Channels[0].Channel.Custom
	owner, _ := custom["support_owner_id"].(string)
	// A row may name a conversation in an app another customer shares, as an import may bring
	// one: the channel says whose it is, as ownedBy reads it.
	if custom[CustomerField] != customer || owner == "" {
		return OwnerKindNoOwner, nil
	}
	if _, recorded := custom[ownerKindField]; recorded {
		return OwnerKindRecorded, nil
	}
	kinds, err := held.Sessions.CallerKinds(ctx, customer, cid, owner)
	if err != nil {
		return OwnerKindFailed, err
	}
	if len(kinds) != 1 || !auth.Kind(kinds[0]).Verified() {
		return OwnerKindLeftAlone, nil
	}
	kind := auth.Kind(kinds[0])
	if kind != auth.KindServer {
		guest, err := held.Users.GetGuest(ctx, customer, owner)
		if err != nil && !errors.Is(err, store.ErrNotAGuest) {
			return OwnerKindFailed, err
		}
		minted := guest.Minted
		if kind == auth.KindAuthenticated && mode != auth.Proxy && guest.MintedElsewhere && !minted {
			// Their token is refreshed in the app their customer acts in, wherever this is kept. A
			// customer that acts in none, or in none it may write to, acts in none of its own.
			acting, err := s.chats.Placed(ctx, customer)
			refused := errors.Is(err, streamapp.ErrNoIdentity) || parked(err)
			if err != nil && !refused {
				return OwnerKindFailed, stack.Wrap(err)
			}
			minted = !refused && streamapp.Own(customer, acting, s.chats.DeploymentApp())
		}
		keepsTheRole := err == nil && !minted && mode != auth.Proxy
		if kind == auth.KindGuest && (!keepsTheRole || guest.MintedElsewhere) {
			return OwnerKindLeftAlone, nil
		}
		if kind == auth.KindAuthenticated && keepsTheRole {
			return OwnerKindLeftAlone, nil
		}
	}
	if kind == auth.KindAuthenticated {
		guest, err := held.Sessions.HeldAsGuest(ctx, customer, owner)
		if err != nil {
			return OwnerKindFailed, err
		}
		if guest {
			return OwnerKindLeftAlone, nil
		}
	}
	if write {
		if _, err := client.Chat().UpdateChannelPartial(ctx, channelType, id, &getstream.UpdateChannelPartialRequest{
			Set: map[string]any{ownerKindField: kind}, Unset: []string{},
		}); err != nil {
			return OwnerKindFailed, stack.Wrap(err)
		}
	}
	return OwnerKindStamped, nil
}
