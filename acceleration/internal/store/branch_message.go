package store

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// AgentBranchMessage is a copy of a Stream Chat message a rewind hid, so the branch it was on
// can be shown and sent again.
type AgentBranchMessage struct {
	bun.BaseModel `bun:"table:agent_branch_messages,alias:abm"`

	SessionID         string `bun:"session_id,pk"`
	OriginalMessageID string `bun:"original_message_id,pk"`
	CustomerID        string `bun:"customer_id,notnull"`
	// ResponseID is the response the message belongs to, empty for one that belongs to none.
	ResponseID    string    `bun:"response_id,notnull"`
	SwitchID      string    `bun:"switch_id,notnull"`
	ChatCreatedAt time.Time `bun:"chat_created_at,notnull"`
	AuthorID      string    `bun:"author_id,notnull"`
	// Message is what the message said: its text, attachments and custom data.
	Message map[string]any `bun:"message,type:jsonb,notnull"`
}

// Validate refuses a copy that names no session, message or customer.
func (m *AgentBranchMessage) Validate() error {
	return validate("branch message").
		required("session_id", m.SessionID).
		required("original_message_id", m.OriginalMessageID).
		required("customer_id", m.CustomerID).
		done()
}

// BranchMessages are the copies of messages rewinds hid, in the shard of the session.
type BranchMessages struct {
	store *Store
}

// UpsertMany keeps copies of messages a rewind is about to hide. Saving one again replaces it
// with what it says now: a switch retried after the channel moved on copies the messages
// again, and the later copy is the one that was hidden.
func (r *BranchMessages) UpsertMany(ctx context.Context, messages []AgentBranchMessage) error {
	if len(messages) == 0 {
		return nil
	}
	for i := range messages {
		if err := messages[i].Validate(); err != nil {
			return stack.Wrap(err)
		}
		messages[i].Message = orEmptyMap(messages[i].Message)
	}
	// No conflict target is named: every unique index arbitrates the insert, so it depends on
	// none of them. The copies kept already are replaced below, the customer's own only.
	var inserted []string
	err := r.store.shard.NewInsert().Model(&messages).On("CONFLICT DO NOTHING").
		Returning("original_message_id").Scan(ctx, &inserted)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: save branch messages: %w", err))
	}
	kept := slices.DeleteFunc(slices.Clone(messages), func(m AgentBranchMessage) bool {
		return slices.Contains(inserted, m.OriginalMessageID)
	})
	if len(kept) == 0 {
		return nil
	}
	_, err = r.store.shard.NewUpdate().With("saved", r.store.shard.NewValues(&kept)).
		Model((*AgentBranchMessage)(nil)).TableExpr("saved").
		Set("response_id = saved.response_id").
		Set("switch_id = saved.switch_id").
		Set("chat_created_at = saved.chat_created_at").
		Set("author_id = saved.author_id").
		Set("message = saved.message").
		Where("abm.customer_id = saved.customer_id").
		Where("abm.session_id = saved.session_id").
		Where("abm.original_message_id = saved.original_message_id").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: save branch messages: %w", err))
	}
	return nil
}

// Select returns the copies of a session's hidden messages that belong to responseIDs, or all
// of them when it names none, oldest first.
func (r *BranchMessages) Select(ctx context.Context, customerID, sessionID string, responseIDs []string) ([]AgentBranchMessage, error) {
	if customerID == "" || sessionID == "" {
		return nil, stack.Wrap(errors.New("store: a customer and a session id are required"))
	}
	query := r.store.shard.NewSelect().Model((*AgentBranchMessage)(nil)).
		Where("customer_id = ?", customerID).
		Where("session_id = ?", sessionID).
		Order("chat_created_at ASC", "original_message_id ASC")
	if len(responseIDs) > 0 {
		query = query.Where("response_id IN (?)", bun.In(responseIDs))
	}
	var messages []AgentBranchMessage
	if err := query.Scan(ctx, &messages); err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: branch messages: %w", err))
	}
	return messages, nil
}

// DeleteUser deletes every copy of a hidden message in the sessions of an app's end user, and
// answers how many it deleted here. The changes recorded of those copies are emptied with them,
// since they hold the same words (forgottenChanges). With other shards, each deletes its own
// when it runs the job this queues (ShardJobDeleteBranches).
func (r *BranchMessages) DeleteUser(ctx context.Context, appID, userID string) (int64, error) {
	if appID == "" || userID == "" {
		return 0, stack.Wrap(errors.New("store: an app and a user id are required"))
	}
	if !r.store.HasShards() {
		return r.deleteUserHere(ctx, appID, userID)
	}
	job, err := r.store.ShardJobs.queue(ctx, r.store.global, ShardJobDeleteBranches, appID, userArgs{UserID: userID})
	if err != nil {
		return 0, err
	}
	deleted, err := here[deletedRows](ctx, r.store, job)
	return deleted.Deleted, err
}

// deleteUserHere is DeleteUser on this shard alone.
func (r *BranchMessages) deleteUserHere(ctx context.Context, appID, userID string) (int64, error) {
	db := r.store.shard
	sessions := "(SELECT id FROM agent_sessions WHERE customer_id = ? AND user_id = ?)"
	// A data-modifying WITH runs to the end whether or not the statement reads it.
	changes := forgottenChanges(db, appID, "agent_branch_messages").Where("payload->>'session_id' IN "+sessions, appID, userID)
	result, err := db.NewDelete().With("changes", changes).Model((*AgentBranchMessage)(nil)).
		Where("customer_id = ?", appID).
		Where("session_id IN "+sessions, appID, userID).
		Exec(ctx)
	if err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: delete user branch messages: %w", err))
	}
	deleted, err := result.RowsAffected()
	if err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: delete user branch messages: %w", err))
	}
	return deleted, nil
}

// DeleteMessage deletes the copies of a message Stream Chat deleted: those of the sessions in
// scope's app on the conversation (a CID) it was in, and only customerID's when it is not empty.
// An activation sends a copy again as a new message naming it (resent_from), so the copies of
// every message on that chain go with it: resentFrom is what the deleted message names, and the
// copies name the rest. Each holds the same words, and the newest of them is what a branch
// shows and sends again.
//
// It answers how many copies it deleted here, none for a message nothing copied. The changes
// recorded of those copies are emptied, since they hold its words, and so is what a deleted
// question's response says was asked (AgentResponse.Said). With other shards, each deletes its
// own when it runs the job this queues (ShardJobDeleteBranchMessage), queued only when the
// shared index places a session on the conversation on another shard, or cannot say.
//
// A switch that read the message from Chat before the delete and copies it after this ran keeps
// a copy of a deleted message: the delete is not delivered again for it. Nothing here orders the
// two, and the copy goes with the session, a memory truncate or forget-user.
func (r *BranchMessages) DeleteMessage(ctx context.Context, scope AppScope, customerID, conversationID, messageID, resentFrom string) (int64, error) {
	if conversationID == "" || messageID == "" {
		return 0, stack.Wrap(errors.New("store: a conversation and a message id are required"))
	}
	args := messageArgs{App: scope.App, Unpinned: scope.Unpinned, Conversation: conversationID, Message: messageID, ResentFrom: resentFrom}
	if !r.store.HasShards() {
		return r.deleteMessageHere(ctx, customerID, args)
	}
	elsewhere, err := r.onOtherShards(ctx, customerID, args)
	if err != nil {
		return 0, err
	}
	if !elsewhere {
		return r.deleteMessageHere(ctx, customerID, args)
	}
	job, err := r.store.ShardJobs.queue(ctx, r.store.global, ShardJobDeleteBranchMessage, customerID, args)
	if err != nil {
		return 0, err
	}
	deleted, err := here[deletedRows](ctx, r.store, job)
	return deleted.Deleted, err
}

// onOtherShards reports whether a session on the conversation may be on another shard: the
// shared index lists one there, or there is no customer to read the index by (its rows are
// keyed by customer), or no index is kept. A session is indexed seconds after it opens
// (indexShipEvery), long before a switch copies anything of it.
func (r *BranchMessages) onOtherShards(ctx context.Context, customerID string, args messageArgs) (bool, error) {
	if customerID == "" || !r.store.Indexed() {
		return true, nil
	}
	scope := AppScope{App: args.App, Unpinned: args.Unpinned}
	found, err := r.store.global.NewSelect().TableExpr(indexedSessions.index).
		Where("customer_id = ?", customerID).
		Where("conversation_id = ?", args.Conversation).
		Where("shard <> ?", r.store.ShardName()).
		ApplyQueryBuilder(scope.where).
		Exists(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: find a conversation's sessions on other shards: %w", err))
	}
	return found, nil
}

// messageArgs name the message a job deletes the copies of, the one it was itself a copy of,
// and where its sessions are.
type messageArgs struct {
	App          int64  `json:"app"`
	Unpinned     bool   `json:"unpinned"`
	Conversation string `json:"conversation"`
	Message      string `json:"message"`
	ResentFrom   string `json:"resent_from,omitempty"`
}

// heldCopy is what deleteMessageHere reads of a copy to follow its chain.
type heldCopy struct {
	OriginalMessageID string `bun:"original_message_id"`
	ResentFrom        string `bun:"resent_from"`
}

// deletedCopy is a copy deleteMessageHere deleted.
type deletedCopy struct {
	CustomerID        string `bun:"customer_id"`
	SessionID         string `bun:"session_id"`
	OriginalMessageID string `bun:"original_message_id"`
	ResponseID        string `bun:"response_id"`
	Role              string `bun:"role"`
}

// deleteMessageHere is DeleteMessage on this shard alone.
func (r *BranchMessages) deleteMessageHere(ctx context.Context, customerID string, args messageArgs) (int64, error) {
	var deleted []deletedCopy
	err := r.store.shard.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		scope := AppScope{App: args.App, Unpinned: args.Unpinned}
		sessions := func() *bun.SelectQuery {
			query := tx.NewSelect().Model((*AgentSession)(nil)).Column("id").
				Where("conversation_id = ?", args.Conversation).
				ApplyQueryBuilder(scope.where)
			if customerID != "" {
				query = query.Where("customer_id = ?", customerID)
			}
			return query
		}
		var held []heldCopy
		err := tx.NewSelect().Model((*AgentBranchMessage)(nil)).Column("abm.original_message_id").
			ColumnExpr("COALESCE(abm.message->'custom'->>?, '') AS resent_from", resentFromKey).
			Where("abm.session_id IN (?)", sessions()).
			Scan(ctx, &held)
		if err != nil {
			return fmt.Errorf("store: read a conversation's branch copies: %w", err)
		}
		chain := resentChain(held, args.Message, args.ResentFrom)
		err = tx.NewDelete().Model((*AgentBranchMessage)(nil)).
			Where("session_id IN (?)", sessions()).
			Where("original_message_id IN (?)", bun.In(chain)).
			Returning("customer_id, session_id, original_message_id, response_id, "+
				"COALESCE(message->'custom'->'support_runtime'->>'role', '') AS role").
			Scan(ctx, &deleted)
		if err != nil {
			return fmt.Errorf("store: delete a message's branch copies: %w", err)
		}
		return forgetDeletedCopies(ctx, tx, deleted)
	})
	if err != nil {
		return 0, stack.Wrap(err)
	}
	return int64(len(deleted)), nil
}

// resentFromKey is the custom field a message sent again names the message it copies by
// (conversation.ResentFromField, which this package cannot import).
const resentFromKey = "resent_from"

// resentChain is every message id on the chain of copies sent again that message is on: what it
// was a copy of (resentFrom, and what that one names in turn), and what copied it.
func resentChain(held []heldCopy, message, resentFrom string) []string {
	reached := map[string]bool{message: true}
	if resentFrom != "" {
		reached[resentFrom] = true
	}
	// Each pass adds at least one id or ends, so it ends within len(held) passes.
	for grew := true; grew; {
		grew = false
		for _, m := range held {
			if m.ResentFrom == "" || reached[m.OriginalMessageID] == reached[m.ResentFrom] {
				continue
			}
			reached[m.OriginalMessageID], reached[m.ResentFrom] = true, true
			grew = true
		}
	}
	return slices.Sorted(maps.Keys(reached))
}

// forgetDeletedCopies empties what the deleted copies leave of their words: the changes recorded
// of them, and the question a deleted person's message asked on its response's row.
func forgetDeletedCopies(ctx context.Context, tx bun.Tx, deleted []deletedCopy) error {
	type key struct{ customer, session string }
	messages, questions := map[key][]string{}, map[key][]string{}
	for _, m := range deleted {
		k := key{m.CustomerID, m.SessionID}
		messages[k] = append(messages[k], m.OriginalMessageID)
		if m.Role == "user" && m.ResponseID != "" {
			questions[k] = append(questions[k], m.ResponseID)
		}
	}
	for k, ids := range messages {
		_, err := forgottenChanges(tx, k.customer, "agent_branch_messages").
			Where("payload->>'session_id' = ?", k.session).
			Where("payload->>'original_message_id' IN (?)", bun.In(ids)).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("store: forget a deleted message's branch copies: %w", err)
		}
	}
	for k, ids := range questions {
		_, err := tx.NewUpdate().Model((*AgentResponse)(nil)).Set("said = ''").
			Where("customer_id = ?", k.customer).
			Where("session_id = ?", k.session).
			Where("id IN (?)", bun.In(ids)).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("store: forget a deleted question: %w", err)
		}
	}
	return nil
}

// CountUser counts what DeleteUser would delete, on this shard.
func (r *BranchMessages) CountUser(ctx context.Context, appID, userID string) (int64, error) {
	if appID == "" || userID == "" {
		return 0, stack.Wrap(errors.New("store: an app and a user id are required"))
	}
	count, err := r.store.shard.NewSelect().Model((*AgentBranchMessage)(nil)).
		Where("customer_id = ?", appID).
		Where("session_id IN (SELECT id FROM agent_sessions WHERE customer_id = ? AND user_id = ?)", appID, userID).
		Count(ctx)
	if err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: count user branch messages: %w", err))
	}
	return int64(count), nil
}
