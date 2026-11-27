package store

import (
	"context"
	"errors"
	"fmt"
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
// It answers how many it deleted here, none for a message nothing copied, and empties the changes
// recorded of those copies, since they hold its words. With other shards, each deletes its own
// when it runs the job this queues (ShardJobDeleteBranchMessage).
func (r *BranchMessages) DeleteMessage(ctx context.Context, scope AppScope, customerID, conversationID, messageID string) (int64, error) {
	if conversationID == "" || messageID == "" {
		return 0, stack.Wrap(errors.New("store: a conversation and a message id are required"))
	}
	args := messageArgs{App: scope.App, Unpinned: scope.Unpinned, Conversation: conversationID, Message: messageID}
	if !r.store.HasShards() {
		return r.deleteMessageHere(ctx, customerID, args)
	}
	job, err := r.store.ShardJobs.queue(ctx, r.store.global, ShardJobDeleteBranchMessage, customerID, args)
	if err != nil {
		return 0, err
	}
	deleted, err := here[deletedRows](ctx, r.store, job)
	return deleted.Deleted, err
}

// messageArgs name the message a job deletes the copies of, and where its sessions are.
type messageArgs struct {
	App          int64  `json:"app"`
	Unpinned     bool   `json:"unpinned"`
	Conversation string `json:"conversation"`
	Message      string `json:"message"`
}

// deleteMessageHere is DeleteMessage on this shard alone.
func (r *BranchMessages) deleteMessageHere(ctx context.Context, customerID string, args messageArgs) (int64, error) {
	db := r.store.shard
	scope := AppScope{App: args.App, Unpinned: args.Unpinned}
	sessions := func(column string) *bun.SelectQuery {
		query := db.NewSelect().Model((*AgentSession)(nil)).Column(column).
			Where("conversation_id = ?", args.Conversation).
			ApplyQueryBuilder(scope.where)
		if customerID != "" {
			query = query.Where("customer_id = ?", customerID)
		}
		return query
	}
	// A data-modifying WITH runs to the end whether or not the statement reads it.
	changes := db.NewUpdate().Model((*dataChange)(nil)).Set("op = ?", ChangeDelete).Set("payload = NULL").
		Where("table_name = ? AND op <> ?", "agent_branch_messages", ChangeDelete).
		Where("customer_id IN (?)", sessions("customer_id")).
		Where("payload->>'original_message_id' = ?", args.Message).
		Where("payload->>'session_id' IN (?)", sessions("id"))
	result, err := db.NewDelete().With("changes", changes).Model((*AgentBranchMessage)(nil)).
		Where("original_message_id = ?", args.Message).
		Where("session_id IN (?)", sessions("id")).
		Exec(ctx)
	if err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: delete a message's branch copies: %w", err))
	}
	deleted, err := result.RowsAffected()
	if err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: delete a message's branch copies: %w", err))
	}
	return deleted, nil
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
