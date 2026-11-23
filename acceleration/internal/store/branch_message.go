package store

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
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
		if messages[i].Message == nil {
			messages[i].Message = map[string]any{}
		}
	}
	_, err := r.store.shard.NewInsert().Model(&messages).
		On("CONFLICT (session_id, original_message_id) DO UPDATE").
		Set("response_id = EXCLUDED.response_id").
		Set("switch_id = EXCLUDED.switch_id").
		Set("chat_created_at = EXCLUDED.chat_created_at").
		Set("author_id = EXCLUDED.author_id").
		Set("message = EXCLUDED.message").
		Where("abm.customer_id = EXCLUDED.customer_id").
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
	lists, err := everyShard(ctx, r.store, func(ctx context.Context, db bun.IDB) ([]AgentBranchMessage, error) {
		query := db.NewSelect().Model((*AgentBranchMessage)(nil)).
			Where("customer_id = ?", customerID).
			Where("session_id = ?", sessionID).
			Order("chat_created_at ASC", "original_message_id ASC")
		if len(responseIDs) > 0 {
			query = query.Where("response_id IN (?)", bun.In(responseIDs))
		}
		var messages []AgentBranchMessage
		err := query.Scan(ctx, &messages)
		return messages, err
	})
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: branch messages: %w", err))
	}
	// A switch finished by a router of another shard copies the messages again into its own.
	all := merged(lists, func(a, b AgentBranchMessage) int {
		return cmp.Or(a.ChatCreatedAt.Compare(b.ChatCreatedAt), strings.Compare(a.OriginalMessageID, b.OriginalMessageID))
	}, 0)
	return distinct(all, func(m AgentBranchMessage) string { return m.OriginalMessageID }), nil
}

// Delete deletes every copy of a session's hidden messages, and answers how many it deleted.
func (r *BranchMessages) Delete(ctx context.Context, customerID, sessionID string) (int64, error) {
	if customerID == "" || sessionID == "" {
		return 0, stack.Wrap(errors.New("store: a customer and a session id are required"))
	}
	deleted, err := onEveryShard(ctx, r.store, func(ctx context.Context, db bun.IDB) (sql.Result, error) {
		return db.NewDelete().Model((*AgentBranchMessage)(nil)).
			Where("customer_id = ?", customerID).
			Where("session_id = ?", sessionID).
			Exec(ctx)
	})
	if err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: delete branch messages: %w", err))
	}
	return deleted, nil
}

// DeleteUser deletes every copy of a hidden message in the sessions of an app's end user, on
// every shard, and answers how many it deleted. The changes recorded of those copies are
// emptied with them, since they hold the same words (forgottenCopies).
func (r *BranchMessages) DeleteUser(ctx context.Context, appID, userID string) (int64, error) {
	if appID == "" || userID == "" {
		return 0, stack.Wrap(errors.New("store: an app and a user id are required"))
	}
	sessions := "(SELECT id FROM agent_sessions WHERE customer_id = ? AND user_id = ?)"
	deleted, err := onEveryShard(ctx, r.store, func(ctx context.Context, db bun.IDB) (sql.Result, error) {
		// A data-modifying WITH runs to the end whether or not the statement reads it.
		changes := forgottenCopies(db, appID).Where("payload->>'session_id' IN "+sessions, appID, userID)
		return db.NewDelete().With("changes", changes).Model((*AgentBranchMessage)(nil)).
			Where("customer_id = ?", appID).
			Where("session_id IN "+sessions, appID, userID).
			Exec(ctx)
	})
	if err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: delete user branch messages: %w", err))
	}
	return deleted, nil
}

// CountUser counts what DeleteUser would delete, on every shard.
func (r *BranchMessages) CountUser(ctx context.Context, appID, userID string) (int64, error) {
	if appID == "" || userID == "" {
		return 0, stack.Wrap(errors.New("store: an app and a user id are required"))
	}
	counts, err := everyShard(ctx, r.store, func(ctx context.Context, db bun.IDB) (int, error) {
		return db.NewSelect().Model((*AgentBranchMessage)(nil)).
			Where("customer_id = ?", appID).
			Where("session_id IN (SELECT id FROM agent_sessions WHERE customer_id = ? AND user_id = ?)", appID, userID).
			Count(ctx)
	})
	if err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: count user branch messages: %w", err))
	}
	var total int64
	for _, count := range counts {
		total += int64(count)
	}
	return total, nil
}
