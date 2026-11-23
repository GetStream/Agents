package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// ToolDefinition is one tool as a session's model was offered it.
type ToolDefinition struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Parameters  map[string]any `json:"parameters,omitempty"`
	// Source is where the tool came from, as session.ToolSource names it. Empty in a row
	// kept before it was recorded.
	Source string `json:"source,omitempty"`
}

// agentSessionTools is the tools a session was offered, kept apart from its row: a large
// jsonb read only when somebody asks for it, off a row every session list reads.
type agentSessionTools struct {
	bun.BaseModel `bun:"table:agent_session_tools"`

	SessionID string           `bun:"session_id,pk"`
	Tools     []ToolDefinition `bun:"tools,type:jsonb,notnull"`
	UpdatedAt time.Time        `bun:"updated_at,notnull"`
}

// Validate refuses tools that name no session.
func (t *agentSessionTools) Validate() error {
	return validate("session tools").required("session_id", t.SessionID).done()
}

// ErrNoSessionTools is a session that recorded no tools.
var ErrNoSessionTools = errors.New("store: the session recorded no tools")

// SessionTools are what each session's model was offered, in the shard that ran it.
type SessionTools struct {
	store *Store
}

// Upsert records what a session of the customer's model is offered, replacing what it was
// offered before: a reopened session is offered what it has now. A session the customer does
// not have is ErrNoSession.
func (r *SessionTools) Upsert(ctx context.Context, customerID, sessionID string, tools []ToolDefinition) error {
	if customerID == "" {
		return stack.Wrap(errors.New("store: a customer is required"))
	}
	if tools == nil {
		tools = []ToolDefinition{}
	}
	row := &agentSessionTools{SessionID: sessionID, Tools: tools, UpdatedAt: time.Now().UTC()}
	if err := row.Validate(); err != nil {
		return stack.Wrap(err)
	}
	encoded, err := json.Marshal(row.Tools)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: save session tools: %w", err))
	}
	// INSERT ... SELECT, which the builder cannot write: the table has no customer, so the row
	// is written only beside a session of the customer's, on a first write as on a replace.
	result, err := r.store.shard.NewRaw(`INSERT INTO agent_session_tools (session_id, tools, updated_at)
SELECT ?, ?::jsonb, ? WHERE EXISTS (SELECT 1 FROM agent_sessions WHERE id = ? AND customer_id = ?)
ON CONFLICT (session_id) DO UPDATE SET tools = EXCLUDED.tools, updated_at = EXCLUDED.updated_at`,
		row.SessionID, string(encoded), row.UpdatedAt, row.SessionID, customerID).Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: save session tools: %w", err))
	}
	written, err := result.RowsAffected()
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: save session tools: %w", err))
	}
	if written == 0 {
		return unknownStoredSession(sessionID)
	}
	return nil
}

// Get is what a session of the customer's model was last offered. A session that recorded
// none, or that the customer does not have, is ErrNoSessionTools.
func (r *SessionTools) Get(ctx context.Context, customerID, sessionID string) ([]ToolDefinition, error) {
	stored, err := whicheverShard(ctx, r.store, func(ctx context.Context, db bun.IDB) (agentSessionTools, error) {
		var stored agentSessionTools
		// The table has no customer: the session's row says whose the tools are.
		err := db.NewSelect().Model(&stored).Where("session_id = ?", sessionID).
			Where("EXISTS (SELECT 1 FROM agent_sessions AS s WHERE s.id = ?TableAlias.session_id AND s.customer_id = ?)", customerID).
			Scan(ctx)
		return stored, err
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, stack.Wrap(ErrNoSessionTools)
	}
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: session tools: %w", err))
	}
	return stored.Tools, nil
}
