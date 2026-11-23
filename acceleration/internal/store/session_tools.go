package store

import (
	"context"
	"database/sql"
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

// Upsert records what a session's model is offered, replacing what it was offered before: a
// reopened session is offered what it has now.
func (r *SessionTools) Upsert(ctx context.Context, sessionID string, tools []ToolDefinition) error {
	if tools == nil {
		tools = []ToolDefinition{}
	}
	row := &agentSessionTools{SessionID: sessionID, Tools: tools, UpdatedAt: time.Now().UTC()}
	if err := row.Validate(); err != nil {
		return stack.Wrap(err)
	}
	_, err := r.store.shard.NewInsert().Model(row).
		On("CONFLICT (session_id) DO UPDATE").
		Set("tools = EXCLUDED.tools").
		Set("updated_at = EXCLUDED.updated_at").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: save session tools: %w", err))
	}
	return nil
}

// Get is what a session's model was last offered. A session that recorded none is
// ErrNoSessionTools.
func (r *SessionTools) Get(ctx context.Context, sessionID string) ([]ToolDefinition, error) {
	stored, err := whicheverShard(ctx, r.store, func(ctx context.Context, db bun.IDB) (agentSessionTools, error) {
		var stored agentSessionTools
		err := db.NewSelect().Model(&stored).Where("session_id = ?", sessionID).Scan(ctx)
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
