package store

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// ConversationDocument is a file a person attached to a conversation of theirs, read as text.
type ConversationDocument struct {
	bun.BaseModel `bun:"table:conversation_documents,alias:cd"`

	ID         string `bun:"id,pk"`
	CustomerID string `bun:"customer_id,notnull"`
	// OwnerUserID is the person whose conversation it is.
	OwnerUserID string `bun:"owner_user_id,notnull"`
	SessionID   string `bun:"session_id,notnull"`
	// ConversationID is the Chat channel the conversation is kept in, empty for none.
	ConversationID string `bun:"conversation_id,notnull"`
	// ResponseID is the turn it was sent with, empty for one carried into a fork.
	ResponseID string    `bun:"response_id,notnull"`
	Name       string    `bun:"name,notnull"`
	Pages      int       `bun:"pages,notnull"`
	SHA256     string    `bun:"sha256,notnull"`
	Markdown   string    `bun:"markdown,notnull"`
	CreatedAt  time.Time `bun:"created_at,notnull"`
}

// Validate refuses a document that names no id, customer or session.
func (d *ConversationDocument) Validate() error {
	return validate("conversation document").
		required("id", d.ID).
		required("customer_id", d.CustomerID).
		required("session_id", d.SessionID).
		nonNegative("pages", float64(d.Pages)).
		done()
}

// DocumentScope is the conversation whose documents are read, and whose they must be.
type DocumentScope struct {
	// ConversationID reads a conversation kept in Chat, across every session that held it.
	// Empty reads SessionID's alone.
	ConversationID string
	SessionID      string
	// Owner is the person reading. A document of anybody else's is never read.
	Owner string
	// UntilResponse leaves out documents sent with turns after this one, as a fork that stops
	// there carries none of them. Empty is all of them.
	UntilResponse string
}

// ConversationDocuments are the files people attached to conversations, beside the turns
// they were sent with.
type ConversationDocuments struct {
	store *Store
}

// Insert keeps a file attached to a conversation. A retried write of the same id is not an
// error.
func (r *ConversationDocuments) Insert(ctx context.Context, document *ConversationDocument) error {
	if err := document.Validate(); err != nil {
		return stack.Wrap(err)
	}
	if document.CreatedAt.IsZero() {
		document.CreatedAt = time.Now().UTC()
	}
	// As Responses.Insert: a retry matches its own row, another customer's id none.
	result, err := r.store.shard.NewInsert().Model(document).
		On("CONFLICT (id) DO UPDATE").
		Set("id = EXCLUDED.id").
		Where("cd.customer_id = EXCLUDED.customer_id").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: add conversation document: %w", err))
	}
	written, err := result.RowsAffected()
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: add conversation document: %w", err))
	}
	if written == 0 {
		return stack.Wrap(fmt.Errorf("%w: conversation document %s", ErrIDTaken, document.ID))
	}
	return nil
}

// CopyResponse keeps the documents sent with one of a session's turns again under another,
// as a regenerate answers the same question as a new turn: the old turn keeps its own, so
// bringing its branch back finds them. Each copy has an id of its own, and its original's
// owner, conversation and attachment time.
//
// The copies are written beside the turn this router records as toResponseID: a document is
// read beside its turn's row, which is how it drops out once that turn is rewound.
func (r *ConversationDocuments) CopyResponse(ctx context.Context, customerID, sessionID, fromResponseID, toResponseID string) error {
	if customerID == "" || sessionID == "" || fromResponseID == "" || toResponseID == "" {
		return stack.Wrap(errors.New("store: a customer, a session and two response ids are required"))
	}
	var copies []ConversationDocument
	err := r.store.shard.NewSelect().Model((*ConversationDocument)(nil)).
		Where("cd.customer_id = ?", customerID).
		Where("cd.session_id = ?", sessionID).
		Where("cd.response_id = ?", fromResponseID).
		Scan(ctx, &copies)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: copy response documents: %w", err))
	}
	for i := range copies {
		copies[i].ID, copies[i].ResponseID = NewID(), toResponseID
	}
	if len(copies) == 0 {
		return nil
	}
	if _, err := r.store.shard.NewInsert().Model(&copies).Exec(ctx); err != nil {
		return stack.Wrap(fmt.Errorf("store: copy response documents: %w", err))
	}
	return nil
}

// Select returns the documents in scope, oldest first, but for those sent with a turn since
// rewound.
func (r *ConversationDocuments) Select(ctx context.Context, customerID string, scope DocumentScope) ([]ConversationDocument, error) {
	if customerID == "" || scope.ConversationID == "" && scope.SessionID == "" {
		return nil, stack.Wrap(errors.New("store: a customer and a conversation or session id are required"))
	}
	var until time.Time
	if scope.UntilResponse != "" {
		var stopped AgentResponse
		err := r.store.shard.NewSelect().Model(&stopped).Column("created_at").
			Where("id = ?", scope.UntilResponse).Where("customer_id = ?", customerID).Scan(ctx)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, stack.Wrap(ErrUnknownResponse)
		}
		if err != nil {
			return nil, stack.Wrap(fmt.Errorf("store: conversation documents: %w", err))
		}
		until = stopped.CreatedAt
	}
	query := r.store.shard.NewSelect().Model((*ConversationDocument)(nil)).
		Where("cd.customer_id = ?", customerID).
		Where("cd.owner_user_id = ?", scope.Owner).
		Where("NOT EXISTS (SELECT 1 FROM agent_responses AS r WHERE r.id = cd.response_id AND r.customer_id = cd.customer_id AND r.rewound_at IS NOT NULL)").
		Order("cd.created_at ASC", "cd.id ASC")
	if scope.ConversationID != "" {
		query = query.Where("cd.conversation_id = ?", scope.ConversationID)
	} else {
		query = query.Where("cd.session_id = ?", scope.SessionID)
	}
	// A document is written by the router that took its turn, beside the turn's row.
	if !until.IsZero() {
		query = query.Where("cd.response_id = '' OR EXISTS (SELECT 1 FROM agent_responses AS sent "+
			"WHERE sent.id = cd.response_id AND sent.customer_id = cd.customer_id AND sent.created_at <= ?)", until)
	}
	var documents []ConversationDocument
	if err := query.Scan(ctx, &documents); err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: conversation documents: %w", err))
	}
	sortDocuments(documents)
	return documents, nil
}

// sortDocuments puts documents read from several shards back in the order they were attached.
func sortDocuments(documents []ConversationDocument) {
	slices.SortFunc(documents, func(a, b ConversationDocument) int {
		return cmp.Or(a.CreatedAt.Compare(b.CreatedAt), cmp.Compare(a.ID, b.ID))
	})
}
