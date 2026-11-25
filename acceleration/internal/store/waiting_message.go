package store

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// channelWaitingKeep is how long a reply waits for the message that links its thread
// (WaitingMessages.Wait). Slack's last retry of an event comes about 6 minutes after its first
// delivery: one «nearly immediately», then after 1 minute and after 5 minutes
// (https://docs.slack.dev/apis/events-api/, «Retries»). 10 minutes is that with a margin, a
// choice rather than a measurement. Other providers' retry spans are unverified.
const channelWaitingKeep = 10 * time.Minute

// waitingMessage is a customer's message on an external thread no thread channel is linked to
// yet.
type waitingMessage struct {
	bun.BaseModel `bun:"table:channel_thread_waiting,alias:ctw"`

	CustomerID        string    `bun:"customer_id,pk"`
	ConnectorID       string    `bun:"connector_id,pk"`
	ProviderUnitID    string    `bun:"provider_unit_id,pk"`
	ThreadKey         string    `bun:"thread_key,pk"`
	ProviderMessageID string    `bun:"provider_message_id,pk"`
	AuthorID          string    `bun:"author_id,notnull"`
	Text              string    `bun:"text,notnull"`
	Raw               []byte    `bun:"raw,notnull"`
	CreatedAt         time.Time `bun:"created_at,notnull"`
}

// Validate refuses a message its thread could not find again.
func (m *waitingMessage) Validate() error {
	return validate("waiting message").
		required("customer_id", m.CustomerID).
		required("connector_id", m.ConnectorID).
		required("thread_key", m.ThreadKey).
		required("provider_message_id", m.ProviderMessageID).
		check(m.Raw != nil, "raw", "is required").
		done()
}

// WaitingMessages are the messages waiting for the message that links their thread, minutes of
// them, in the shared database.
type WaitingMessages struct {
	store *Store
}

// Wait keeps a customer's message on an external thread no thread channel is linked to, for
// the message that links the thread to take (ChannelInbounds.Accept). False is one already
// waiting, delivered again. Rows past channelWaitingKeep are dropped first, so the table holds
// minutes of them.
func (r *WaitingMessages) Wait(ctx context.Context, customerID string, message core.InboundMessage) (bool, error) {
	now := time.Now().UTC()
	row := &waitingMessage{
		CustomerID: customerID, ConnectorID: message.ConnectorID, ProviderUnitID: message.ProviderUnitID,
		ThreadKey: message.ThreadKey, ProviderMessageID: message.ProviderMessageID,
		AuthorID: message.AuthorID, Text: message.Text, Raw: message.Raw, CreatedAt: now,
	}
	if err := row.Validate(); err != nil {
		return false, stack.Wrap(err)
	}
	_, err := r.store.global.NewDelete().Model((*waitingMessage)(nil)).Where("created_at < ?", now.Add(-channelWaitingKeep)).Exec(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: wait channel thread message: %w", err))
	}
	result, err := r.store.global.NewInsert().Model(row).On("CONFLICT DO NOTHING").Exec(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: wait channel thread message: %w", err))
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: wait channel thread message: %w", err))
	}
	return affected == 1, nil
}

// Unwait takes one waiting message back. False is one that is not waiting: the message that
// linked its thread took it first, or it was never kept.
func (r *WaitingMessages) Unwait(ctx context.Context, customerID string, message core.InboundMessage) (bool, error) {
	result, err := r.store.global.NewDelete().Model((*waitingMessage)(nil)).
		Where("customer_id = ? AND connector_id = ? AND provider_unit_id = ? AND thread_key = ?",
			customerID, message.ConnectorID, message.ProviderUnitID, message.ThreadKey).
		Where("provider_message_id = ?", message.ProviderMessageID).
		Exec(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: unwait channel thread message: %w", err))
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: unwait channel thread message: %w", err))
	}
	return affected == 1, nil
}

// takeWaitingMessages takes on db, the store's or a transaction's, every message waiting on a
// customer's external thread that is not past channelWaitingKeep, oldest first. Each is taken
// once: a message that takes itself back at the same time (Unwait) gets it, or this does.
func takeWaitingMessages(ctx context.Context, db bun.IDB, customerID, connectorID, providerUnitID, threadKey string) ([]core.InboundMessage, error) {
	var rows []waitingMessage
	err := db.NewDelete().Model((*waitingMessage)(nil)).
		Where("customer_id = ? AND connector_id = ? AND provider_unit_id = ? AND thread_key = ?",
			customerID, connectorID, providerUnitID, threadKey).
		Where("created_at >= ?", time.Now().UTC().Add(-channelWaitingKeep)).
		Returning("connector_id, provider_unit_id, thread_key, provider_message_id, author_id, text, raw, created_at").
		Scan(ctx, &rows)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: take waiting channel thread messages: %w", err))
	}
	slices.SortStableFunc(rows, func(a, b waitingMessage) int { return a.CreatedAt.Compare(b.CreatedAt) })
	messages := make([]core.InboundMessage, 0, len(rows))
	for _, row := range rows {
		messages = append(messages, core.InboundMessage{
			ConnectorID: row.ConnectorID, ProviderUnitID: row.ProviderUnitID, ThreadKey: row.ThreadKey,
			AuthorID: row.AuthorID, Text: row.Text, ProviderMessageID: row.ProviderMessageID, Raw: row.Raw,
		})
	}
	return messages, nil
}
