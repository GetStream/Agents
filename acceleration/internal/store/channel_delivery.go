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

// channelDelivery is one message a line was delivered (ChannelDeliveries.Claim).
type channelDelivery struct {
	bun.BaseModel `bun:"table:channel_deliveries,alias:cdel"`

	AccountID   string    `bun:"account_id,pk"`
	MessageID   string    `bun:"message_id,pk"`
	ReceivedAt  time.Time `bun:"received_at,notnull"`
	FromAddress string    `bun:"from_address,notnull"`
}

// Validate refuses a delivery of no message to no line.
func (d *channelDelivery) Validate() error {
	return validate("channel delivery").
		required("account_id", d.AccountID).
		required("message_id", d.MessageID).
		done()
}

// ChannelDeliveries are the messages lines were delivered, and who sent them, in the shared
// database.
type ChannelDeliveries struct {
	store *Store
}

// Claim records a message as delivered, and who sent it. False is one already taken, which a
// provider retrying a slow delivery sends.
func (r *ChannelDeliveries) Claim(ctx context.Context, accountID, messageID, from string) (bool, error) {
	delivery := channelDelivery{AccountID: accountID, MessageID: messageID, ReceivedAt: time.Now().UTC(), FromAddress: from}
	if err := delivery.Validate(); err != nil {
		return false, stack.Wrap(err)
	}
	result, err := r.store.global.NewInsert().Model(&delivery).On("CONFLICT DO NOTHING").Exec(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: claim channel message: %w", err))
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: claim channel message: %w", err))
	}
	return affected == 1, nil
}

// Release gives back a message claimed but not taken, so the provider's next delivery of it
// claims it again.
func (r *ChannelDeliveries) Release(ctx context.Context, accountID, messageID string) error {
	_, err := r.store.global.NewDelete().Model((*channelDelivery)(nil)).
		Where("account_id = ? AND message_id = ?", accountID, messageID).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: release channel message: %w", err))
	}
	return nil
}

// LastAt is when somebody last wrote to a line, or the zero time when they never have.
// WhatsApp's window for writing first is counted from it.
func (r *ChannelDeliveries) LastAt(ctx context.Context, accountID, from string) (time.Time, error) {
	var received time.Time
	err := r.store.global.NewSelect().Model((*channelDelivery)(nil)).
		Column("received_at").
		Where("account_id = ? AND from_address = ?", accountID, from).
		OrderExpr("received_at DESC").
		Limit(1).
		Scan(ctx, &received)
	if errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, stack.Wrap(fmt.Errorf("store: last channel message: %w", err))
	}
	return received, nil
}
