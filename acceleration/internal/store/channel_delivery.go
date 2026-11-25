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
	return claimDelivery(ctx, r.store.global, accountID, messageID, from)
}

// ClaimStop is Claim of a STOP, recording its opt-out (OptOuts.Insert) in the same
// transaction: the message is taken only once the opt-out is stored, so a write that failed
// leaves it to be delivered again, and a delivery of it racing this one waits for the outcome.
func (r *ChannelDeliveries) ClaimStop(ctx context.Context, accountID, messageID, from string, optOut *OptOut) (bool, error) {
	return r.claimWith(ctx, accountID, messageID, from, func(ctx context.Context, tx bun.Tx) error {
		return insertOptOut(ctx, tx, optOut)
	})
}

// ClaimStart is ClaimStop of a START, lifting the recipient's opt-outs (OptOuts.RevokeAll),
// whether kept under recipient or under from as the sender wrote it.
func (r *ChannelDeliveries) ClaimStart(ctx context.Context, accountID, messageID, from, customerID, recipient, channel string) (bool, error) {
	return r.claimWith(ctx, accountID, messageID, from, func(ctx context.Context, tx bun.Tx) error {
		return revokeAllOptOuts(ctx, tx, customerID, channel, recipient, from)
	})
}

// claimWith claims a message and, when it is fresh, runs write in the same transaction.
func (r *ChannelDeliveries) claimWith(ctx context.Context, accountID, messageID, from string, write func(context.Context, bun.Tx) error) (bool, error) {
	var fresh bool
	err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		var err error
		fresh, err = claimDelivery(ctx, tx, accountID, messageID, from)
		if err != nil || !fresh {
			return err
		}
		return write(ctx, tx)
	})
	if err != nil {
		return false, stack.Wrap(err)
	}
	return fresh, nil
}

// claimDelivery is Claim on db.
func claimDelivery(ctx context.Context, db bun.IDB, accountID, messageID, from string) (bool, error) {
	delivery := channelDelivery{AccountID: accountID, MessageID: messageID, ReceivedAt: time.Now().UTC(), FromAddress: from}
	if err := delivery.Validate(); err != nil {
		return false, stack.Wrap(err)
	}
	result, err := db.NewInsert().Model(&delivery).On("CONFLICT DO NOTHING").Exec(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: claim channel message: %w", err))
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: claim channel message: %w", err))
	}
	return affected == 1, nil
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
