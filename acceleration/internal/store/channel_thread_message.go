package store

import (
	"context"
	"fmt"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// channelMessageKeep is how long an inbound message's id is kept to drop a retried delivery of
// it. Slack's last retry comes 5 minutes after the first delivery
// (https://docs.slack.dev/apis/events-api/, «Retries»); a day is that with a wide margin, a
// choice rather than a measurement. Other providers' retry spans are unverified.
const channelMessageKeep = 24 * time.Hour

// What a claimed message of a thread channel is, each with its own ids.
const (
	// ClaimInbound is a provider's message the channel bridge took, by the provider's id.
	ClaimInbound = "inbound"
	// ClaimTurn is a person's message the message hook handed to the session, by its Stream
	// Chat id.
	ClaimTurn = "turn"
	// ClaimReply is an agent's reply the bridge sent to the external thread, by its Stream
	// Chat id.
	ClaimReply = "reply"
)

// channelThreadMessage is one claimed message of a thread channel (ChannelThreadMessages.Claim).
type channelThreadMessage struct {
	bun.BaseModel `bun:"table:channel_thread_messages,alias:ctmsg"`

	ChannelID string    `bun:"channel_id,pk"`
	Kind      string    `bun:"kind,pk"`
	MessageID string    `bun:"message_id,pk"`
	CreatedAt time.Time `bun:"created_at,notnull"`
}

// Validate refuses a claim that names no message of a step.
func (m *channelThreadMessage) Validate() error {
	return validate("channel thread message").
		required("channel_id", m.ChannelID).
		oneOf("kind", m.Kind, ClaimInbound, ClaimTurn, ClaimReply).
		required("message_id", m.MessageID).
		done()
}

// ChannelThreadMessages are the messages of thread channels each step acted on, kept a day to
// drop a second delivery, in the shared database.
type ChannelThreadMessages struct {
	store *Store
}

// Claim records a message of a thread channel as acted on by one step, kind, by that step's id
// for it. False is one already taken: delivered or told again. Ids kept longer than
// channelMessageKeep are dropped first, so the table holds a day of them.
func (r *ChannelThreadMessages) Claim(ctx context.Context, channelID, kind, messageID string) (bool, error) {
	return claimChannelThreadMessage(ctx, r.store.global, channelID, kind, messageID)
}

// Release forgets a claim, so the step that took it can take it again: a reply whose send
// failed is sent on a later attempt.
func (r *ChannelThreadMessages) Release(ctx context.Context, channelID, kind, messageID string) error {
	_, err := r.store.global.NewDelete().Model((*channelThreadMessage)(nil)).
		Where("channel_id = ? AND kind = ? AND message_id = ?", channelID, kind, messageID).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: release channel thread message: %w", err))
	}
	return nil
}

// claimChannelThreadMessage is ChannelThreadMessages.Claim on db, the store's or a
// transaction's.
func claimChannelThreadMessage(ctx context.Context, db bun.IDB, channelID, kind, messageID string) (bool, error) {
	claim := &channelThreadMessage{ChannelID: channelID, Kind: kind, MessageID: messageID, CreatedAt: time.Now().UTC()}
	if err := claim.Validate(); err != nil {
		return false, stack.Wrap(err)
	}
	_, err := db.NewDelete().Model((*channelThreadMessage)(nil)).
		Where("channel_id = ? AND created_at < ?", channelID, claim.CreatedAt.Add(-channelMessageKeep)).
		Exec(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: claim channel thread message: %w", err))
	}
	result, err := db.NewInsert().Model(claim).On("CONFLICT DO NOTHING").Exec(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: claim channel thread message: %w", err))
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: claim channel thread message: %w", err))
	}
	return affected == 1, nil
}
