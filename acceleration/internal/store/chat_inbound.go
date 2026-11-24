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

// States of a queued Stream Chat message (ChatInbound.State).
const (
	ChatInboundQueued = "queued"
	ChatInboundDone   = "done"
	ChatInboundFailed = "failed"
)

// ErrNoChatInbound is a queued Stream Chat message no row is stored for (ChatInbounds.Get).
var ErrNoChatInbound = errors.New("store: no such queued chat inbound message")

// chatInboundDoneKeep is how long an answered message's row is kept, so a delivery Stream
// makes again queues nothing. It is channelMessageKeep, the span thread channels already drop
// a message's second delivery in.
const chatInboundDoneKeep = channelMessageKeep

// chatInboundFailedKeep is how long the row of a message whose retries ran out is kept, for
// someone to find why it was never answered. 30 days is a choice, not a measurement, the same
// as channelInboundFailedKeep.
const chatInboundFailedKeep = 30 * 24 * time.Hour

// ChatInbound is one Stream Chat message queued to be answered
// (20261205140000_chat_inbound.sql), with where its delivery came from.
type ChatInbound struct {
	bun.BaseModel `bun:"table:chat_inbound,alias:cd"`

	// Seq is the order the message was queued in.
	Seq int64 `bun:"seq,pk"`
	// StreamAppPK and MessageID are the Stream app and its id for the message. The app is always
	// known, as the internal endpoint reads it from the signed body.
	StreamAppPK int64  `bun:"stream_app_pk,notnull"`
	MessageID   string `bun:"message_id,notnull"`
	// CustomerID, Deployment and APIKey are, with StreamAppPK, where the delivery came from as
	// the router verified it.
	CustomerID string `bun:"customer_id,notnull"`
	Deployment bool   `bun:"deployment,notnull"`
	APIKey     string `bun:"api_key,notnull"`
	// Body is the verified event as Stream sent it.
	Body  []byte `bun:"body,notnull"`
	State string `bun:"state,notnull"`
	// Attempts is how many times the message was claimed, the current claim included: the
	// claim a write holds the row by.
	Attempts int `bun:"attempts,notnull"`
	// NextAttemptAt is when the message is due; on a claimed row, the end of the claim's lease.
	NextAttemptAt time.Time  `bun:"next_attempt_at,notnull"`
	ReceivedAt    time.Time  `bun:"received_at,notnull"`
	FinishedAt    *time.Time `bun:"finished_at"`
	// Heard is what the voice recording sent with the message said, once it was heard
	// (ChatInbounds.KeepHeard); nil before.
	Heard *string `bun:"heard"`
}

// Validate refuses a message that could not be answered, or that would be queued again once
// its app is known.
func (d *ChatInbound) Validate() error {
	return validate("chat inbound").
		check(d.StreamAppPK != 0, "stream_app_pk", "is required: a message queued without one would be queued again once the app is known").
		required("message_id", d.MessageID).
		check(len(d.Body) > 0, "body", "is required").
		oneOf("state", d.State, ChatInboundQueued, ChatInboundDone, ChatInboundFailed).
		done()
}

// ChatInbounds are the Stream Chat messages queued to be answered, a queue of them, in the
// shared database.
type ChatInbounds struct {
	store *Store
}

// chatInboundColumns are the columns a ChatInbound is read from, named rather than taken
// with *, so a router older than a migration that adds one still reads the rows.
const chatInboundColumns = "cd.seq, cd.stream_app_pk, cd.message_id, cd.customer_id, cd.deployment, cd.api_key, cd.body, " +
	"cd.state, cd.attempts, cd.next_attempt_at, cd.received_at, cd.finished_at, cd.heard"

// Queue queues delivery to be answered, due at delivery.NextAttemptAt. A message already
// queued under the same app and id, answered or given up within its keep, is not queued again:
// queued is false.
func (r *ChatInbounds) Queue(ctx context.Context, delivery ChatInbound) (queued bool, err error) {
	return queueChatInbound(ctx, r.store.global, delivery)
}

// queueChatInbound is Queue on db, the store's or a transaction's.
func queueChatInbound(ctx context.Context, db bun.IDB, delivery ChatInbound) (bool, error) {
	delivery.State = ChatInboundQueued
	if err := delivery.Validate(); err != nil {
		return false, stack.Wrap(err)
	}
	delivery.NextAttemptAt = delivery.NextAttemptAt.UTC()
	result, err := db.NewInsert().Model(&delivery).
		Column("stream_app_pk", "message_id", "customer_id", "deployment", "api_key", "body", "next_attempt_at").
		On("CONFLICT (stream_app_pk, message_id) DO NOTHING").
		Exec(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: queue chat inbound: %w", err))
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: queue chat inbound: %w", err))
	}
	return affected == 1, nil
}

// QueueFirst records a delivery under key, as DeliveriesSeen.First does, and queues it, in one
// transaction: the delivery is queued exactly when it is the first under key. queued is false
// when key was recorded before, by the message webhook or an earlier delivery of the same
// message: that one answers it. An error records nothing, so a delivery made again is queued.
func (r *ChatInbounds) QueueFirst(ctx context.Context, key string, delivery ChatInbound) (queued bool, err error) {
	if err := r.store.DeliveriesSeen.prune(ctx); err != nil {
		return false, stack.Wrap(err)
	}
	err = r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		first, err := firstSeen(ctx, tx, DeliveryScopeHook, key)
		if err != nil || !first {
			return err
		}
		queued, err = queueChatInbound(ctx, tx, delivery)
		return err
	})
	if err != nil {
		return false, stack.Wrap(err)
	}
	return queued, nil
}

// chatInbounds is the queue of Stream Chat messages to answer.
var chatInbounds = queue{name: "chat inbound", alias: "cd", key: "seq"}

// queuedChatInbound narrows to the messages still to answer.
func queuedChatInbound(query *bun.SelectQuery) *bun.SelectQuery {
	return query.Where("cd.state = ?", ChatInboundQueued)
}

// Claim takes at most limit messages due now, oldest due first, and moves each one's next
// attempt to the end of lease, so no other worker takes it while this one answers it. Rows another
// worker is claiming at the same moment are skipped, not waited for.
func (r *ChatInbounds) Claim(ctx context.Context, limit int, lease time.Duration) ([]ChatInbound, error) {
	return r.claim(ctx, limit, lease, queuedChatInbound)
}

// claim is Claim narrowed by pick, queuedChatInbound or a test's variant of it with a pause in
// it.
func (r *ChatInbounds) claim(ctx context.Context, limit int, lease time.Duration,
	pick func(*bun.SelectQuery) *bun.SelectQuery) ([]ChatInbound, error) {
	return claim[ChatInbound](ctx, r.store.global, chatInbounds, lease, limit, pick, chatInboundColumns)
}

// Get returns the message queued as seq, in whatever state it is.
func (r *ChatInbounds) Get(ctx context.Context, seq int64) (ChatInbound, error) {
	var delivery ChatInbound
	err := r.store.global.NewSelect().Model(&delivery).Where("cd.seq = ?", seq).Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return ChatInbound{}, stack.Wrap(fmt.Errorf("%w: %d", ErrNoChatInbound, seq))
	}
	if err != nil {
		return ChatInbound{}, stack.Wrap(fmt.Errorf("store: chat inbound: %w", err))
	}
	return delivery, nil
}

// NextDue is when the queued message due first is due, leased ones at the end of their lease.
// found is false when no message is queued at all.
func (r *ChatInbounds) NextDue(ctx context.Context) (next time.Time, found bool, err error) {
	return nextDue[ChatInbound](ctx, r.store.global, chatInbounds, queuedChatInbound)
}

// Finish records that a claimed message was answered, at at.
//
// It, Retry and Fail change the row only while the claim still holds it: a worker whose lease
// ran out while it answered, and whose message another worker took since, changes nothing, so
// it cannot finish or reschedule the other's attempt. changed is false then. A row the answer
// settled as it started (StartAnswer) keeps its lease and when it left the queue, so it is
// still this claim's to finish.
func (r *ChatInbounds) Finish(ctx context.Context, claimed ChatInbound, at time.Time) (changed bool, err error) {
	return r.updateClaimed(ctx, claimed, []string{ChatInboundQueued, ChatInboundDone},
		r.store.global.NewUpdate().Model((*ChatInbound)(nil)).
			Set("state = ?", ChatInboundDone).
			Set("finished_at = COALESCE(finished_at, ?)", at.UTC()))
}

// Fail records that a claimed message's last attempt failed and it is given up, at at.
func (r *ChatInbounds) Fail(ctx context.Context, claimed ChatInbound, at time.Time) (changed bool, err error) {
	return r.updateClaimed(ctx, claimed, []string{ChatInboundQueued}, r.store.global.NewUpdate().Model((*ChatInbound)(nil)).
		Set("state = ?", ChatInboundFailed).
		Set("finished_at = ?", at.UTC()))
}

// Retry records a failed attempt of a claimed message and when the next one is due.
func (r *ChatInbounds) Retry(ctx context.Context, claimed ChatInbound, next time.Time) (changed bool, err error) {
	return r.updateClaimed(ctx, claimed, []string{ChatInboundQueued}, r.store.global.NewUpdate().Model((*ChatInbound)(nil)).
		Set("next_attempt_at = ?", next.UTC()))
}

func (r *ChatInbounds) updateClaimed(ctx context.Context, claimed ChatInbound, states []string, update *bun.UpdateQuery) (bool, error) {
	result, err := chatInbounds.heldBy(update, claimed.Seq, claimed.Attempts).
		Where("state IN (?)", bun.In(states)).
		Exec(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: update chat inbound: %w", err))
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: update chat inbound: %w", err))
	}
	return affected == 1, nil
}

// StartAnswer settles a queued message as its answer starts, at at, so no later try
// of it, on any router, answers it again: a peer that answered and whose reply was lost, an
// attempt Close cut short after the peer took it, or a router that stopped before it recorded
// the answer. started is false when the message's answer already started, it was given up, or
// nothing is queued under streamApp and messageID: there is nothing to answer then. The lease
// is left as it is, for the worker holding it to finish the row (Finish).
func (r *ChatInbounds) StartAnswer(ctx context.Context, streamApp int64, messageID string, at time.Time) (started bool, err error) {
	result, err := r.store.global.NewUpdate().Model((*ChatInbound)(nil)).
		Set("state = ?", ChatInboundDone).
		Set("finished_at = ?", at.UTC()).
		Where("stream_app_pk = ? AND message_id = ?", streamApp, messageID).
		Where("state = ?", ChatInboundQueued).
		Exec(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: start chat inbound answer: %w", err))
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: start chat inbound answer: %w", err))
	}
	return affected == 1, nil
}

// KeepHeard keeps what the voice recording of the message queued under streamApp and
// messageID said, so a later try of it hands that on rather than hearing it again. What was
// kept first stays.
func (r *ChatInbounds) KeepHeard(ctx context.Context, streamApp int64, messageID, heard string) error {
	_, err := r.store.global.NewUpdate().Model((*ChatInbound)(nil)).
		Set("heard = ?", heard).
		Where("stream_app_pk = ? AND message_id = ?", streamApp, messageID).
		Where("heard IS NULL").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: keep chat inbound heard: %w", err))
	}
	return nil
}

// ReleaseAnswer queues again a message whose answer started (StartAnswer)
// and that nothing then took, such as a dispatch worker that was not connected, so a later try
// answers it. The lease is left as it is, for the worker holding it to retry the row.
func (r *ChatInbounds) ReleaseAnswer(ctx context.Context, streamApp int64, messageID string) error {
	_, err := r.store.global.NewUpdate().Model((*ChatInbound)(nil)).
		Set("state = ?", ChatInboundQueued).
		Set("finished_at = NULL").
		Where("stream_app_pk = ? AND message_id = ?", streamApp, messageID).
		Where("state = ?", ChatInboundDone).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: release chat inbound answer: %w", err))
	}
	return nil
}

// Prune deletes the rows of answered messages finished a day before now, and of
// given-up ones 30 days before, and returns how many it deleted.
func (r *ChatInbounds) Prune(ctx context.Context, now time.Time) (int, error) {
	pruned, err := deleteInBatches(ctx, r.store.global, "chat_inbound", "(state = ? AND finished_at < ?) OR (state = ? AND finished_at < ?)",
		ChatInboundDone, now.Add(-chatInboundDoneKeep).UTC(), ChatInboundFailed, now.Add(-chatInboundFailedKeep).UTC())
	if err != nil {
		return int(pruned), stack.Wrap(fmt.Errorf("store: prune chat inbound: %w", err))
	}
	return int(pruned), nil
}
