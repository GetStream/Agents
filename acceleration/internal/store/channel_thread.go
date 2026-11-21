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

// ChannelThread links one external thread, such as a Slack thread, to the thread channel the
// channel bridge writes it into (20261007003100_channel_threads.sql).
type ChannelThread struct {
	bun.BaseModel `bun:"table:channel_threads,alias:ct"`

	// ChannelID is the thread channel's id in the agent channel type.
	ChannelID      string `bun:"channel_id,pk"`
	CustomerID     string `bun:"customer_id,notnull"`
	ConnectorID    string `bun:"connector_id,notnull"`
	ProviderUnitID string `bun:"provider_unit_id,notnull"`
	ThreadKey      string `bun:"thread_key,notnull"`
	// ConnectionID is the app-owned connection replies are sent with.
	ConnectionID string `bun:"connection_id,notnull"`
	// ThreadParts are the thread key's named parts, which a reply names as {thread.<name>}.
	ThreadParts map[string]string `bun:"thread_parts,type:jsonb,notnull"`
	// StreamAppPK is the Stream app the thread channel is in; zero, stored as NULL, is the
	// deployment's own.
	StreamAppPK int64 `bun:"stream_app_pk,nullzero"`
	// ChatPerson is the author id of the one person a chat thread is with, while its latest
	// message says the chat is no group; empty for any other thread (SetChatPerson).
	ChatPerson string `bun:"chat_person,notnull"`
	// Starter is the person who started the thread's chat, by the contact map address the
	// episode their message opens has; empty until recorded (SetStarter).
	Starter       string    `bun:"starter,notnull"`
	LastInboundAt time.Time `bun:"last_inbound_at,notnull"`
	CreatedAt     time.Time `bun:"created_at,notnull"`
}

// Validate refuses a link a reply could not be sent back on.
func (t *ChannelThread) Validate() error {
	return validate("channel thread").
		required("channel_id", t.ChannelID).
		required("customer_id", t.CustomerID).
		required("connector_id", t.ConnectorID).
		required("thread_key", t.ThreadKey).
		required("connection_id", t.ConnectionID).
		done()
}

// ErrNoChannelThread says no external thread is linked to a thread channel: the channel is a
// plain Stream Chat conversation, which the channel bridge sends nothing from.
var ErrNoChannelThread = errors.New("store: no external thread is linked to this channel")

// ChannelThreads are the external threads linked to thread channels, in the shared database.
type ChannelThreads struct {
	store *Store
}

// Link returns the thread channel of the external thread thread names, linking it to
// thread.ChannelID when the thread has none yet. created is whether it did. An existing link
// keeps its channel, pin, chat person and starter, takes thread's connection, which the latest
// message names, and records the message as the last inbound one. Two first messages of one
// thread at once link one channel: the unique index on the thread decides which.
func (r *ChannelThreads) Link(ctx context.Context, thread *ChannelThread) (created bool, err error) {
	if err := thread.Validate(); err != nil {
		return false, stack.Wrap(err)
	}
	proposed := thread.ChannelID
	now := time.Now().UTC().Truncate(time.Microsecond)
	thread.LastInboundAt, thread.CreatedAt = now, now
	if thread.ThreadParts == nil {
		thread.ThreadParts = map[string]string{}
	}
	_, err = r.store.global.NewInsert().Model(thread).
		On("CONFLICT (customer_id, connector_id, provider_unit_id, thread_key) DO UPDATE").
		Set("connection_id = EXCLUDED.connection_id").
		Set("last_inbound_at = EXCLUDED.last_inbound_at").
		Returning("*").
		Exec(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: link channel thread: %w", err))
	}
	return thread.ChannelID == proposed, nil
}

// Get returns the external thread linked to a thread channel, or ErrNoChannelThread.
func (r *ChannelThreads) Get(ctx context.Context, channelID string) (ChannelThread, error) {
	if channelID == "" {
		return ChannelThread{}, stack.Wrap(errors.New("store: a channel id is required"))
	}
	var thread ChannelThread
	err := r.store.global.NewSelect().Model(&thread).Where("ct.channel_id = ?", channelID).Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return ChannelThread{}, stack.Wrap(fmt.Errorf("%w: %s", ErrNoChannelThread, channelID))
	}
	if err != nil {
		return ChannelThread{}, stack.Wrap(fmt.Errorf("store: channel thread: %w", err))
	}
	return thread, nil
}

// Linked is whether an external thread of a customer's provider unit is linked to a thread
// channel already.
func (r *ChannelThreads) Linked(ctx context.Context, customerID, connectorID, providerUnitID, threadKey string) (bool, error) {
	linked, err := r.store.global.NewSelect().Model((*ChannelThread)(nil)).
		Where("customer_id = ? AND connector_id = ? AND provider_unit_id = ? AND thread_key = ?",
			customerID, connectorID, providerUnitID, threadKey).
		Exists(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: channel thread linked: %w", err))
	}
	return linked, nil
}

// SetChatPerson records person as the one person a thread channel's chat is with, as the
// chat's latest message names them; empty forgets the one recorded, so the chat is no one
// person's.
func (r *ChannelThreads) SetChatPerson(ctx context.Context, channelID, person string) error {
	if channelID == "" {
		return stack.Wrap(errors.New("store: a channel id is required"))
	}
	_, err := r.store.global.NewUpdate().Model((*ChannelThread)(nil)).
		Set("chat_person = ?", person).
		Where("channel_id = ?", channelID).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: set channel thread chat person: %w", err))
	}
	return nil
}

// SetStarter records starter as the person who started a thread channel's chat, by the contact
// map address the episode their message opens has
// (20261217090000_agent_config_episode_cards_record.sql). One already recorded is left as it
// is, so the first admitted message's author stays the chat's starter.
func (r *ChannelThreads) SetStarter(ctx context.Context, channelID, starter string) error {
	if channelID == "" || starter == "" {
		return stack.Wrap(errors.New("store: a channel id and a starter are required"))
	}
	_, err := r.store.global.NewUpdate().Model((*ChannelThread)(nil)).
		Set("starter = ?", starter).
		Where("channel_id = ?", channelID).
		Where("starter = ''").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: set channel thread starter: %w", err))
	}
	return nil
}

// Starter is the person who started a thread channel's chat (SetStarter), read as it is now,
// or "" when none is recorded.
func (r *ChannelThreads) Starter(ctx context.Context, channelID string) (string, error) {
	var starter []string
	err := r.store.global.NewSelect().Model((*ChannelThread)(nil)).
		Column("starter").
		Where("channel_id = ?", channelID).
		Scan(ctx, &starter)
	if err != nil {
		return "", stack.Wrap(fmt.Errorf("store: channel thread starter: %w", err))
	}
	if len(starter) == 0 {
		return "", nil
	}
	return starter[0], nil
}
