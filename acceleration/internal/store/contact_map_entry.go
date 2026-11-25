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

// How a person is known to the contact map.
const (
	// ContactPhone is an E.164 number: a call, an SMS, a WhatsApp message or an iMessage.
	ContactPhone = "phone"
	// ContactSlack is a Slack user in one workspace, «<team id>:<user id>».
	ContactSlack = "slack"
	// ContactStreamUser is a Stream user of the app chat in one Stream app, by the user id
	// Stream signed, «<app pk>:<user id>» (StreamUserAddress).
	ContactStreamUser = "stream_user"
)

// ContactMapEntry is one address a person is reached at, for one agent of a customer: the
// omni-channel their episode cards go to, or, for a channel line's sender, the end user they
// are and the conversation the next message carries on.
type ContactMapEntry struct {
	bun.BaseModel `bun:"table:contact_map,alias:cm"`

	ID         string `bun:"id,pk"`
	CustomerID string `bun:"customer_id,notnull"`
	ConfigID   string `bun:"config_id,notnull"`
	// Kind is ContactPhone, ContactSlack or ContactStreamUser, and Address the number, the Slack
	// user or the Stream user in its app; retired:<id> for a number's row a link set aside
	// (retireHistory). A channel line's sender is of the line's kind (whatsapp, sms or
	// imessage), and Address the number that writes, or a chat handle where there is no number.
	Kind    string `bun:"kind,notnull"`
	Address string `bun:"address,notnull"`
	// ConversationID is the omni-channel's cid, agent:omni-<uuid>. For a channel line's sender
	// it is the conversation what was said last time is carried on in; empty until the first
	// turn settles.
	ConversationID string `bun:"conversation_id,notnull"`
	// UserID is the end user the address is linked to; empty until linked.
	UserID string `bun:"user_id,nullzero"`
	// Thread is where a message is sent to reach a channel line's sender again: the chat, for
	// iMessage, whose replies go to a conversation rather than to a person. Empty where a reply
	// goes to the address itself.
	Thread string `bun:"thread,notnull"`
	// StreamAppPK is the Stream app the omni-channel is in; zero, stored as NULL, is the
	// deployment's own.
	StreamAppPK int64     `bun:"stream_app_pk,nullzero"`
	CreatedAt   time.Time `bun:"created_at,notnull"`
	UpdatedAt   time.Time `bun:"updated_at,notnull"`
}

// Validate refuses a row no one could be found by.
func (e *ContactMapEntry) Validate() error {
	return validate("contact map entry").
		required("customer_id", e.CustomerID).
		required("config_id", e.ConfigID).
		oneOf("kind", e.Kind, ContactPhone, ContactSlack, ContactStreamUser, "whatsapp", "sms", "imessage").
		required("address", e.Address).
		done()
}

// ErrNoContact says the contact map holds no row for a person: the agent has had no episode
// with them, so they have no omni-channel and no cards, or the number never wrote to a line.
var ErrNoContact = errors.New("store: no contact map row for this person")

// ContactMapEntries are the contact map's rows, in the shared database.
type ContactMapEntries struct {
	store *Store
}

// Map returns the contact map row of the address entry names, making it with
// entry.ConversationID and entry.StreamAppPK when the address has none yet. created is
// whether it did. An existing row keeps its omni-channel and its pin. Two first sightings of
// one address at once make one row: the unique index on the address decides which. A row made
// for a linked Stream user or number takes the omni-channel and pin of its partner's row under
// the same agent, the older (joinPartner).
func (r *ContactMapEntries) Map(ctx context.Context, entry *ContactMapEntry) (created bool, err error) {
	if err := entry.Validate(); err != nil {
		return false, stack.Wrap(err)
	}
	if entry.ConversationID == "" {
		return false, stack.Wrap(&ValidationError{Model: "contact map entry", Field: "conversation_id", Reason: "is required"})
	}
	proposed := entry.ConversationID
	now := time.Now().UTC().Truncate(time.Microsecond)
	entry.CreatedAt, entry.UpdatedAt = now, now
	if entry.ID == "" {
		entry.ID = newID()
	}
	// The update is there only so RETURNING hands back the row that won: DO NOTHING returns
	// none.
	_, err = r.store.global.NewInsert().Model(entry).
		On("CONFLICT (customer_id, config_id, kind, address) DO UPDATE").
		Set("updated_at = EXCLUDED.updated_at").
		Returning("*").
		Exec(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: map contact: %w", err))
	}
	created = entry.ConversationID == proposed
	if !created {
		return false, nil
	}
	return true, r.joinPartner(ctx, entry)
}

// joinPartner points a row just made at its linked partner's omni-channel of the same agent,
// when the customer linked its Stream user or its number (UserPhones.Link), and reads the row
// back. Read after the row is committed, so a link made at the same time is either seen here
// or sees the row. Only a new row: a row that was there when the link was made was joined
// then, so a message on a known row costs no extra query.
func (r *ContactMapEntries) joinPartner(ctx context.Context, entry *ContactMapEntry) error {
	link, linked, err := r.store.UserPhones.linkOf(ctx, entry)
	if err != nil || !linked {
		return err
	}
	if err := r.store.UserPhones.joinLinked(ctx, link); err != nil {
		return err
	}
	if err := r.store.global.NewSelect().Model(entry).WherePK().Scan(ctx); err != nil {
		return stack.Wrap(fmt.Errorf("store: read a joined contact: %w", err))
	}
	return nil
}

// GetByAddress is the contact map row of an address, for one agent of a customer. Unlike Map
// it never makes one: reading a person's cards writes nothing.
func (r *ContactMapEntries) GetByAddress(ctx context.Context, customerID, configID, kind, address string) (ContactMapEntry, error) {
	if customerID == "" || configID == "" || kind == "" || address == "" {
		return ContactMapEntry{}, stack.Wrap(ErrNoContact)
	}
	var entry ContactMapEntry
	err := r.store.global.NewSelect().Model(&entry).
		Where("customer_id = ?", customerID).Where("config_id = ?", configID).
		Where("kind = ?", kind).Where("address = ?", address).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return ContactMapEntry{}, stack.Wrap(ErrNoContact)
	}
	if err != nil {
		return ContactMapEntry{}, stack.Wrap(fmt.Errorf("store: contact: %w", err))
	}
	return entry, nil
}

// GetByUser is where one end user is reached on a channel line: the row an agent holds for them
// of the line's kind, written by whichever of their messages settled who they are. A user known
// at more than one address is reached at the one settled last.
func (r *ContactMapEntries) GetByUser(ctx context.Context, customerID, configID, kind, userID string) (ContactMapEntry, error) {
	if customerID == "" || configID == "" || kind == "" || userID == "" {
		return ContactMapEntry{}, stack.Wrap(ErrNoContact)
	}
	var entry ContactMapEntry
	err := r.store.global.NewSelect().Model(&entry).
		Where("customer_id = ?", customerID).
		Where("config_id = ?", configID).
		Where("kind = ?", kind).
		Where("user_id = ?", userID).
		Order("updated_at DESC", "id DESC").
		Limit(1).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return ContactMapEntry{}, stack.Wrap(ErrNoContact)
	}
	if err != nil {
		return ContactMapEntry{}, stack.Wrap(fmt.Errorf("store: contact by user: %w", err))
	}
	return entry, nil
}

// Upsert records who a channel line's sender is, their conversation and where a reply reaches
// them, and reads the row back into entry. A row that has a conversation keeps it: two first
// messages from one number on two routers each open one, and the second is handed the first
// to carry on in.
func (r *ContactMapEntries) Upsert(ctx context.Context, entry *ContactMapEntry) error {
	if err := entry.Validate(); err != nil {
		return stack.Wrap(err)
	}
	now := time.Now().UTC()
	entry.UpdatedAt = now
	if entry.ID == "" {
		entry.ID = newID()
		entry.CreatedAt = now
	}
	_, err := r.store.global.NewInsert().Model(entry).
		On("CONFLICT (customer_id, config_id, kind, address) DO UPDATE").
		Set("user_id = EXCLUDED.user_id").
		Set("conversation_id = COALESCE(NULLIF(?TableAlias.conversation_id, ''), EXCLUDED.conversation_id)").
		Set("thread = EXCLUDED.thread").
		Set("updated_at = EXCLUDED.updated_at").
		Returning("*").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: save contact: %w", err))
	}
	return nil
}

// GetByThread is the contact map row of the person a thread channel's episodes are with,
// for one agent of a customer: the one who started the thread, since Episodes.Open keeps the
// first message's episode for the later ones. The newest episode of the thread decides. A
// thread with no episode, or only one of another customer or agent, has none. A row a link set
// aside is answered with its number as its address (reachedAt): the thread still reaches it.
func (r *ContactMapEntries) GetByThread(ctx context.Context, customerID, configID, threadChannel string) (ContactMapEntry, error) {
	var entry ContactMapEntry
	err := r.store.global.NewSelect().Model(&entry).
		ColumnExpr("cm.*").ColumnExpr(reachedAt+" AS address").
		Join("JOIN episodes AS ep ON ep.contact_id = cm.id").
		Where("ep.customer_id = ?", customerID).Where("ep.thread_channel = ?", threadChannel).
		Where("ep.session_id IS NULL").
		Where("cm.customer_id = ?", customerID).Where("cm.config_id = ?", configID).
		OrderExpr("ep.started_at DESC").Limit(1).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return ContactMapEntry{}, stack.Wrap(ErrNoContact)
	}
	if err != nil {
		return ContactMapEntry{}, stack.Wrap(fmt.Errorf("store: thread contact: %w", err))
	}
	return entry, nil
}
