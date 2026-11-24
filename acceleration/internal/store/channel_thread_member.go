package store

import (
	"context"
	"fmt"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// channelThreadMember is a person a thread channel's chat reaches, by the recipient their
// opt-outs name (20261117110000_channel_thread_members.sql).
type channelThreadMember struct {
	bun.BaseModel `bun:"table:channel_thread_members,alias:ctm"`

	ChannelID string `bun:"channel_id,pk"`
	Member    string `bun:"member,pk"`
}

// Validate refuses a member of no channel, or no one.
func (m *channelThreadMember) Validate() error {
	return validate("channel thread member").
		required("channel_id", m.ChannelID).
		required("member", m.Member).
		done()
}

// ChannelThreadMembers are the people thread channels' chats reach, in the shared database.
type ChannelThreadMembers struct {
	store *Store
}

// Insert records member as a person a thread channel's chat reaches, under the recipient an
// opt-out of theirs is kept as (optOutRecipient). One already recorded is left as it is.
func (r *ChannelThreadMembers) Insert(ctx context.Context, channelID, member string) error {
	member, _ = optOutRecipient(member, "")
	row := &channelThreadMember{ChannelID: channelID, Member: member}
	if err := row.Validate(); err != nil {
		return stack.Wrap(err)
	}
	if _, err := r.store.global.NewInsert().Model(row).On("CONFLICT DO NOTHING").Exec(ctx); err != nil {
		return stack.Wrap(fmt.Errorf("store: add channel thread member: %w", err))
	}
	return nil
}

// OptedOut is whether a person recorded in a thread channel's chat has a live opt-out of the
// customer's channel, or of all of them, as OptedOut reads one.
func (r *ChannelThreadMembers) OptedOut(ctx context.Context, customerID, channelID, channel string) (bool, error) {
	found, err := r.store.global.NewSelect().Model((*OptOut)(nil)).
		Join("JOIN channel_thread_members AS ctm ON ctm.member = oo.recipient").
		Where("ctm.channel_id = ?", channelID).
		Where("oo.customer_id = ?", customerID).
		Where("oo.channel IN (?)", bun.In([]string{channel, OptOutAll})).
		Where("oo.revoked_at IS NULL").
		Exists(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: channel thread member opted out: %w", err))
	}
	return found, nil
}
