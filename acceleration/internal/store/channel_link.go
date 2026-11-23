package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// ChannelLink is a code that ties the number texting it to an end user. A code is unique within
// its app, which is where it is claimed.
type ChannelLink struct {
	bun.BaseModel `bun:"table:channel_links,alias:cl"`

	CustomerID string    `bun:"customer_id,pk"`
	Code       string    `bun:"code,pk"`
	ConfigID   string    `bun:"config_id,notnull"`
	UserID     string    `bun:"user_id,notnull"`
	CreatedAt  time.Time `bun:"created_at,notnull"`
	ExpiresAt  time.Time `bun:"expires_at,notnull"`
	// UsedAt is set only by a release older than the one that deletes a code it claims.
	UsedAt *time.Time `bun:"used_at"`
}

// Validate refuses a code that ties a number to no one.
func (l *ChannelLink) Validate() error {
	return validate("channel link").
		required("code", l.Code).
		required("customer_id", l.CustomerID).
		required("config_id", l.ConfigID).
		required("user_id", l.UserID).
		check(!l.ExpiresAt.IsZero(), "expires_at", "is required").
		done()
}

// ErrUnknownChannelLink is a code nobody was given, one already spent, or one past its time.
var ErrUnknownChannelLink = errors.New("store: there is no such code")

// ErrChannelLinkElsewhere is a code of the app that was given for another of its agents. It
// is not spent, so it still claims the number on that agent's line.
var ErrChannelLinkElsewhere = errors.New("store: the code is for another agent")

// channelLinkDraws is how many codes Insert draws before it gives up. A draw is taken only when
// the app already holds the code drawn, which for six digits and ten thousand codes held at
// once is one draw in a hundred: four in a row taken means the draw is broken, and looping on
// it would never end.
const channelLinkDraws = 4

// ChannelLinks are the codes somebody may text to claim their number, in the shared database.
type ChannelLinks struct {
	store *Store
}

// Insert stores a code somebody may text to claim their number, drawn by draw: a code the app
// already holds is drawn again, so two people linking at once never get one code. Codes past
// their time, of any app, are deleted in the same statement, so the table holds only the codes
// that may still be claimed.
func (r *ChannelLinks) Insert(ctx context.Context, link *ChannelLink, draw func() (string, error)) error {
	for range channelLinkDraws {
		code, err := draw()
		if err != nil {
			return err
		}
		link.Code = code
		if err := link.Validate(); err != nil {
			return stack.Wrap(err)
		}
		link.CreatedAt = time.Now().UTC()
		expired := r.store.global.NewDelete().Model((*ChannelLink)(nil)).
			Where("expires_at <= ?", link.CreatedAt)
		result, err := r.store.global.NewInsert().With("expired", expired).Model(link).
			On("CONFLICT DO NOTHING").
			Exec(ctx)
		if err != nil {
			return stack.Wrap(fmt.Errorf("store: save channel link: %w", err))
		}
		// A conflict is no error to the driver: no row written is what says the code is taken.
		written, err := result.RowsAffected()
		if err != nil {
			return stack.Wrap(fmt.Errorf("store: save channel link: %w", err))
		}
		if written == 1 {
			return nil
		}
	}
	return stack.Wrap(fmt.Errorf("store: save channel link: %d codes drawn were all taken", channelLinkDraws))
}

// Claim spends a code by deleting it. A code already spent or past its time is no code at all,
// so a number cannot be claimed twice over by somebody who saw the message go by. A code of
// another of the app's agents is ErrChannelLinkElsewhere and stays, so texting it to the wrong
// line does not spend it.
func (r *ChannelLinks) Claim(ctx context.Context, customerID, configID, code string) (ChannelLink, error) {
	if customerID == "" || configID == "" || code == "" {
		return ChannelLink{}, stack.Wrap(ErrUnknownChannelLink)
	}

	now := time.Now().UTC()
	var claimed []ChannelLink
	// A code an older release marked used is spent already, so it is not claimed again.
	err := r.store.global.NewDelete().Model(&claimed).
		Where("customer_id = ?", customerID).
		Where("code = ?", code).
		Where("config_id = ?", configID).
		Where("used_at IS NULL").
		Where("expires_at > ?", now).
		Returning("*").
		Scan(ctx)
	if err != nil {
		return ChannelLink{}, stack.Wrap(fmt.Errorf("store: claim channel link: %w", err))
	}
	if len(claimed) == 1 {
		return claimed[0], nil
	}

	// The delete takes a code of this agent, so a code of the app still to claim is another agent's.
	elsewhere, err := r.store.global.NewSelect().Model((*ChannelLink)(nil)).
		Where("customer_id = ?", customerID).
		Where("code = ?", code).
		Where("used_at IS NULL").
		Where("expires_at > ?", now).
		Exists(ctx)
	if err != nil {
		return ChannelLink{}, stack.Wrap(fmt.Errorf("store: claim channel link: %w", err))
	}
	if elsewhere {
		return ChannelLink{}, stack.Wrap(ErrChannelLinkElsewhere)
	}
	return ChannelLink{}, stack.Wrap(ErrUnknownChannelLink)
}
