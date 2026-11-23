package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// ChannelLink is a code that ties the number texting it to an end user.
type ChannelLink struct {
	bun.BaseModel `bun:"table:channel_links,alias:cl"`

	Code       string     `bun:"code,pk"`
	CustomerID string     `bun:"customer_id,notnull"`
	ConfigID   string     `bun:"config_id,notnull"`
	UserID     string     `bun:"user_id,notnull"`
	CreatedAt  time.Time  `bun:"created_at,notnull"`
	ExpiresAt  time.Time  `bun:"expires_at,notnull"`
	UsedAt     *time.Time `bun:"used_at"`
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

// ChannelLinks are the codes somebody may text to claim their number, in the shared database.
type ChannelLinks struct {
	store *Store
}

// Insert stores a code somebody may text to claim their number.
func (r *ChannelLinks) Insert(ctx context.Context, link *ChannelLink) error {
	if err := link.Validate(); err != nil {
		return stack.Wrap(err)
	}
	link.CreatedAt = time.Now().UTC()
	if _, err := r.store.global.NewInsert().Model(link).Exec(ctx); err != nil {
		return stack.Wrap(fmt.Errorf("store: save channel link: %w", err))
	}
	return nil
}

// Claim spends a code. A code already spent or past its time is no code at all, so a number
// cannot be claimed twice over by somebody who saw the message go by.
func (r *ChannelLinks) Claim(ctx context.Context, customerID, code string) (ChannelLink, error) {
	if customerID == "" || code == "" {
		return ChannelLink{}, stack.Wrap(ErrUnknownChannelLink)
	}

	now := time.Now().UTC()
	var link ChannelLink
	result, err := r.store.global.NewUpdate().Model(&link).
		Set("used_at = ?", now).
		Where("code = ?", code).
		Where("customer_id = ?", customerID).
		Where("used_at IS NULL").
		Where("expires_at > ?", now).
		Returning("*").
		Exec(ctx)
	if err != nil {
		return ChannelLink{}, stack.Wrap(fmt.Errorf("store: claim channel link: %w", err))
	}
	// An update that matched nothing is not an error to the driver, so the count is what
	// says the code was already spent, has run out, or was never anybody's.
	affected, err := result.RowsAffected()
	if err != nil {
		return ChannelLink{}, stack.Wrap(fmt.Errorf("store: claim channel link: %w", err))
	}
	if affected == 0 {
		return ChannelLink{}, stack.Wrap(ErrUnknownChannelLink)
	}
	return link, nil
}
