package store

import (
	"context"
	"fmt"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// channelLinkFailure is how many messages one number texted a line in one window that
// claimed no code (ChannelLinkFailures).
type channelLinkFailure struct {
	bun.BaseModel `bun:"table:channel_link_failures,alias:clf"`

	AccountID   string    `bun:"account_id,pk"`
	FromAddress string    `bun:"from_address,pk"`
	WindowStart time.Time `bun:"window_start,pk"`
	Failures    int       `bun:"failures,notnull"`
}

// Validate refuses a failure of no number on no line.
func (f *channelLinkFailure) Validate() error {
	return validate("channel link failure").
		required("account_id", f.AccountID).
		required("from_address", f.FromAddress).
		check(!f.WindowStart.IsZero(), "window_start", "is required").
		done()
}

// ChannelLinkFailures count the messages that claimed no link code, by line, number and
// window, in the shared database. A code is six digits, so without a count anybody could
// text guesses until one ties their number to somebody else.
type ChannelLinkFailures struct {
	store *Store
}

// Count is how many failures a line had in the window starting at window, from every number
// and from this one.
func (r *ChannelLinkFailures) Count(ctx context.Context, accountID, from string, window time.Time) (line, sender int, err error) {
	err = r.store.global.NewSelect().Model((*channelLinkFailure)(nil)).
		ColumnExpr("COALESCE(SUM(failures), 0)").
		ColumnExpr("COALESCE(SUM(failures) FILTER (WHERE from_address = ?), 0)", from).
		Where("account_id = ? AND window_start = ?", accountID, window.UTC()).
		Scan(ctx, &line, &sender)
	if err != nil {
		return 0, 0, stack.Wrap(fmt.Errorf("store: count channel link failures: %w", err))
	}
	return line, sender, nil
}

// Record counts one failure of a number on a line in the window starting at window. Windows
// that started before forgetBefore, on any line, are deleted in the same statement, so the
// table holds no more than the windows still counted.
func (r *ChannelLinkFailures) Record(ctx context.Context, accountID, from string, window, forgetBefore time.Time) error {
	failure := channelLinkFailure{AccountID: accountID, FromAddress: from, WindowStart: window.UTC(), Failures: 1}
	if err := failure.Validate(); err != nil {
		return stack.Wrap(err)
	}
	forget := r.store.global.NewDelete().Model((*channelLinkFailure)(nil)).
		Where("window_start < ?", forgetBefore.UTC())
	_, err := r.store.global.NewInsert().With("forgotten", forget).Model(&failure).
		On("CONFLICT (account_id, from_address, window_start) DO UPDATE").
		Set("failures = clf.failures + 1").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: record a channel link failure: %w", err))
	}
	return nil
}
