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

// ChannelLinkFailures count the messages that claimed no link code, and those still being tried
// as one, by line, number and window, in the shared database. A code is six digits, so without
// a count anybody could text guesses until one ties their number to somebody else.
type ChannelLinkFailures struct {
	store *Store
}

// channelLinkFailureLockSeed is the second argument to hashtextextended for the lock a line's
// codes are reserved under; the value only has to differ from the other seeds.
const channelLinkFailureLockSeed = 3637

// Reserve counts one code a number texted a line in the window starting at window, before it is
// tried, and is false, counting nothing, when the number already has senderCap codes counted in
// the window or the line lineCap from every number together. Windows that started before
// forgetBefore, on any line, are deleted in the same statement, so the table holds no more than
// the windows still counted.
//
// The line's total spans a row per number, and one statement does not see a row another
// number's statement inserts at the same moment, so codes texted at once from many numbers would
// all be tried on one total. The line is locked for the count and the write instead.
func (r *ChannelLinkFailures) Reserve(ctx context.Context, accountID, from string, window, forgetBefore time.Time,
	senderCap, lineCap int) (bool, error) {
	failure := channelLinkFailure{AccountID: accountID, FromAddress: from, WindowStart: window.UTC(), Failures: 1}
	if err := failure.Validate(); err != nil {
		return false, stack.Wrap(err)
	}
	var reserved []int
	err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if err := lockUntilCommit(ctx, tx, accountID, channelLinkFailureLockSeed); err != nil {
			return err
		}
		return tx.NewRaw(`WITH forgotten AS (DELETE FROM channel_link_failures WHERE window_start < ?),
			counted AS (
				SELECT COALESCE(SUM(failures), 0) AS line,
					COALESCE(SUM(failures) FILTER (WHERE from_address = ?), 0) AS sender
				FROM channel_link_failures WHERE account_id = ? AND window_start = ?)
			INSERT INTO channel_link_failures AS clf (account_id, from_address, window_start, failures)
			SELECT ?, ?, ?, 1 FROM counted WHERE counted.line < ? AND counted.sender < ?
			ON CONFLICT (account_id, from_address, window_start) DO UPDATE SET failures = clf.failures + 1
			RETURNING clf.failures`,
			forgetBefore.UTC(), from, accountID, failure.WindowStart,
			accountID, from, failure.WindowStart, lineCap, senderCap).Scan(ctx, &reserved)
	})
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: reserve a channel link code: %w", err))
	}
	return len(reserved) > 0, nil
}

// Release takes back one code Reserve counted for a number on a line in the window starting at
// window: a code that was no wrong one.
func (r *ChannelLinkFailures) Release(ctx context.Context, accountID, from string, window time.Time) error {
	_, err := r.store.global.NewUpdate().Model((*channelLinkFailure)(nil)).
		Set("failures = failures - 1").
		Where("account_id = ? AND from_address = ? AND window_start = ?", accountID, from, window.UTC()).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: release a channel link code: %w", err))
	}
	return nil
}
