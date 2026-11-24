package store

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
	"github.com/GetStream/Vision-Agents/acceleration/internal/usage"
)

// UsageReset says that from WindowStart's window on, a subject's limits in Interval count its
// spend less BaselineMicros.
type UsageReset struct {
	bun.BaseModel `bun:"table:usage_resets,alias:ur"`

	ID             int64          `bun:"id,pk,autoincrement"`
	OrganizationID string         `bun:"organization_id,notnull"`
	Subject        usage.Subject  `bun:"subject,notnull"`
	AppID          string         `bun:"app_id,notnull"`
	APIKey         string         `bun:"api_key,notnull"`
	UserID         string         `bun:"user_id,notnull"`
	Category       string         `bun:"category,notnull"`
	Interval       usage.Interval `bun:"interval,notnull"`
	WindowStart    time.Time      `bun:"window_start,notnull"`
	BaselineMicros int64          `bun:"baseline_micros,notnull"`
	Reason         string         `bun:"reason,notnull"`
	Actor          string         `bun:"actor,notnull"`
	CreatedAt      time.Time      `bun:"created_at,nullzero,notnull,default:current_timestamp"`
}

// Ref is the subject the reset is of.
func (r UsageReset) Ref() usage.SubjectRef {
	return usage.SubjectRef{OrganizationID: r.OrganizationID, Subject: r.Subject, AppID: r.AppID, APIKey: r.APIKey, UserID: r.UserID, Category: r.Category}
}

// Validate refuses a reset its table's checks would: one outside an organization, of a subject
// or interval there is none of, with a negative baseline, or without a window.
func (r *UsageReset) Validate() error {
	return validate("usage reset").
		required("organization_id", r.OrganizationID).
		check(len(limitLevels[r.Subject]) > 0, "subject", fmt.Sprintf("is %q, which no limit names", r.Subject)).
		check(r.Interval.Valid(), "interval", fmt.Sprintf("is %q, want daily, weekly, biweekly or monthly", r.Interval)).
		check(!r.WindowStart.IsZero(), "window_start", "is required").
		check(r.BaselineMicros >= 0, "baseline_micros", "is negative").
		done()
}

// UsageResets are the resets of subjects' windows, in the shared database. Each write records
// its audit row in the transaction it commits in.
type UsageResets struct {
	store *Store
}

// Select is the organizations' resets of windows that started at since or later.
func (r *UsageResets) Select(ctx context.Context, organizationIDs []string, since time.Time) ([]UsageReset, error) {
	resets := []UsageReset{}
	if len(organizationIDs) == 0 {
		return resets, nil
	}
	if err := r.store.global.NewSelect().Model(&resets).Where("organization_id IN (?)", bun.In(organizationIDs)).
		Where("window_start >= ?", since.UTC()).Order("id").Scan(ctx); err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: resets: %w", err))
	}
	return resets, nil
}

// InsertMany writes resets in one statement, and their audit rows in another, in one transaction.
func (r *UsageResets) InsertMany(ctx context.Context, resets []UsageReset, audit LimitAudit) error {
	for i := range resets {
		if err := resets[i].Validate(); err != nil {
			return stack.Wrap(err)
		}
	}
	err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if len(resets) == 0 {
			return nil
		}
		for i := range resets {
			resets[i].WindowStart = resets[i].WindowStart.UTC()
		}
		if _, err := tx.NewInsert().Model(&resets).Returning("*").Exec(ctx); err != nil {
			return err
		}
		entries := make([]AuditEntry, len(resets))
		for i := range resets {
			reset := &resets[i]
			after := map[string]any{
				"subject": reset.Subject, "organization_id": reset.OrganizationID, "interval": reset.Interval,
				"window_start": reset.WindowStart.Format(time.RFC3339), "baseline_micros": reset.BaselineMicros, "reason": reset.Reason,
			}
			for name, value := range map[string]string{"app_id": reset.AppID, "api_key": reset.APIKey, "user_id": reset.UserID, "category": reset.Category} {
				if value != "" {
					after[name] = value
				}
			}
			entries[i] = *audit.entry("reset:"+strconv.FormatInt(reset.ID, 10), string(reset.Subject), AuditCreated, fieldChanges(nil, after))
		}
		return recordAuditEntries(ctx, tx, entries)
	})
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: add resets: %w", err))
	}
	return nil
}
