package store

import (
	"context"
	"fmt"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// ActivityDigest is one UTC day whose activity digest was claimed for the operator's channel
// (activity_digests), so the deployment posts each day's once whatever its routers and restarts.
type ActivityDigest struct {
	bun.BaseModel `bun:"table:activity_digests,alias:ad"`

	// Day is the UTC day the digest is of, as a date.
	Day       string     `bun:"day,pk"`
	ClaimedAt time.Time  `bun:"claimed_at,nullzero,notnull,default:current_timestamp"`
	PostedAt  *time.Time `bun:"posted_at"`
}

// ActivityDigests are the days whose activity digest was claimed or posted, in the shared
// database.
type ActivityDigests struct {
	store *Store
}

// Claim claims the activity digest of day, a UTC day, unless it was posted or another claim of
// it is younger than stale, so one whose post failed, or whose router stopped before posting it,
// is posted by a later check. claimed says this call holds the claim, and so posts the digest.
func (r *ActivityDigests) Claim(ctx context.Context, day time.Time, stale time.Duration) (claimed bool, err error) {
	result, err := r.store.global.NewInsert().Model(&ActivityDigest{Day: day.UTC().Format(time.DateOnly)}).
		Column("day").
		On("CONFLICT (day) DO UPDATE").
		Set("claimed_at = now()").
		Where("ad.posted_at IS NULL").
		Where("ad.claimed_at < now() - ? * interval '1 millisecond'", stale.Milliseconds()).
		Exec(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: claim activity digest: %w", err))
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: claim activity digest: %w", err))
	}
	return affected > 0, nil
}

// MarkPosted records that the activity digest of day, a UTC day, was posted.
func (r *ActivityDigests) MarkPosted(ctx context.Context, day time.Time) error {
	_, err := r.store.global.NewUpdate().Model((*ActivityDigest)(nil)).
		Set("posted_at = now()").
		Where("day = ?::date", day.UTC().Format(time.DateOnly)).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: mark activity digest posted: %w", err))
	}
	return nil
}

// Prune deletes the activity digests of the UTC days more than activityKeepDays before now's,
// as Activities.Prune does the activity, and returns how many it deleted.
func (r *ActivityDigests) Prune(ctx context.Context, now time.Time) (int, error) {
	return pruneDays(ctx, r.store.global, "activity_digests", now)
}
