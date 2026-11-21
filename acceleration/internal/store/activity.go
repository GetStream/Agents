package store

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// ActivityDelta is what one app did in one area of the API, from one client, on one UTC day,
// since the last time it was written: customer_activity's row, as counts to add to it.
type ActivityDelta struct {
	OrganizationID string
	AppID          string
	Area           string
	// Client is one of the audit sources a client may name itself as (AuditSource).
	Client string
	// Day is the UTC day the requests arrived on; only its date is read.
	Day      time.Time
	Requests int64
	Refused  int64
	// LastRefusalCode is the code of the latest refusal counted, and LastRefusalAt when it
	// was; both are empty when none was.
	LastRefusalCode string
	LastRefusalAt   time.Time
	FirstAt         time.Time
	LastAt          time.Time
}

// Validate refuses a delta that names no row: without an app, an area, a client or a day.
func (d *ActivityDelta) Validate() error {
	return validate("activity").
		required("app_id", d.AppID).
		required("area", d.Area).
		required("client", d.Client).
		check(!d.Day.IsZero(), "day", "is required").
		done()
}

// Activities are what each app did in each area of the API, from each client, on each UTC day
// (customer_activity), in the shared database.
type Activities struct {
	store *Store
}

// activityKey is what one customer_activity row is known by.
type activityKey struct {
	appID, area, client, day string
}

// activityBatch is how many rows one statement writes. Each takes 11 parameters, and a
// statement may carry at most 65535 (PostgreSQL's Bind message counts them in an Int16:
// https://www.postgresql.org/docs/current/protocol-message-formats.html), so a thousand
// leave room to spare.
const activityBatch = 1000

// Add adds deltas to the rows they name, making the ones that are not there yet.
// Counts are added, the first and last times widen, and a refusal later than the one the
// row holds replaces it. Deltas naming the same row are summed first: one statement cannot
// update a row twice.
func (r *Activities) Add(ctx context.Context, deltas []ActivityDelta) error {
	for i := range deltas {
		if err := deltas[i].Validate(); err != nil {
			return stack.Wrap(err)
		}
	}
	folded := make(map[activityKey]*ActivityDelta, len(deltas))
	order := make([]activityKey, 0, len(deltas))
	for _, delta := range deltas {
		key := activityKey{delta.AppID, delta.Area, delta.Client, delta.Day.UTC().Format(time.DateOnly)}
		held, found := folded[key]
		if !found {
			copied := delta
			folded[key] = &copied
			order = append(order, key)
			continue
		}
		held.Add(delta)
	}
	// Every replica writes the rows it shares with another in the same order, so two writes
	// at once wait for each other rather than deadlock.
	slices.SortFunc(order, func(a, b activityKey) int {
		return cmp.Or(cmp.Compare(a.appID, b.appID), cmp.Compare(a.area, b.area),
			cmp.Compare(a.client, b.client), cmp.Compare(a.day, b.day))
	})

	// One transaction, so a failed write wrote nothing and the caller can keep the deltas
	// for its next try without counting any of them twice.
	err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		for start := 0; start < len(order); start += activityBatch {
			if err := writeActivity(ctx, tx, folded, order[start:min(start+activityBatch, len(order))]); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: record activity: %w", err))
	}
	return nil
}

// activityUpsert is one customer_activity row as writeActivity upserts it.
type activityUpsert struct {
	bun.BaseModel `bun:"table:customer_activity,alias:act"`

	OrganizationID  string    `bun:"organization_id,notnull"`
	AppID           string    `bun:"app_id,pk"`
	Area            string    `bun:"area,pk"`
	Client          string    `bun:"client,pk"`
	Day             string    `bun:"day,pk"`
	Requests        int64     `bun:"requests,notnull"`
	Refused         int64     `bun:"refused,notnull"`
	LastRefusalCode string    `bun:"last_refusal_code,nullzero"`
	LastRefusalAt   time.Time `bun:"last_refusal_at,nullzero"`
	FirstAt         time.Time `bun:"first_at,notnull"`
	LastAt          time.Time `bun:"last_at,notnull"`
}

// writeActivity upserts the rows of keys in one statement.
func writeActivity(ctx context.Context, tx bun.Tx, folded map[activityKey]*ActivityDelta, keys []activityKey) error {
	rows := make([]activityUpsert, 0, len(keys))
	for _, key := range keys {
		delta := folded[key]
		rows = append(rows, activityUpsert{
			OrganizationID: delta.OrganizationID, AppID: key.appID, Area: key.area, Client: key.client, Day: key.day,
			Requests: delta.Requests, Refused: delta.Refused, LastRefusalCode: delta.LastRefusalCode,
			LastRefusalAt: delta.LastRefusalAt, FirstAt: delta.FirstAt, LastAt: delta.LastAt,
		})
	}
	_, err := tx.NewInsert().Model(&rows).
		On("CONFLICT (app_id, area, client, day) DO UPDATE").
		Set("organization_id = EXCLUDED.organization_id").
		Set("requests = act.requests + EXCLUDED.requests").
		Set("refused = act.refused + EXCLUDED.refused").
		Set(`last_refusal_code = CASE
        WHEN act.last_refusal_at IS NULL OR EXCLUDED.last_refusal_at >= act.last_refusal_at
        THEN EXCLUDED.last_refusal_code
        ELSE act.last_refusal_code END`).
		Set("last_refusal_at = GREATEST(act.last_refusal_at, EXCLUDED.last_refusal_at)").
		Set("first_at = LEAST(act.first_at, EXCLUDED.first_at)").
		Set("last_at = GREATEST(act.last_at, EXCLUDED.last_at)").
		Exec(ctx)
	return err
}

// Add sums other, a delta for the same row, into d. The organization is the latest one's.
func (d *ActivityDelta) Add(other ActivityDelta) {
	d.Requests += other.Requests
	d.Refused += other.Refused
	if !other.LastRefusalAt.Before(d.LastRefusalAt) {
		d.LastRefusalCode, d.LastRefusalAt = other.LastRefusalCode, other.LastRefusalAt
	}
	if other.FirstAt.Before(d.FirstAt) {
		d.FirstAt = other.FirstAt
	}
	if other.LastAt.After(d.LastAt) {
		d.LastAt = other.LastAt
	}
	d.OrganizationID = other.OrganizationID
}

// ActivityArea is what some apps did in one area of the API from one client, summed over the
// days read.
type ActivityArea struct {
	Area     string    `bun:"area"`
	Client   string    `bun:"client"`
	FirstAt  time.Time `bun:"first_at"`
	LastAt   time.Time `bun:"last_at"`
	Requests int64     `bun:"requests"`
	Refused  int64     `bun:"refused"`
	// LastRefusalCode is the code of the latest refusal, empty when there was none.
	LastRefusalCode string `bun:"last_refusal_code"`
}

// Areas sums the activity of apps, at least one, from the UTC day of since on, by area
// and client. The record keeps days, so the day since falls on is read whole.
func (r *Activities) Areas(ctx context.Context, apps []string, since time.Time) ([]ActivityArea, error) {
	var areas []ActivityArea
	err := r.store.global.NewSelect().
		TableExpr("customer_activity").
		ColumnExpr("area, client, MIN(first_at) AS first_at, MAX(last_at) AS last_at").
		ColumnExpr("SUM(requests) AS requests, SUM(refused) AS refused").
		ColumnExpr("COALESCE((array_agg(last_refusal_code ORDER BY last_refusal_at DESC)"+
			" FILTER (WHERE last_refusal_at IS NOT NULL))[1], '') AS last_refusal_code").
		Where("app_id IN (?)", bun.In(apps)).
		Where("day >= ?::date", since.UTC().Format(time.DateOnly)).
		Group("area", "client").
		Order("area", "client").
		Scan(ctx, &areas)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: activity areas: %w", err))
	}
	return areas, nil
}

// Days is how many UTC days from the day of since on apps had a request answered, not
// refused, in any of areas.
func (r *Activities) Days(ctx context.Context, apps []string, since time.Time, areas []string) (int, error) {
	var days int
	err := r.store.global.NewSelect().
		TableExpr("customer_activity").
		ColumnExpr("COUNT(DISTINCT day)").
		Where("app_id IN (?)", bun.In(apps)).
		Where("day >= ?::date", since.UTC().Format(time.DateOnly)).
		Where("area IN (?)", bun.In(areas)).
		Where("requests > refused").
		Scan(ctx, &days)
	if err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: activity days: %w", err))
	}
	return days, nil
}
