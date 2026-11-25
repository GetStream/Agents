package store

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
	"github.com/GetStream/Vision-Agents/acceleration/internal/usage"
)

// Spend reads what customers spent, for limits, alerts and meters with no usage service to
// count it. It writes nothing.
type Spend struct {
	store *Store
}

// spendRows is where spend is read from: this shard's requests, or the hourly sums of every
// shard's (shard_spend_hourly) when other shards keep requests too. The sums are whole hours,
// so a window that starts inside an hour leaves that hour out.
type spendRows struct {
	db    bun.IDB
	table string
	// rows counts the rows read, and requests those that are requests (requestCount).
	rows, requests string
}

func (s *Store) spendRows() spendRows {
	if s.StatsShipped() {
		return spendRows{db: s.global, table: shippedSpend.name, rows: "COALESCE(SUM(rows), 0)",
			requests: "COALESCE(SUM(rows) FILTER (WHERE modality NOT IN ('" + RuntimeModality + "', '" + SandboxModality + "')), 0)"}
	}
	return spendRows{db: s.shard, table: "requests", rows: "COUNT(*)", requests: requestCount}
}

// Since returns what one customer has spent since a moment, across every modality.
func (r *Spend) Since(ctx context.Context, customerID string, since time.Time) (int64, error) {
	spent, err := spendOf(ctx, r.store, []string{customerID}, since)
	if err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: spend since: %w", err))
	}
	return spent, nil
}

// spendOf is what some customers have spent since a moment, in this shard's requests: a limit
// window starts inside an hour, and the hourly sums lag a ship behind.
func spendOf(ctx context.Context, s *Store, customerIDs []string, since time.Time) (int64, error) {
	var spent int64
	err := s.shard.NewSelect().TableExpr("requests").
		ColumnExpr("COALESCE(SUM(cost_micros), 0)").
		Where("customer_id IN (?)", bun.In(customerIDs)).
		Where("started_at >= ?", since).
		Scan(ctx, &spent)
	return spent, err
}

// TeamAppSpend is what one team's rows in one app cost since each of the moments asked about,
// in their order, and whether any of them started at or after the month's start.
type TeamAppSpend struct {
	AppID     string
	Category  string
	Spent     []int64
	ThisMonth bool
}

// Teams sums the billable cost of the rows of every app of the
// organization that carry a team's category since each of since, per app and team, for a
// deployment with no usage service to count it. Rows are read from the earliest moment. A
// billed organization's apps are its Members; an app in no organization, whose id
// organizationID is, is its own only app. Neither counts an app of another customer's whose id
// is the same, as Subject does not.
func (r *Spend) Teams(ctx context.Context, organizationID string, since []time.Time, month time.Time, billed bool) ([]TeamAppSpend, error) {
	if len(since) == 0 {
		return nil, nil
	}
	apps := []string{organizationID}
	if billed {
		var err error
		if apps, err = r.store.AppOrganizations.Members(ctx, organizationID); err != nil {
			return nil, err
		}
	}
	earliest := slices.MinFunc(append(slices.Clone(since), month), func(a, b time.Time) int { return a.Compare(b) })
	var sums strings.Builder
	args := make([]any, 0, len(since)+4)
	for i, start := range since {
		fmt.Fprintf(&sums, "COALESCE(SUM(cost_micros) FILTER (WHERE billable AND started_at >= ?), 0) AS s%d, ", i)
		args = append(args, start)
	}
	args = append(args, month, bun.In(apps), earliest)
	from := r.store.spendRows()
	query := `SELECT customer_id AS app_id, team.category, ` + sums.String() + `bool_or(started_at >= ?) AS this_month
FROM ` + from.table + `
CROSS JOIN LATERAL unnest(usage_categories) AS team(category)
WHERE customer_id IN (?) AND started_at >= ? AND usage_categories <> '{}' AND team.category ILIKE 'team:%'
GROUP BY 1, 2`
	rows, err := from.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: team spend since: %w", err))
	}
	defer rows.Close() //nolint:errcheck // a read-only result
	var spent []TeamAppSpend
	for rows.Next() {
		row := TeamAppSpend{Spent: make([]int64, len(since))}
		targets := []any{&row.AppID, &row.Category}
		for i := range row.Spent {
			targets = append(targets, &row.Spent[i])
		}
		if err := rows.Scan(append(targets, &row.ThisMonth)...); err != nil {
			return nil, stack.Wrap(fmt.Errorf("store: team spend since: %w", err))
		}
		spent = append(spent, row)
	}
	if err := rows.Err(); err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: team spend since: %w", err))
	}
	return spent, nil
}

// Subject sums the billable cost of a subject's rows since a moment, for a deployment with no usage service to count it. A subject of the whole
// organization (the organization, a category) counts every app seen under it when billed, or,
// for an app in no organization, whose id ref.OrganizationID is, that app alone. Neither
// counts an app of another customer's whose id is the same: an app whose id is a billed
// organization's, or one seen under an organization whose id is an unbilled app's.
func (r *Spend) Subject(ctx context.Context, ref usage.SubjectRef, since time.Time, billed bool) (int64, error) {
	apps := []string{ref.AppID}
	if ref.Subject == usage.SubjectOrganization || ref.Subject == usage.SubjectCategory {
		apps = []string{ref.OrganizationID}
		if billed {
			apps = nil
			if err := r.store.global.NewSelect().Model((*AppOrganization)(nil)).Column("app_id").
				Where("organization_id = ?", ref.OrganizationID).Scan(ctx, &apps); err != nil {
				return 0, stack.Wrap(fmt.Errorf("store: subject spend since: %w", err))
			}
		}
	}
	// This shard's requests, for spendOf's reasons.
	query := r.store.shard.NewSelect().TableExpr("requests").
		ColumnExpr("COALESCE(SUM(cost_micros), 0)").
		Where("customer_id IN (?)", bun.In(apps)).
		Where("started_at >= ?", since).
		Where("billable")
	switch ref.Subject {
	case usage.SubjectAPIKey:
		query = query.Where("api_key = ?", ref.APIKey)
	case usage.SubjectUser:
		query = query.Where("user_id = ?", ref.UserID)
	case usage.SubjectCategory, usage.SubjectAppCategory:
		// Said apart, so the sum reads requests_category_spend_idx alone.
		query = query.Where("usage_categories @> ARRAY[?]::text[]", ref.Category).Where("usage_categories <> '{}'")
	}
	var spent int64
	if err := query.Scan(ctx, &spent); err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: subject spend since: %w", err))
	}
	return spent, nil
}

// DaySpend is what an organization's billable requests cost in one UTC day.
type DaySpend struct {
	OrganizationID string    `bun:"organization_id"`
	Day            time.Time `bun:"day"`
	CostMicros     int64     `bun:"cost_micros"`
}

// Daily is every organization's billable spend in each UTC day its requests started in,
// from from to before to, ordered by organization and day. A day without spend has no row. A
// request that names no organization yet is left out: the usage sweeper writes its
// organization within a sweep of it.
func (r *Spend) Daily(ctx context.Context, from, to time.Time) ([]DaySpend, error) {
	source := r.store.spendRows()
	var all []DaySpend
	err := source.db.NewSelect().TableExpr(source.table).
		ColumnExpr("organization_id").
		ColumnExpr("(started_at AT TIME ZONE 'UTC')::date AS day").
		ColumnExpr("SUM(cost_micros) AS cost_micros").
		Where("started_at >= ?", from).
		Where("started_at < ?", to).
		Where("billable").
		Where("organization_id <> ''").
		GroupExpr("1, 2").
		OrderExpr(`organization_id COLLATE "C", day`).
		Scan(ctx, &all)
	if err != nil {
		return nil, stack.Wrap(err)
	}
	for i := range all {
		all[i].Day = all[i].Day.UTC()
	}
	return all, nil
}

// MeterUse is what some apps' work of one modality came to. Requests leaves out the sliced
// time of runtime and sandboxes, which DurationMs measures instead.
type MeterUse struct {
	Modality     string `bun:"modality"`
	Requests     int64  `bun:"requests"`
	InputTokens  int64  `bun:"input_tokens"`
	OutputTokens int64  `bun:"output_tokens"`
	AudioMs      int64  `bun:"audio_ms"`
	Characters   int64  `bun:"characters"`
	DurationMs   int64  `bun:"duration_ms"`
	CostMicros   int64  `bun:"cost_micros"`
}

// Shards names the shards Meters reads, this one's first: those in Options.Shards, or
// this shard alone, named by a digest of its DSN, when that is unset.
func (r *Spend) Shards() []string { return slices.Clone(r.store.shardNames) }

// Meters sums the requests apps, at least one, made from since on by modality, in the order
// of their modality.
func (r *Spend) Meters(ctx context.Context, apps []string, since time.Time) ([]MeterUse, error) {
	from := r.store.spendRows()
	var uses []MeterUse
	err := from.db.NewSelect().
		TableExpr(from.table).
		ColumnExpr("modality").
		ColumnExpr(from.requests+" AS requests").
		ColumnExpr("COALESCE(SUM(input_tokens), 0) AS input_tokens").
		ColumnExpr("COALESCE(SUM(output_tokens), 0) AS output_tokens").
		ColumnExpr("COALESCE(SUM(audio_ms), 0) AS audio_ms").
		ColumnExpr("COALESCE(SUM(characters), 0) AS characters").
		ColumnExpr("COALESCE(SUM(duration_ms), 0)::bigint AS duration_ms").
		ColumnExpr("COALESCE(SUM(cost_micros), 0) AS cost_micros").
		Where("customer_id IN (?)", bun.In(apps)).
		Where("started_at >= ?", since).
		Group("modality").
		Order("modality").
		Scan(ctx, &uses)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: meter uses: %w", err))
	}
	return uses, nil
}
