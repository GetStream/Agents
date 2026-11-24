package store

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
	"github.com/GetStream/Vision-Agents/acceleration/internal/usage"
)

// Spend reads what customers spent from the requests of every shard, for limits, alerts and
// meters with no usage service to count it. It writes nothing.
type Spend struct {
	store *Store
}

// Since returns what one customer has spent since a moment, across every modality.
func (r *Spend) Since(ctx context.Context, customerID string, since time.Time) (int64, error) {
	spent, err := spendOf(ctx, r.store, []string{customerID}, since)
	if err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: spend since: %w", err))
	}
	return spent, nil
}

// spendOf is what some customers have spent since a moment, in every shard.
func spendOf(ctx context.Context, s *Store, customerIDs []string, since time.Time) (int64, error) {
	answers, err := everyShard(ctx, s, func(ctx context.Context, db bun.IDB) (int64, error) {
		var spent int64
		err := db.NewSelect().Model((*Request)(nil)).
			ColumnExpr("COALESCE(SUM(cost_micros), 0)").
			Where("customer_id IN (?)", bun.In(customerIDs)).
			Where("started_at >= ?", since).
			Scan(ctx, &spent)
		return spent, err
	})
	var spent int64
	for _, answer := range answers {
		spent += answer
	}
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

// Teams sums, across every shard, the billable cost of the rows of every app of the
// organization that carry a team's category since each of since, per app and team, for a
// deployment with no usage service to count it. Rows are read from the earliest moment.
func (r *Spend) Teams(ctx context.Context, organizationID string, since []time.Time, month time.Time) ([]TeamAppSpend, error) {
	if len(since) == 0 {
		return nil, nil
	}
	apps, err := r.store.AppOrganizations.Apps(ctx, organizationID)
	if err != nil {
		return nil, err
	}
	earliest := slices.MinFunc(append(slices.Clone(since), month), func(a, b time.Time) int { return a.Compare(b) })
	var sums strings.Builder
	args := make([]any, 0, len(since)+4)
	for i, start := range since {
		fmt.Fprintf(&sums, "COALESCE(SUM(cost_micros) FILTER (WHERE billable AND started_at >= ?), 0) AS s%d, ", i)
		args = append(args, start)
	}
	args = append(args, month, bun.In(apps), earliest)
	query := `SELECT customer_id AS app_id, team.category, ` + sums.String() + `bool_or(started_at >= ?) AS this_month
FROM requests
CROSS JOIN LATERAL unnest(usage_categories) AS team(category)
WHERE customer_id IN (?) AND started_at >= ? AND usage_categories <> '{}' AND team.category ILIKE 'team:%'
GROUP BY 1, 2`
	answers, err := everyShard(ctx, r.store, func(ctx context.Context, db bun.IDB) ([]TeamAppSpend, error) {
		rows, err := db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		defer rows.Close() //nolint:errcheck // a read-only result
		var read []TeamAppSpend
		for rows.Next() {
			row := TeamAppSpend{Spent: make([]int64, len(since))}
			targets := []any{&row.AppID, &row.Category}
			for i := range row.Spent {
				targets = append(targets, &row.Spent[i])
			}
			if err := rows.Scan(append(targets, &row.ThisMonth)...); err != nil {
				return nil, err
			}
			read = append(read, row)
		}
		return read, rows.Err()
	})
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: team spend since: %w", err))
	}
	type key struct{ app, category string }
	merged := map[key]*TeamAppSpend{}
	var order []key
	for _, answer := range answers {
		for _, row := range answer {
			k := key{row.AppID, row.Category}
			sum, ok := merged[k]
			if !ok {
				sum = &TeamAppSpend{AppID: row.AppID, Category: row.Category, Spent: make([]int64, len(since))}
				merged[k] = sum
				order = append(order, k)
			}
			for i, micros := range row.Spent {
				sum.Spent[i] += micros
			}
			sum.ThisMonth = sum.ThisMonth || row.ThisMonth
		}
	}
	spent := make([]TeamAppSpend, 0, len(order))
	for _, k := range order {
		spent = append(spent, *merged[k])
	}
	return spent, nil
}

// Subject sums the billable cost of a subject's rows since a moment, across every
// shard, for a deployment with no usage service to count it. A subject of the whole
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
	answers, err := everyShard(ctx, r.store, func(ctx context.Context, db bun.IDB) (int64, error) {
		var spent int64
		query := db.NewSelect().Model((*Request)(nil)).
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
		err := query.Scan(ctx, &spent)
		return spent, err
	})
	if err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: subject spend since: %w", err))
	}
	var spent int64
	for _, answer := range answers {
		spent += answer
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
// from from to before to, summed over every shard and ordered by organization and day. A day
// without spend has no row. A request that names no organization yet is left out: the usage
// sweeper writes its organization within a sweep of it.
func (r *Spend) Daily(ctx context.Context, from, to time.Time) ([]DaySpend, error) {
	lists, err := everyShard(ctx, r.store, func(ctx context.Context, db bun.IDB) ([]DaySpend, error) {
		var found []DaySpend
		err := db.NewSelect().Model((*Request)(nil)).
			ColumnExpr("organization_id").
			ColumnExpr("(started_at AT TIME ZONE 'UTC')::date AS day").
			ColumnExpr("SUM(cost_micros) AS cost_micros").
			Where("started_at >= ?", from).
			Where("started_at < ?", to).
			Where("billable").
			Where("organization_id <> ''").
			GroupExpr("1, 2").
			Scan(ctx, &found)
		return found, err
	})
	if err != nil {
		return nil, stack.Wrap(err)
	}
	type key struct {
		organization string
		day          time.Time
	}
	sums := map[key]int64{}
	for _, list := range lists {
		for _, spend := range list {
			sums[key{spend.OrganizationID, spend.Day.UTC()}] += spend.CostMicros
		}
	}
	var all []DaySpend
	for k, cost := range sums {
		all = append(all, DaySpend{OrganizationID: k.organization, Day: k.day, CostMicros: cost})
	}
	slices.SortFunc(all, func(a, b DaySpend) int {
		return cmp.Or(cmp.Compare(a.OrganizationID, b.OrganizationID), a.Day.Compare(b.Day))
	})
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

// Meters sums the requests apps, at least one, made from since on by modality, over every
// shard, in the order of their modality.
func (r *Spend) Meters(ctx context.Context, apps []string, since time.Time) ([]MeterUse, error) {
	lists, err := everyShard(ctx, r.store, func(ctx context.Context, db bun.IDB) ([]MeterUse, error) {
		var used []MeterUse
		err := db.NewSelect().
			TableExpr("requests").
			ColumnExpr("modality").
			ColumnExpr(requestCount+" AS requests").
			ColumnExpr("COALESCE(SUM(input_tokens), 0) AS input_tokens").
			ColumnExpr("COALESCE(SUM(output_tokens), 0) AS output_tokens").
			ColumnExpr("COALESCE(SUM(audio_ms), 0) AS audio_ms").
			ColumnExpr("COALESCE(SUM(characters), 0) AS characters").
			ColumnExpr("COALESCE(SUM(duration_ms), 0)::bigint AS duration_ms").
			ColumnExpr("COALESCE(SUM(cost_micros), 0) AS cost_micros").
			Where("customer_id IN (?)", bun.In(apps)).
			Where("started_at >= ?", since).
			Group("modality").
			Scan(ctx, &used)
		return used, err
	})
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: meter uses: %w", err))
	}
	// Sorted first, so the rows of one modality from every shard are next to each other.
	var all []MeterUse
	for _, list := range lists {
		all = append(all, list...)
	}
	slices.SortStableFunc(all, func(a, b MeterUse) int { return cmp.Compare(a.Modality, b.Modality) })
	var uses []MeterUse
	for _, used := range all {
		if last := len(uses) - 1; last >= 0 && uses[last].Modality == used.Modality {
			held := &uses[last]
			held.Requests += used.Requests
			held.InputTokens += used.InputTokens
			held.OutputTokens += used.OutputTokens
			held.AudioMs += used.AudioMs
			held.Characters += used.Characters
			held.DurationMs += used.DurationMs
			held.CostMicros += used.CostMicros
			continue
		}
		uses = append(uses, used)
	}
	return uses, nil
}
