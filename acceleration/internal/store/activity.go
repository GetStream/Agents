package store

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/schema"

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

// activityBatch is how many rows one statement writes. Each takes 12 parameters, and a
// statement may carry at most 65535 (PostgreSQL's Bind message counts them in an Int16:
// https://www.postgresql.org/docs/current/protocol-message-formats.html), so a thousand
// leave room to spare.
const activityBatch = 1000

// Add adds deltas to the rows they name, making the ones that are not there yet.
// A delta naming no organization, such as a hook's or a forward's, which name only the app, is
// the organization app_organizations holds for its app, when it holds one. Counts are added,
// the first and last times widen, and a refusal later than the one the row holds replaces it.
// Deltas naming the same row are summed first: one statement cannot update a row twice.
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

	OrganizationID  activityOrganization `bun:"organization_id,notnull"`
	AppID           string               `bun:"app_id,pk"`
	Area            string               `bun:"area,pk"`
	Client          string               `bun:"client,pk"`
	Day             string               `bun:"day,pk"`
	Requests        int64                `bun:"requests,notnull"`
	Refused         int64                `bun:"refused,notnull"`
	LastRefusalCode string               `bun:"last_refusal_code,nullzero"`
	LastRefusalAt   time.Time            `bun:"last_refusal_at,nullzero"`
	FirstAt         time.Time            `bun:"first_at,notnull"`
	LastAt          time.Time            `bun:"last_at,notnull"`
}

// activityOrganization is the organization a row is written with: the one its delta names, or,
// when it names none, the one app_organizations holds for its app, or none.
type activityOrganization struct {
	named, appID string
}

// AppendQuery writes o as the SQL expression that picks it, so bun puts it in the row's VALUES.
func (o activityOrganization) AppendQuery(gen schema.QueryGen, b []byte) ([]byte, error) {
	return gen.AppendQuery(b, "COALESCE(NULLIF(?, ''), (SELECT organization_id FROM app_organizations WHERE app_id = ?), '')",
		o.named, o.appID), nil
}

// writeActivity upserts the rows of keys in one statement.
func writeActivity(ctx context.Context, tx bun.Tx, folded map[activityKey]*ActivityDelta, keys []activityKey) error {
	rows := make([]activityUpsert, 0, len(keys))
	for _, key := range keys {
		delta := folded[key]
		rows = append(rows, activityUpsert{
			OrganizationID: activityOrganization{delta.OrganizationID, key.appID},
			AppID:          key.appID, Area: key.area, Client: key.client, Day: key.day,
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

// ActivityAreaDay is the first UTC day some apps had a request answered, not refused, in an area.
type ActivityAreaDay struct {
	Area string    `bun:"area"`
	Day  time.Time `bun:"day"`
}

// FirstAnswered is, by area, the first UTC day apps, at least one, had a request answered,
// not refused, over every day recorded. An area with no request answered is absent.
func (r *Activities) FirstAnswered(ctx context.Context, apps []string) ([]ActivityAreaDay, error) {
	var days []ActivityAreaDay
	err := r.store.global.NewSelect().
		TableExpr("customer_activity").
		ColumnExpr("area, MIN(day) AS day").
		Where("app_id IN (?)", bun.In(apps)).
		Where("requests > refused").
		Group("area").
		Order("area").
		Scan(ctx, &days)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: activity first answered: %w", err))
	}
	return days, nil
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

// OrganizationDayArea is what one organization's apps did in one area of the API from one
// client on one UTC day.
type OrganizationDayArea struct {
	OrganizationID string `bun:"organization_id"`
	// FirstDay says the organization has no activity on any earlier day.
	FirstDay bool `bun:"first_day"`
	ActivityArea
}

// Day is what every organization's apps did on day, a UTC day, by organization, area
// and client in that order. Rows that name no organization are left out: they are an app's
// whose organization app_organizations did not hold when the row was written.
func (r *Activities) Day(ctx context.Context, day time.Time) ([]OrganizationDayArea, error) {
	date := day.UTC().Format(time.DateOnly)
	var areas []OrganizationDayArea
	err := r.store.global.NewSelect().
		TableExpr("customer_activity AS ca").
		ColumnExpr("ca.organization_id, ca.area, ca.client, MIN(ca.first_at) AS first_at, MAX(ca.last_at) AS last_at").
		ColumnExpr("SUM(ca.requests) AS requests, SUM(ca.refused) AS refused").
		ColumnExpr("COALESCE((array_agg(ca.last_refusal_code ORDER BY ca.last_refusal_at DESC)"+
			" FILTER (WHERE ca.last_refusal_at IS NOT NULL))[1], '') AS last_refusal_code").
		ColumnExpr("NOT EXISTS (SELECT 1 FROM customer_activity AS earlier"+
			" WHERE earlier.organization_id = ca.organization_id AND earlier.day < ?::date) AS first_day", date).
		Where("ca.day = ?::date", date).
		Where("ca.organization_id <> ''").
		Group("ca.organization_id", "ca.area", "ca.client").
		Order("ca.organization_id", "ca.area", "ca.client").
		Scan(ctx, &areas)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: day activity: %w", err))
	}
	return areas, nil
}

// ActivityRow is one customer_activity row: what one app of an organization did in one area
// from one client on one UTC day.
type ActivityRow struct {
	OrganizationID string    `bun:"organization_id"`
	AppID          string    `bun:"app_id"`
	Day            time.Time `bun:"day"`
	ActivityArea
}

// ActivityPosition is where a page of List ended: its last row's whole sort key, which ends
// with the row's primary key, so no two rows share one.
type ActivityPosition struct {
	OrganizationID string `json:"o"`
	Day            string `json:"d"`
	AppID          string `json:"a"`
	Area           string `json:"r"`
	Client         string `json:"c"`
}

// List is up to limit rows of the UTC days from from to to, both included, after the position
// after when it is not nil, by organization, day, app, area and client. Rows that name no
// organization are left out, as in Day.
func (r *Activities) List(ctx context.Context, from, to time.Time, after *ActivityPosition, limit int) ([]ActivityRow, error) {
	query := r.store.global.NewSelect().
		TableExpr("customer_activity").
		ColumnExpr("organization_id, app_id, day, area, client, first_at, last_at, requests, refused").
		ColumnExpr("COALESCE(last_refusal_code, '') AS last_refusal_code").
		Where("day BETWEEN ?::date AND ?::date", from.UTC().Format(time.DateOnly), to.UTC().Format(time.DateOnly)).
		Where("organization_id <> ''")
	if after != nil {
		query = query.Where("(organization_id, day, app_id, area, client) > (?, ?::date, ?, ?, ?)",
			after.OrganizationID, after.Day, after.AppID, after.Area, after.Client)
	}
	var rows []ActivityRow
	err := query.Order("organization_id", "day", "app_id", "area", "client").Limit(limit).Scan(ctx, &rows)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: list activity: %w", err))
	}
	return rows, nil
}

// OrganizationArea is what one organization's apps did in one area of the API, every client
// summed: the requests and refusals of the days read, and the first UTC day ever with a
// request answered, nil when none was.
type OrganizationArea struct {
	OrganizationID string     `bun:"organization_id"`
	Area           string     `bun:"area"`
	Requests       int64      `bun:"requests"`
	Refused        int64      `bun:"refused"`
	FirstAnswered  *time.Time `bun:"first_answered"`
}

// OrganizationAreas is, for each of organizations and each area its apps ever made a request
// in, what Areas and FirstAnswered read for an app list: the counts of the UTC days from from
// to to, both included, and the first day answered over every day recorded. By organization,
// then area.
func (r *Activities) OrganizationAreas(ctx context.Context, organizations []string, from, to time.Time) ([]OrganizationArea, error) {
	inRange := bun.SafeQuery("day BETWEEN ?::date AND ?::date", from.UTC().Format(time.DateOnly), to.UTC().Format(time.DateOnly))
	var areas []OrganizationArea
	err := r.store.global.NewSelect().
		TableExpr("customer_activity").
		ColumnExpr("organization_id, area").
		ColumnExpr("COALESCE(SUM(requests) FILTER (WHERE ?), 0) AS requests", inRange).
		ColumnExpr("COALESCE(SUM(refused) FILTER (WHERE ?), 0) AS refused", inRange).
		ColumnExpr("MIN(day) FILTER (WHERE requests > refused) AS first_answered").
		Where("organization_id IN (?)", bun.In(organizations)).
		Group("organization_id", "area").
		Order("organization_id", "area").
		Scan(ctx, &areas)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: organization activity areas: %w", err))
	}
	return areas, nil
}

// OrganizationDays is, for each of organizations, how many UTC days from from to to, both
// included, its apps had a request answered, not refused, in any of areas, as Days counts them
// for an app list. An organization with none is absent.
func (r *Activities) OrganizationDays(ctx context.Context, organizations []string, from, to time.Time, areas []string) (map[string]int, error) {
	var counted []struct {
		OrganizationID string `bun:"organization_id"`
		Days           int    `bun:"days"`
	}
	err := r.store.global.NewSelect().
		TableExpr("customer_activity").
		ColumnExpr("organization_id, COUNT(DISTINCT day) AS days").
		Where("organization_id IN (?)", bun.In(organizations)).
		Where("day BETWEEN ?::date AND ?::date", from.UTC().Format(time.DateOnly), to.UTC().Format(time.DateOnly)).
		Where("area IN (?)", bun.In(areas)).
		Where("requests > refused").
		Group("organization_id").
		Scan(ctx, &counted)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: organization activity days: %w", err))
	}
	days := make(map[string]int, len(counted))
	for _, row := range counted {
		days[row.OrganizationID] = row.Days
	}
	return days, nil
}

// FirstAnswer is an organization whose apps had a request of an area answered for the first
// time on a day.
type FirstAnswer struct {
	OrganizationID string `bun:"organization_id"`
	// Days is how many days before that day the organization's apps made their first request,
	// of any area, refused ones included.
	Days int `bun:"days"`
}

// FirstAnswers are the organizations whose apps had a request of area answered, not refused,
// on day, a UTC day, and on no earlier day, by organization. Rows that name no organization
// are left out, as in Day.
func (r *Activities) FirstAnswers(ctx context.Context, area string, day time.Time) ([]FirstAnswer, error) {
	date := day.UTC().Format(time.DateOnly)
	var answers []FirstAnswer
	err := r.store.global.NewSelect().
		TableExpr("customer_activity AS ca").
		ColumnExpr("ca.organization_id").
		ColumnExpr("?::date - (SELECT MIN(any_area.day) FROM customer_activity AS any_area"+
			" WHERE any_area.organization_id = ca.organization_id) AS days", date).
		Where("ca.day = ?::date", date).
		Where("ca.area = ?", area).
		Where("ca.requests > ca.refused").
		Where("ca.organization_id <> ''").
		Where("NOT EXISTS (SELECT 1 FROM customer_activity AS earlier"+
			" WHERE earlier.organization_id = ca.organization_id AND earlier.area = ca.area"+
			" AND earlier.day < ca.day AND earlier.requests > earlier.refused)").
		Group("ca.organization_id").
		Order("ca.organization_id").
		Scan(ctx, &answers)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: first answers: %w", err))
	}
	return answers, nil
}

// ActiveOrganizations is how many organizations' apps made a request on day, a UTC day,
// refused ones included.
func (r *Activities) ActiveOrganizations(ctx context.Context, day time.Time) (int, error) {
	var active int
	err := r.store.global.NewSelect().
		TableExpr("customer_activity").
		ColumnExpr("COUNT(DISTINCT organization_id)").
		Where("day = ?::date", day.UTC().Format(time.DateOnly)).
		Where("organization_id <> ''").
		Scan(ctx, &active)
	if err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: active organizations: %w", err))
	}
	return active, nil
}

// ActivityKeepDays is how many UTC days back from today customer_activity and activity_digests
// keep rows for: 13 months and a margin, so staff can compare a month with the same month a year
// before. Rows are kept no longer than that need, as GDPR's storage limitation principle asks
// (Art. 5(1)(e), https://eur-lex.europa.eu/eli/reg/2016/679/oj).
const ActivityKeepDays = 400

// Prune deletes the activity rows of the UTC days more than ActivityKeepDays before now's, and
// returns how many it deleted.
func (r *Activities) Prune(ctx context.Context, now time.Time) (int, error) {
	return pruneDays(ctx, r.store.global, "customer_activity", now)
}

// pruneDays deletes the rows of table whose day is more than ActivityKeepDays before now's UTC
// day, and returns how many it deleted.
func pruneDays(ctx context.Context, db bun.IDB, table string, now time.Time) (int, error) {
	before := now.UTC().AddDate(0, 0, -ActivityKeepDays).Format(time.DateOnly)
	result, err := db.NewDelete().TableExpr(table).Where("day < ?::date", before).Exec(ctx)
	if err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: prune %s: %w", table, err))
	}
	deleted, _ := result.RowsAffected()
	return int(deleted), nil
}
