package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// grantUsesRefused counts what a grant's owner check turned away, so a grant that pays for
// less than it admitted shows why.
var grantUsesRefused = promauto.NewCounter(prometheus.CounterOpts{
	Namespace: "router",
	Name:      "usage_grant_uses_refused_total",
	Help:      "Attempts to count a usage row toward a live grant of neither its organization nor its app, or scoped to another app, that had not counted it before",
})

// GrantUse is one row counted toward one grant: its billable cost, by its usage id.
// CustomerID and OrganizationID are the row's: a grant of neither the row's organization nor
// its app's counts nothing.
type GrantUse struct {
	GrantID        int64
	CustomerID     string
	OrganizationID string
	UsageID        uuid.UUID
	Micros         int64
}

// GrantUsesOf is what the rows written of requests (written names them by usage id) count
// toward their grants: each billable row with a cost, once per grant it names.
func GrantUsesOf(requests []Request, written []uuid.UUID) []GrantUse {
	wrote := make(map[uuid.UUID]bool, len(written))
	for _, id := range written {
		wrote[id] = true
	}
	var uses []GrantUse
	for _, request := range requests {
		if !wrote[request.UsageID] || !request.Billable || request.CostMicros <= 0 {
			continue
		}
		// Only the first of two rows sent with one usage id was written.
		delete(wrote, request.UsageID)
		for _, grant := range slices.Compact(slices.Sorted(slices.Values(request.LimitGrants))) {
			uses = append(uses, GrantUse{GrantID: grant, CustomerID: request.CustomerID, OrganizationID: request.OrganizationID, UsageID: request.UsageID, Micros: request.CostMicros})
		}
	}
	return uses
}

// LimitGrantUses are which request rows counted toward which grant, in the shared database,
// and where the reconcile of each shard's rows got.
type LimitGrantUses struct {
	store *Store
}

const (
	// grantUsesChunk is how many uses one statement counts.
	grantUsesChunk = 1_000
	// GrantUsesKept is how long which rows counted toward a grant is kept once it expired or
	// was revoked; after that it counts nothing more, so what it paid for stays as it was.
	GrantUsesKept = 30 * 24 * time.Hour
	// grantUsesPruned is how many of them one statement of Prune forgets.
	grantUsesPruned = 5_000
)

// grantEndedBefore is the SQL condition a grant g expired or was revoked before ? seconds ago.
const grantEndedBefore = "least(g.revoked_at, g.expires_at) < now() - make_interval(secs => ?)"

// InsertMany counts the rows toward their grants in the shared database: each (grant, usage
// id) once, however often it is sent, added to the grant's used_micros. A grant that is not
// there, is of neither the row's organization nor its app's, is scoped to another app, or ended
// more than GrantUsesKept ago, counts nothing; one refused on whose it is is counted on
// /metrics. It says how many it had not counted before.
func (r *LimitGrantUses) InsertMany(ctx context.Context, uses []GrantUse) (int, error) {
	added := 0
	for start := 0; start < len(uses); start += grantUsesChunk {
		chunk := uses[start:min(start+grantUsesChunk, len(uses))]
		err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
			n, err := addGrantUses(ctx, tx, chunk)
			added += n
			return err
		})
		if err != nil {
			return added, stack.Wrap(fmt.Errorf("store: add grant uses: %w", err))
		}
	}
	return added, nil
}

// addGrantUses is InsertMany within tx. The grants are locked in id order first, so two
// writers counting rows toward the same grants cannot deadlock.
func addGrantUses(ctx context.Context, tx bun.Tx, uses []GrantUse) (int, error) {
	if len(uses) == 0 {
		return 0, nil
	}
	grants := make([]int64, len(uses))
	customers := make([]string, len(uses))
	organizations := make([]string, len(uses))
	usageIDs := make([]string, len(uses))
	micros := make([]int64, len(uses))
	for i, use := range uses {
		grants[i], customers[i], organizations[i], usageIDs[i], micros[i] = use.GrantID, use.CustomerID, use.OrganizationID, use.UsageID.String(), use.Micros
	}
	_, err := tx.NewSelect().Model((*LimitGrant)(nil)).
		Column("lg.id").
		Where("lg.id = ANY(?::bigint[])", pgdialect.Array(slices.Compact(slices.Sorted(slices.Values(grants))))).
		Order("lg.id").
		For("UPDATE").
		Exec(ctx)
	if err != nil {
		return 0, err
	}
	// A grant is the row's organization's, or that of the row's app as the enforcer drew it: the
	// organization the app was last seen under, or for an app in none the app itself. Either
	// one serves, since a row is written without its organization when the app could not be
	// looked up, and the app's membership is recorded in the background after its first grant
	// may be drawn. A grant scoped to an app is that app's alone.
	var counts struct {
		Added   int `bun:"added"`
		Refused int `bun:"refused"`
	}
	err = tx.NewRaw(`
WITH sent AS (
    SELECT * FROM unnest(?::bigint[], ?::text[], ?::text[], ?::uuid[], ?::bigint[]) AS s (grant_id, customer_id, organization_id, usage_id, micros)
), granted AS (
    SELECT s.grant_id, s.usage_id, s.micros,
        g.organization_id IN (s.organization_id, coalesce(ao.organization_id, s.customer_id)) AND g.app_id IN ('', s.customer_id) AS owned
    FROM sent s
    LEFT JOIN app_organizations ao ON ao.app_id = s.customer_id
    JOIN limit_grants g ON g.id = s.grant_id
    WHERE NOT coalesce(`+grantEndedBefore+`, false)
), counted AS (
    INSERT INTO limit_grant_uses (grant_id, usage_id, micros)
    SELECT grant_id, usage_id, micros FROM granted WHERE owned
    ON CONFLICT DO NOTHING
    RETURNING grant_id, micros
), raised AS (
    UPDATE limit_grants g SET used_micros = g.used_micros + c.micros
    FROM (SELECT grant_id, sum(micros) AS micros FROM counted GROUP BY grant_id) c
    WHERE g.id = c.grant_id
    RETURNING 1
)
SELECT (SELECT count(*) FROM counted) AS added,
    (SELECT count(*) FROM granted r WHERE NOT r.owned
        AND NOT EXISTS (SELECT 1 FROM limit_grant_uses u WHERE u.grant_id = r.grant_id AND u.usage_id = r.usage_id)) AS refused`,
		pgdialect.Array(grants), pgdialect.Array(customers), pgdialect.Array(organizations), pgdialect.Array(usageIDs), pgdialect.Array(micros),
		GrantUsesKept.Seconds()).Scan(ctx, &counts)
	if err != nil {
		return 0, err
	}
	grantUsesRefused.Add(float64(counts.Refused))
	return counts.Added, nil
}

// Prune forgets which rows counted toward the grants that expired or were revoked
// more than GrantUsesKept ago, grantUsesPruned at a time until none is left; each grant keeps
// what it paid for. It says how many it forgot.
func (r *LimitGrantUses) Prune(ctx context.Context) (int64, error) {
	var pruned int64
	for {
		result, err := r.store.global.ExecContext(ctx, `
DELETE FROM limit_grant_uses WHERE (grant_id, usage_id) IN (
    SELECT u.grant_id, u.usage_id FROM limit_grants g JOIN limit_grant_uses u ON u.grant_id = g.id
    WHERE `+grantEndedBefore+` LIMIT ?)`, GrantUsesKept.Seconds(), grantUsesPruned)
		if err != nil {
			return pruned, stack.Wrap(fmt.Errorf("store: prune grant uses: %w", err))
		}
		n, err := result.RowsAffected()
		pruned += n
		if err != nil || n < grantUsesPruned {
			return pruned, err
		}
	}
}

// grantUse is one row counted toward a grant (limit_grant_uses).
type grantUse struct {
	bun.BaseModel `bun:"table:limit_grant_uses,alias:lgu"`

	GrantID int64     `bun:"grant_id,pk"`
	UsageID uuid.UUID `bun:"usage_id,pk,type:uuid"`
	Micros  int64     `bun:"micros,notnull"`
}

// grantReconcile is where a shard's reconcile of grant uses got (limit_grant_reconciles).
type grantReconcile struct {
	bun.BaseModel `bun:"table:limit_grant_reconciles,alias:lgr"`

	Shard     string    `bun:"shard,pk"`
	StartedAt time.Time `bun:"started_at,notnull"`
	UpdatedAt time.Time `bun:"updated_at,notnull"`
}

// Count is, of the rows named by usage id, how many grants each counts toward.
func (r *LimitGrantUses) Count(ctx context.Context, usageIDs []uuid.UUID) (map[uuid.UUID]int, error) {
	counted := map[uuid.UUID]int{}
	if len(usageIDs) == 0 {
		return counted, nil
	}
	ids := make([]string, len(usageIDs))
	for i, id := range usageIDs {
		ids[i] = id.String()
	}
	var rows []struct {
		UsageID uuid.UUID `bun:"usage_id"`
		Grants  int       `bun:"grants"`
	}
	err := r.store.global.NewSelect().Model((*grantUse)(nil)).
		Column("usage_id").
		ColumnExpr("count(*) AS grants").
		Where("usage_id = ANY(?::uuid[])", pgdialect.Array(ids)).
		Group("usage_id").
		Scan(ctx, &rows)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: grant uses counted: %w", err))
	}
	for _, row := range rows {
		counted[row.UsageID] = row.Grants
	}
	return counted, nil
}

const (
	// GrantReconcileOverlap is how far before where it last got the reconcile reads a shard
	// again: a row written at most this long after it started is found. One written later,
	// whose count its writer dropped too, is not.
	GrantReconcileOverlap = time.Hour
	// grantReconcilePage is how many rows one read of a shard takes; where the reconcile got
	// is kept after each, so one cut short goes on from there.
	grantReconcilePage = 2_000
)

// ErrReconcileBusy is LimitGrantUses.Reconcile finding another router reconciling.
var ErrReconcileBusy = errors.New("store: another router is reconciling grant uses")

// Reconcile counts toward their grants the billable rows naming one, in every shard,
// that are not counted yet: a row whose writer wrote it to its shard and dropped, or stopped
// before, counting it in the shared database. Each shard is read on from where the last
// reconcile of it got (limit_grant_reconciles), less GrantReconcileOverlap, in started_at
// order (requests_limit_grants_started_idx). One router reconciles at a time, under an advisory lock
// in the shared database: another finds ErrReconcileBusy. With one database the writer counts
// every row as it writes it, so there is nothing to do. It says how many it counted.
func (r *LimitGrantUses) Reconcile(ctx context.Context) (int, error) {
	if !r.store.Sharded() {
		return 0, nil
	}
	tx, err := r.store.global.BeginTx(ctx, nil)
	if err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: reconcile grant uses: %w", err))
	}
	defer tx.Rollback() //nolint:errcheck // it writes nothing; ending it lets go of the lock
	var locked bool
	if err := tx.QueryRowContext(ctx, "SELECT pg_try_advisory_xact_lock(hashtextextended('limit_grant_reconciles', 0))").Scan(&locked); err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: reconcile grant uses: lock: %w", err))
	}
	if !locked {
		return 0, ErrReconcileBusy
	}
	now := time.Now().UTC()
	shards := r.store.shards()
	counted := make([]int, len(shards))
	failed := make([]error, len(shards))
	var wg sync.WaitGroup
	for i, shard := range shards {
		wg.Go(func() { counted[i], failed[i] = r.reconcileShard(ctx, shard, r.store.shardNames[i], now) })
	}
	wg.Wait()
	total := 0
	for _, n := range counted {
		total += n
	}
	if err := errors.Join(failed...); err != nil {
		return total, stack.Wrap(fmt.Errorf("store: reconcile grant uses: %w", err))
	}
	return total, nil
}

// reconcileGrantsQuery is one page of a shard's rows naming a grant, on from (started_at, id),
// read by requests_limit_grants_started_idx.
const reconcileGrantsQuery = "SELECT id, started_at, customer_id, organization_id, usage_id, cost_micros, limit_grants FROM requests " +
	"WHERE limit_grants <> '{}' AND (started_at, id) > (?, ?) AND billable AND cost_micros > 0 AND usage_id IS NOT NULL " +
	"ORDER BY started_at, id LIMIT ?"

// reconcileShard is Reconcile for one shard, read up to now.
func (r *LimitGrantUses) reconcileShard(ctx context.Context, shard bun.IDB, name string, now time.Time) (int, error) {
	var mark time.Time
	err := r.store.global.NewSelect().Model((*grantReconcile)(nil)).Column("started_at").Where("shard = ?", name).Scan(ctx, &mark)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("shard %s: %w", name, err)
	}
	var after time.Time
	var afterID int64
	if !mark.IsZero() {
		after = mark.Add(-GrantReconcileOverlap)
	}
	counted := 0
	for {
		var rows []struct {
			ID             int64     `bun:"id"`
			StartedAt      time.Time `bun:"started_at"`
			CustomerID     string    `bun:"customer_id"`
			OrganizationID string    `bun:"organization_id"`
			UsageID        uuid.UUID `bun:"usage_id"`
			CostMicros     int64     `bun:"cost_micros"`
			LimitGrants    []int64   `bun:"limit_grants,array"`
		}
		err := shard.NewRaw(reconcileGrantsQuery, after, afterID, grantReconcilePage).Scan(ctx, &rows)
		if err != nil {
			return counted, fmt.Errorf("shard %s: %w", name, err)
		}
		var uses []GrantUse
		for _, row := range rows {
			for _, grant := range slices.Compact(slices.Sorted(slices.Values(row.LimitGrants))) {
				uses = append(uses, GrantUse{GrantID: grant, CustomerID: row.CustomerID, OrganizationID: row.OrganizationID, UsageID: row.UsageID, Micros: row.CostMicros})
			}
		}
		n, err := r.InsertMany(ctx, uses)
		counted += n
		if err != nil {
			return counted, fmt.Errorf("shard %s: %w", name, err)
		}
		reached := now
		if len(rows) == grantReconcilePage {
			last := rows[len(rows)-1]
			after, afterID = last.StartedAt, last.ID
			if last.StartedAt.Before(now) {
				reached = last.StartedAt
			}
		}
		_, err = r.store.global.NewInsert().Model(&grantReconcile{Shard: name, StartedAt: reached}).
			Column("shard", "started_at").
			On("CONFLICT (shard) DO UPDATE").
			Set("started_at = GREATEST(lgr.started_at, EXCLUDED.started_at)").
			Set("updated_at = now()").
			Exec(ctx)
		if err != nil {
			return counted, fmt.Errorf("shard %s: keep where it got: %w", name, err)
		}
		if len(rows) < grantReconcilePage {
			return counted, nil
		}
	}
}
