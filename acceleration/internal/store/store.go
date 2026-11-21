// Package store persists request rows in Postgres and aggregates them into hourly and
// daily rollups. Uptime, latency percentiles, billable usage and cost all come from the
// same request rows, so there is no separate health-probe pipeline to keep in sync.
//
// Rows from every modality share one table, distinguished by a modality column, because
// every question worth asking about usage is asked the same way whatever the modality is.
package store

import (
	"cmp"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/rueidis"
	"github.com/redis/rueidis/rueidisaside"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"
	"github.com/uptrace/bun/extra/bunotel"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// maxConns is how many Postgres connections one router holds at most.
const maxConns = 40

// Store reads and writes router statistics.
type Store struct {
	// repos are the repositories, one per model, each holding the handle its table lives in.
	repos
	// global is the database every shard shares, and shard this one's, which is global for a
	// deployment of one shard. peers are the other shards' (see shards.go).
	global *bun.DB
	shard  *bun.DB
	peers  []*bun.DB
	// cache is the shared Redis, holding copies of rows read on every request, and redis the
	// shard's, holding the state of its running calls. Both are nil without Redis.
	cache rueidisaside.CacheAsideClient
	redis rueidis.Client
	// shardNames names each of shards(), as Options.Shards does, for what is kept per shard
	// in the shared database.
	shardNames []string
	// globalIsShard says the shared database is also some shard's own, so its calls are not
	// strays to drop.
	globalIsShard bool
	LogDrops      atomic.Int64
	// shapes is what each table an export carries looks like, read from the catalogue
	// once rather than kept in a list here that a migration could leave behind.
	shapes tableShapes
	// pins says how Stream app pins cross between deployments, once the router has said.
	pins pinsHolder
	// deliveriesPruned is when hook deliveries were last forgotten, in Unix nanoseconds.
	deliveriesPruned atomic.Int64
}

// Open connects to Postgres using a pgdriver DSN, for example
// postgres://user:pass@localhost:5432/router?sslmode=disable.
func Open(dsn string) (*Store, error) {
	return OpenShards(Options{DSN: dsn})
}

// connect opens a pool of at most conns connections.
func connect(dsn string, conns int) *bun.DB {
	sqldb := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(dsn)))
	// database/sql keeps two idle connections by default, so every burst of writes past two
	// dials Postgres and shakes hands over TLS again, and under load those handshakes are
	// what times out. The pool is kept instead, and bounded so that two pods, each running
	// overlapping generations, stay well inside the server's connection limit.
	sqldb.SetMaxOpenConns(conns)
	sqldb.SetMaxIdleConns(conns)
	sqldb.SetConnMaxIdleTime(5 * time.Minute)
	// A release rolled back past a migration that added a column still reads the row.
	db := bun.NewDB(sqldb, pgdialect.New(), bun.WithDiscardUnknownColumns())
	// A span per query, so a request's trace says which reads it waited on. Queries are
	// recorded unformatted: the arguments of these are customer ids and api keys.
	db.AddQueryHook(bunotel.NewQueryHook(bunotel.WithDBName("router")))
	return db
}

// Server names the shared database and the address its server answers on, as it sees itself.
// The address is empty over a unix socket.
func (s *Store) Server(ctx context.Context) (database, address string, err error) {
	var server sql.NullString
	err = s.global.QueryRowContext(ctx,
		"SELECT current_database(), host(inet_server_addr()) || ':' || inet_server_port()").Scan(&database, &server)
	if err != nil {
		return "", "", stack.Wrap(fmt.Errorf("store: server: %w", err))
	}
	return database, server.String, nil
}

// Close releases the connection pools.
func (s *Store) Close() error {
	s.closeRedis()
	return errors.Join(s.closeShards(), s.global.Close())
}

// Ping verifies the shared database and this shard's are usable. Another shard being down
// is not this router failing: it only leaves that shard's calls off a page.
func (s *Store) Ping(ctx context.Context) error {
	if err := s.global.PingContext(ctx); err != nil {
		return stack.Wrap(err)
	}
	return stack.Wrap(s.shard.PingContext(ctx))
}

// OrganizationApps is every app of an organization: those requests last named it for, those
// registered under it that no request named another for, and the app whose id is the
// organization's, for an app that is its own.
func (s *Store) OrganizationApps(ctx context.Context, organizationID string) ([]string, error) {
	// One statement, so an app a request records under the organization meanwhile is in one
	// half or the other.
	namedOther := s.global.NewSelect().Model((*AppOrganization)(nil)).
		ColumnExpr("1").
		Where("ao.app_id = ap.id AND ao.organization_id <> ?", organizationID)
	named := s.global.NewSelect().Model((*AppOrganization)(nil)).Column("app_id").Where("organization_id = ?", organizationID).
		Union(s.global.NewSelect().Model((*App)(nil)).
			Column("id").
			Where("ap.organization_id = ?", organizationID).
			Where("NOT EXISTS (?)", namedOther))
	var members []string
	err := s.global.NewSelect().TableExpr("(?) AS member", named).Column("member.app_id").OrderExpr("1").Scan(ctx, &members)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: organization apps: %w", err))
	}
	apps := []string{organizationID}
	for _, app := range members {
		if app != organizationID {
			apps = append(apps, app)
		}
	}
	return apps, nil
}

// OrganizationActivityApps is OrganizationApps for a read of what an organization did: the
// app whose id is the organization's counts only when the organization is the one the latest
// request named it for, or, when none did, the one it is registered under, so an app of
// another organization with that id is not told as this one's.
func (s *Store) OrganizationActivityApps(ctx context.Context, organizationID string) ([]string, error) {
	apps, err := s.OrganizationApps(ctx, organizationID)
	if err != nil {
		return nil, err
	}
	var own bool
	err = s.global.NewRaw(`SELECT EXISTS (SELECT 1 FROM app_organizations WHERE app_id = ?0 AND organization_id = ?0)
  OR (NOT EXISTS (SELECT 1 FROM app_organizations WHERE app_id = ?0)
      AND NOT EXISTS (SELECT 1 FROM apps WHERE id = ?0 AND organization_id <> ?0))`, organizationID).Scan(ctx, &own)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: organization activity apps: %w", err))
	}
	if !own {
		// OrganizationApps puts the organization's own id first.
		return apps[1:], nil
	}
	return apps, nil
}

// CustomerActivity returns how much one customer's agents were used per bucket, oldest
// first, and by how many distinct people.
//
// A bucket with nothing in it is left out rather than returned as zeroes, the same way the
// stats paths leave out a bucket nobody used.
func (s *Store) CustomerActivity(
	ctx context.Context,
	customerID string,
	granularity ActivityGranularity,
	from, to time.Time,
) ([]ActivityBucket, error) {
	if !granularity.Valid() {
		return nil, stack.Wrap(fmt.Errorf("store: unknown activity granularity %q", granularity))
	}
	if customerID == "" {
		return nil, stack.Wrap(errors.New("store: customer id is required"))
	}

	unit := granularity.truncateUnit()
	// Who was seen is read apart from the counts: a person is one person however many shards
	// they talked to in a bucket, and a guest who signed up is the account they became, which
	// the shared database knows rather than the shard.
	people := fmt.Sprintf(`
SELECT DISTINCT bucket, user_id FROM (
    SELECT date_trunc('%[1]s', s.created_at) AS bucket, s.user_id, s.caller_kind
    FROM agent_sessions AS s
    WHERE s.customer_id = ? AND s.created_at >= ? AND s.created_at < ?
    UNION ALL
    SELECT date_trunc('%[1]s', a.created_at) AS bucket, s.user_id, s.caller_kind
    FROM agent_responses AS a
    JOIN agent_sessions AS s ON s.id = a.session_id
    WHERE a.customer_id = ? AND a.created_at >= ? AND a.created_at < ?
) AS seen
WHERE user_id <> '' AND caller_kind <> 'anonymous'`, unit)
	query := fmt.Sprintf(`
WITH session_counts AS (
    SELECT date_trunc('%[1]s', created_at) AS bucket, COUNT(*) AS n
    FROM agent_sessions
    WHERE customer_id = ? AND created_at >= ? AND created_at < ?
    GROUP BY bucket
),
message_counts AS (
    SELECT date_trunc('%[1]s', created_at) AS bucket, COUNT(*) AS n
    FROM agent_responses
    WHERE customer_id = ? AND created_at >= ? AND created_at < ?
    GROUP BY bucket
),
call_counts AS (
    SELECT
        date_trunc('%[1]s', started_at) AS bucket,
        COUNT(*) AS n,
        SUM(EXTRACT(EPOCH FROM (COALESCE(ended_at, now()) - started_at)))::double precision / 60
            AS voice_minutes,
        COALESCE(SUM(EXTRACT(EPOCH FROM (COALESCE(ended_at, now()) - started_at)))
            FILTER (WHERE from_number IS NOT NULL OR to_number IS NOT NULL), 0)::double precision / 60
            AS phone_minutes
    FROM calls
    WHERE customer_id = ? AND started_at >= ? AND started_at < ?
    GROUP BY bucket
),
buckets AS (
    SELECT bucket FROM session_counts
    UNION SELECT bucket FROM message_counts
    UNION SELECT bucket FROM call_counts
)
SELECT
    b.bucket AS bucket,
    COALESCE(s.n, 0) AS sessions,
    COALESCE(m.n, 0) AS messages,
    COALESCE(c.n, 0) AS calls,
    COALESCE(c.voice_minutes, 0) AS voice_minutes,
    COALESCE(c.phone_minutes, 0) AS phone_minutes
FROM buckets AS b
LEFT JOIN session_counts AS s ON s.bucket = b.bucket
LEFT JOIN message_counts AS m ON m.bucket = b.bucket
LEFT JOIN call_counts AS c ON c.bucket = b.bucket`, unit)

	type seenUser struct {
		Bucket time.Time `bun:"bucket"`
		UserID string    `bun:"user_id"`
	}
	type shardActivity struct {
		counts []ActivityBucket
		seen   []seenUser
	}
	answers, err := everyShard(ctx, s, func(ctx context.Context, db bun.IDB) (shardActivity, error) {
		var read shardActivity
		if err := db.NewRaw(query,
			customerID, from, to,
			customerID, from, to,
			customerID, from, to,
		).Scan(ctx, &read.counts); err != nil {
			return read, err
		}
		err := db.NewRaw(people, customerID, from, to, customerID, from, to).Scan(ctx, &read.seen)
		return read, err
	})
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: customer activity: %w", err))
	}

	var seen []seenUser
	ids := map[string]bool{}
	for _, answer := range answers {
		for _, user := range answer.seen {
			seen = append(seen, user)
			ids[user.UserID] = true
		}
	}
	claimed := map[string]string{}
	if len(ids) > 0 {
		var guests []User
		err := s.global.NewSelect().Model(&guests).Column("id", "claimed_by").
			Where("customer_id = ?", customerID).
			Where("kind = ?", UserKindGuest).
			Where("claimed_by IS NOT NULL AND claimed_by <> ''").
			Where("id IN (?)", bun.In(slices.Collect(maps.Keys(ids)))).
			Scan(ctx)
		if err != nil {
			return nil, stack.Wrap(fmt.Errorf("store: customer activity: claimed guests: %w", err))
		}
		for _, guest := range guests {
			claimed[guest.ID] = guest.ClaimedBy
		}
	}

	byBucket := map[int64]*ActivityBucket{}
	at := func(bucket time.Time) *ActivityBucket {
		key := bucket.Unix()
		if byBucket[key] == nil {
			byBucket[key] = &ActivityBucket{Bucket: bucket}
		}
		return byBucket[key]
	}
	for _, answer := range answers {
		for _, counted := range answer.counts {
			b := at(counted.Bucket)
			b.Sessions += counted.Sessions
			b.Messages += counted.Messages
			b.Calls += counted.Calls
			b.VoiceMinutes += counted.VoiceMinutes
			b.PhoneMinutes += counted.PhoneMinutes
		}
	}
	type person struct {
		bucket int64
		id     string
	}
	counted := map[person]bool{}
	for _, user := range seen {
		id := cmp.Or(claimed[user.UserID], user.UserID)
		if key := (person{user.Bucket.Unix(), id}); !counted[key] {
			counted[key] = true
			at(user.Bucket).ActiveUsers++
		}
	}

	var buckets []ActivityBucket
	for _, key := range slices.Sorted(maps.Keys(byBucket)) {
		buckets = append(buckets, *byBucket[key])
	}
	return buckets, nil
}

// CustomerConcurrencyPeak returns the most sessions one customer had open at the same time
// in a window. A session still open counts up to now, and one that closed at the instant
// another opened did not overlap it.
func (s *Store) CustomerConcurrencyPeak(ctx context.Context, customerID string, from, to time.Time) (ConcurrencyPeak, error) {
	if customerID == "" {
		return ConcurrencyPeak{}, stack.Wrap(errors.New("store: customer id is required"))
	}

	const spans = `
SELECT GREATEST(created_at, ?0) AS opened, LEAST(COALESCE(closed_at, now()), ?1) AS closed
FROM agent_sessions
WHERE customer_id = ?2 AND created_at < ?1 AND (closed_at IS NULL OR closed_at > ?0)`
	if len(s.peers) > 0 {
		return s.peakAcrossShards(ctx, spans, customerID, from, to)
	}

	var peaks []ConcurrencyPeak
	err := s.shard.NewRaw(`
WITH spans AS (`+spans+`
),
edges AS (
    SELECT opened AS at, 1 AS delta FROM spans WHERE closed > opened
    UNION ALL
    SELECT closed AS at, -1 AS delta FROM spans WHERE closed > opened
),
running AS (
    SELECT at, SUM(delta) OVER (ORDER BY at, delta ROWS UNBOUNDED PRECEDING) AS sessions
    FROM edges
)
SELECT sessions, at FROM running ORDER BY sessions DESC, at ASC LIMIT 1`,
		from, to, customerID,
	).Scan(ctx, &peaks)
	if err != nil {
		return ConcurrencyPeak{}, stack.Wrap(fmt.Errorf("store: concurrency peak: %w", err))
	}
	if len(peaks) == 0 || peaks[0].Sessions <= 0 {
		return ConcurrencyPeak{}, nil
	}
	return peaks[0], nil
}

// peakAcrossShards is CustomerConcurrencyPeak over sessions open in several shards at once,
// which no one shard's query can see: it reads every shard's spans and walks their edges here,
// in the order the query does.
func (s *Store) peakAcrossShards(ctx context.Context, spans, customerID string, from, to time.Time) (ConcurrencyPeak, error) {
	type span struct {
		Opened time.Time `bun:"opened"`
		Closed time.Time `bun:"closed"`
	}
	lists, err := everyShard(ctx, s, func(ctx context.Context, db bun.IDB) ([]span, error) {
		var read []span
		err := db.NewRaw(spans, from, to, customerID).Scan(ctx, &read)
		return read, err
	})
	if err != nil {
		return ConcurrencyPeak{}, stack.Wrap(fmt.Errorf("store: concurrency peak: %w", err))
	}
	type edge struct {
		at    time.Time
		delta int64
	}
	var edges []edge
	for _, list := range lists {
		for _, open := range list {
			if open.Closed.After(open.Opened) {
				edges = append(edges, edge{open.Opened, 1}, edge{open.Closed, -1})
			}
		}
	}
	slices.SortFunc(edges, func(a, b edge) int {
		return cmp.Or(a.at.Compare(b.at), cmp.Compare(a.delta, b.delta))
	})
	var peak ConcurrencyPeak
	var running int64
	for _, e := range edges {
		running += e.delta
		if running > peak.Sessions {
			at := e.at
			peak = ConcurrencyPeak{Sessions: running, At: &at}
		}
	}
	return peak, nil
}

// CustomerToolUsage returns how every tool one customer's agents ran in a window was used,
// most called first, and the same figures over all of them. A configID keeps to the
// sessions of that agent config, and empty is every one. A web search the agent made has no
// result item, so it is not a tool call here.
//
// A turn is a top-level response that was not rewound, and a call made in a delegated task
// belongs to the turn that delegated it.
func (s *Store) CustomerToolUsage(ctx context.Context, customerID, configID string, from, to time.Time) (ToolUsage, []ToolUsage, error) {
	if customerID == "" {
		return ToolUsage{}, nil, stack.Wrap(errors.New("store: customer id is required"))
	}

	type usageRow struct {
		Tool              string     `bun:"tool"`
		Overall           bool       `bun:"overall"`
		Calls             int64      `bun:"calls"`
		Failed            int64      `bun:"failed"`
		Sessions          int64      `bun:"sessions"`
		Firsts            int64      `bun:"firsts"`
		AvgTurnsBeforeUse *float64   `bun:"avg_turns_before_use"`
		AvgTokens         float64    `bun:"avg_tokens"`
		AvgMs             *float64   `bun:"avg_ms"`
		P99Ms             *float64   `bun:"p99_ms"`
		OfferedAt         *time.Time `bun:"offered_at"`
		Loading           string     `bun:"loading"`
		DefinitionTokens  *int64     `bun:"definition_tokens"`
		SummaryTokens     *int64     `bun:"summary_tokens"`
	}
	lists, err := everyShard(ctx, s, func(ctx context.Context, db bun.IDB) ([]usageRow, error) {
		var rows []usageRow
		err := db.NewRaw(`
WITH turns AS (
    SELECT
        a.id,
        ROW_NUMBER() OVER (PARTITION BY a.session_id ORDER BY a.created_at, a.id) - 1 AS before
    FROM agent_responses AS a
    JOIN agent_sessions AS s ON s.id = a.session_id
    WHERE a.customer_id = ?3 AND (?6 = '' OR s.config_id = ?6) AND a.created_at < ?5
        AND a.parent_response_id IS NULL AND a.rewound_at IS NULL
),
ran AS (
    SELECT
        r.tool_name AS tool,
        a.session_id,
        t.before,
        r.at,
        r.payload->>'error' IS NOT NULL AS failed,
        COALESCE((r.payload->>'tokens')::double precision,
            (octet_length(r.tool_name) + octet_length(COALESCE(r.payload->>'arguments', ''))
                + octet_length(r.text) + ?0 - 1) / ?0) AS tokens,
        EXTRACT(EPOCH FROM (r.at - started.at)) * 1000 AS ms,
        started.payload->>'loading' AS loading,
        (started.payload->>'definition_tokens')::bigint AS definition_tokens,
        (started.payload->>'summary_tokens')::bigint AS summary_tokens
    FROM agent_responses AS a
    JOIN agent_sessions AS s ON s.id = a.session_id
    JOIN agent_response_items AS r ON r.response_id = a.id AND r.kind = ?1
    LEFT JOIN turns AS t ON t.id = COALESCE(a.parent_response_id, a.id)
    LEFT JOIN LATERAL (
        SELECT c.at, c.payload FROM agent_response_items AS c
        WHERE c.response_id = r.response_id AND c.kind = ?2 AND c.tool_name = r.tool_name
            AND c.ordinal < r.ordinal
            AND COALESCE(c.payload->>'call_id', '') = COALESCE(r.payload->>'call_id', '')
        ORDER BY c.ordinal DESC LIMIT 1
    ) AS started ON true
    WHERE a.customer_id = ?3 AND (?6 = '' OR s.config_id = ?6) AND a.created_at < ?5
        AND r.at >= ?4 AND r.at < ?5
),
firsts AS (
    SELECT tool, COUNT(*) AS firsts, AVG(before)::double precision AS avg_turns_before_use
    FROM (SELECT tool, MIN(before) AS before FROM ran WHERE before IS NOT NULL GROUP BY tool, session_id) AS first
    GROUP BY tool
),
offered AS (
    SELECT DISTINCT ON (tool) tool, at, loading, definition_tokens, summary_tokens
    FROM ran WHERE definition_tokens IS NOT NULL
    ORDER BY tool, at DESC
)
SELECT
    COALESCE(ran.tool, '') AS tool,
    GROUPING(ran.tool) = 1 AS overall,
    COUNT(*) AS calls,
    COUNT(*) FILTER (WHERE failed) AS failed,
    COUNT(DISTINCT ran.session_id) AS sessions,
    COALESCE(f.firsts, 0) AS firsts,
    f.avg_turns_before_use,
    COALESCE(AVG(tokens), 0)::double precision AS avg_tokens,
    AVG(ms)::double precision AS avg_ms,
    percentile_cont(0.99) WITHIN GROUP (ORDER BY ms) AS p99_ms,
    o.at AS offered_at,
    COALESCE(o.loading, '') AS loading,
    o.definition_tokens,
    o.summary_tokens
FROM ran
LEFT JOIN firsts AS f ON f.tool = ran.tool
LEFT JOIN offered AS o ON o.tool = ran.tool
GROUP BY GROUPING SETS (
    (ran.tool, f.firsts, f.avg_turns_before_use, o.at, o.loading, o.definition_tokens, o.summary_tokens),
    ())
ORDER BY overall DESC, calls DESC, tool ASC`,
			charsPerToken, ItemToolResult, ItemToolCall, customerID, from, to, configID,
		).Scan(ctx, &rows)
		return rows, err
	})
	if err != nil {
		return ToolUsage{}, nil, stack.Wrap(fmt.Errorf("store: tool usage: %w", err))
	}

	// Averages and the 99th percentile of several shards are each weighted by the calls behind
	// them (see shard_merge.go); with one shard they are its own.
	type usageKey struct {
		tool    string
		overall bool
	}
	var order []usageKey
	sums := map[usageKey]*ToolUsage{}
	tokens, ms, p99s, calls := map[usageKey][]*float64{}, map[usageKey][]*float64{}, map[usageKey][]*float64{}, map[usageKey][]int64{}
	befores, firsts := map[usageKey][]*float64{}, map[usageKey][]int64{}
	// The definition is the latest call's on any shard: a tool's description can change.
	offeredAt := map[usageKey]time.Time{}
	for _, rows := range lists {
		for _, row := range rows {
			k := usageKey{row.Tool, row.Overall}
			if sums[k] == nil {
				sums[k] = &ToolUsage{Tool: row.Tool}
				order = append(order, k)
			}
			sums[k].Calls += row.Calls
			sums[k].Failed += row.Failed
			sums[k].Sessions += row.Sessions
			avgTokens := row.AvgTokens
			tokens[k] = append(tokens[k], &avgTokens)
			ms[k] = append(ms[k], row.AvgMs)
			p99s[k] = append(p99s[k], row.P99Ms)
			calls[k] = append(calls[k], row.Calls)
			befores[k] = append(befores[k], row.AvgTurnsBeforeUse)
			firsts[k] = append(firsts[k], row.Firsts)
			if row.OfferedAt != nil && row.OfferedAt.After(offeredAt[k]) {
				offeredAt[k] = *row.OfferedAt
				sums[k].Loading, sums[k].DefinitionTokens, sums[k].SummaryTokens = row.Loading, row.DefinitionTokens, row.SummaryTokens
			}
		}
	}
	var overall ToolUsage
	tools := make([]ToolUsage, 0, len(order))
	for _, k := range order {
		usage := *sums[k]
		if avg := weighted(tokens[k], calls[k]); avg != nil {
			usage.AvgTokens = *avg
		}
		usage.AvgMs, usage.P99Ms = weighted(ms[k], calls[k]), weighted(p99s[k], calls[k])
		usage.AvgTurnsBeforeUse = weighted(befores[k], firsts[k])
		if len(lists) == 1 {
			usage.AvgMs, usage.P99Ms = ms[k][0], p99s[k][0]
		}
		if k.overall {
			overall = usage
			continue
		}
		tools = append(tools, usage)
	}
	slices.SortStableFunc(tools, func(a, b ToolUsage) int {
		return cmp.Or(cmp.Compare(b.Calls, a.Calls), strings.Compare(a.Tool, b.Tool))
	})
	return overall, tools, nil
}

// CustomerTurns counts the sessions one customer's agents took a turn in during a window,
// and those turns, as CustomerToolUsage counts a turn. A configID keeps to the sessions of
// that agent config, and empty is every one.
func (s *Store) CustomerTurns(ctx context.Context, customerID, configID string, from, to time.Time) (SessionTurns, error) {
	if customerID == "" {
		return SessionTurns{}, stack.Wrap(errors.New("store: customer id is required"))
	}
	counts, err := everyShard(ctx, s, func(ctx context.Context, db bun.IDB) (SessionTurns, error) {
		var counts SessionTurns
		q := db.NewSelect().Model((*AgentResponse)(nil)).
			ColumnExpr("COUNT(DISTINCT ars.session_id) AS sessions, COUNT(*) AS turns").
			Join("JOIN agent_sessions AS asn ON asn.id = ars.session_id").
			Where("ars.customer_id = ?", customerID).
			Where("ars.created_at >= ? AND ars.created_at < ?", from, to).
			Where("ars.parent_response_id IS NULL AND ars.rewound_at IS NULL")
		if configID != "" {
			q.Where("asn.config_id = ?", configID)
		}
		err := q.Scan(ctx, &counts)
		return counts, err
	})
	if err != nil {
		return SessionTurns{}, stack.Wrap(fmt.Errorf("store: turns: %w", err))
	}
	var total SessionTurns
	for _, shard := range counts {
		total.Sessions += shard.Sessions
		total.Turns += shard.Turns
	}
	return total, nil
}

// charsPerToken is the rule of thumb llm.Compose estimates tool use with.
const charsPerToken = 4

// RecordNumber stores a number a customer now holds.
func (s *Store) RecordNumber(ctx context.Context, number *PhoneNumber) error {
	if !IsE164(number.E164) {
		return stack.Wrap(fmt.Errorf("%w: %q", ErrNotE164, number.E164))
	}
	if number.Vendor == "" {
		return stack.Wrap(errors.New("store: vendor is required"))
	}
	if number.CustomerID == "" {
		return stack.Wrap(errors.New("store: customer id is required"))
	}
	if number.PurchasedAt.IsZero() {
		number.PurchasedAt = time.Now().UTC()
	}
	// A vendor that says nothing about what a number carries is not the same as a null
	// column, and the array is not nullable.
	if number.Capabilities == nil {
		number.Capabilities = []string{}
	}

	if _, err := s.global.NewInsert().Model(number).Exec(ctx); err != nil {
		return stack.Wrap(fmt.Errorf("store: record number: %w", err))
	}
	return nil
}

// ReleaseNumber marks a number as given back. The row stays, because what it cost while
// it was held is still part of that month's bill.
func (s *Store) ReleaseNumber(ctx context.Context, customerID, e164 string, at time.Time) error {
	if customerID == "" || e164 == "" {
		return stack.Wrap(errors.New("store: a customer and a number are required"))
	}
	return releaseNumber(ctx, s.global, customerID, e164, at)
}

// releaseNumber marks a number as given back, in db.
func releaseNumber(ctx context.Context, db bun.IDB, customerID, e164 string, at time.Time) error {
	if at.IsZero() {
		at = time.Now().UTC()
	}

	result, err := db.NewUpdate().Model((*PhoneNumber)(nil)).
		Set("released_at = ?", at).
		Set("attached_at = NULL").
		Set("config_id = NULL").
		Set("sip_trunk_id = NULL").
		Set("bridge_uri = NULL").
		Set("bridge_username = NULL").
		Set("bridge_password_sealed = NULL").
		Set("bridge_password_kek_version = NULL").
		Where("customer_id = ?", customerID).
		Where("e164 = ?", e164).
		Where("released_at IS NULL").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: release number: %w", err))
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: release number: %w", err))
	}
	if affected == 0 {
		return stack.Wrap(fmt.Errorf("store: %s is not a number %s holds", e164, customerID))
	}
	return nil
}

// AttachNumber records which SIP trunk calls to a number arrive on and which Stream call
// they land in.
//
// The call is recorded as well as the trunk because an inbound call arrives over a webhook
// that names the call, so without it there is nothing to attribute the call to.
func (s *Store) AttachNumber(ctx context.Context, customerID, e164 string, attached NumberAttachment) error {
	if customerID == "" || e164 == "" {
		return stack.Wrap(errors.New("store: a customer and a number are required"))
	}
	if attached.TrunkID == "" {
		return stack.Wrap(errors.New("store: a trunk id is required"))
	}

	update := s.global.NewUpdate().Model((*PhoneNumber)(nil))
	if attached.ConfigID != "" {
		update.Set("config_id = ?", attached.ConfigID)
	}
	result, err := update.
		Set("stream_trunk_id = ?", attached.TrunkID).
		Set("stream_route_id = ?", nullable(attached.RouteID)).
		Set("stream_app_pk = ?", nullablePin(attached.StreamAppPK)).
		Set("stream_call_id = ?", attached.CallID).
		Set("stream_call_type = ?", attached.CallType).
		Set("bridge_uri = ?", nullable(attached.BridgeURI)).
		Set("bridge_username = ?", nullable(attached.BridgeUsername)).
		Set("bridge_password_sealed = ?", nullableBytes(attached.BridgePasswordSealed)).
		Set("bridge_password_kek_version = ?", nullableVersion(attached.BridgePasswordKEKVersion)).
		Set("attached_at = now()").
		Where("customer_id = ?", customerID).
		Where("e164 = ?", e164).
		Where("released_at IS NULL").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: attach number: %w", err))
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: attach number: %w", err))
	}
	if affected == 0 {
		return stack.Wrap(fmt.Errorf("store: %s is not a number %s holds", e164, customerID))
	}
	return nil
}

// NumberAttachment is what attaching a number made in Stream, and where its calls go.
type NumberAttachment struct {
	TrunkID string
	RouteID string
	// StreamAppPK is the app the trunk and route were made in, zero for the deployment's.
	StreamAppPK int64
	CallType    string
	CallID      string
	// ConfigID is the agent the number keeps answering with. Empty leaves its agent as it is.
	ConfigID string
	// BridgeURI and BridgeUsername are the trunk's address, kept whenever a number is attached.
	// The sealed password is empty for a service with no key to seal it under.
	BridgeURI                string
	BridgeUsername           string
	BridgePasswordSealed     []byte
	BridgePasswordKEKVersion int
}

// AttachedBridge is the Stream trunk an attached number's calls go to, with its password
// still sealed.
type AttachedBridge struct {
	CustomerID     string
	URI            string
	Username       string
	PasswordSealed []byte
	KEKVersion     int
}

// ErrNoStoredBridge is AttachedBridge's error for a number that is attached but whose trunk
// credentials were not kept: it was attached before they were, or arrived by a data move,
// which leaves the password behind. Attaching it again keeps them.
var ErrNoStoredBridge = errors.New("store: the number's trunk credentials were not kept")

// AttachedBridge returns the trunk calls to a number of the vendor go to. It matches
// ErrNoNumber when nobody holds the number at that vendor or it is not attached, and
// ErrNoStoredBridge when it was attached without its credentials kept.
func (s *Store) AttachedBridge(ctx context.Context, vendor, e164 string) (AttachedBridge, error) {
	if vendor == "" || e164 == "" {
		return AttachedBridge{}, stack.Wrap(errors.New("store: a vendor and a number are required"))
	}

	var number PhoneNumber
	err := s.global.NewSelect().Model(&number).
		Where("vendor = ?", vendor).
		Where("e164 = ?", e164).
		Where("released_at IS NULL").
		Where("attached_at IS NOT NULL").
		Limit(1).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return AttachedBridge{}, stack.Wrap(numberNotHeld{fmt.Sprintf("store: %s is not an attached %s number", e164, vendor)})
	}
	if err != nil {
		return AttachedBridge{}, stack.Wrap(fmt.Errorf("store: attached bridge: %w", err))
	}
	if number.BridgeURI == "" || len(number.BridgePasswordSealed) == 0 {
		return AttachedBridge{}, stack.Wrap(fmt.Errorf("%w: %s", ErrNoStoredBridge, e164))
	}
	return AttachedBridge{
		CustomerID:     number.CustomerID,
		URI:            number.BridgeURI,
		Username:       number.BridgeUsername,
		PasswordSealed: number.BridgePasswordSealed,
		KEKVersion:     number.BridgePasswordKEKVersion,
	}, nil
}

func nullableBytes(value []byte) any {
	if len(value) == 0 {
		return nil
	}
	return value
}

func nullableVersion(version int) any {
	if version == 0 {
		return nil
	}
	return version
}

// nullablePin stores the deployment's own app as NULL, as every pin written before apps
// had identities reads.
func nullablePin(app int64) any {
	if app == 0 {
		return nil
	}
	return app
}

// CustomerNumbers returns the numbers a customer holds, newest first. Released numbers
// are left out unless asked for, since what is normally wanted is what can be called.
func (s *Store) CustomerNumbers(ctx context.Context, customerID string, includeReleased bool) ([]PhoneNumber, error) {
	if customerID == "" {
		return nil, stack.Wrap(errors.New("store: customer id is required"))
	}

	query := s.global.NewSelect().Model((*PhoneNumber)(nil)).
		Where("customer_id = ?", customerID).
		Order("purchased_at DESC")
	if !includeReleased {
		query = query.Where("released_at IS NULL")
	}

	var numbers []PhoneNumber
	if err := query.Scan(ctx, &numbers); err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: customer numbers: %w", err))
	}
	return numbers, nil
}

// Number returns one number a customer holds.
func (s *Store) Number(ctx context.Context, customerID, e164 string) (PhoneNumber, error) {
	if customerID == "" || e164 == "" {
		return PhoneNumber{}, stack.Wrap(errors.New("store: a customer and a number are required"))
	}
	return heldNumber(ctx, s.global.NewSelect(), customerID, e164)
}

// SetNumberAgent makes a config answer an attached number's calls, replacing any it had.
func (s *Store) SetNumberAgent(ctx context.Context, customerID, e164, configID string) error {
	if customerID == "" || e164 == "" || configID == "" {
		return stack.Wrap(errors.New("store: a customer, a number and a config are required"))
	}
	// Only an attached number has an agent, and a detach can land between the caller's
	// read and this write.
	result, err := s.global.NewUpdate().Model((*PhoneNumber)(nil)).
		Set("config_id = ?", configID).
		Where("customer_id = ?", customerID).
		Where("e164 = ?", e164).
		Where("released_at IS NULL").
		Where("attached_at IS NOT NULL").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: set number agent: %w", err))
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: set number agent: %w", err))
	}
	if affected == 0 {
		return stack.Wrap(numberNotHeld{fmt.Sprintf("store: %s is not a number %s holds attached", e164, customerID)})
	}
	return nil
}

// ClearNumberAgent sends a number's calls back to dispatch. A number with no agent is left
// as it is.
func (s *Store) ClearNumberAgent(ctx context.Context, customerID, e164 string) error {
	if customerID == "" || e164 == "" {
		return stack.Wrap(errors.New("store: a customer and a number are required"))
	}
	result, err := s.global.NewUpdate().Model((*PhoneNumber)(nil)).
		Set("config_id = NULL").
		Where("customer_id = ?", customerID).
		Where("e164 = ?", e164).
		Where("released_at IS NULL").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: clear number agent: %w", err))
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: clear number agent: %w", err))
	}
	if affected == 0 {
		return stack.Wrap(numberNotHeld{fmt.Sprintf("store: %s is not a number %s holds", e164, customerID)})
	}
	return nil
}

// NumbersAnsweredBy returns the numbers a config answers, sorted.
func (s *Store) NumbersAnsweredBy(ctx context.Context, customerID, configID string) ([]string, error) {
	if customerID == "" || configID == "" {
		return nil, stack.Wrap(errors.New("store: a customer and a config are required"))
	}
	var numbers []string
	err := s.global.NewSelect().Model((*PhoneNumber)(nil)).
		Column("e164").
		Where("customer_id = ?", customerID).
		Where("config_id = ?", configID).
		Where("released_at IS NULL").
		// A router from before agents on numbers detaches without clearing the agent.
		Where("attached_at IS NOT NULL").
		Order("e164").
		Scan(ctx, &numbers)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: numbers answered by: %w", err))
	}
	return numbers, nil
}

// heldNumber is one number a customer holds, read by query.
func heldNumber(ctx context.Context, query *bun.SelectQuery, customerID, e164 string) (PhoneNumber, error) {
	// Nobody holds a number written another way, and the vendor may read it as one somebody
	// else holds.
	if !IsE164(e164) {
		return PhoneNumber{}, stack.Wrap(numberNotHeld{fmt.Sprintf("store: %s is not a number %s holds", e164, customerID)})
	}
	var number PhoneNumber
	err := query.Model(&number).
		Where("customer_id = ?", customerID).
		Where("e164 = ?", e164).
		Where("released_at IS NULL").
		Limit(1).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return PhoneNumber{}, stack.Wrap(numberNotHeld{fmt.Sprintf("store: %s is not a number %s holds", e164, customerID)})
	}
	if err != nil {
		return PhoneNumber{}, stack.Wrap(fmt.Errorf("store: number: %w", err))
	}
	return number, nil
}

// newID is the handle a caller holds a row by. It is random rather than sequential because
// it is the only thing standing between two customers who both guessed at an id.
func newID() string {
	raw := make([]byte, 16)
	// rand.Read on crypto/rand never returns an error, which is why the result is not
	// checked: the alternative would be a row that could not be created.
	_, _ = rand.Read(raw)
	return hex.EncodeToString(raw)
}

// NewID is newID for callers that have to name something before the row holding it exists,
// as an uploaded file must be given an object key before it can be recorded.
func NewID() string { return newID() }

// lockUntilCommit takes the advisory lock key and seed name, and holds it until tx ends.
func lockUntilCommit(ctx context.Context, tx bun.IDB, key string, seed int64) error {
	_, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtextextended(?, ?))", key, seed)
	return err
}

// databaseNow is the database's clock, which routers whose clocks disagree still share.
func databaseNow(ctx context.Context, db bun.IDB) (time.Time, error) {
	var now time.Time
	err := db.QueryRowContext(ctx, "SELECT now()").Scan(&now)
	return now, err
}
