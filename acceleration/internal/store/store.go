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

	"github.com/google/uuid"
	"github.com/redis/rueidis"
	"github.com/redis/rueidis/rueidisaside"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"
	"github.com/uptrace/bun/extra/bunotel"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// maxConns is how many Postgres connections one router's serving pool holds at most: every
// query but those on the connection a credential or provider app lock is held on (lockConns).
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
	// locks is the shared database again, in a pool of its own that credential and provider
	// app locks are held on (acquireCredentialLock).
	locks *bun.DB
	// globalDSN and shardDSN are what global and shard were opened with, for the pool a
	// migration runs on.
	globalDSN, shardDSN string
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
	return errors.Join(s.closeShards(), s.locks.Close(), s.global.Close())
}

// Ping verifies the shared database and this shard's are usable. Another shard being down
// is not this router failing: it only leaves that shard's calls off a page.
func (s *Store) Ping(ctx context.Context) error {
	if err := s.global.PingContext(ctx); err != nil {
		return stack.Wrap(err)
	}
	return stack.Wrap(s.shard.PingContext(ctx))
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

// RecordedUsage is which of the usage ids any shard holds a row for: work billed before, which
// is not billed again.
func (s *Store) RecordedUsage(ctx context.Context, usageIDs []uuid.UUID) (map[uuid.UUID]bool, error) {
	recorded := make(map[uuid.UUID]bool, len(usageIDs))
	for batch := range slices.Chunk(usageIDs, 1000) {
		lists, err := everyShard(ctx, s, func(ctx context.Context, db bun.IDB) ([]uuid.UUID, error) {
			var found []uuid.UUID
			err := db.NewSelect().Model((*Request)(nil)).Column("usage_id").
				Where("usage_id IN (?)", bun.In(batch)).Scan(ctx, &found)
			return found, err
		})
		if err != nil {
			return nil, stack.Wrap(fmt.Errorf("store: recorded usage: %w", err))
		}
		for _, id := range slices.Concat(lists...) {
			recorded[id] = true
		}
	}
	return recorded, nil
}
