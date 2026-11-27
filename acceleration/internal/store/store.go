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
	"sync"
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
	// deployment of one shard (see shards.go).
	global *bun.DB
	shard  *bun.DB
	// locks is the shared database again, in a pool of its own that credential and provider
	// app locks are held on (tryCredentialLock).
	locks *bun.DB
	// lockSlots caps how many of locks' connections one customer's locks hold.
	lockSlots lockSlots
	// globalDSN and shardDSN are what a migration of global and shard connects with: the
	// migrate DSNs, or what they were opened with.
	globalDSN, shardDSN string
	// cache is the shared Redis, holding copies of rows read on every request, and redis the
	// shard's, holding the state of its running calls. Both are nil without Redis.
	cache rueidisaside.CacheAsideClient
	redis rueidis.Client
	// shardName names this shard, and shardNames every shard of the deployment, this one's
	// first, for what is kept per shard in the shared database.
	shardName  string
	shardNames []string
	// homed says sessions here keep the shard they opened in (SessionHomes): this router names
	// its shard, or has one of its own.
	homed    bool
	LogDrops atomic.Int64
	// shapes is what each table an export carries looks like, read from the catalogue
	// once rather than kept in a list here that a migration could leave behind.
	shapes tableShapes
	// pins says how Stream app pins cross between deployments, once the router has said.
	pins pinsHolder
	// configRule is what an import holds the agent configs it writes to (SetConfigRule).
	configRule atomic.Pointer[ConfigRule]
	// retries holds a slot for each deletion forgetLater is trying again, and lost one for each
	// lost commit it is waiting on, which it can do for longer; polls bounds how many of those
	// ask Postgres at once. stop ends them
	// when the store closes, and retrying is what Close waits on. retryMu guards closed, so no
	// retry starts once Close is waiting.
	retries  chan struct{}
	lost     chan struct{}
	polls    chan struct{}
	closing  context.Context
	stop     context.CancelFunc
	retrying sync.WaitGroup
	retryMu  sync.Mutex
	closed   bool
	// writeTimeout bounds detachedWrite.
	writeTimeout time.Duration
}

// writeTimeout is how long the statements of a write of a cached row may take when Options
// does not say. Its COMMIT is bounded by the DSN's read_timeout instead (detachedWrite).
const writeTimeout = 10 * time.Second

// detachedWrite runs write in a transaction of the shared database that outlives ctx's
// cancellation, its statements within the store's write timeout. pgdriver sends the COMMIT on
// context.Background(), so only the DSN's read_timeout bounds that, and a write can take the
// write timeout plus the read timeout. It then drops from the cache the keys that
// keys names, whatever the commit answered. A request that gives up therefore leaves no
// statement for Postgres to commit after a read has cached the old row again: the transaction
// is rolled back when its connection goes, since Postgres commits nothing without the COMMIT.
// keys is read once write has run, and names nothing when write found no row.
//
// A COMMIT whose answer was lost may land after the deletion, so its keys are dropped again
// on each of the next few attempts, which Close lets run, and once more when Postgres says the
// transaction has ended, which Close does not wait for (forgetLater).
func (s *Store) detachedWrite(ctx context.Context, write func(context.Context, bun.Tx) error, keys func() []string) error {
	_, err := s.detachedWriteTold(ctx, write, keys)
	return err
}

// detachedWriteTold is detachedWrite, and says whether every replica was told, the keys'
// deletion having landed in Redis.
func (s *Store) detachedWriteTold(ctx context.Context, write func(context.Context, bun.Tx) error, keys func() []string) (told bool, err error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.writeTimeout)
	defer cancel()
	return s.cachedWrite(ctx, s.global, write, keys, func() bool { return true })
}

// cachedWrite is detachedWriteTold on ctx itself, for a write too long for the write timeout. ctx
// ending before the COMMIT is sent rolls the transaction back. cached is asked in the transaction
// whether there is a key to drop, which keys may take a query of its own to say (Policies.keys). db
// may be another than the shared database only for a write that has none: a lost commit is asked
// about there (ended).
func (s *Store) cachedWrite(ctx context.Context, db *bun.DB, write func(context.Context, bun.Tx) error, keys func() []string, cached func() bool) (told bool, err error) {
	var committing bool
	var xid sql.NullInt64
	err = db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if err := write(ctx, tx); err != nil {
			return err
		}
		// The transaction's id, for asking Postgres later whether a COMMIT whose answer was
		// lost has landed. Without a cache there is nothing to drop again, and a write that
		// changed nothing has no id, so nothing to wait for either.
		if s.cache != nil && cached() {
			if err := tx.NewSelect().ColumnExpr("pg_current_xact_id_if_assigned()").Scan(ctx, &xid); err != nil {
				return err
			}
		}
		committing = true
		return nil
	})
	names := keys()
	told = s.forget(ctx, names...)
	var answered pgdriver.Error
	if err != nil && committing && !errors.As(err, &answered) && xid.Valid && len(names) > 0 {
		s.forgetLater(names, err, xid.Int64)
	}
	return told, err
}

// Open connects to Postgres using a pgdriver DSN, for example
// postgres://user:pass@localhost:5432/router?sslmode=disable.
func Open(dsn string) (*Store, error) {
	return OpenShards(Options{DSN: dsn})
}

// The server's own limits on what a serving connection runs. pgdriver gives up on a read
// after 10 s and drops the socket, but the server runs the statement to its end and commits
// it, so a write the caller was told failed would still land. statementTimeout ends the
// statement on the server first, and stays below the driver's read timeout for that.
// lockTimeout bounds a wait on another session's lock, and idleInTransactionTimeout how long
// a transaction whose router went quiet keeps its locks. Work that legitimately takes longer
// raises them for its own transaction (boundTransaction).
const (
	statementTimeout         = 8 * time.Second
	lockTimeout              = 3 * time.Second
	idleInTransactionTimeout = 30 * time.Second
)

// sessionDefaults sets the limits above, and UTC as the time zone days and hours are cut in,
// on every connection, leaving any of them the DSN sets itself.
func sessionDefaults(conf *pgdriver.Config) {
	defaults := map[string]any{
		"statement_timeout":                   fmt.Sprintf("%dms", statementTimeout.Milliseconds()),
		"lock_timeout":                        fmt.Sprintf("%dms", lockTimeout.Milliseconds()),
		"idle_in_transaction_session_timeout": fmt.Sprintf("%dms", idleInTransactionTimeout.Milliseconds()),
		"timezone":                            "UTC",
	}
	// Postgres reads a parameter's name in any case, so a DSN's TimeZone is timezone here.
	for name, value := range conf.ConnParams {
		defaults[strings.ToLower(name)] = value
	}
	conf.ConnParams = defaults
}

// boundTransaction gives the rest of tx statement, lock and idle limits of its own, for work
// known to take longer than the serving defaults allow.
func boundTransaction(ctx context.Context, tx bun.Tx, statement, lock, idle time.Duration) error {
	_, err := tx.ExecContext(ctx, "SELECT set_config('statement_timeout', ?, true), set_config('lock_timeout', ?, true), "+
		"set_config('idle_in_transaction_session_timeout', ?, true)",
		fmt.Sprintf("%dms", statement.Milliseconds()), fmt.Sprintf("%dms", lock.Milliseconds()), fmt.Sprintf("%dms", idle.Milliseconds()))
	return err
}

// writeBatch is how many rows one statement of a prune or a rename touches. A backlog is
// worked through in statements of this many, each well inside the statement limit: one
// statement over all of it would run out of time and roll back, every time, while the backlog
// grew.
var writeBatch = 5000

// deleteInBatches deletes the rows of table that where matches, writeBatch of them per
// statement, and returns how many it deleted.
func deleteInBatches(ctx context.Context, db bun.IDB, table, where string, args ...any) (int64, error) {
	var deleted int64
	for {
		batch := db.NewSelect().TableExpr(table).Column("ctid").Where(where, args...).Limit(writeBatch)
		result, err := db.NewDelete().TableExpr(table).Where("ctid IN (?)", batch).Exec(ctx)
		if err != nil {
			return deleted, err
		}
		n, err := result.RowsAffected()
		deleted += n
		if err != nil || n < int64(writeBatch) {
			return deleted, err
		}
	}
}

// connectorFor is pgdriver's connector of a DSN.
func connectorFor(dsn string, opts ...pgdriver.Option) (_ *pgdriver.Connector, err error) {
	// pgdriver panics on a DSN it cannot parse, and the panic holds the whole DSN, password
	// included, which would reach the logs past their redaction.
	defer func() {
		if recover() != nil {
			err = stack.Wrap(errors.New("store: the database DSN does not parse"))
		}
	}()
	return pgdriver.NewConnector(append([]pgdriver.Option{pgdriver.WithDSN(dsn)}, opts...)...), nil
}

// connect opens a pool of at most conns connections.
func connect(dsn string, conns int) (*bun.DB, error) {
	connector, err := connectorFor(dsn, sessionDefaults)
	if err != nil {
		return nil, err
	}
	sqldb := sql.OpenDB(primaryOnly{connector})
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
	return db, nil
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

// Close releases the connection pools, once the deletions forgetLater holds have run, or
// forgetGrace has passed and the rest have stopped.
func (s *Store) Close() error {
	s.retryMu.Lock()
	s.closed = true
	s.retryMu.Unlock()
	retried := make(chan struct{})
	go func() {
		s.retrying.Wait()
		close(retried)
	}()
	select {
	case <-retried:
	case <-time.After(forgetGrace):
	}
	s.stop()
	s.retrying.Wait()
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
// stats paths leave out a bucket nobody used. Calls and voice minutes count only the calls
// rows of a call: a text session writes one per stretch too, with no call id.
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
		return nil, stack.Wrap(errCustomerRequired)
	}

	unit := granularity.truncateUnit()
	type seenUser struct {
		Bucket time.Time `bun:"bucket"`
		UserID string    `bun:"user_id"`
	}
	var counts []ActivityBucket
	var people []seenUser
	if s.StatsShipped() {
		err := s.global.NewRaw(fmt.Sprintf(`
SELECT date_trunc('%[1]s', bucket, 'UTC') AS bucket, SUM(sessions) AS sessions, SUM(messages) AS messages,
    SUM(calls) AS calls, SUM(voice_minutes) AS voice_minutes, SUM(phone_minutes) AS phone_minutes
FROM shard_activity_hourly
WHERE customer_id = ? AND bucket >= ? AND bucket < ?
GROUP BY 1`, unit), customerID, from, to).Scan(ctx, &counts)
		if err != nil {
			return nil, stack.Wrap(fmt.Errorf("store: customer activity: %w", err))
		}
		err = s.global.NewRaw(fmt.Sprintf(`
SELECT DISTINCT date_trunc('%[1]s', bucket, 'UTC') AS bucket, user_id
FROM shard_people_hourly
WHERE customer_id = ? AND bucket >= ? AND bucket < ?`, unit), customerID, from, to).Scan(ctx, &people)
		if err != nil {
			return nil, stack.Wrap(fmt.Errorf("store: customer activity: %w", err))
		}
	} else {
		// Who was seen is read apart from the counts: a guest who signed up is the account they
		// became, which the shared database knows rather than the shard.
		err := s.shard.NewRaw(fmt.Sprintf(`
WITH session_counts AS (
    SELECT date_trunc('%[1]s', created_at, 'UTC') AS bucket, COUNT(*) AS n
    FROM agent_sessions
    WHERE customer_id = ?0 AND created_at >= ?1 AND created_at < ?2
    GROUP BY bucket
),
message_counts AS (
    SELECT date_trunc('%[1]s', created_at, 'UTC') AS bucket, COUNT(*) AS n
    FROM agent_responses
    WHERE customer_id = ?0 AND created_at >= ?1 AND created_at < ?2
    GROUP BY bucket
),
call_counts AS (
    SELECT
        date_trunc('%[1]s', started_at, 'UTC') AS bucket,
        COUNT(*) AS n,
        SUM(EXTRACT(EPOCH FROM (COALESCE(ended_at, now()) - started_at)))::double precision / 60
            AS voice_minutes,
        COALESCE(SUM(EXTRACT(EPOCH FROM (COALESCE(ended_at, now()) - started_at)))
            FILTER (WHERE from_number IS NOT NULL OR to_number IS NOT NULL), 0)::double precision / 60
            AS phone_minutes
    FROM calls
    WHERE customer_id = ?0 AND started_at >= ?1 AND started_at < ?2 AND call_id <> ''
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
LEFT JOIN call_counts AS c ON c.bucket = b.bucket`, unit), customerID, from, to).Scan(ctx, &counts)
		if err != nil {
			return nil, stack.Wrap(fmt.Errorf("store: customer activity: %w", err))
		}
		err = s.shard.NewRaw(fmt.Sprintf(`
SELECT DISTINCT bucket, user_id FROM (
    SELECT date_trunc('%[1]s', sess.created_at, 'UTC') AS bucket, sess.user_id, sess.caller_kind
    FROM agent_sessions AS sess
    WHERE sess.customer_id = ?0 AND sess.created_at >= ?1 AND sess.created_at < ?2
    UNION ALL
    SELECT date_trunc('%[1]s', a.created_at, 'UTC') AS bucket, s.user_id, s.caller_kind
    FROM agent_responses AS a
    JOIN agent_sessions AS s ON s.customer_id = a.customer_id AND s.id = a.session_id
    WHERE a.customer_id = ?0 AND a.created_at >= ?1 AND a.created_at < ?2
) AS seen
WHERE user_id <> '' AND caller_kind <> 'anonymous'`, unit), customerID, from, to).Scan(ctx, &people)
		if err != nil {
			return nil, stack.Wrap(fmt.Errorf("store: customer activity: %w", err))
		}
	}

	ids := map[string]bool{}
	for _, user := range people {
		ids[user.UserID] = true
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
	for _, counted := range counts {
		b := at(counted.Bucket)
		b.Sessions += counted.Sessions
		b.Messages += counted.Messages
		b.Calls += counted.Calls
		b.VoiceMinutes += counted.VoiceMinutes
		b.PhoneMinutes += counted.PhoneMinutes
	}
	type person struct {
		bucket int64
		id     string
	}
	counted := map[person]bool{}
	for _, user := range people {
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
// another opened did not overlap it. With other shards it is read from what each shard shipped
// of every minute (shard_concurrency_minute): the most of any minute, summed over the shards,
// counting a session open at any moment of the minute, and the minute is when.
func (s *Store) CustomerConcurrencyPeak(ctx context.Context, customerID string, from, to time.Time) (ConcurrencyPeak, error) {
	if customerID == "" {
		return ConcurrencyPeak{}, stack.Wrap(errCustomerRequired)
	}

	var peaks []ConcurrencyPeak
	var err error
	if s.StatsShipped() {
		err = s.global.NewRaw(`
SELECT SUM(sessions) AS sessions, minute AS at
FROM shard_concurrency_minute
WHERE customer_id = ? AND minute >= date_trunc('minute', ?::timestamptz) AND minute < ?
GROUP BY minute
ORDER BY sessions DESC, minute ASC
LIMIT 1`, customerID, from, to).Scan(ctx, &peaks)
	} else {
		err = s.shard.NewRaw(`
WITH spans AS (
    SELECT id, GREATEST(created_at, ?0) AS opened, LEAST(COALESCE(closed_at, now()), ?1) AS closed
    FROM agent_sessions
    WHERE customer_id = ?2 AND created_at < ?1 AND (closed_at IS NULL OR closed_at > ?0)
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
	}
	if err != nil {
		return ConcurrencyPeak{}, stack.Wrap(fmt.Errorf("store: concurrency peak: %w", err))
	}
	if len(peaks) == 0 || peaks[0].Sessions <= 0 {
		return ConcurrencyPeak{}, nil
	}
	return peaks[0], nil
}

// CustomerToolUsage returns how every tool one customer's agents ran in a window was used,
// most called first, and the same figures over all of them. A configID keeps to the
// sessions of that agent config, and empty is every one. A web search the agent made has no
// result item, so it is not a tool call here.
//
// A turn is a response that was not rewound. Work a turn delegated is recorded on that turn's
// response when it comes back while the turn is open, and as a turn of its own after.
//
// With other shards it is read from what each shard shipped of every hour, whose 99th
// percentiles are weighed by their calls.
func (s *Store) CustomerToolUsage(ctx context.Context, customerID, configID string, from, to time.Time) (ToolUsage, []ToolUsage, error) {
	if customerID == "" {
		return ToolUsage{}, nil, stack.Wrap(errCustomerRequired)
	}

	type usageRow struct {
		ToolUsage
		Overall bool `bun:"overall"`
	}
	var rows []usageRow
	var err error
	if s.StatsShipped() {
		err = s.global.NewRaw(`
WITH hours AS (
    SELECT * FROM shard_tool_hourly
    WHERE customer_id = ?0 AND (?1 = '' OR config_id = ?1) AND bucket >= ?2 AND bucket < ?3
),
used AS (
    SELECT tool, session_id, MIN(first_before) AS before
    FROM shard_tool_sessions_hourly
    WHERE customer_id = ?0 AND (?1 = '' OR config_id = ?1) AND bucket >= ?2 AND bucket < ?3
    GROUP BY tool, session_id
),
sessions AS (
    SELECT COALESCE(tool, '') AS tool, GROUPING(tool) = 1 AS overall, COUNT(DISTINCT session_id) AS sessions,
        AVG(before)::double precision AS avg_turns_before_use
    FROM used
    GROUP BY GROUPING SETS ((tool), ())
),
offered AS (
    SELECT DISTINCT ON (tool) tool, loading, definition_tokens, summary_tokens
    FROM hours WHERE offered_at IS NOT NULL
    ORDER BY tool, offered_at DESC
),
calls AS (
    SELECT tool, tool = '' AS overall,
        SUM(calls) AS calls, SUM(failed) AS failed,
        COALESCE(SUM(tokens_total) / NULLIF(SUM(calls), 0), 0)::double precision AS avg_tokens,
        (SUM(ms_total) / NULLIF(SUM(ms_count), 0))::double precision AS avg_ms,
        (SUM(p99_ms * calls) FILTER (WHERE p99_ms IS NOT NULL)
            / NULLIF(SUM(calls) FILTER (WHERE p99_ms IS NOT NULL), 0))::double precision AS p99_ms
    FROM hours
    GROUP BY tool
)
SELECT c.tool, c.overall, c.calls, c.failed, COALESCE(u.sessions, 0) AS sessions,
    CASE WHEN c.overall THEN NULL ELSE u.avg_turns_before_use END AS avg_turns_before_use,
    c.avg_tokens, c.avg_ms, c.p99_ms,
    CASE WHEN c.overall THEN '' ELSE COALESCE(o.loading, '') END AS loading,
    CASE WHEN c.overall THEN NULL ELSE o.definition_tokens END AS definition_tokens,
    CASE WHEN c.overall THEN NULL ELSE o.summary_tokens END AS summary_tokens
FROM calls AS c
LEFT JOIN sessions AS u ON u.tool = c.tool AND u.overall = c.overall
LEFT JOIN offered AS o ON o.tool = c.tool
WHERE c.calls > 0
ORDER BY c.overall DESC, c.calls DESC, c.tool COLLATE "C" ASC`,
			customerID, configID, from, to,
		).Scan(ctx, &rows)
	} else {
		err = s.shard.NewRaw(`
WITH turns AS (
    SELECT
        a.id,
        a.created_at,
        ROW_NUMBER() OVER (PARTITION BY a.session_id ORDER BY a.created_at, a.id COLLATE "C") - 1 AS before
    FROM agent_responses AS a
    JOIN agent_sessions AS s ON s.customer_id = a.customer_id AND s.id = a.session_id
    WHERE a.customer_id = ?3 AND (?6 = '' OR s.config_id = ?6) AND a.created_at < ?5
        AND a.rewound_at IS NULL
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
    JOIN agent_sessions AS s ON s.customer_id = a.customer_id AND s.id = a.session_id
    JOIN agent_response_items AS r ON r.response_id = a.id AND r.kind = ?1
    LEFT JOIN turns AS t ON t.id = a.id
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
first_use AS (
    SELECT DISTINCT ON (tool, session_id) tool, session_id, before
    FROM ran WHERE before IS NOT NULL
    ORDER BY tool, session_id, before
),
firsts AS (
    SELECT u.tool, AVG(u.before)::double precision AS avg_turns_before_use
    FROM first_use AS u
    GROUP BY u.tool
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
    f.avg_turns_before_use,
    COALESCE(AVG(tokens), 0)::double precision AS avg_tokens,
    AVG(ms)::double precision AS avg_ms,
    percentile_cont(0.99) WITHIN GROUP (ORDER BY ms) AS p99_ms,
    COALESCE(o.loading, '') AS loading,
    o.definition_tokens,
    o.summary_tokens
FROM ran
LEFT JOIN firsts AS f ON f.tool = ran.tool
LEFT JOIN offered AS o ON o.tool = ran.tool
GROUP BY GROUPING SETS (
    (ran.tool, f.avg_turns_before_use, o.loading, o.definition_tokens, o.summary_tokens),
    ())
ORDER BY overall DESC, calls DESC, COALESCE(ran.tool, '') COLLATE "C" ASC`,
			charsPerToken, ItemToolResult, ItemToolCall, customerID, from, to, configID,
		).Scan(ctx, &rows)
	}
	if err != nil {
		return ToolUsage{}, nil, stack.Wrap(fmt.Errorf("store: tool usage: %w", err))
	}

	var overall ToolUsage
	tools := make([]ToolUsage, 0, len(rows))
	for _, row := range rows {
		if row.Overall {
			overall = row.ToolUsage
			continue
		}
		tools = append(tools, row.ToolUsage)
	}
	return overall, tools, nil
}

// CustomerTurns counts the sessions one customer's agents took a turn in during a window,
// and those turns: every response that was not rewound, as CustomerToolUsage counts a turn.
// A configID keeps to the sessions of that agent config, and empty is every one. With other
// shards it is read from what each shard shipped of every hour.
func (s *Store) CustomerTurns(ctx context.Context, customerID, configID string, from, to time.Time) (SessionTurns, error) {
	if customerID == "" {
		return SessionTurns{}, stack.Wrap(errCustomerRequired)
	}
	var total SessionTurns
	var q *bun.SelectQuery
	if s.StatsShipped() {
		q = s.global.NewSelect().TableExpr("shard_session_turns_hourly").
			ColumnExpr("COUNT(DISTINCT session_id) AS sessions, COALESCE(SUM(turns), 0) AS turns").
			Where("customer_id = ?", customerID).
			Where("bucket >= ? AND bucket < ?", from, to)
		if configID != "" {
			q.Where("config_id = ?", configID)
		}
	} else {
		q = s.shard.NewSelect().Model((*AgentResponse)(nil)).
			ColumnExpr("COUNT(DISTINCT ars.session_id) AS sessions, COUNT(*) AS turns").
			Join("JOIN agent_sessions AS asn ON asn.customer_id = ars.customer_id AND asn.id = ars.session_id").
			Where("ars.customer_id = ?", customerID).
			Where("ars.created_at >= ? AND ars.created_at < ?", from, to).
			Where("ars.rewound_at IS NULL")
		if configID != "" {
			q.Where("asn.config_id = ?", configID)
		}
	}
	if err := q.Scan(ctx, &total); err != nil {
		return SessionTurns{}, stack.Wrap(fmt.Errorf("store: turns: %w", err))
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

// transactionStart is when db's transaction started, by the database's clock, which routers
// whose clocks disagree still share. It is not when it is asked: every call in one transaction
// gets the same answer, and outside a transaction it is when the statement started.
func transactionStart(ctx context.Context, db bun.IDB) (time.Time, error) {
	var now time.Time
	err := db.QueryRowContext(ctx, "SELECT now()").Scan(&now)
	return now, err
}

// databaseClock is the database's clock when it is asked, unlike transactionStart. A write that
// waited for a lock reads it to stamp the time it was made, not the time it began waiting.
func databaseClock(ctx context.Context, db bun.IDB) (time.Time, error) {
	var now time.Time
	err := db.QueryRowContext(ctx, "SELECT clock_timestamp()").Scan(&now)
	return now, err
}

// RecordedUsage is which of the usage ids this shard holds a row for: work billed before, which
// is not billed again. A leg's usage id names the leg, which only its shard bills.
func (s *Store) RecordedUsage(ctx context.Context, usageIDs []uuid.UUID) (map[uuid.UUID]bool, error) {
	recorded := make(map[uuid.UUID]bool, len(usageIDs))
	for batch := range slices.Chunk(usageIDs, 1000) {
		var found []uuid.UUID
		err := s.shard.NewSelect().Model((*Request)(nil)).Column("usage_id").
			Where("usage_id IN (?)", bun.In(batch)).Scan(ctx, &found)
		if err != nil {
			return nil, stack.Wrap(fmt.Errorf("store: recorded usage: %w", err))
		}
		for _, id := range found {
			recorded[id] = true
		}
	}
	return recorded, nil
}

// usageClaim is the shard that bills a usage id any shard could bill.
type usageClaim struct {
	bun.BaseModel `bun:"table:usage_claims"`

	UsageID uuid.UUID `bun:"usage_id,pk"`
	Shard   string    `bun:"shard,notnull"`
}

// UsageToBill is which of the usage ids, each the same whichever shard bills it (a number's
// month, a 10DLC fee), this shard is to bill: those it holds no row for, that no other shard
// claimed first. The ones it bills it claims, in the shared database, and bills again only
// while its own row is missing.
func (s *Store) UsageToBill(ctx context.Context, usageIDs []uuid.UUID) (map[uuid.UUID]bool, error) {
	recorded, err := s.RecordedUsage(ctx, usageIDs)
	if err != nil {
		return nil, err
	}
	due := make(map[uuid.UUID]bool, len(usageIDs))
	for _, id := range usageIDs {
		if !recorded[id] {
			due[id] = true
		}
	}
	if !s.homed || len(due) == 0 {
		return due, nil
	}
	claims := make([]usageClaim, 0, len(due))
	for id := range due {
		claims = append(claims, usageClaim{UsageID: id, Shard: s.ShardName()})
	}
	for batch := range slices.Chunk(claims, 1000) {
		if _, err := s.global.NewInsert().Model(&batch).On("CONFLICT DO NOTHING").Exec(ctx); err != nil {
			return nil, stack.Wrap(fmt.Errorf("store: claim usage: %w", err))
		}
		ids := make([]uuid.UUID, len(batch))
		for i, claim := range batch {
			ids[i] = claim.UsageID
		}
		var elsewhere []uuid.UUID
		err := s.global.NewSelect().Model((*usageClaim)(nil)).Column("usage_id").
			Where("usage_id IN (?)", bun.In(ids)).Where("shard <> ?", s.ShardName()).Scan(ctx, &elsewhere)
		if err != nil {
			return nil, stack.Wrap(fmt.Errorf("store: claim usage: %w", err))
		}
		for _, id := range elsewhere {
			delete(due, id)
		}
	}
	return due, nil
}
