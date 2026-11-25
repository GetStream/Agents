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
	// deployment of one shard. peers are the other shards' (see shards.go).
	global *bun.DB
	shard  *bun.DB
	peers  []*bun.DB
	// locks is the shared database again, in a pool of its own that credential and provider
	// app locks are held on (tryCredentialLock).
	locks *bun.DB
	// lockSlots caps how many of locks' connections one customer's locks hold.
	lockSlots lockSlots
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
	// shardsDown is when each shard that did not answer was last logged (settledWithout).
	shardsDown shardWarnings
	// globalIsShard says the shared database is also some shard's own, so its calls are not
	// strays to drop.
	globalIsShard bool
	LogDrops      atomic.Int64
	// shapes is what each table an export carries looks like, read from the catalogue
	// once rather than kept in a list here that a migration could leave behind.
	shapes tableShapes
	// pins says how Stream app pins cross between deployments, once the router has said.
	pins pinsHolder
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
	return s.cachedWrite(ctx, s.global, write, keys)
}

// cachedWrite is detachedWriteTold on ctx itself, for a write too long for the write timeout. ctx
// ending before the COMMIT is sent rolls the transaction back. db may be another than the shared
// database only for a write whose keys name nothing: a lost commit is asked about there (ended).
func (s *Store) cachedWrite(ctx context.Context, db *bun.DB, write func(context.Context, bun.Tx) error, keys func() []string) (told bool, err error) {
	var committing bool
	var xid sql.NullInt64
	err = db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if err := write(ctx, tx); err != nil {
			return err
		}
		// The transaction's id, for asking Postgres later whether a COMMIT whose answer was
		// lost has landed. Without a cache there is nothing to drop again, and a write that
		// changed nothing has no id, so nothing to wait for either.
		if s.cache != nil {
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

// connect opens a pool of at most conns connections.
func connect(dsn string, conns int) (_ *bun.DB, err error) {
	// pgdriver panics on a DSN it cannot parse, and the panic holds the whole DSN, password
	// included, which would reach the logs past their redaction.
	defer func() {
		if recover() != nil {
			err = stack.Wrap(errors.New("store: the database DSN does not parse"))
		}
	}()
	sqldb := sql.OpenDB(primaryOnly{pgdriver.NewConnector(pgdriver.WithDSN(dsn), sessionDefaults)})
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
	// A session carried on in another shard has a row in each. With several shards it is
	// counted, and its person seen, by the row it began in, wherever the window starts: the
	// rows that carry it on count nothing, and what was said in them is counted as messages.
	originOnly := ""
	if len(s.peers) > 0 {
		originOnly = " AND NOT " + carriedIn("sess.id", "sess.customer_id")
	}
	// Who was seen is read apart from the counts: a person is one person however many shards
	// they talked to in a bucket, and a guest who signed up is the account they became, which
	// the shared database knows rather than the shard.
	people := fmt.Sprintf(`
SELECT DISTINCT bucket, user_id FROM (
    SELECT date_trunc('%[1]s', sess.created_at, 'UTC') AS bucket, sess.user_id, sess.caller_kind
    FROM agent_sessions AS sess
    WHERE sess.customer_id = ? AND sess.created_at >= ? AND sess.created_at < ?%[2]s
    UNION ALL
    SELECT date_trunc('%[1]s', a.created_at, 'UTC') AS bucket, s.user_id, s.caller_kind
    FROM agent_responses AS a
    JOIN agent_sessions AS s ON s.customer_id = a.customer_id AND s.id = a.session_id
    WHERE a.customer_id = ? AND a.created_at >= ? AND a.created_at < ?
) AS seen
WHERE user_id <> '' AND caller_kind <> 'anonymous'`, unit, originOnly)
	query := fmt.Sprintf(`
WITH session_counts AS (
    SELECT date_trunc('%[1]s', created_at, 'UTC') AS bucket, COUNT(*) AS n
    FROM agent_sessions AS sess
    WHERE customer_id = ? AND created_at >= ? AND created_at < ?%[2]s
    GROUP BY bucket
),
message_counts AS (
    SELECT date_trunc('%[1]s', created_at, 'UTC') AS bucket, COUNT(*) AS n
    FROM agent_responses
    WHERE customer_id = ? AND created_at >= ? AND created_at < ?
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
    WHERE customer_id = ? AND started_at >= ? AND started_at < ? AND call_id <> ''
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
LEFT JOIN call_counts AS c ON c.bucket = b.bucket`, unit, originOnly)

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

// answersAfterAnotherShard is the SQL condition, on a session id and its customer's, that the session has a
// response here answered after one this shard does not hold: it was carried on here from
// another shard, whose newest response a reopened session answers after (Responses.ActiveLeaf).
func answersAfterAnotherShard(id, customer string) string {
	return fmt.Sprintf(`EXISTS (SELECT 1 FROM agent_responses AS carried
    WHERE carried.session_id = %[1]s AND carried.customer_id = %[2]s AND carried.parent_response_id IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM agent_responses AS parent WHERE parent.id = carried.parent_response_id AND parent.customer_id = carried.customer_id))`, id, customer)
}

// carriedIn is the SQL condition, on a session id and its customer's, that this shard's row only carries
// the session on: its first turn, a response with no parent, is elsewhere. A carried row with
// no response yet cannot be told from the one it began in.
func carriedIn(id, customer string) string {
	return fmt.Sprintf(`(%[3]s AND NOT EXISTS (SELECT 1 FROM agent_responses AS began
    WHERE began.session_id = %[1]s AND began.customer_id = %[2]s AND began.parent_response_id IS NULL))`, id, customer, answersAfterAnotherShard(id, customer))
}

// CustomerConcurrencyPeak returns the most sessions one customer had open at the same time
// in a window. A session still open counts up to now, and one that closed at the instant
// another opened did not overlap it. A session carried on in another shard has a row in each,
// and is one session while both are open.
func (s *Store) CustomerConcurrencyPeak(ctx context.Context, customerID string, from, to time.Time) (ConcurrencyPeak, error) {
	if customerID == "" {
		return ConcurrencyPeak{}, stack.Wrap(errCustomerRequired)
	}

	const spans = `
SELECT id, GREATEST(created_at, ?0) AS opened, LEAST(COALESCE(closed_at, now()), ?1) AS closed
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
// which no one shard's query can see: it reads every shard's spans, joins the overlapping
// spans of one session, and walks their edges here, in the order the query does.
func (s *Store) peakAcrossShards(ctx context.Context, spans, customerID string, from, to time.Time) (ConcurrencyPeak, error) {
	type span struct {
		ID     string    `bun:"id"`
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
	bySession := map[string][]span{}
	for _, list := range lists {
		for _, open := range list {
			if open.Closed.After(open.Opened) {
				bySession[open.ID] = append(bySession[open.ID], open)
			}
		}
	}
	var edges []edge
	for _, sessionSpans := range bySession {
		slices.SortFunc(sessionSpans, func(a, b span) int { return a.Opened.Compare(b.Opened) })
		joined := sessionSpans[0]
		for _, next := range sessionSpans[1:] {
			if next.Opened.After(joined.Closed) {
				edges = append(edges, edge{joined.Opened, 1}, edge{joined.Closed, -1})
				joined = next
				continue
			}
			if next.Closed.After(joined.Closed) {
				joined.Closed = next.Closed
			}
		}
		edges = append(edges, edge{joined.Opened, 1}, edge{joined.Closed, -1})
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
// A turn is a response that was not rewound. Work a turn delegated is recorded on that turn's
// response when it comes back while the turn is open, and as a turn of its own after.
func (s *Store) CustomerToolUsage(ctx context.Context, customerID, configID string, from, to time.Time) (ToolUsage, []ToolUsage, error) {
	if customerID == "" {
		return ToolUsage{}, nil, stack.Wrap(errCustomerRequired)
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
		SessionHashes     []int64    `bun:"session_hashes,array"`
		// The sessions' first uses of the tool, one entry per session in each, with several
		// shards (firstUse).
		FirstHashes   []int64     `bun:"first_hashes,array"`
		FirstBefores  []int64     `bun:"first_befores,array"`
		FirstAts      []time.Time `bun:"first_ats,array"`
		FirstTurns    []string    `bun:"first_turns,array"`
		FirstCarriers []string    `bun:"first_carriers,array"`
	}
	// A session carried on in another shard has turns in each. With several shards its sessions
	// are counted from a hash of their ids once every shard has answered, and each session's
	// first use of a tool comes back on its own to be numbered across shards
	// (turnsBeforeFirstUse).
	sharded := len(s.peers) > 0
	sessionHashes, firstUses, carriers, carriedJoin := "NULL::bigint[]", `
    NULL::bigint[] AS first_hashes, NULL::bigint[] AS first_befores,
    NULL::timestamptz[] AS first_ats, NULL::text[] AS first_turns, NULL::text[] AS first_carriers`, "", ""
	if sharded {
		sessionHashes = "array_agg(DISTINCT hashtextextended(ran.session_id, 0))"
		firstUses = `
    array_agg(hashtextextended(u.session_id, 0) ORDER BY u.session_id) AS first_hashes,
    array_agg(u.before ORDER BY u.session_id) AS first_befores,
    array_agg(u.turn_at ORDER BY u.session_id) AS first_ats,
    array_agg(u.turn_id ORDER BY u.session_id) AS first_turns,
    array_agg(CASE WHEN c.session_id IS NULL THEN '' ELSE u.session_id END ORDER BY u.session_id) AS first_carriers`
		carriers = `
carriers AS (
    SELECT DISTINCT session_id FROM first_use WHERE ` + answersAfterAnotherShard("first_use.session_id", "?3") + `
),`
		carriedJoin = "\n    LEFT JOIN carriers AS c ON c.session_id = u.session_id"
	}
	lists, err := everyShard(ctx, s, func(ctx context.Context, db bun.IDB) ([]usageRow, error) {
		var rows []usageRow
		// Turns are ordered by id as bytes after their time, as Go compares them across shards.
		err := db.NewRaw(`
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
        t.created_at AS turn_at,
        t.id AS turn_id,
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
    SELECT DISTINCT ON (tool, session_id) tool, session_id, before, turn_at, turn_id
    FROM ran WHERE before IS NOT NULL
    ORDER BY tool, session_id, before
),`+carriers+`
firsts AS (
    SELECT
        u.tool,
        COUNT(*) AS firsts,
        AVG(u.before)::double precision AS avg_turns_before_use,`+firstUses+`
    FROM first_use AS u`+carriedJoin+`
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
    `+sessionHashes+` AS session_hashes,
    COALESCE(f.firsts, 0) AS firsts,
    f.avg_turns_before_use,
    f.first_hashes, f.first_befores, f.first_ats, f.first_turns, f.first_carriers,
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
    (ran.tool, f.firsts, f.avg_turns_before_use,
        f.first_hashes, f.first_befores, f.first_ats, f.first_turns, f.first_carriers,
        o.at, o.loading, o.definition_tokens, o.summary_tokens),
    ())
ORDER BY overall DESC, calls DESC, tool ASC`,
			charsPerToken, ItemToolResult, ItemToolCall, customerID, from, to, configID,
		).Scan(ctx, &rows)
		return rows, err
	})
	if err != nil {
		return ToolUsage{}, nil, stack.Wrap(fmt.Errorf("store: tool usage: %w", err))
	}
	var crossShardBefore map[string]*float64
	if sharded {
		var uses []firstUse
		for _, rows := range lists {
			for _, row := range rows {
				for i, hash := range row.FirstHashes {
					uses = append(uses, firstUse{Tool: row.Tool, Session: hash, Before: row.FirstBefores[i],
						At: row.FirstAts[i], Turn: row.FirstTurns[i], Carrier: row.FirstCarriers[i]})
				}
			}
		}
		if crossShardBefore, err = s.turnsBeforeFirstUse(ctx, customerID, uses); err != nil {
			return ToolUsage{}, nil, err
		}
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
	sessions := map[usageKey]map[int64]bool{}
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
			if sharded {
				if sessions[k] == nil {
					sessions[k] = map[int64]bool{}
				}
				for _, hash := range row.SessionHashes {
					sessions[k][hash] = true
				}
				sums[k].Sessions = int64(len(sessions[k]))
			} else {
				sums[k].Sessions += row.Sessions
			}
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
		switch {
		case !sharded:
			usage.AvgTurnsBeforeUse = weighted(befores[k], firsts[k])
		case !k.overall:
			usage.AvgTurnsBeforeUse = crossShardBefore[k.tool]
		default:
			usage.AvgTurnsBeforeUse = nil
		}
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

// firstUse is the turn in which one session first used one tool, as one shard saw it: its
// session's hash, the turns before it on that shard, and the turn's time and id. Carrier is the
// session's id when the session was carried on to that shard from another, whose turns this
// shard cannot count, and empty otherwise.
type firstUse struct {
	Tool    string
	Session int64
	Before  int64
	At      time.Time
	Turn    string
	Carrier string
}

// earlierTurn orders turns as the tool usage query does: by time, then by id as bytes.
func earlierTurn(a, b firstUse) bool {
	return a.At.Before(b.At) || (a.At.Equal(b.At) && a.Turn < b.Turn)
}

// turnsBeforeFirstUse averages, per tool, how many turns each session took before it first
// used the tool, across every shard. A session's first use is the earliest any shard saw. Where
// that shard holds all the session's turns before it, its own count is the answer; where the
// session was carried on to it, every shard counts that session's turns before the use, which
// keeps what is sent to the shards to the carried sessions.
//
// A turn in another shard counts whatever the window's agent config: it is the same session's.
func (s *Store) turnsBeforeFirstUse(ctx context.Context, customerID string, uses []firstUse) (map[string]*float64, error) {
	type toolSession struct {
		tool    string
		session int64
	}
	earliest := map[toolSession]firstUse{}
	for _, use := range uses {
		key := toolSession{use.Tool, use.Session}
		if held, seen := earliest[key]; !seen || earlierTurn(use, held) {
			earliest[key] = use
		}
	}
	type turn struct{ session, id string }
	var sessionIDs, turnIDs []string
	var ats []time.Time
	asked := map[turn]bool{}
	for _, use := range earliest {
		if t := (turn{use.Carrier, use.Turn}); use.Carrier != "" && !asked[t] {
			asked[t] = true
			sessionIDs, turnIDs, ats = append(sessionIDs, t.session), append(turnIDs, t.id), append(ats, use.At)
		}
	}
	before := map[turn]int64{}
	if len(asked) > 0 {
		type earlier struct {
			SessionID string `bun:"session_id"`
			TurnID    string `bun:"turn_id"`
			Before    int64  `bun:"before"`
		}
		counts, err := everyShard(ctx, s, func(ctx context.Context, db bun.IDB) ([]earlier, error) {
			var read []earlier
			err := db.NewRaw(`
SELECT x.session_id, x.turn_id, COUNT(*) AS before
FROM unnest(?::text[], ?::text[], ?::timestamptz[]) AS x(session_id, turn_id, at)
JOIN agent_responses AS a ON a.session_id = x.session_id AND a.customer_id = ?
    AND a.rewound_at IS NULL
    AND (a.created_at < x.at OR (a.created_at = x.at AND a.id COLLATE "C" < x.turn_id COLLATE "C"))
GROUP BY x.session_id, x.turn_id`,
				pgdialect.Array(sessionIDs), pgdialect.Array(turnIDs), pgdialect.Array(ats), customerID,
			).Scan(ctx, &read)
			return read, err
		})
		if err != nil {
			return nil, stack.Wrap(fmt.Errorf("store: tool usage: turns before first use: %w", err))
		}
		for _, list := range counts {
			for _, row := range list {
				before[turn{row.SessionID, row.TurnID}] += row.Before
			}
		}
	}
	sums, sessions := map[string]int64{}, map[string]int64{}
	for key, use := range earliest {
		if use.Carrier != "" {
			sums[key.tool] += before[turn{use.Carrier, use.Turn}]
		} else {
			sums[key.tool] += use.Before
		}
		sessions[key.tool]++
	}
	averages := make(map[string]*float64, len(sums))
	for tool, n := range sessions {
		avg := float64(sums[tool]) / float64(n)
		averages[tool] = &avg
	}
	return averages, nil
}

// CustomerTurns counts the sessions one customer's agents took a turn in during a window,
// and those turns: every response that was not rewound, as CustomerToolUsage counts a turn.
// A configID keeps to the sessions of that agent config, and empty is every one.
func (s *Store) CustomerTurns(ctx context.Context, customerID, configID string, from, to time.Time) (SessionTurns, error) {
	if customerID == "" {
		return SessionTurns{}, stack.Wrap(errCustomerRequired)
	}
	type shardTurns struct {
		SessionTurns
		SessionHashes []int64 `bun:"session_hashes,array"`
	}
	// A session carried on in another shard has turns in each, so with several shards the
	// sessions are counted from a hash of their ids once every shard has answered.
	sessionHashes := "NULL::bigint[]"
	if len(s.peers) > 0 {
		sessionHashes = "array_agg(DISTINCT hashtextextended(ars.session_id, 0))"
	}
	counts, err := everyShard(ctx, s, func(ctx context.Context, db bun.IDB) (shardTurns, error) {
		var counts shardTurns
		q := db.NewSelect().Model((*AgentResponse)(nil)).
			ColumnExpr("COUNT(DISTINCT ars.session_id) AS sessions, COUNT(*) AS turns").
			ColumnExpr(sessionHashes+" AS session_hashes").
			Join("JOIN agent_sessions AS asn ON asn.customer_id = ars.customer_id AND asn.id = ars.session_id").
			Where("ars.customer_id = ?", customerID).
			Where("ars.created_at >= ? AND ars.created_at < ?", from, to).
			Where("ars.rewound_at IS NULL")
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
	sessions := map[int64]bool{}
	for _, shard := range counts {
		total.Sessions += shard.Sessions
		total.Turns += shard.Turns
		for _, hash := range shard.SessionHashes {
			sessions[hash] = true
		}
	}
	if len(s.peers) > 0 {
		total.Sessions = int64(len(sessions))
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
