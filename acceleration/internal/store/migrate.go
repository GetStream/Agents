package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io/fs"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"

	"github.com/GetStream/Vision-Agents/acceleration/migrations"
)

// migrationSet is one of the two migration histories: the shared database's and a shard's.
// A deployment of one database runs both on it, the shared one first.
type migrationSet struct {
	name  string
	files fs.FS
	// table is where goose records the versions of this set applied.
	table string
	// marker is a table of this set that every database which ran the single history before
	// the split holds, so such a database is told from a new one.
	marker string
	// squashed is the last version of the history squashed into the set's initial migration,
	// whose schema is the one that history left.
	squashed int64
}

var (
	globalSet = migrationSet{name: "global", files: migrations.Global, table: "goose_global_version", marker: "agent_configs", squashed: 20270124093000}
	shardSet  = migrationSet{name: "shard", files: migrations.Shard, table: "goose_shard_version", marker: "requests", squashed: 20270126000000}
)

// Migrate applies every pending migration: the shared set to the shared database and the
// shard set to this shard's, which for a deployment of one database is the same one. It
// connects with the migrate DSNs, whose role owns the schema.
func (s *Store) Migrate(ctx context.Context) error {
	if err := s.migrate(ctx, s.globalDSN, globalSet); err != nil {
		return err
	}
	if err := s.migrate(ctx, s.shardDSN, shardSet); err != nil {
		return fmt.Errorf("store: shard: %w", err)
	}
	return nil
}

// CheckMigrated refuses a schema Migrate would still change in a way this release needs: a
// migration, an index or a change trigger of its own missing, or a table or sequence the
// serving role is not granted. It changes nothing and runs on the serving DSNs, so a router
// whose role does not own the schema starts with it. The schema a later release's expand-only
// migrations leave passes, so this one's routers restart during its rollout.
func (s *Store) CheckMigrated(ctx context.Context) error {
	if err := s.checkMigrated(ctx, s.global, globalSet); err != nil {
		return err
	}
	if err := s.checkMigrated(ctx, s.shard, shardSet); err != nil {
		return fmt.Errorf("store: shard: %w", err)
	}
	return nil
}

func (s *Store) checkMigrated(ctx context.Context, db *bun.DB, set migrationSet) error {
	conn, err := db.DB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("store: check %s: %w", set.name, err)
	}
	defer conn.Close() //nolint:errcheck // back to the pool
	provider, err := goose.NewProvider(goose.DialectPostgres, db.DB, set.files)
	if err != nil {
		return fmt.Errorf("store: check %s: %w", set.name, err)
	}
	// Without USAGE on it no table is found, and the database would read as never migrated.
	var usable bool
	if err := conn.QueryRowContext(ctx, "SELECT current_schema() IS NOT NULL").Scan(&usable); err != nil {
		return fmt.Errorf("store: check %s: %w", set.name, err)
	}
	if !usable {
		return errors.New("store: the serving role lacks USAGE on the schema")
	}
	applied, err := checkHistory(ctx, conn, provider, set)
	if err != nil {
		return err
	}
	sources := provider.ListSources()
	for _, source := range sources {
		if !applied[source.Version] {
			return fmt.Errorf("store: the database has not applied %s migration %d: run router migrate with this release", set.name, source.Version)
		}
		delete(applied, source.Version)
	}
	// What is left past this release's newest is a later release's.
	var later bool
	for version := range applied {
		later = later || version > sources[len(sources)-1].Version
	}
	for _, index := range usageIndexes {
		if index.set != set.name {
			continue
		}
		valid, _, err := indexState(ctx, conn, index.name)
		if err != nil {
			return err
		}
		if !valid {
			return fmt.Errorf("store: the database has no valid index %s: run router migrate with this release", index.name)
		}
	}
	if err := checkGranted(ctx, db, set); err != nil {
		return err
	}
	return s.checkDataChangesWatched(ctx, set, later)
}

// checkGranted refuses a database whose tables and sequences the connected role may not all
// write. A table a later release dropped is none of them.
func checkGranted(ctx context.Context, db *bun.DB, set migrationSet) error {
	tables, err := setTables(set)
	if err != nil {
		return fmt.Errorf("store: check %s: %w", set.name, err)
	}
	// Each privilege on its own: a list is granted when any one of it is.
	var missing []string
	if err := db.QueryRowContext(ctx, `
WITH held AS (
  SELECT to_regclass(name) AS oid FROM unnest(?::text[]) AS name
), lacking AS (
  SELECT privilege || ' on table ' || held.oid::text AS what
  FROM held, unnest(ARRAY['SELECT', 'INSERT', 'UPDATE', 'DELETE']) AS privilege
  WHERE NOT has_table_privilege(held.oid, privilege)
  UNION ALL
  SELECT privilege || ' on sequence ' || c.oid::regclass::text
  FROM held
  JOIN pg_depend d ON d.refclassid = 'pg_class'::regclass AND d.refobjid = held.oid AND d.classid = 'pg_class'::regclass
  JOIN pg_class c ON c.oid = d.objid AND c.relkind = 'S',
  unnest(ARRAY['USAGE', 'SELECT', 'UPDATE']) AS privilege
  WHERE CASE WHEN c.relkind = 'S' THEN NOT has_sequence_privilege(c.oid, privilege) END
)
SELECT COALESCE(array_agg(what ORDER BY what), '{}') FROM lacking`,
		pgdialect.Array(append(tables, changeLogTables...))).Scan(pgdialect.Array(&missing)); err != nil {
		return fmt.Errorf("store: check the grants of %s: %w", set.name, err)
	}
	if len(missing) > 0 {
		return fmt.Errorf("store: the serving role lacks %s", strings.Join(missing, ", "))
	}
	return nil
}

func (s *Store) migrate(ctx context.Context, dsn string, set migrationSet) error {
	// A pool of its own with no read timeout: the serving pool gives up on a statement after
	// the driver's 10 s, and a backfill or an index build on a large table takes longer. The
	// server would go on running it, holding its locks, while the router restarted into it.
	connector, err := connectorFor(dsn, pgdriver.WithReadTimeout(0))
	if err != nil {
		return err
	}
	db := sql.OpenDB(connector)
	defer db.Close() //nolint:errcheck // a pool the migration is done with
	// Every shard's routers start against the shared database at once in a rollout, and
	// two goose runs on one database collide, so one migrates while the others wait.
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("store: migrate: %w", err)
	}
	locked := false
	defer func() {
		if !locked {
			_ = conn.Close()
			return
		}
		// Released by hand: closing hands the connection back to the pool, session and lock
		// intact. A failed unlock ends the session instead, which releases it.
		if _, err := conn.ExecContext(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock(hashtextextended('router.migrate', 0))"); err != nil {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
		_ = conn.Close()
	}()
	if err := lockMigrations(ctx, conn); err != nil {
		return err
	}
	locked = true

	versions, err := database.NewStore(database.DialectPostgres, set.table)
	if err != nil {
		return fmt.Errorf("store: migrate %s: %w", set.name, err)
	}
	// A provider of its own rather than goose's globals: the two sets record their versions
	// in tables of their own, and the ledger migrates in the same process.
	provider, err := goose.NewProvider("", db, set.files, goose.WithStore(versions))
	if err != nil {
		return fmt.Errorf("store: migrate %s: %w", set.name, err)
	}
	if _, err := checkHistory(ctx, conn, provider, set); err != nil {
		return err
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("store: migrate %s: %w", set.name, err)
	}
	// Attached here rather than in a migration of their own, so that a table added later
	// is covered by the deployment reaching it rather than by somebody remembering. Before the
	// indexes, whose build can be long.
	if err := s.watchDataChanges(ctx, dsn, set); err != nil {
		return err
	}
	return ensureUsageIndexes(ctx, conn, set)
}

// checkHistory refuses a database whose history the set's migrations no longer hold, which
// the initial migration would otherwise be wrong about: one that ran the single history from
// before the split, on which it would fail to create tables already there, and one that ran
// part of the history squashed into it, whose missing migrations it would claim as applied.
// One that ran the whole history holds the initial migration's schema and starts as it is, and
// so does one cleaned of it once deployed, which holds no version of it. A version after the
// squashed history with no file is a later release's, from before a rollback to this one.
// It returns the versions applied.
func checkHistory(ctx context.Context, conn *sql.Conn, provider *goose.Provider, set migrationSet) (map[int64]bool, error) {
	var recorded, legacy bool
	err := conn.QueryRowContext(ctx, "SELECT to_regclass($1) IS NOT NULL, to_regclass($2) IS NOT NULL", set.table, set.marker).Scan(&recorded, &legacy)
	if err != nil {
		return nil, fmt.Errorf("store: migrate %s: %w", set.name, err)
	}
	if !recorded {
		if legacy {
			return nil, fmt.Errorf("store: migrate %s: the database ran the migrations from before they were split; recreate it, or start a release from before the history was squashed first", set.name)
		}
		return nil, nil
	}
	known := map[int64]bool{0: true}
	for _, source := range provider.ListSources() {
		known[source.Version] = true
	}
	// A version counts by its latest row: rolling one back adds a row that is not applied.
	rows, err := conn.QueryContext(ctx, `SELECT version_id FROM (
		SELECT DISTINCT ON (version_id) version_id, is_applied FROM `+set.table+` ORDER BY version_id, id DESC) AS latest
		WHERE is_applied`)
	if err != nil {
		return nil, fmt.Errorf("store: migrate %s: %w", set.name, err)
	}
	defer rows.Close()
	applied := map[int64]bool{}
	var unknown, squashed bool
	for rows.Next() {
		var version int64
		if err := rows.Scan(&version); err != nil {
			return nil, fmt.Errorf("store: migrate %s: %w", set.name, err)
		}
		applied[version] = true
		unknown = unknown || (!known[version] && version < set.squashed)
		squashed = squashed || version == set.squashed
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: migrate %s: %w", set.name, err)
	}
	if unknown && !squashed {
		return nil, fmt.Errorf("store: migrate %s: the database has not applied migration %d, the last of the history squashed into the initial one; recreate it, or start a release from before the history was squashed first", set.name, set.squashed)
	}
	return applied, nil
}

// migrateLockFirst and migrateLockMost are the pause between two tries at the migrate lock:
// the first, doubled after each miss up to the most, which bounds how long a router idles
// after the one migrating is done.
const (
	migrateLockFirst = 20 * time.Millisecond
	migrateLockMost  = 500 * time.Millisecond
)

// lockMigrations takes the migrate lock on conn, trying it until it is free. A router waiting
// in a blocking pg_advisory_lock holds a snapshot all the while, and CREATE INDEX CONCURRENTLY
// in the migration the holder runs waits for every older snapshot to end, so the two would
// wait on each other forever. Between two tries the waiter holds none.
func lockMigrations(ctx context.Context, conn *sql.Conn) error {
	pause := migrateLockFirst
	for {
		var locked bool
		if err := conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock(hashtextextended('router.migrate', 0))").Scan(&locked); err != nil {
			// The lock may have been granted with the answer lost; dropping the connection
			// ends the session, which releases it.
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			return fmt.Errorf("store: migrate: lock: %w", err)
		}
		if locked {
			return nil
		}
		timer := time.NewTimer(pause)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("store: migrate: wait for lock: %w", context.Cause(ctx))
		case <-timer.C:
		}
		pause = min(2*pause, migrateLockMost)
	}
}

// usageIndexes are built CONCURRENTLY, so building them stops no write. A build that fails
// leaves an invalid index behind, which the migration's IF NOT EXISTS then skips once goose
// has recorded it, and one dropped by hand is never built again by the migration. Writing
// usage needs requests_usage_id_idx: ON CONFLICT (usage_id) fails without it.
var usageIndexes = []struct{ set, name, create string }{
	{"shard", "requests_usage_id_idx", "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS requests_usage_id_idx ON requests (usage_id)"},
	{"shard", "requests_to_ship_idx", "CREATE INDEX CONCURRENTLY IF NOT EXISTS requests_to_ship_idx ON requests (started_at) WHERE to_ship"},
	// What a subject spent, summed with no usage service.
	{"shard", "requests_key_spend_idx", "CREATE INDEX CONCURRENTLY IF NOT EXISTS requests_key_spend_idx ON requests (customer_id, api_key, started_at) " +
		"INCLUDE (cost_micros) WHERE billable"},
	{"shard", "requests_user_spend_idx", "CREATE INDEX CONCURRENTLY IF NOT EXISTS requests_user_spend_idx ON requests (customer_id, user_id, started_at) " +
		"INCLUDE (cost_micros) WHERE billable AND user_id <> ''"},
	{"shard", "requests_category_spend_idx", "CREATE INDEX CONCURRENTLY IF NOT EXISTS requests_category_spend_idx ON requests (customer_id, started_at) " +
		"INCLUDE (cost_micros, usage_categories) WHERE billable AND usage_categories <> '{}'"},
	// The rows naming a grant, which a reconcile counts again.
	{"shard", "requests_limit_grants_started_idx", "CREATE INDEX CONCURRENTLY IF NOT EXISTS requests_limit_grants_started_idx ON requests (started_at, id) " +
		"WHERE limit_grants <> '{}'"},
	// What a copied agent log is known by: an import resolves its conflicts on it.
	{"global", "agent_logs_source_idx", "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS agent_logs_source_idx ON agent_logs (customer_id, source_id) " +
		"WHERE source_id IS NOT NULL"},
	// What a link's join finds one omni-channel's rows of an agent by.
	{"global", "contact_map_conversation", "CREATE INDEX CONCURRENTLY IF NOT EXISTS contact_map_conversation ON contact_map (customer_id, config_id, conversation_id)"},
	// The rows a link set aside, by their number.
	{"global", "contact_map_retired", "CREATE INDEX CONCURRENTLY IF NOT EXISTS contact_map_retired ON contact_map " +
		"(customer_id, (regexp_replace(address, '^retired:[^:]*:', ''))) WHERE address LIKE 'retired:%'"},
	// The leg digits are pressed on.
	{"shard", "call_legs_vendor_call_idx", "CREATE INDEX CONCURRENTLY IF NOT EXISTS call_legs_vendor_call_idx ON call_legs (vendor_call_id) " +
		"WHERE vendor_call_id IS NOT NULL"},
	// The legs a join delivered after its call ended revives.
	{"shard", "call_legs_unanswered_idx", "CREATE INDEX CONCURRENTLY IF NOT EXISTS call_legs_unanswered_idx ON call_legs (call_type, call_id) " +
		"WHERE direction = 'outbound' AND end_reason = 'unanswered'"},
	// A transfer leg, by its call and the vendor's id for it, which a write tried again hits.
	{"shard", "call_legs_transfer_idx", "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS call_legs_transfer_idx ON call_legs " +
		"(call_type, call_id, COALESCE(stream_app_pk, 0), vendor_call_id) WHERE placed_at IS NULL AND vendor_call_id IS NOT NULL"},
	// The placed leg a status post is about, by its token's hash, and the legs whose status
	// url is still open.
	{"shard", "call_legs_token_hash_key", "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS call_legs_token_hash_key ON call_legs (token_hash)"},
	{"shard", "call_legs_token_hash_placed_idx", "CREATE INDEX CONCURRENTLY IF NOT EXISTS call_legs_token_hash_placed_idx ON call_legs (placed_at) " +
		"WHERE token_hash IS NOT NULL"},
	// A session's log lines, newest first.
	{"global", "agent_logs_session_order", "CREATE INDEX CONCURRENTLY IF NOT EXISTS agent_logs_session_order ON agent_logs (customer_id, session_id, id DESC)"},
	// A campaign's attempts.
	{"global", "campaign_attempts_campaign_idx", "CREATE INDEX CONCURRENTLY IF NOT EXISTS campaign_attempts_campaign_idx ON campaign_attempts (campaign_id)"},
	// The queued image jobs of each kind, in the order a claim takes them.
	{"global", "image_jobs_undecided", "CREATE INDEX CONCURRENTLY IF NOT EXISTS image_jobs_undecided ON image_jobs (next_attempt_at, id) " +
		"WHERE state = 'queued' AND message = ''"},
	{"global", "image_jobs_decided", "CREATE INDEX CONCURRENTLY IF NOT EXISTS image_jobs_decided ON image_jobs (next_attempt_at, id) " +
		"WHERE state = 'queued' AND message <> ''"},
	// A session's calls, which deleting the session sets free.
	{"shard", "calls_session_started_idx", "CREATE INDEX CONCURRENTLY IF NOT EXISTS calls_session_started_idx ON calls (session_id, started_at DESC) " +
		"WHERE session_id IS NOT NULL"},
	// An agent's and a campaign's calls.
	{"shard", "calls_customer_agent_idx", "CREATE INDEX CONCURRENTLY IF NOT EXISTS calls_customer_agent_idx ON calls (customer_id, agent_id, started_at DESC)"},
	{"shard", "calls_customer_campaign_idx", "CREATE INDEX CONCURRENTLY IF NOT EXISTS calls_customer_campaign_idx ON calls (customer_id, campaign_id, started_at DESC) " +
		"WHERE campaign_id IS NOT NULL"},
	// A response's alternatives, the responses sharing its parent.
	{"shard", "agent_responses_parent_idx", "CREATE INDEX CONCURRENTLY IF NOT EXISTS agent_responses_parent_idx ON agent_responses (session_id, parent_response_id)"},
	// An app's end user's call legs, which forgetting them rewrites.
	{"shard", "call_legs_user_idx", "CREATE INDEX CONCURRENTLY IF NOT EXISTS call_legs_user_idx ON call_legs (customer_id, user_id) " +
		"WHERE user_id <> ''"},
	// A customer's session's call episode.
	{"global", "episodes_customer_call_session", "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS episodes_customer_call_session " +
		"ON episodes (customer_id, session_id)"},
	// The recorded changes of requests naming an end user, by request, which renaming them rewrites.
	{"global", "data_changes_user_idx", "CREATE INDEX CONCURRENTLY IF NOT EXISTS data_changes_user_idx ON data_changes " +
		"(customer_id, (payload->>'user_id'), (payload->>'id')) WHERE table_name = 'requests' AND payload->>'user_id' IS NOT NULL"},
	// The recorded changes of a task and of its runs, which deleting them rewrites.
	{"global", "data_changes_tasks_idx", "CREATE INDEX CONCURRENTLY IF NOT EXISTS data_changes_tasks_idx ON data_changes " +
		"(customer_id, (COALESCE(payload->>'task_id', payload->>'id')), (payload->>'id')) " +
		"WHERE table_name IN ('scheduled_tasks', 'scheduled_task_runs') AND op <> 'delete'"},
	// Who holds a number.
	{"global", "phone_numbers_e164_held_idx", "CREATE INDEX CONCURRENTLY IF NOT EXISTS phone_numbers_e164_held_idx " +
		"ON phone_numbers (e164) WHERE released_at IS NULL"},
}

// ensureUsageIndexes builds each usage index of the set that is missing, dropping one a failed
// build left invalid first. A build that fails again fails Migrate, so the router does not
// start and fail every write; its next start tries again.
func ensureUsageIndexes(ctx context.Context, conn *sql.Conn, set migrationSet) error {
	for _, index := range usageIndexes {
		if index.set != set.name {
			continue
		}
		valid, found, err := indexState(ctx, conn, index.name)
		if err != nil {
			return err
		}
		if found && valid {
			continue
		}
		if found {
			if _, err := conn.ExecContext(ctx, "DROP INDEX CONCURRENTLY IF EXISTS "+index.name); err != nil {
				return fmt.Errorf("store: migrate: drop invalid %s: %w", index.name, err)
			}
		}
		if _, err := conn.ExecContext(ctx, index.create); err != nil {
			return fmt.Errorf("store: migrate: build %s: %w", index.name, err)
		}
		if valid, found, err = indexState(ctx, conn, index.name); err != nil {
			return err
		}
		if !found || !valid {
			return fmt.Errorf("store: migrate: %s did not build", index.name)
		}
	}
	return nil
}

// indexState reports whether the index named is on the search path, and if it is, whether
// it is valid.
func indexState(ctx context.Context, conn *sql.Conn, name string) (valid, found bool, err error) {
	var state sql.NullBool
	err = conn.QueryRowContext(ctx, "SELECT (SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass($1))", name).Scan(&state)
	if err != nil {
		return false, false, fmt.Errorf("store: migrate: check %s: %w", name, err)
	}
	return state.Bool, state.Valid, nil
}

// changeLogTables are kept by both sets: a row's change is recorded in the database it is
// written to, under the capture starts' lock.
var changeLogTables = []string{"data_change_capture", "data_change_starts", "data_changes"}

// tableChange is a statement of a migration's Up that creates, drops or renames tables.
var tableChange = regexp.MustCompile(`(?i)(CREATE TABLE (?:IF NOT EXISTS )?|DROP TABLE (?:IF EXISTS )?|ALTER TABLE (?:IF EXISTS )?)([a-z_0-9]+(?:\s*,\s*[a-z_0-9]+)*)(?: RENAME TO ([a-z_0-9]+))?`)

// setTables are the tables the set's migrations leave, read from their Up SQL in order. A
// table is created by one set alone, except the change log both keep.
func setTables(set migrationSet) ([]string, error) {
	files, err := fs.Glob(set.files, "*.sql")
	if err != nil {
		return nil, err
	}
	var tables []string
	for _, name := range files {
		raw, err := fs.ReadFile(set.files, name)
		if err != nil {
			return nil, err
		}
		up, _, _ := strings.Cut(string(raw), "-- +goose Down")
		for _, match := range tableChange.FindAllStringSubmatch(up, -1) {
			verb, renamed := strings.ToUpper(match[1]), match[3]
			for _, table := range strings.Split(match[2], ",") {
				table = strings.TrimSpace(table)
				switch {
				case strings.HasPrefix(verb, "CREATE"):
					if !slices.Contains(tables, table) && !slices.Contains(changeLogTables, table) {
						tables = append(tables, table)
					}
				case strings.HasPrefix(verb, "DROP"):
					tables = slices.DeleteFunc(tables, func(held string) bool { return held == table })
				case renamed != "":
					if i := slices.Index(tables, table); i >= 0 {
						tables[i] = renamed
					}
				}
			}
		}
	}
	slices.Sort(tables)
	return tables, nil
}
