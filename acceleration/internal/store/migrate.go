package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io/fs"
	"log/slog"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
	"github.com/uptrace/bun"

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
}

var (
	globalSet = migrationSet{name: "global", files: migrations.Global, table: "goose_global_version", marker: "agent_configs"}
	shardSet  = migrationSet{name: "shard", files: migrations.Shard, table: "goose_shard_version", marker: "requests"}
)

const (
	// legacyHead is the last version of the single history every database ran before the
	// split. A database that applied it is marked as having applied the baseline of each set
	// rather than running it.
	legacyHead int64 = 20270103090000
	// baseline is the version of each set's first migration, the schema at legacyHead.
	baseline int64 = 20270110000000
)

// Migrate applies every pending migration: the shared set to the shared database and the
// shard set to this shard's, which for a deployment of one database is the same one.
func (s *Store) Migrate(ctx context.Context) error {
	if err := s.migrate(ctx, s.global, globalSet); err != nil {
		return err
	}
	if err := s.migrate(ctx, s.shard, shardSet); err != nil {
		return fmt.Errorf("store: shard: %w", err)
	}
	// Attached here rather than in a migration of their own, so that a table added later
	// is covered by the deployment reaching it rather than by somebody remembering.
	if err := s.watchDataChanges(ctx, s.global); err != nil {
		return err
	}
	if s.Sharded() {
		return s.watchDataChanges(ctx, s.shard)
	}
	return nil
}

func (s *Store) migrate(ctx context.Context, db *bun.DB, set migrationSet) error {
	// Every shard's routers start against the shared database at once in a rollout, and
	// two goose runs on one database collide, so one migrates while the others wait.
	conn, err := db.DB.Conn(ctx)
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
	if err := adoptLegacy(ctx, conn, versions, set); err != nil {
		return err
	}
	// A provider of its own rather than goose's globals: the two sets record their versions
	// in tables of their own, and the ledger migrates in the same process.
	provider, err := goose.NewProvider("", db.DB, set.files, goose.WithStore(versions))
	if err != nil {
		return fmt.Errorf("store: migrate %s: %w", set.name, err)
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("store: migrate %s: %w", set.name, err)
	}
	return ensureUsageIndexes(ctx, conn, set)
}

// adoptLegacy marks a database that ran the single history before the split as having
// applied the set's baseline, which is the schema that history left. One that stopped short
// of its last version cannot be adopted: the baseline would claim migrations it never ran.
func adoptLegacy(ctx context.Context, conn *sql.Conn, versions database.Store, set migrationSet) error {
	var recorded, legacy bool
	err := conn.QueryRowContext(ctx, "SELECT to_regclass($1) IS NOT NULL, to_regclass($2) IS NOT NULL", set.table, set.marker).Scan(&recorded, &legacy)
	if err != nil {
		return fmt.Errorf("store: migrate %s: %w", set.name, err)
	}
	if recorded || !legacy {
		return nil
	}
	var head bool
	err = conn.QueryRowContext(ctx, `SELECT to_regclass('goose_db_version') IS NOT NULL AND EXISTS (
		SELECT 1 FROM goose_db_version WHERE version_id = $1 AND is_applied)`, legacyHead).Scan(&head)
	if err != nil {
		return fmt.Errorf("store: migrate %s: %w", set.name, err)
	}
	if !head {
		return fmt.Errorf("store: migrate %s: the database has not applied migration %d; start a release from before the migrations were split first", set.name, legacyHead)
	}
	if err := versions.CreateVersionTable(ctx, conn); err != nil {
		return fmt.Errorf("store: migrate %s: %w", set.name, err)
	}
	if err := versions.Insert(ctx, conn, database.InsertRequest{Version: 0}); err != nil {
		return fmt.Errorf("store: migrate %s: %w", set.name, err)
	}
	if err := versions.Insert(ctx, conn, database.InsertRequest{Version: baseline}); err != nil {
		return fmt.Errorf("store: migrate %s: %w", set.name, err)
	}
	slog.Default().Info("adopted a database that ran the single migration history", "set", set.name)
	return nil
}

// DropStrays drops the tables of the other set that each database kept from before the split,
// when every database carried every table: the shared set's on this shard, and the shard
// set's on the shared database unless it is some shard's own too. It returns the tables it
// dropped and those it left. A deployment of one database has none.
//
// It is not part of Migrate: a router of the release before the split, still serving during a
// rollout, reads those tables. Run it once every router is on a release with two sets.
func (s *Store) DropStrays(ctx context.Context) (dropped, left []string, err error) {
	if !s.Sharded() {
		return nil, nil, nil
	}
	if dropped, left, err = dropStrays(ctx, s.shard, globalSet); err != nil || s.globalIsShard {
		return dropped, left, err
	}
	more, kept, err := dropStrays(ctx, s.global, shardSet)
	return append(dropped, more...), append(left, kept...), err
}

// dropStrays drops the empty tables of the other set that db holds. Only an empty one is
// dropped: rows in one are a state nobody should lose to a command, so it is left for a
// person. A stray another stray points at is dropped on a later pass, once the one pointing
// is gone.
func dropStrays(ctx context.Context, db *bun.DB, other migrationSet) (dropped, left []string, err error) {
	tables, err := createdTables(other)
	if err != nil {
		return nil, nil, err
	}
	var strays []string
	for _, table := range tables {
		var present, empty bool
		if err := db.QueryRowContext(ctx, "SELECT to_regclass(?) IS NOT NULL", table).Scan(&present); err != nil {
			return nil, nil, fmt.Errorf("store: strays: %w", err)
		}
		if !present {
			continue
		}
		if err := db.NewRaw("SELECT NOT EXISTS (SELECT 1 FROM ?)", bun.Ident(table)).Scan(ctx, &empty); err != nil {
			return nil, nil, fmt.Errorf("store: strays: %w", err)
		}
		if !empty {
			left = append(left, table)
			continue
		}
		strays = append(strays, table)
	}
	for len(strays) > 0 {
		var kept []string
		for _, table := range strays {
			if _, err := db.ExecContext(ctx, "DROP TABLE ?", bun.Ident(table)); err != nil {
				kept = append(kept, table)
				continue
			}
			dropped = append(dropped, table)
		}
		if len(kept) == len(strays) {
			return dropped, append(left, kept...), nil
		}
		strays = kept
	}
	return dropped, left, nil
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
	// The placed leg a status post is about, by its token's hash, and the legs whose status
	// url is still open.
	{"shard", "call_legs_token_hash_key", "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS call_legs_token_hash_key ON call_legs (token_hash)"},
	{"shard", "call_legs_token_hash_placed_idx", "CREATE INDEX CONCURRENTLY IF NOT EXISTS call_legs_token_hash_placed_idx ON call_legs (placed_at) " +
		"WHERE token_hash IS NOT NULL"},
	// The queued image jobs of each kind, in the order a claim takes them.
	{"global", "image_jobs_undecided", "CREATE INDEX CONCURRENTLY IF NOT EXISTS image_jobs_undecided ON image_jobs (next_attempt_at, id) " +
		"WHERE state = 'queued' AND message = ''"},
	{"global", "image_jobs_decided", "CREATE INDEX CONCURRENTLY IF NOT EXISTS image_jobs_decided ON image_jobs (next_attempt_at, id) " +
		"WHERE state = 'queued' AND message <> ''"},
	// A session's calls, which deleting the session sets free.
	{"shard", "calls_session_started_idx", "CREATE INDEX CONCURRENTLY IF NOT EXISTS calls_session_started_idx ON calls (session_id, started_at DESC) " +
		"WHERE session_id IS NOT NULL"},
	// A response's alternatives, the responses sharing its parent.
	{"shard", "agent_responses_parent_idx", "CREATE INDEX CONCURRENTLY IF NOT EXISTS agent_responses_parent_idx ON agent_responses (session_id, parent_response_id)"},
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
	return migratedTables(set, false)
}

// createdTables are the tables the set's migrations ever create, a later one dropping or
// renaming them included, which a database of the other set may still hold from before the
// split.
func createdTables(set migrationSet) ([]string, error) {
	return migratedTables(set, true)
}

// migratedTables reads the set's tables from its migrations' Up SQL in order. ever keeps the
// tables a later migration drops, and the old name of one it renames.
func migratedTables(set migrationSet, ever bool) ([]string, error) {
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
					if !ever {
						tables = slices.DeleteFunc(tables, func(held string) bool { return held == table })
					}
				case renamed != "":
					if i := slices.Index(tables, table); i >= 0 && !ever {
						tables[i] = renamed
					} else if i >= 0 && !slices.Contains(tables, renamed) {
						tables = append(tables, renamed)
					}
				}
			}
		}
	}
	slices.Sort(tables)
	return tables, nil
}
