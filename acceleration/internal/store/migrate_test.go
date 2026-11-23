//go:build integration

package store

import (
	"context"
	"database/sql"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
)

func (s *ShardedStoreSuite) TestTheSchemaFilesAreCurrent() {
	global, shard, err := s.store.Schema(s.ctx)
	s.Require().NoError(err)

	for name, described := range map[string]string{"global.sql": global, "shard.sql": shard} {
		committed, err := os.ReadFile(filepath.Join("..", "..", "schema", name))
		s.Require().NoError(err)
		s.Equal(string(committed), described, "schema/%s is out of date: run go run ./cmd/schema in acceleration/", name)
	}
}

func (s *ShardedStoreSuite) TestEachDatabaseHoldsOnlyItsOwnSet() {
	var configs, requests bool
	s.Require().NoError(s.store.shard.QueryRowContext(s.ctx, "SELECT to_regclass('agent_configs') IS NOT NULL").Scan(&configs))
	s.Require().NoError(s.store.global.QueryRowContext(s.ctx, "SELECT to_regclass('requests') IS NOT NULL").Scan(&requests))
	s.False(configs, "a shard keeps no agent configs")
	s.False(requests, "the shared database keeps no requests")
}

// A shard that ran the single history kept every table. The empty ones of the shared set are
// dropped; one holding rows is left for a person.
func (s *ShardedStoreSuite) TestAShardDropsTheEmptyTablesOfTheSharedSet() {
	global, shard := s.emptyDatabase("strays_global"), s.emptyDatabase("strays_shard")
	everything, err := Open(shard)
	s.Require().NoError(err)
	s.Require().NoError(everything.Migrate(s.ctx))
	_, err = everything.global.ExecContext(s.ctx, "INSERT INTO skills (id, customer_id, name, description, instructions) VALUES ('kept', 'app', 'kept', '', '')")
	s.Require().NoError(err)
	s.Require().NoError(everything.Close())

	split, err := OpenShards(Options{DSN: global, ShardDSN: shard})
	s.Require().NoError(err)
	defer split.Close()
	s.Require().NoError(split.Migrate(s.ctx))
	var before bool
	s.Require().NoError(split.shard.QueryRowContext(s.ctx, "SELECT to_regclass('agent_configs') IS NOT NULL").Scan(&before))
	s.True(before, "migrating drops nothing a router of the previous release may still read")

	dropped, left, err := split.DropStrays(s.ctx)
	s.Require().NoError(err)
	s.Contains(dropped, "agent_configs")
	s.Equal([]string{"skills"}, left)

	var configs, skills, requests bool
	s.Require().NoError(split.shard.QueryRowContext(s.ctx,
		"SELECT to_regclass('agent_configs') IS NOT NULL, to_regclass('skills') IS NOT NULL, to_regclass('requests') IS NOT NULL").
		Scan(&configs, &skills, &requests))
	s.False(configs, "an empty table of the shared set is dropped")
	s.True(skills, "a table of the shared set holding rows is kept")
	s.True(requests, "the shard's own tables stay")
}

// record_data_change locks the one data_change_starts row on a repeatable read write, on either
// set, so a new database of each starts with it.
func (s *ShardedStoreSuite) TestARepeatableReadWriteOnANewDatabaseIsRecorded() {
	split, err := OpenShards(Options{DSN: s.emptyDatabase("fresh_global"), ShardDSN: s.emptyDatabase("fresh_shard")})
	s.Require().NoError(err)
	defer split.Close()
	s.Require().NoError(split.Migrate(s.ctx))

	for name, db := range map[string]*bun.DB{"shared": split.global, "shard": split.shard} {
		var starts int
		s.Require().NoError(db.QueryRowContext(s.ctx, "SELECT count(*) FROM data_change_starts").Scan(&starts))
		s.Equal(1, starts, "the %s database starts with its data_change_starts row", name)
	}
	err = split.shard.RunInTx(s.ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead}, func(ctx context.Context, tx bun.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO call_bridges (token_hash, customer_id, call_id, vendor, trunk_uri, expires_at)
			VALUES ('t', 'app', 'call', 'telnyx', 'sip:x', now() + interval '1 minute')`)
		return err
	})
	s.NoError(err, "a repeatable read write on a new shard")
}

// legacy turns the suite's database back into one that ran the single history up to version:
// the baseline's schema, recorded in the single history's table.
func (s *StoreSuite) legacy(dsn string, version int64) *Store {
	legacy, err := Open(dsn)
	s.Require().NoError(err)
	s.upTo(legacy, globalSet, baseline)
	s.upTo(legacy, shardSet, baseline)
	_, err = legacy.global.ExecContext(s.ctx, `DROP TABLE goose_global_version, goose_shard_version;
		CREATE TABLE goose_db_version (id serial PRIMARY KEY, version_id bigint NOT NULL, is_applied boolean NOT NULL, tstamp timestamp DEFAULT now());
		INSERT INTO goose_db_version (version_id, is_applied) VALUES (0, true), (?, true)`, version)
	s.Require().NoError(err)
	return legacy
}

func (s *StoreSuite) TestADatabaseThatRanTheSingleHistoryIsAdopted() {
	legacy := s.legacy(s.emptyDatabase("adopted"), legacyHead)
	defer legacy.Close()

	s.Require().NoError(legacy.Migrate(s.ctx))

	for _, table := range []string{"goose_global_version", "goose_shard_version"} {
		var applied bool
		s.Require().NoError(legacy.global.QueryRowContext(s.ctx,
			"SELECT EXISTS (SELECT 1 FROM "+table+" WHERE version_id = ? AND is_applied)", baseline).Scan(&applied))
		s.True(applied, table)
	}
}

func (s *StoreSuite) TestADatabaseShortOfTheSingleHistorysLastMigrationIsRefused() {
	legacy := s.legacy(s.emptyDatabase("behind"), legacyHead-1)
	defer legacy.Close()

	s.ErrorContains(legacy.Migrate(s.ctx), "has not applied migration 20270103090000")
}

// A database that ran a migration past the single history's last holds a schema the baseline
// does not describe, so it is not adopted as though it were the baseline's.
func (s *StoreSuite) TestADatabaseThatRanAMigrationNoSetHasIsRefused() {
	legacy := s.legacy(s.emptyDatabase("unknown"), legacyHead)
	defer legacy.Close()
	_, err := legacy.global.ExecContext(s.ctx, "INSERT INTO goose_db_version (version_id, is_applied) VALUES (20270105090000, true)")
	s.Require().NoError(err)

	s.ErrorContains(legacy.Migrate(s.ctx), "applied migrations 20270105090000")
}

// shardTo opens an empty database of the suite's own with the shared set applied and the shard
// set up to version, the order Migrate runs them in.
func (s *StoreSuite) shardTo(name string, version int64) *Store {
	opened, err := Open(s.emptyDatabase(name))
	s.Require().NoError(err)
	s.upTo(opened, globalSet, math.MaxInt64)
	s.upTo(opened, shardSet, version)
	return opened
}

// The backfill runs in batches outside a transaction, so one stopped part way keeps the batches
// before it, and the next start gives every session its one call.
func (s *StoreSuite) TestEverySessionKeepsItsCallThroughTheBatchedBackfill() {
	split := s.shardTo("sessions_calls", 20270127000000-1)
	defer split.Close()
	_, err := split.global.ExecContext(s.ctx, `
INSERT INTO agent_sessions (id, customer_id, agent_id, call_id, call_type, state)
VALUES ('voice', 'app', 'agent', 'stream-call', 'default', 'open'), ('written', 'app', 'agent', 'later-call', 'default', 'open');
INSERT INTO agent_sessions (id, customer_id, agent_id, state)
SELECT 'text-' || lpad(n::text, 4, '0'), 'app', 'agent', 'open' FROM generate_series(1, 2500) AS n;
INSERT INTO calls (id, customer_id, call_id, agent_id) VALUES ('written', 'app', '', 'agent'), ('text-1500', 'app', '', 'agent')`)
	s.Require().NoError(err)

	// A query holding a call of the second batch stops the backfill at its lock timeout.
	held, err := split.shard.BeginTx(s.ctx, nil)
	s.Require().NoError(err)
	defer held.Rollback() //nolint:errcheck // a no-op once rolled back
	_, err = held.ExecContext(s.ctx, "SELECT 1 FROM calls WHERE id = 'text-1500' FOR UPDATE")
	s.Require().NoError(err)
	s.Require().ErrorContains(split.Migrate(s.ctx), "lock timeout")
	var committed int
	s.Require().NoError(split.shard.QueryRowContext(s.ctx, "SELECT count(*) FROM calls WHERE session_id = id").Scan(&committed))
	s.Equal(1000, committed, "the first batch is kept")
	// A call already given its session is not written again by the next start.
	_, err = split.shard.ExecContext(s.ctx, "UPDATE calls SET call_type = 'kept' WHERE id = 'text-0001'")
	s.Require().NoError(err)
	s.Require().NoError(held.Rollback())

	s.Require().NoError(split.Migrate(s.ctx))

	var kept string
	s.Require().NoError(split.shard.QueryRowContext(s.ctx, "SELECT call_type FROM calls WHERE id = 'text-0001'").Scan(&kept))
	s.Equal("kept", kept)
	var calls, sessions int
	s.Require().NoError(split.shard.QueryRowContext(s.ctx, "SELECT count(*) FROM calls WHERE session_id = id").Scan(&calls))
	s.Require().NoError(split.shard.QueryRowContext(s.ctx, "SELECT count(*) FROM agent_sessions").Scan(&sessions))
	s.Equal(2502, sessions)
	s.Equal(sessions, calls, "every session, past the first batch included, has its call")
	var voice, written string
	s.Require().NoError(split.shard.QueryRowContext(s.ctx, "SELECT call_id || '/' || call_type FROM calls WHERE id = 'voice'").Scan(&voice))
	s.Require().NoError(split.shard.QueryRowContext(s.ctx, "SELECT call_id FROM calls WHERE id = 'written'").Scan(&written))
	s.Equal("stream-call/default", voice)
	s.Equal("later-call", written, "a call row written before gains its session's Stream call")
	var validated bool
	s.Require().NoError(split.shard.QueryRowContext(s.ctx, "SELECT convalidated FROM pg_constraint WHERE conname = 'calls_session_id_fkey'").Scan(&validated))
	s.True(validated, "the session key is checked on every row")

	// A database that ran these as one migration, or stopped part way, runs them again.
	_, err = split.shard.ExecContext(s.ctx, "DELETE FROM goose_shard_version WHERE version_id >= 20270127000000")
	s.Require().NoError(err)
	s.Require().NoError(split.Migrate(s.ctx))
}

// A router of the release before may write a session after the backfill passed it; the
// session still has its call once the old columns are gone, and so does one it voiced after the
// backfill gave it a call with no Stream call.
func (s *StoreSuite) TestASessionWrittenAfterTheBackfillKeepsItsCall() {
	split := s.shardTo("sessions_late", 20270127000000-1)
	defer split.Close()
	_, err := split.global.ExecContext(s.ctx, `INSERT INTO agent_sessions (id, customer_id, agent_id, state) VALUES ('voiced', 'app', 'agent', 'open')`)
	s.Require().NoError(err)
	s.upTo(split, shardSet, 20270127000000)
	_, err = split.global.ExecContext(s.ctx, `UPDATE agent_sessions SET call_id = 'voiced-call', call_type = 'default' WHERE id = 'voiced';
INSERT INTO agent_sessions (id, customer_id, agent_id, call_id, call_type, state)
VALUES ('late', 'app', 'agent', 'stream-call', 'default', 'open'), ('late-text', 'app', 'agent', NULL, NULL, 'open')`)
	s.Require().NoError(err)

	s.Require().NoError(split.Migrate(s.ctx))

	var late, text, voiced string
	s.Require().NoError(split.shard.QueryRowContext(s.ctx, "SELECT call_id || '/' || call_type FROM calls WHERE session_id = 'late'").Scan(&late))
	s.Require().NoError(split.shard.QueryRowContext(s.ctx, "SELECT call_id || '/' || call_type FROM calls WHERE session_id = 'late-text'").Scan(&text))
	s.Require().NoError(split.shard.QueryRowContext(s.ctx, "SELECT call_id || '/' || call_type FROM calls WHERE session_id = 'voiced'").Scan(&voiced))
	s.Equal("stream-call/default", late)
	s.Equal("/", text)
	s.Equal("voiced-call/default", voiced)
}

// An index built concurrently waits for every write open on its table, which may be longer than
// the serving pool's read timeout; the migration still finishes.
func (s *StoreSuite) TestAMigrationThatWaitsPastTheReadTimeoutFinishes() {
	split := s.shardTo("migrate_waits", 20270127000100-1)
	defer split.Close()
	writing, err := split.shard.BeginTx(s.ctx, nil)
	s.Require().NoError(err)
	defer writing.Rollback() //nolint:errcheck // a no-op once rolled back
	_, err = writing.ExecContext(s.ctx, "UPDATE calls SET summary = summary WHERE false")
	s.Require().NoError(err)
	ended := time.AfterFunc(11*time.Second, func() { _ = writing.Rollback() })
	defer ended.Stop()

	s.NoError(split.Migrate(s.ctx))
}

// A session a router of the release before commits while the old columns are dropped keeps its
// call: the drop waits for it, and the catch-up after the wait sees it.
func (s *StoreSuite) TestASessionWrittenWhileTheOldColumnsAreDroppedKeepsItsCall() {
	split := s.shardTo("sessions_racing", 20270127000500-1)
	defer split.Close()
	writing, err := split.global.BeginTx(s.ctx, nil)
	s.Require().NoError(err)
	defer writing.Rollback() //nolint:errcheck // a no-op once committed
	_, err = writing.ExecContext(s.ctx, `INSERT INTO agent_sessions (id, customer_id, agent_id, call_id, call_type, state)
VALUES ('racing', 'app', 'agent', 'racing-call', 'default', 'open')`)
	s.Require().NoError(err)
	committed := time.AfterFunc(time.Second, func() { _ = writing.Commit() })
	defer committed.Stop()

	s.Require().NoError(split.Migrate(s.ctx))

	var call string
	s.Require().NoError(split.shard.QueryRowContext(s.ctx, "SELECT call_id FROM calls WHERE session_id = 'racing'").Scan(&call))
	s.Equal("racing-call", call)
}

// The session key is added unchecked, so adding it reads no row of calls under its lock.
func (s *StoreSuite) TestTheSessionKeyIsAddedUnchecked() {
	split := s.shardTo("key_unchecked", 20270127000000-1)
	defer split.Close()
	var validated bool
	s.Require().NoError(split.shard.QueryRowContext(s.ctx, "SELECT convalidated FROM pg_constraint WHERE conname = 'calls_session_id_fkey'").Scan(&validated))
	s.False(validated)
}

// A version the single history did not have, but which was rolled back, left nothing behind.
func (s *StoreSuite) TestADatabaseThatRolledBackAMigrationNoSetHasIsAdopted() {
	legacy := s.legacy(s.emptyDatabase("rolled_back"), legacyHead)
	defer legacy.Close()
	_, err := legacy.global.ExecContext(s.ctx, "INSERT INTO goose_db_version (version_id, is_applied) VALUES (20270105090000, true), (20270105090000, false)")
	s.Require().NoError(err)

	s.Require().NoError(legacy.Migrate(s.ctx))
}

// A database that ran 20270117000400 while its function still refused writes through old
// triggers ends with the function that records them, on the shared database and the shard.
func (s *StoreSuite) TestADatabaseThatRanTheRefusingFunctionGetsTheOneThatRecords() {
	opened, err := OpenShards(Options{DSN: s.emptyDatabase("refusing_global"), ShardDSN: s.emptyDatabase("refusing_shard")})
	s.Require().NoError(err)
	defer opened.Close()
	refusing := `CREATE OR REPLACE FUNCTION record_data_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'record_data_change: % was attached without its secret columns', TG_TABLE_NAME; END $$`
	for set, db := range map[migrationSet]*bun.DB{globalSet: opened.global, shardSet: opened.shard} {
		s.upToOn(db, set, 20270127000400-1)
		_, err := db.ExecContext(s.ctx, refusing)
		s.Require().NoError(err)
	}

	s.Require().NoError(opened.Migrate(s.ctx))

	for name, db := range map[string]*bun.DB{"shared": opened.global, "shard": opened.shard} {
		var body string
		s.Require().NoError(db.QueryRowContext(s.ctx, "SELECT prosrc FROM pg_proc WHERE proname = 'record_data_change'").Scan(&body))
		s.NotContains(body, "RAISE EXCEPTION", "the %s database", name)
		s.Contains(body, "jsonb_object_keys", "the %s database", name)
	}
}

// placedCalls opens a shard of the suite's own just before placed calls moved onto their legs,
// holding the one outbound leg given and two placed calls that both match it.
func (s *StoreSuite) placedCalls(name, legVendorCallID string, first, second string) *Store {
	merged := s.shardTo(name, 20270116000001-1)
	dialed := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)
	_, err := merged.global.ExecContext(s.ctx, `
INSERT INTO call_legs (customer_id, call_type, call_id, vendor, vendor_call_id, direction, local_number, remote_number, dialed_at)
VALUES ('app', 'default', 'call', 'telnyx', NULLIF(?, ''), 'outbound', '+1000', '+2000', ?);
INSERT INTO phone_calls (id, token, customer_id, call_type, call_id, vendor, local_number, remote_number, reports, vendor_call_id, placed_at)
VALUES `+first+`, `+second, legVendorCallID, dialed)
	s.Require().NoError(err)
	// Only through the merge: a later migration moves the token into token_hash.
	s.upTo(merged, shardSet, 20270116000001)
	return merged
}

// tokenOnTheLeg is the token of the placed call the one dialled leg was given.
func (s *StoreSuite) tokenOnTheLeg(merged *Store) string {
	var token string
	s.Require().NoError(merged.shard.QueryRowContext(s.ctx, "SELECT token FROM call_legs WHERE dialed_at = '2026-03-01 10:00:00+00'").Scan(&token))
	return token
}

func (s *StoreSuite) TestALegTwoPlacedCallsMatchGoesToTheOneWithItsVendorCallID() {
	merged := s.placedCalls("merge_vendor", "v1",
		"(1, 'with-id', 'app', 'default', 'call', 'telnyx', '+1000', '+2000', false, 'v1', '2026-03-01 09:59:00+00')",
		"(2, 'newer', 'app', 'default', 'call', 'telnyx', '+1000', '+2000', false, NULL, '2026-03-01 09:59:59+00')")
	defer merged.Close()

	s.Equal("with-id", s.tokenOnTheLeg(merged))
}

func (s *StoreSuite) TestALegTwoPlacedCallsMatchGoesToTheOneNearestInTime() {
	merged := s.placedCalls("merge_nearest", "",
		"(1, 'nearest', 'app', 'default', 'call', 'telnyx', '+1000', '+2000', false, NULL, '2026-03-01 09:59:59+00')",
		"(2, 'farther', 'app', 'default', 'call', 'telnyx', '+1000', '+2000', false, NULL, '2026-03-01 09:59:00+00')")
	defer merged.Close()

	s.Equal("nearest", s.tokenOnTheLeg(merged))
}

// A trigger attached by the release before passes no secret columns, and stays in place from
// the migration that replaces the function until Migrate attaches the triggers again. A write
// in between is recorded without its secrets rather than refused.
func (s *StoreSuite) TestAWriteThroughATriggerAttachedBeforeIsRecordedWithoutItsSecrets() {
	opened, err := Open(s.emptyDatabase("old_trigger"))
	s.Require().NoError(err)
	defer opened.Close()
	s.writeThroughAnOldTrigger(opened)
}

// The shared database of a deployment with shards runs the shared set's copy of the function
// alone, where a database of one runs the shard set's copy last.
func (s *StoreSuite) TestAWriteThroughATriggerAttachedBeforeOnTheSharedDatabaseIsRecordedWithoutItsSecrets() {
	opened, err := OpenShards(Options{DSN: s.emptyDatabase("old_trigger_global"), ShardDSN: s.emptyDatabase("old_trigger_shard")})
	s.Require().NoError(err)
	defer opened.Close()
	s.writeThroughAnOldTrigger(opened)
}

// writeThroughAnOldTrigger migrates opened, attaches triggers of the release before to two
// shared tables and checks what a captured write through them records.
func (s *StoreSuite) writeThroughAnOldTrigger(opened *Store) {
	s.Require().NoError(opened.Migrate(s.ctx))
	// provider_keys carries no trigger of its own: one there shows a secret column the release
	// before did not know of is left out too.
	_, err := opened.global.ExecContext(s.ctx, `CREATE OR REPLACE TRIGGER data_changes_recorded
AFTER INSERT OR UPDATE OR DELETE ON users FOR EACH ROW EXECUTE FUNCTION record_data_change('customer_id', '', '', 'customer_id', 'id');
CREATE OR REPLACE TRIGGER data_changes_recorded
AFTER INSERT OR UPDATE OR DELETE ON provider_keys FOR EACH ROW EXECUTE FUNCTION record_data_change('customer_id', '', '', 'customer_id', 'provider', 'region')`)
	s.Require().NoError(err)
	s.Require().NoError(opened.StartDataCapture(s.ctx, "acme", time.Hour, time.Second))
	_, hash, err := NewGuestSecret()
	s.Require().NoError(err)

	s.Require().NoError(opened.RecordGuest(s.ctx, &User{ID: "guest-1", CustomerID: "acme", Name: "Alice", RefreshSecretHash: hash}))
	_, err = opened.global.ExecContext(s.ctx, `INSERT INTO provider_keys (customer_id, provider, region, api_key_sealed, kek_version, key_hint)
		VALUES ('acme', 'openai', 'us', 'sealed', 1, 'sk-...abcd')`)
	s.Require().NoError(err)

	changes, _, err := opened.Changes(s.ctx, "acme", 0, 100)
	s.Require().NoError(err)
	byTable := map[string][]DataChange{}
	for _, change := range changes {
		byTable[change.Table] = append(byTable[change.Table], change)
	}
	s.Require().Len(byTable["users"], 1)
	s.JSONEq(`{"customer_id": "acme", "id": "guest-1"}`, string(byTable["users"][0].Key))
	s.Contains(string(byTable["users"][0].Payload), "Alice")
	s.NotContains(string(byTable["users"][0].Payload), "refresh_secret_hash")
	s.Require().Len(byTable["provider_keys"], 1)
	s.Contains(string(byTable["provider_keys"][0].Payload), "sk-...abcd")
	s.NotContains(string(byTable["provider_keys"][0].Payload), "api_key_sealed")
}

// The Down of each migration before the connector tables were folded undoes its Up, so the
// set can be stepped back over them and run forward again.
func (s *StoreSuite) TestTheFirstSharedMigrationsStepBackAndForward() {
	stepped := s.migratedTo("stepped", 20270112000300)
	defer stepped.Close()
	versions, err := database.NewStore(database.DialectPostgres, globalSet.table)
	s.Require().NoError(err)
	provider, err := goose.NewProvider("", stepped.global.DB, globalSet.files, goose.WithStore(versions))
	s.Require().NoError(err)

	_, err = provider.DownTo(s.ctx, 20270111000000)
	s.Require().NoError(err)
	var plugins, held bool
	s.Require().NoError(stepped.global.QueryRowContext(s.ctx, `SELECT to_regclass('agent_plugin_connections') IS NOT NULL,
		EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'simulation_runs' AND column_name = 'held_until')`).Scan(&plugins, &held))
	s.True(plugins, "the plugin tables are back")
	s.True(held, "the run's lease has its old name")
	var sandbox, index bool
	s.Require().NoError(stepped.global.QueryRowContext(s.ctx, `SELECT
		EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'agent_configs' AND column_name = 'sandbox'),
		to_regclass('knowledge_urls_due_idx') IS NOT NULL`).Scan(&sandbox, &index))
	s.True(sandbox, "agent_configs has its old column names")
	s.False(index, "the due index is gone")

	_, err = provider.UpTo(s.ctx, 20270112000300)
	s.Require().NoError(err)
}

// migratedTo opens an empty database of the suite's own with the shared set applied up to
// version, so a test can write the rows a later migration rewrites.
func (s *StoreSuite) migratedTo(name string, version int64) *Store {
	opened, err := Open(s.emptyDatabase(name))
	s.Require().NoError(err)
	s.upTo(opened, globalSet, version)
	return opened
}

// upTo applies set's migrations to the store's one database up to version.
func (s *StoreSuite) upTo(opened *Store, set migrationSet, version int64) {
	s.upToOn(opened.global, set, version)
}

// upToOn applies set's migrations to db up to version.
func (s *StoreSuite) upToOn(db *bun.DB, set migrationSet, version int64) {
	versions, err := database.NewStore(database.DialectPostgres, set.table)
	s.Require().NoError(err)
	provider, err := goose.NewProvider("", db.DB, set.files, goose.WithStore(versions))
	s.Require().NoError(err)
	_, err = provider.UpTo(s.ctx, version)
	s.Require().NoError(err)
}

func (s *StoreSuite) TestFoldingTheConnectorSideTablesKeepsTheirRows() {
	folded := s.migratedTo("folded", 20270113000000-1)
	defer folded.Close()
	checked := time.Date(2026, 3, 1, 10, 0, 0, 123456000, time.UTC)
	expires := checked.Add(time.Hour)
	_, err := folded.global.ExecContext(s.ctx, `
INSERT INTO connector_connections (id, customer_id, connector_id, definition_revision, owner_type, auth_scheme, status)
VALUES ('with', 'app', 'acme', 1, 'app', 'test_key', 'connected'), ('without', 'app', 'acme', 1, 'app', 'test_key', 'pending');
INSERT INTO connector_connection_clients (connection_id, registration, client_id, updated_at) VALUES ('with', 'dcr', 'issued', ?);
INSERT INTO connector_connection_validations (connection_id, revision, status, code, error, checked_at)
VALUES ('with', 3, 'failed', '503', 'Service Unavailable', ?);
INSERT INTO connector_audit (id, customer_id, connection_id, connector_id, owner_type, action, created_at)
VALUES ('granted', 'app', 'with', 'acme', 'app', 'grant_created', ?), ('called', 'app', 'with', 'acme', 'app', 'proxy_call', ?);
INSERT INTO connector_audit_credentials (audit_id, access_fingerprint, refresh_fingerprint, previous_refresh_fingerprint, rotated, access_expires_at)
VALUES ('granted', 'a1', 'r2', 'r1', true, ?);
INSERT INTO connector_invocations (id, customer_id, connection_id, connector_id, config_id, binding, tool, started_at, latency_ms)
VALUES ('shaped', 'app', 'with', 'acme', 'config', 'acme', 'search', ?, 5), ('unshaped', 'app', 'with', 'acme', 'config', 'acme', 'search', ?, 5);
INSERT INTO connector_invocation_arguments (invocation_id, shape) VALUES ('shaped', '[{"name": "q", "type": "string", "length": 4}]');
`, checked, checked, checked, checked.Add(-time.Second), expires, checked, checked.Add(-time.Second))
	s.Require().NoError(err)

	s.Require().NoError(folded.Migrate(s.ctx))

	with, err := folded.ConnectorConnections.Get(s.ctx, "app", "with")
	s.Require().NoError(err)
	s.Equal(ConnectorConnectionClient{Registration: core.ClientDCR, ClientID: "issued", UpdatedAt: checked}, with.Client)
	s.Equal(ConnectorConnectionValidation{Revision: 3, Status: "failed", Code: "503", Error: "Service Unavailable", CheckedAt: checked}, with.Validation)
	without, err := folded.ConnectorConnections.Get(s.ctx, "app", "without")
	s.Require().NoError(err)
	s.Zero(without.Client)
	s.Zero(without.Validation)

	events, err := folded.ConnectorAuditEvents.Select(s.ctx, "app", AuditFilter{ConnectionID: "with"})
	s.Require().NoError(err)
	s.Require().Len(events, 2)
	s.Equal(&ConnectorAuditCredential{AccessFingerprint: "a1", RefreshFingerprint: "r2", PreviousRefreshFingerprint: "r1",
		Rotated: true, AccessExpiresAt: expires}, normalized(events[0].Credential))
	s.Nil(events[1].Credential, "a row without a credential has none")

	invocations, err := folded.ConnectorInvocations.Select(s.ctx, "app", InvocationFilter{ConnectionID: "with"})
	s.Require().NoError(err)
	s.Require().Len(invocations, 2)
	length := 4
	s.Equal([]ArgumentShape{{Name: "q", Type: "string", Length: &length}}, invocations[0].Arguments)
	s.Nil(invocations[1].Arguments, "a call whose arguments were not an object has no shape")
}

// normalized is credential with its times in UTC, as they were written.
func normalized(credential *ConnectorAuditCredential) *ConnectorAuditCredential {
	if credential == nil {
		return nil
	}
	out := *credential
	out.AccessExpiresAt, out.RefreshExpiresAt = out.AccessExpiresAt.UTC(), out.RefreshExpiresAt.UTC()
	return &out
}
