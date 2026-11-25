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

// ranHistory is a database of the suite's own that recorded, besides each set's initial
// migration, the version given of the history squashed into it.
func (s *StoreSuite) ranHistory(name string, global, shard int64) *Store {
	db, err := Open(s.emptyDatabase(name))
	s.Require().NoError(err)
	s.Require().NoError(db.Migrate(s.ctx))
	for _, ran := range []struct {
		set     migrationSet
		version int64
	}{{globalSet, global}, {shardSet, shard}} {
		_, err := db.global.ExecContext(s.ctx, "INSERT INTO ? (version_id, is_applied) VALUES (?, true)", bun.Ident(ran.set.table), ran.version)
		s.Require().NoError(err)
	}
	return db
}

func (s *StoreSuite) TestTheSessionLogAndCampaignAttemptIndexesStepBackAndForward() {
	stepped := s.shardTo("stepped_indexes", math.MaxInt64)
	defer stepped.Close()
	versions, err := database.NewStore(database.DialectPostgres, globalSet.table)
	s.Require().NoError(err)
	provider, err := goose.NewProvider("", stepped.global.DB, globalSet.files, goose.WithStore(versions))
	s.Require().NoError(err)
	built := func() (logs, attempts bool) {
		s.Require().NoError(stepped.global.QueryRowContext(s.ctx, `SELECT to_regclass('agent_logs_session_order') IS NOT NULL,
			to_regclass('campaign_attempts_campaign_idx') IS NOT NULL`).Scan(&logs, &attempts))
		return logs, attempts
	}

	_, err = provider.ApplyVersion(s.ctx, 20270202000000, false)
	s.Require().NoError(err)
	logs, attempts := built()
	s.False(logs, "the session log index is gone")
	s.False(attempts, "the campaign attempt index is gone")

	_, err = provider.ApplyVersion(s.ctx, 20270202000000, true)
	s.Require().NoError(err)
	logs, attempts = built()
	s.True(logs)
	s.True(attempts)
}

// Every database deployed before the squash ran the whole history, so it holds the initial
// migration's schema and starts as it is.
func (s *StoreSuite) TestADatabaseThatRanTheSquashedHistoryStartsAsItIs() {
	db := s.ranHistory("squashed", globalSet.squashed, shardSet.squashed)
	defer db.Close()

	s.Require().NoError(db.Migrate(s.ctx))
}

func (s *StoreSuite) TestADatabaseThatRanPartOfTheSquashedHistoryIsRefused() {
	db := s.ranHistory("behind", globalSet.squashed-1, shardSet.squashed)
	defer db.Close()

	s.ErrorContains(db.Migrate(s.ctx), "has not applied migration 20270124093000")
}

func (s *StoreSuite) TestADatabaseFromBeforeTheSplitIsRefused() {
	db, err := Open(s.emptyDatabase("unsplit"))
	s.Require().NoError(err)
	defer db.Close()
	_, err = db.global.ExecContext(s.ctx, "CREATE TABLE agent_configs (id text PRIMARY KEY)")
	s.Require().NoError(err)

	s.ErrorContains(db.Migrate(s.ctx), "ran the migrations from before they were split")
}

// A version counts by its latest row, so one recorded and then rolled back is not applied: a
// database holding such a version with no file is no history the squash left out.
func (s *StoreSuite) TestARolledBackVersionWithNoFileIsNotRefused() {
	db, err := Open(s.emptyDatabase("rolled_back"))
	s.Require().NoError(err)
	defer db.Close()
	s.Require().NoError(db.Migrate(s.ctx))
	_, err = db.global.ExecContext(s.ctx, "INSERT INTO goose_global_version (version_id, is_applied) VALUES (20270105090000, true), (20270105090000, false)")
	s.Require().NoError(err)

	s.Require().NoError(db.Migrate(s.ctx))
}

// Staging's global version was 20270202000000 before the sync_hash columns merged, and a build refuses to start
// on a database with a migration older than its newest applied one. So the migration sorts after that version,
// and a database that ran it under its first version, 20270201001200, applies it again as a no-op.
func (s *StoreSuite) TestTheSyncHashMigrationSortsAfterAGlobalVersionStagingHad() {
	db, err := Open(s.emptyDatabase("sync_hash_slot"))
	s.Require().NoError(err)
	defer db.Close()
	hasColumns := func() (hasColumn bool) {
		s.Require().NoError(db.global.QueryRowContext(s.ctx, `SELECT count(*) = 2 FROM information_schema.columns
			WHERE column_name = 'sync_hash' AND table_name IN ('connector_definitions', 'router_configs')`).Scan(&hasColumn))
		return hasColumn
	}

	s.upTo(db, globalSet, 20270202000000)
	s.False(hasColumns(), "a database at 20270202000000 has not run the sync_hash migration")
	for _, table := range []string{"connector_definitions", "router_configs"} {
		_, err = db.global.ExecContext(s.ctx, "ALTER TABLE ? ADD COLUMN sync_hash TEXT NOT NULL DEFAULT ''", bun.Ident(table))
		s.Require().NoError(err)
	}
	_, err = db.global.ExecContext(s.ctx, "INSERT INTO ? (version_id, is_applied) VALUES (20270201001200, true)", bun.Ident(globalSet.table))
	s.Require().NoError(err)

	s.Require().NoError(db.Migrate(s.ctx))
	s.True(hasColumns())
	var recorded bool
	s.Require().NoError(db.global.QueryRowContext(s.ctx, "SELECT EXISTS (SELECT 1 FROM ? WHERE version_id = 20270202000100 AND is_applied)",
		bun.Ident(globalSet.table)).Scan(&recorded))
	s.True(recorded, "the migration is recorded under its new version")
}

// A database a later release migrated holds versions this one has no file for, after the
// squashed history and before any of it is recorded once the cleanup has run: rolling back to
// this release starts on it.
func (s *StoreSuite) TestADatabaseALaterReleaseMigratedStartsAfterTheCleanup() {
	db, err := Open(s.emptyDatabase("later_release"))
	s.Require().NoError(err)
	defer db.Close()
	s.Require().NoError(db.Migrate(s.ctx))
	for _, set := range []migrationSet{globalSet, shardSet} {
		_, err = db.global.ExecContext(s.ctx, "INSERT INTO ? (version_id, is_applied) VALUES (29991231000000, true)", bun.Ident(set.table))
		s.Require().NoError(err)
	}

	s.Require().NoError(db.Migrate(s.ctx))
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

	fresh, err := OpenShards(Options{DSN: s.emptyDatabase("recording_global"), ShardDSN: s.emptyDatabase("recording_shard")})
	s.Require().NoError(err)
	defer fresh.Close()
	s.Require().NoError(fresh.Migrate(s.ctx))
	body := func(db *bun.DB) string {
		var body string
		s.Require().NoError(db.QueryRowContext(s.ctx, "SELECT pg_get_functiondef('record_data_change'::regproc)").Scan(&body))
		return body
	}
	s.Equal(body(fresh.global), body(opened.global), "the shared database ends with the function a new one has")
	s.Equal(body(fresh.shard), body(opened.shard), "the shard ends with the function a new one has")
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

	s.Require().NoError(opened.Users.InsertGuest(s.ctx, &User{ID: "guest-1", CustomerID: "acme", Name: "Alice", RefreshSecretHash: hash}))
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
