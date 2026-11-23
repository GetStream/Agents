//go:build integration

package store

import (
	"context"
	"database/sql"
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
	versions, err := database.NewStore(database.DialectPostgres, set.table)
	s.Require().NoError(err)
	provider, err := goose.NewProvider("", opened.global.DB, set.files, goose.WithStore(versions))
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
