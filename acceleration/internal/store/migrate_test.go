//go:build integration

package store

import (
	"os"
	"path/filepath"

	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
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

// legacy turns the suite's database back into one that ran the single history up to version.
func (s *StoreSuite) legacy(dsn string, version int64) *Store {
	legacy, err := Open(dsn)
	s.Require().NoError(err)
	for _, set := range []migrationSet{globalSet, shardSet} {
		versions, err := database.NewStore(database.DialectPostgres, set.table)
		s.Require().NoError(err)
		provider, err := goose.NewProvider("", legacy.global.DB, set.files, goose.WithStore(versions))
		s.Require().NoError(err)
		_, err = provider.UpTo(s.ctx, baseline)
		s.Require().NoError(err)
	}
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
