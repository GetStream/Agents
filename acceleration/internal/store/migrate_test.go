//go:build integration

package store

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"

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
