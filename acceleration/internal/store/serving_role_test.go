//go:build integration

package store

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uptrace/bun"
)

// ServingRoleSuite serves with a role that owns nothing, on databases another role migrated.
type ServingRoleSuite struct {
	suite.Suite
	ctx            context.Context
	owner, serving *url.Userinfo
	sharded        bool
	migrated       []*bun.DB
	// paths is the store's suite on the serving role's store, and move the data move's between
	// it and another deployment's.
	paths StoreSuite
	move  DataMoveSuite
	// admin and adminMoved are the same stores as a superuser's, which empties the tables
	// between tests.
	admin, adminMoved StoreSuite
}

func TestServingRoleSuite(t *testing.T) {
	suite.Run(t, new(ServingRoleSuite))
}

func TestShardedServingRoleSuite(t *testing.T) {
	suite.Run(t, &ServingRoleSuite{sharded: true})
}

func (s *ServingRoleSuite) SetupSuite() {
	s.ctx = context.Background()
	s.use(s.T())
	names := []string{"serving"}
	if s.sharded {
		names = []string{"serving_global", "serving_shard"}
	}
	s.paths.base = time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)
	s.deploy(&s.admin, &s.paths, false, names...)
	s.paths.countEveryQuery()
	var moved StoreSuite
	moved.SetT(s.T())
	moved.ctx = s.ctx
	s.deploy(&s.adminMoved, &moved, true, "serving_moved")
	s.move.ctx, s.move.source, s.move.destination = s.ctx, s.paths.store, moved.store
}

// use gives the suites this one runs the tests of the test running.
func (s *ServingRoleSuite) use(t *testing.T) {
	for _, reused := range []*StoreSuite{&s.paths, &s.admin, &s.adminMoved} {
		reused.SetT(t)
		reused.ctx = s.ctx
	}
	s.move.SetT(t)
}

// deploy opens the databases named, the shared one and the shard's, as a superuser and as the
// serving role, and migrates them as the owner, before the serving role is granted anything on
// them or after.
func (s *ServingRoleSuite) deploy(admin, serving *StoreSuite, alreadyMigrated bool, names ...string) {
	dsns := []string{admin.emptyDatabase(names[0])}
	dsns = append(dsns, dsns[0])
	if len(names) > 1 {
		dsns[1] = admin.emptyDatabase(names[1])
	}
	var err error
	admin.dsn, admin.shardDSN = dsns[0], dsns[1]
	admin.store, err = OpenShards(Options{DSN: admin.dsn, ShardDSN: admin.shardDSN})
	s.Require().NoError(err)
	if s.owner == nil {
		s.createRoles(admin.store.global)
	}
	databases := []*bun.DB{admin.store.global}
	if admin.store.Sharded() {
		databases = append(databases, admin.store.shard)
	}
	serving.dsn, serving.shardDSN = s.as(s.serving, dsns[0]), s.as(s.serving, dsns[1])
	options := Options{DSN: serving.dsn, ShardDSN: serving.shardDSN, MigrateDSN: s.as(s.owner, dsns[0]),
		Redis: serving.redis(), redisHook: &serving.roundTrips}
	if len(names) > 1 {
		options.ShardMigrateDSN = s.as(s.owner, dsns[1])
	}
	serving.store, err = OpenShards(options)
	s.Require().NoError(err)

	// The grants README.md lists, in "A serving role that owns no table".
	const (
		schema = `GRANT USAGE, CREATE ON SCHEMA public TO router_owner;
GRANT USAGE ON SCHEMA public TO router_serving`
		defaults = `ALTER DEFAULT PRIVILEGES FOR ROLE router_owner IN SCHEMA public
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO router_serving;
ALTER DEFAULT PRIVILEGES FOR ROLE router_owner IN SCHEMA public
  GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO router_serving`
		existing = `GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO router_serving;
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO router_serving`
	)
	grant := func(grants string) {
		named := strings.NewReplacer("router_owner", s.owner.Username(), "router_serving", s.serving.Username()).Replace(grants)
		for _, db := range databases {
			s.exec(db, named)
		}
	}
	s.migrated = append(s.migrated, databases...)
	grant(schema)
	if !alreadyMigrated {
		grant(defaults)
	}
	s.Require().NoError(serving.store.Migrate(s.ctx))
	if alreadyMigrated {
		grant(defaults)
		grant(existing)
	}
	s.Require().NoError(serving.store.CheckMigrated(s.ctx))
}

// createRoles makes the owner and the serving role, under names of their own: roles belong to
// the server, which other runs share.
func (s *ServingRoleSuite) createRoles(db *bun.DB) {
	id := strings.ReplaceAll(uuid.NewString(), "-", "")
	s.owner, s.serving = url.UserPassword("owner_"+id, uuid.NewString()), url.UserPassword("serving_"+id, uuid.NewString())
	for _, role := range []*url.Userinfo{s.owner, s.serving} {
		password, _ := role.Password()
		s.exec(db, "CREATE ROLE ? LOGIN PASSWORD ?", bun.Ident(role.Username()), password)
	}
}

// as is dsn as another role.
func (s *ServingRoleSuite) as(role *url.Userinfo, dsn string) string {
	parsed, err := url.Parse(dsn)
	s.Require().NoError(err)
	parsed.User = role
	return parsed.String()
}

func (s *ServingRoleSuite) exec(db *bun.DB, query string, args ...any) {
	_, err := db.ExecContext(s.ctx, query, args...)
	s.Require().NoError(err)
}

func (s *ServingRoleSuite) TearDownSuite() {
	s.use(s.T())
	for _, open := range []*Store{s.paths.store, s.move.destination} {
		if open != nil {
			s.NoError(open.Close())
		}
	}
	for _, db := range s.migrated {
		// One at a time: Postgres 16 fails on the default privileges both are part of.
		s.exec(db, "DROP OWNED BY ? CASCADE", bun.Ident(s.serving.Username()))
		s.exec(db, "DROP OWNED BY ? CASCADE", bun.Ident(s.owner.Username()))
	}
	if s.owner != nil {
		s.exec(s.admin.store.global, "DROP ROLE ?, ?", bun.Ident(s.owner.Username()), bun.Ident(s.serving.Username()))
	}
	for _, open := range []*Store{s.admin.store, s.adminMoved.store} {
		if open != nil {
			s.NoError(open.Close())
		}
	}
}

// SetupTest empties the tables, and brings back what the test before took out of the schema.
func (s *ServingRoleSuite) SetupTest() {
	s.use(s.T())
	s.empty()
	s.Require().NoError(s.paths.store.Migrate(s.ctx))
}

func (s *ServingRoleSuite) empty() {
	s.admin.SetupTest()
	s.adminMoved.SetupTest()
	s.paths.forgetRedis()
	for _, db := range s.migrated {
		s.exec(db, "INSERT INTO data_change_starts DEFAULT VALUES") // the one row the migrations leave
	}
}

// path runs one test of another suite as the serving role.
func (s *ServingRoleSuite) path(name string, test func()) {
	s.Run(name, func() {
		s.use(s.T())
		s.empty()
		test()
	})
}

func (s *ServingRoleSuite) TestTheServingRoleCannotMigrateItself() {
	serving, err := OpenShards(Options{DSN: s.paths.dsn, ShardDSN: s.paths.shardDSN})
	s.Require().NoError(err)
	defer serving.Close()

	s.ErrorContains(serving.Migrate(s.ctx), "permission denied")
	s.NoError(serving.CheckMigrated(s.ctx), "it checks the schema without changing it")
}

func (s *ServingRoleSuite) TestAnEmptyDatabaseIsRefused() {
	empty, err := OpenShards(Options{DSN: s.admin.emptyDatabase("serving_empty")})
	s.Require().NoError(err)
	defer empty.Close()

	s.ErrorContains(empty.CheckMigrated(s.ctx), "the database has not applied global migration 20270110000000: run router migrate")
	var created bool
	s.Require().NoError(empty.global.QueryRowContext(s.ctx, "SELECT to_regclass('goose_global_version') IS NOT NULL").Scan(&created))
	s.False(created, "the check changes nothing")
}

func (s *ServingRoleSuite) TestADatabaseAMigrationBehindIsRefused() {
	for _, behind := range []struct {
		db      *bun.DB
		set     migrationSet
		version int64
	}{{s.admin.store.global, globalSet, 20270128000000}, {s.admin.store.shard, shardSet, 20270127000100}} {
		s.exec(behind.db, "DELETE FROM ? WHERE version_id = ?", bun.Ident(behind.set.table), behind.version)

		s.ErrorContains(s.paths.store.CheckMigrated(s.ctx),
			fmt.Sprintf("has not applied %s migration %d: run router migrate", behind.set.name, behind.version))

		s.exec(behind.db, "INSERT INTO ? (version_id, is_applied) VALUES (?, true)", bun.Ident(behind.set.table), behind.version)
	}
	s.NoError(s.paths.store.CheckMigrated(s.ctx))
}

// A version counts by its latest row: one rolled back is not applied.
func (s *ServingRoleSuite) TestADatabaseThatRolledAMigrationBackIsRefused() {
	s.exec(s.admin.store.global, "INSERT INTO goose_global_version (version_id, is_applied) VALUES (20270128000000, false)")
	defer s.exec(s.admin.store.global, "DELETE FROM goose_global_version WHERE NOT is_applied")

	s.ErrorContains(s.paths.store.CheckMigrated(s.ctx), "has not applied global migration 20270128000000: run router migrate")
}

// One that ran part of the history squashed into the initial migration holds that migration's
// version, and not its schema.
func (s *ServingRoleSuite) TestADatabaseThatRanPartOfTheSquashedHistoryIsNotServed() {
	behind := s.admin.ranHistory("serving_behind", globalSet.squashed, shardSet.squashed-1)
	defer behind.Close()
	s.ErrorContains(behind.CheckMigrated(s.ctx), "has not applied migration 20270126000000, the last of the history squashed")

	whole := s.admin.ranHistory("serving_whole", globalSet.squashed, shardSet.squashed)
	defer whole.Close()
	s.NoError(whole.CheckMigrated(s.ctx))
}

// The routers of this release restart while the next one, which migrated, rolls out.
func (s *ServingRoleSuite) TestADatabaseALaterReleaseMigratedIsServed() {
	for _, set := range []migrationSet{globalSet, shardSet} {
		db := s.admin.store.global
		if set.name == "shard" {
			db = s.admin.store.shard
		}
		s.exec(db, "INSERT INTO ? (version_id, is_applied) VALUES (29991231000000, true)", bun.Ident(set.table))
		defer s.exec(db, "DELETE FROM ? WHERE version_id = 29991231000000", bun.Ident(set.table))
	}

	s.NoError(s.paths.store.CheckMigrated(s.ctx))

	s.exec(s.admin.store.global, "DROP TRIGGER data_changes_recorded ON users")
	s.ErrorContains(s.paths.store.CheckMigrated(s.ctx), "changes of users are not recorded", "a later release attaches it too")
}

// ON CONFLICT (usage_id) fails every request written without its index.
func (s *ServingRoleSuite) TestAMissingIndexIsRefusedUntilTheOwnerBuildsIt() {
	s.exec(s.admin.store.shard, "DROP INDEX requests_usage_id_idx")
	s.ErrorContains(s.paths.store.CheckMigrated(s.ctx), "no valid index requests_usage_id_idx: run router migrate")

	s.Require().NoError(s.paths.store.Migrate(s.ctx))
	s.NoError(s.paths.store.CheckMigrated(s.ctx))

	s.exec(s.admin.store.shard, "UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'requests_usage_id_idx'::regclass")
	s.ErrorContains(s.paths.store.CheckMigrated(s.ctx), "no valid index requests_usage_id_idx")

	s.exec(s.admin.store.global, "DROP INDEX agent_logs_source_idx")
	s.ErrorContains(s.paths.store.CheckMigrated(s.ctx), "no valid index agent_logs_source_idx")
}

func (s *ServingRoleSuite) TestAMissingTriggerIsRefusedUntilTheOwnerAttachesIt() {
	for table, db := range map[string]*bun.DB{"users": s.admin.store.global, "calls": s.admin.store.shard} {
		for _, gone := range []string{"DROP TRIGGER data_changes_recorded ON " + table, "ALTER TABLE " + table + " DISABLE TRIGGER data_changes_recorded"} {
			s.exec(db, gone)
			s.ErrorContains(s.paths.store.CheckMigrated(s.ctx), "changes of "+table+" are not recorded: run router migrate")

			s.Require().NoError(s.paths.store.Migrate(s.ctx))
			s.NoError(s.paths.store.CheckMigrated(s.ctx))
		}
	}
}

// users holds refresh_secret_hash and previous_refresh_secret_hash, which no recorded change may.
func (s *ServingRoleSuite) TestATriggerThatWouldRecordASecretIsRefused() {
	attach := func(arguments string) {
		s.exec(s.admin.store.global, `CREATE OR REPLACE TRIGGER data_changes_recorded
AFTER INSERT OR UPDATE OR DELETE ON users FOR EACH ROW EXECUTE FUNCTION record_data_change(`+arguments+")")
	}
	for _, arguments := range []string{
		"'customer_id', '', '', '{}', 'customer_id', 'id'",
		"'customer_id', '', '', '{other_secret_hash}', 'customer_id', 'id'",
		"'customer_id', '', '', 'customer_id', 'id'",
		"'customer_id', ''",
	} {
		attach(arguments)
		s.ErrorContains(s.paths.store.CheckMigrated(s.ctx), "changes of users would hold refresh_secret_hash: run router migrate")
	}

	attach("'customer_id', '', '', '{refresh_secret_hash}', 'customer_id', 'id'")
	s.ErrorContains(s.paths.store.CheckMigrated(s.ctx), "changes of users would hold previous_refresh_secret_hash: run router migrate")

	attach("'customer_id', '', '', '{later_sealed,previous_refresh_secret_hash,refresh_secret_hash}', 'customer_id', 'id'")
	s.NoError(s.paths.store.CheckMigrated(s.ctx), "a later release's trigger leaves more out")
}

// A later release adds the column before it attaches the trigger that leaves it out, and this
// one's routers restart in between.
// Without a later release, the trigger is this one's migrate to attach.
func (s *ServingRoleSuite) TestASecretColumnALaterReleaseAddedIsServedBeforeItsTriggerIsAttached() {
	for _, added := range []struct {
		db    *bun.DB
		set   migrationSet
		table string
	}{{s.admin.store.global, globalSet, "users"}, {s.admin.store.shard, shardSet, "calls"}} {
		s.exec(added.db, "ALTER TABLE ? ADD COLUMN later_sealed text", bun.Ident(added.table))
		check := func() error {
			fresh, err := OpenShards(Options{DSN: s.paths.dsn, ShardDSN: s.paths.shardDSN})
			s.Require().NoError(err)
			defer fresh.Close()
			return fresh.CheckMigrated(s.ctx)
		}

		// The other set's later release says nothing of this one's column.
		other := globalSet
		otherDB := s.admin.store.global
		if added.set.name == globalSet.name {
			other, otherDB = shardSet, s.admin.store.shard
		}
		refused := "changes of " + added.table + " would hold later_sealed: run router migrate"
		s.ErrorContains(check(), refused)
		s.exec(otherDB, "INSERT INTO ? (version_id, is_applied) VALUES (29991231000000, true)", bun.Ident(other.table))
		s.ErrorContains(check(), refused)
		s.exec(otherDB, "DELETE FROM ? WHERE version_id = 29991231000000", bun.Ident(other.table))
		// The last of the squashed history is an older release's, and a version before this
		// release's newest is a migration since renumbered: neither is a later one's.
		for _, version := range []int64{added.set.squashed, 20270201000050} {
			s.exec(added.db, "INSERT INTO ? (version_id, is_applied) VALUES (?, true)", bun.Ident(added.set.table), version)
			s.ErrorContains(check(), refused)
			s.exec(added.db, "DELETE FROM ? WHERE version_id = ?", bun.Ident(added.set.table), version)
		}

		s.exec(added.db, "INSERT INTO ? (version_id, is_applied) VALUES (29991231000000, true)", bun.Ident(added.set.table))
		s.NoError(check())

		s.exec(added.db, "DELETE FROM ? WHERE version_id = 29991231000000", bun.Ident(added.set.table))
		s.exec(added.db, "ALTER TABLE ? DROP COLUMN later_sealed", bun.Ident(added.table))
	}
}

// An index can wait long on a write, and a column its migration added may not stay recorded
// meanwhile.
func (s *ServingRoleSuite) TestTheTriggersAreAttachedBeforeAnIndexIsBuilt() {
	s.exec(s.admin.store.global, "DROP TRIGGER data_changes_recorded ON users")
	s.exec(s.admin.store.global, "DROP INDEX image_jobs_undecided")
	writing, err := s.admin.store.global.BeginTx(s.ctx, nil)
	s.Require().NoError(err)
	_, err = writing.ExecContext(s.ctx, "UPDATE image_jobs SET customer_id = customer_id WHERE false")
	s.Require().NoError(err)
	migrated := make(chan error, 1)
	go func() { migrated <- s.paths.store.Migrate(s.ctx) }()
	attached := func() bool {
		var found bool
		s.Require().NoError(s.admin.store.global.QueryRowContext(s.ctx, `SELECT EXISTS (SELECT 1 FROM pg_trigger
			WHERE tgrelid = 'users'::regclass AND tgname = 'data_changes_recorded')`).Scan(&found))
		return found
	}

	s.Eventually(attached, 5*time.Second, 50*time.Millisecond)

	s.Require().NoError(writing.Rollback())
	s.NoError(<-migrated)
}

func (s *ServingRoleSuite) TestATableTheServingRoleIsNotGrantedIsRefused() {
	serving := bun.Ident(s.serving.Username())
	for table, db := range map[string]*bun.DB{"users": s.admin.store.global, "calls": s.admin.store.shard, "data_changes": s.admin.store.shard} {
		for _, privilege := range []string{"SELECT", "INSERT", "UPDATE", "DELETE"} {
			s.exec(db, "REVOKE "+privilege+" ON ? FROM ?", bun.Ident(table), serving)
			s.ErrorContains(s.paths.store.CheckMigrated(s.ctx), "the serving role lacks "+privilege+" on table "+table)

			s.exec(db, "GRANT "+privilege+" ON ? TO ?", bun.Ident(table), serving)
			s.NoError(s.paths.store.CheckMigrated(s.ctx))
		}
	}
}

// The versions are read and never written.
func (s *ServingRoleSuite) TestAVersionTableTheServingRoleMayNotReadIsRefused() {
	serving := bun.Ident(s.serving.Username())
	for table, db := range map[string]*bun.DB{"goose_global_version": s.admin.store.global, "goose_shard_version": s.admin.store.shard} {
		s.exec(db, "REVOKE SELECT ON ? FROM ?", bun.Ident(table), serving)
		s.ErrorContains(s.paths.store.CheckMigrated(s.ctx), "the serving role lacks SELECT on table "+table)
		s.exec(db, "GRANT SELECT ON ? TO ?", bun.Ident(table), serving)

		s.exec(db, "REVOKE INSERT, UPDATE, DELETE ON ? FROM ?", bun.Ident(table), serving)
		s.NoError(s.paths.store.CheckMigrated(s.ctx))

		s.exec(db, "REVOKE SELECT ON ? FROM ?", bun.Ident(table), serving)
		s.ErrorContains(s.paths.store.CheckMigrated(s.ctx), "the serving role lacks SELECT on table "+table)

		s.exec(db, "GRANT SELECT, INSERT, UPDATE, DELETE ON ? TO ?", bun.Ident(table), serving)
		s.NoError(s.paths.store.CheckMigrated(s.ctx))
	}
}

func (s *ServingRoleSuite) TestASchemaTheServingRoleMayNotUseIsRefused() {
	serving := bun.Ident(s.serving.Username())
	for _, db := range []*bun.DB{s.admin.store.global, s.admin.store.shard} {
		s.exec(db, "REVOKE USAGE ON SCHEMA public FROM ?", serving)
		s.ErrorContains(s.paths.store.CheckMigrated(s.ctx), "the serving role lacks USAGE on the schema")

		s.exec(db, "GRANT USAGE ON SCHEMA public TO ?", serving)
		s.NoError(s.paths.store.CheckMigrated(s.ctx))
	}
}

func (s *ServingRoleSuite) TestASequenceTheServingRoleIsNotGrantedIsRefused() {
	serving := bun.Ident(s.serving.Username())
	for sequence, db := range map[string]*bun.DB{"agent_logs_id_seq": s.admin.store.global, "chat_inbound_seq_seq": s.admin.store.global,
		"call_legs_id_seq": s.admin.store.shard, "data_changes_seq_seq": s.admin.store.shard} {
		for _, privilege := range []string{"USAGE", "SELECT", "UPDATE"} {
			s.exec(db, "REVOKE "+privilege+" ON SEQUENCE ? FROM ?", bun.Ident(sequence), serving)
			s.ErrorContains(s.paths.store.CheckMigrated(s.ctx), "the serving role lacks "+privilege+" on sequence "+sequence)

			s.exec(db, "GRANT "+privilege+" ON SEQUENCE ? TO ?", bun.Ident(sequence), serving)
			s.NoError(s.paths.store.CheckMigrated(s.ctx))
		}
	}
}

// A later release drops a table this one no longer reads.
func (s *ServingRoleSuite) TestATableALaterReleaseDroppedIsServed() {
	s.exec(s.admin.store.global, "ALTER TABLE activity_digests RENAME TO dropped_digests")
	defer s.exec(s.admin.store.global, "ALTER TABLE dropped_digests RENAME TO activity_digests")

	s.NoError(s.paths.store.CheckMigrated(s.ctx))
}

func (s *ServingRoleSuite) TestATriggerThatAlwaysRunsIsServed() {
	s.exec(s.admin.store.global, "ALTER TABLE users ENABLE ALWAYS TRIGGER data_changes_recorded")
	defer s.exec(s.admin.store.global, "ALTER TABLE users ENABLE TRIGGER data_changes_recorded")

	s.NoError(s.paths.store.CheckMigrated(s.ctx))
}

// A trigger takes a lock that a long write holds up, and every write after it waits behind it.
func (s *ServingRoleSuite) TestAttachingTheTriggersGivesUpBehindALongWrite() {
	for table, db := range map[string]*bun.DB{"users": s.admin.store.global, "calls": s.admin.store.shard} {
		writing, err := db.BeginTx(s.ctx, nil)
		s.Require().NoError(err)
		_, err = writing.ExecContext(s.ctx, "UPDATE "+table+" SET customer_id = customer_id WHERE false")
		s.Require().NoError(err)
		ctx, cancel := context.WithTimeout(s.ctx, 20*time.Second)

		err = s.paths.store.Migrate(ctx)

		cancel()
		s.Require().NoError(writing.Rollback())
		s.ErrorContains(err, "lock timeout")
	}
}

// An export waits for every write open before its capture, the owner's too, whose session the
// serving role sees little of.
func (s *ServingRoleSuite) TestAWriteOfAnotherRoleIsWaitedFor() {
	writing, err := s.admin.store.global.BeginTx(s.ctx, nil)
	s.Require().NoError(err)
	_, err = writing.ExecContext(s.ctx, "INSERT INTO data_changes (customer_id, table_name, op, key) VALUES ('acme', 'users', 'insert', '{}')")
	s.Require().NoError(err)

	s.ErrorIs(s.paths.store.WaitForOpenWrites(s.ctx, 100*time.Millisecond), ErrWritesOpen)

	s.Require().NoError(writing.Rollback())
	s.NoError(s.paths.store.WaitForOpenWrites(s.ctx, 10*time.Second))
}

// The store works as the role that serves, whose grants hold no DDL.
func (s *ServingRoleSuite) TestTheStoresMainPathsRunAsTheServingRole() {
	s.path("TestACallKeepsTheTimeItFirstEnded", s.paths.TestACallKeepsTheTimeItFirstEnded)
	s.path("TestAPlacedLegIsFoundByItsToken", s.paths.TestAPlacedLegIsFoundByItsToken)
	s.path("TestDeletingASessionKeepsItsCallsWithoutIt", s.paths.TestDeletingASessionKeepsItsCallsWithoutIt)
	s.path("TestSavingASessionTwiceIsOneConversation", s.paths.TestSavingASessionTwiceIsOneConversation)
	s.path("TestMovingThePathRewindsTheOldBranchAndBringsBackTheNew", s.paths.TestMovingThePathRewindsTheOldBranchAndBringsBackTheNew)
	s.path("TestABatchLogsEachRowInItsOwnCall", s.paths.TestABatchLogsEachRowInItsOwnCall)
	s.path("TestRecordRequestsSkipsWrittenUsageIDs", s.paths.TestRecordRequestsSkipsWrittenUsageIDs)
	s.path("TestARequestIDADataMoveTookIsWrittenOnTheNextTry", s.paths.TestARequestIDADataMoveTookIsWrittenOnTheNextTry)
	s.path("TestTheRequestIDSequenceIsNeverMovedBack", s.paths.TestTheRequestIDSequenceIsNeverMovedBack)
	s.path("TestAWrittenRowCountsTowardEachGrantItNames", s.paths.TestAWrittenRowCountsTowardEachGrantItNames)
	s.path("TestAUserIsWrittenDownTheFirstTimeTheyCall", s.paths.TestAUserIsWrittenDownTheFirstTimeTheyCall)
	s.path("TestAGuestsRefreshSecretIsSpentOnce", s.paths.TestAGuestsRefreshSecretIsSpentOnce)
	s.path("TestUpdatingAConfigReplacesItsBindings", s.paths.TestUpdatingAConfigReplacesItsBindings)
	s.path("TestAClaimNeverExceedsTheCampaignsConcurrency", s.paths.TestAClaimNeverExceedsTheCampaignsConcurrency)
	s.path("TestClaimingGivesEachDueTaskToOneWorker", s.paths.TestClaimingGivesEachDueTaskToOneWorker)
	s.path("TestDeletingATaskRedactsTheChangesRecordedOfItAndItsRuns", s.paths.TestDeletingATaskRedactsTheChangesRecordedOfItAndItsRuns)
	s.path("TestAnImageJobUnderItsLeaseIsNotClaimedAgain", s.paths.TestAnImageJobUnderItsLeaseIsNotClaimedAgain)
	s.path("TestAChatClaimTakesMessagesDueAtOnceInTheOrderTheyWereQueued", s.paths.TestAChatClaimTakesMessagesDueAtOnceInTheOrderTheyWereQueued)
	s.path("TestTwoFirstMessagesOfOneThreadAtOnceOpenOneEpisode", s.paths.TestTwoFirstMessagesOfOneThreadAtOnceOpenOneEpisode)
	s.path("TestTwoFirstMessagesOfOneThreadAtOnceLinkOneChannel", s.paths.TestTwoFirstMessagesOfOneThreadAtOnceLinkOneChannel)
	s.path("TestSeedingABuiltInStoresTheRevisionsItMarksBroken", s.paths.TestSeedingABuiltInStoresTheRevisionsItMarksBroken)
	s.path("TestLockedCallbacksOnOneConnectionNeverOverlapAcrossRouters", s.paths.TestLockedCallbacksOnOneConnectionNeverOverlapAcrossRouters)
	s.path("TestARevokedKeyStopsWorkingButItsRowSurvives", s.paths.TestARevokedKeyStopsWorkingButItsRowSurvives)
	s.path("TestSavingAPolicyReplacesWhatWasThere", s.paths.TestSavingAPolicyReplacesWhatWasThere)
	s.path("TestAnUnrevokedGrantIsLiveAgainAndAudited", s.paths.TestAnUnrevokedGrantIsLiveAgainAndAudited)
	s.path("TestTheFirstSyncSeedsEverything", s.paths.TestTheFirstSyncSeedsEverything)
	s.path("TestASyncIsAudited", s.paths.TestASyncIsAudited)
	s.path("TestAgentLogsPageFilterAndResume", s.paths.TestAgentLogsPageFilterAndResume)
	s.path("TestForgettingAUserDeletesTheirTasksAndRuns", s.paths.TestForgettingAUserDeletesTheirTasksAndRuns)
	s.path("TestForgettingAUserRenamesThemInTheChangesOfTheRequestsRewrittenAlone", s.paths.TestForgettingAUserRenamesThemInTheChangesOfTheRequestsRewrittenAlone)
	s.path("TestOneRouterHoldsTheCostAnomalyLock", s.paths.TestOneRouterHoldsTheCostAnomalyLock)
	s.path("TestNormalizeMakesAnOptOutStoredBeforeE164MatchTheNumberDialled", s.paths.TestNormalizeMakesAnOptOutStoredBeforeE164MatchTheNumberDialled)
	s.path("TestDeletingAKnowledgeBaseForgetsItsDocumentsAndPages", s.paths.TestDeletingAKnowledgeBaseForgetsItsDocumentsAndPages)
	s.path("TestTwoTransactionsOnOneGroupRunOneAfterTheOther", s.paths.TestTwoTransactionsOnOneGroupRunOneAfterTheOther)
	s.path("TestABacklogOfFinishedImageJobsIsPrunedWhole", s.paths.TestABacklogOfFinishedImageJobsIsPrunedWhole)
}

// A shard's deployment is no move's source.
func (s *ServingRoleSuite) TestAMoveRunsAsTheServingRole() {
	if s.sharded {
		s.T().Skip("a shard's own tables record no changes (watchDataChanges)")
	}
	s.path("TestImportWritesTheRowsUnderTheImportingCustomer", s.move.TestImportWritesTheRowsUnderTheImportingCustomer)
	s.path("TestChangesFollowWhatHappenedAfterTheExport", s.move.TestChangesFollowWhatHappenedAfterTheExport)
	s.path("TestADeleteIsReplayedToo", s.move.TestADeleteIsReplayedToo)
	s.path("TestAChangeToAGuestLeavesTheirRefreshSecretBehind", s.move.TestAChangeToAGuestLeavesTheirRefreshSecretBehind)
	s.path("TestChildRowsFollowTheirParent", s.move.TestChildRowsFollowTheirParent)
	s.path("TestPruningDropsWhatNobodyCanResumeFrom", s.move.TestPruningDropsWhatNobodyCanResumeFrom)
	s.path("TestRenewingAnExpiredCaptureIsAStart", s.move.TestRenewingAnExpiredCaptureIsAStart)
	s.path("TestARepeatableReadWriteOnASnapshotFromBeforeTheCaptureIsNotLost", s.move.TestARepeatableReadWriteOnASnapshotFromBeforeTheCaptureIsNotLost)
	s.path("TestWaitingForOpenWritesHoldsAWriteFromBeforeTheCaptureStarted", s.move.TestWaitingForOpenWritesHoldsAWriteFromBeforeTheCaptureStarted)
	s.path("TestAChangeOfATransactionThatCommitsLateIsNotSteppedOver", s.move.TestAChangeOfATransactionThatCommitsLateIsNotSteppedOver)
}
