//go:build integration

package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/driver/pgdriver"

	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
)

// DSNEnvVar is where the tests look for a Postgres to run against.
const DSNEnvVar = "ROUTER_POSTGRES_DSN"

type StoreSuite struct {
	suite.Suite
	store *Store
	// dsn is the suite's own database, for a test that needs another pool on it, as a second
	// router would have, and shardDSN its shard's, which is dsn unless the suite is sharded.
	dsn      string
	shardDSN string
	ctx      context.Context
	// base is the start of a fixed hour, so bucket boundaries are predictable.
	base time.Time
	// queries and roundTrips count what the store sends Postgres and Redis, for countQueries
	// and countRedis.
	queries    queryCount
	roundTrips roundTrips
	// seededDSN is the database the EXPLAIN checks read, seeded on the suite's first use.
	seededDSN string
}

// insertRequest writes one request as the writer does, through InsertMany, giving it a usage
// id when it has none, and leaves on request what the write set.
func insertRequest(ctx context.Context, db *Store, request *Request) error {
	if request.UsageID == uuid.Nil {
		request.UsageID = uuid.Must(uuid.NewV7())
	}
	rows := []Request{*request}
	_, err := db.Requests.InsertMany(ctx, rows)
	*request = rows[0]
	return err
}

func TestStoreSuite(t *testing.T) {
	suite.Run(t, new(StoreSuite))
}

// ShardedStoreSuite runs every store test again with the shared tables in one database, this
// shard's in another, and a second shard beside it, as a hosted deployment keeps them.
type ShardedStoreSuite struct {
	StoreSuite
	// other is a router of the second shard: the same shared database, the other shard's own.
	other *Store
}

func TestShardedStoreSuite(t *testing.T) {
	suite.Run(t, new(ShardedStoreSuite))
}

func (s *StoreSuite) SetupSuite() {
	s.ctx = context.Background()
	s.dsn = s.emptyDatabase("store")
	s.shardDSN = s.dsn
	store, err := OpenShards(Options{DSN: s.dsn, Redis: s.redis(), redisHook: &s.roundTrips})
	s.Require().NoError(err)
	s.store = store
	s.Require().NoError(store.Ping(s.ctx))
	s.Require().NoError(store.Migrate(s.ctx))
	s.countEveryQuery()
	s.base = time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)
}

func (s *ShardedStoreSuite) SetupSuite() {
	s.ctx = context.Background()
	s.dsn = s.emptyDatabase("store_global")
	shard, peer := s.emptyDatabase("store_shard"), s.emptyDatabase("store_peer")
	s.shardDSN = shard

	shards := map[string]string{"here": shard, "there": peer}
	store, err := OpenShards(Options{DSN: s.dsn, ShardDSN: shard, Shards: shards, Redis: s.redis(), redisHook: &s.roundTrips})
	s.Require().NoError(err)
	s.store = store
	s.Require().NoError(store.Ping(s.ctx))
	s.Require().NoError(store.Migrate(s.ctx))
	s.countEveryQuery()
	s.other, err = OpenShards(Options{DSN: s.dsn, ShardDSN: peer, Shards: shards})
	s.Require().NoError(err)
	s.Require().NoError(s.other.Migrate(s.ctx))
	s.base = time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)
}

func (s *ShardedStoreSuite) TearDownSuite() {
	if s.other != nil {
		s.Require().NoError(s.other.Close())
	}
	s.StoreSuite.TearDownSuite()
}

// emptyDatabase is a database of this suite's own with an empty schema, so the embedded
// migrations are what create the tables. It is the suite's own because it drops the schema and
// empties tables between tests, which is not something to do to a database another package is
// reading.
func (s *StoreSuite) emptyDatabase(name string) string {
	dsn := os.Getenv(DSNEnvVar)
	if dsn == "" {
		s.T().Skipf("%s not set", DSNEnvVar)
	}
	own := testenv.Database(dsn, name)
	db, err := Open(own)
	s.Require().NoError(err)
	defer db.Close()

	var database string
	s.Require().NoError(db.GlobalDB().QueryRowContext(s.ctx, "SELECT current_database()").Scan(&database))
	s.Require().True(strings.HasSuffix(database, "_test"), "refusing to drop the schema of %s, which is not a test database", database)
	_, err = db.GlobalDB().ExecContext(s.ctx, "DROP SCHEMA public CASCADE; CREATE SCHEMA public")
	s.Require().NoError(err)
	return own
}

// openRouter opens another router on the suite's databases, laid out as the suite's are.
func (s *StoreSuite) openRouter() (*Store, error) {
	return OpenShards(Options{DSN: s.dsn, ShardDSN: s.shardDSN})
}

// replica opens another router on the suite's databases and its Redis, which is what a second
// replica of the router is. A test of what one replica's write does to the other's cache is
// skipped when the suite has no Redis.
func (s *StoreSuite) replica() *Store {
	if s.store.cache == nil {
		s.T().Skipf("%s not set", redisAddressVar)
	}
	replica, err := OpenShards(Options{DSN: s.dsn, ShardDSN: s.shardDSN, Redis: s.redis()})
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.NoError(replica.Close()) })
	return replica
}

func (s *StoreSuite) TearDownSuite() {
	if s.store != nil {
		s.Require().NoError(s.store.Close())
	}
}

// emptyTables deletes every row of every table in db, read from the catalogue so a new table
// needs nothing here. It uses DELETE, not TRUNCATE. TRUNCATE makes new files for each table and
// each index. This takes much more time than a DELETE of the few rows that a test writes.
//
// The replica role stops foreign-key checks and row triggers during the DELETE. This gives the
// same result as TRUNCATE, which does not run row triggers. Without the replica role, the
// trigger on price_changes refuses the DELETE.
func (s *StoreSuite) emptyTables(db bun.IDB) {
	var tables []string
	s.Require().NoError(db.NewRaw(`SELECT tablename FROM pg_tables
		WHERE schemaname = 'public' AND tablename NOT LIKE 'goose\_%' ORDER BY tablename`).Scan(s.ctx, &tables))
	statement := "SET LOCAL session_replication_role = replica; DELETE FROM " + strings.Join(tables, "; DELETE FROM ")
	s.Require().NoError(db.RunInTx(s.ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		_, err := tx.ExecContext(ctx, statement)
		return err
	}))
}

// SetupTest empties the tables one time in each database, and the store's keys in Redis. With
// one database, the shard database and the shared database are the same database.
func (s *StoreSuite) SetupTest() {
	databases := s.store.shards()
	if s.store.Sharded() {
		databases = append(databases, s.store.global)
	}
	for _, db := range databases {
		s.emptyTables(db)
	}
	s.forgetRedis()
}

func (s *StoreSuite) TestMigrateIsIdempotent() {
	s.Require().NoError(s.store.Migrate(s.ctx))
}

func (s *StoreSuite) TestOpenRequiresADSN() {
	_, err := Open("")
	s.ErrorContains(err, "dsn is required")
}

func (s *StoreSuite) TestACallReportsTheModelsThatServedIt() {
	s.Require().NoError(insertRequest(s.ctx, s.store, &Request{
		Modality: "stt", CustomerID: "acme", AgentID: "agent-1",
		Provider: "deepgram", Model: "flux-general-en",
		StartedAt: s.base.Add(time.Second), Success: true,
	}))
	s.Require().NoError(insertRequest(s.ctx, s.store, &Request{
		Modality: "llm", CustomerID: "acme", AgentID: "agent-1",
		Provider: "gemini", Model: "gemini-3.5-flash-lite",
		StartedAt: s.base.Add(2 * time.Second), Success: true,
	}))
	s.Require().NoError(insertRequest(s.ctx, s.store, &Request{
		Modality: "llm", CustomerID: "acme", AgentID: "agent-1",
		Provider: "openai", Model: "gpt-5.6-sol",
		StartedAt: s.base.Add(3 * time.Second), Success: true,
	}))
	// A failed start is not what served the call.
	s.Require().NoError(insertRequest(s.ctx, s.store, &Request{
		Modality: "tts", CustomerID: "acme", AgentID: "agent-1",
		Provider: "cartesia", Model: "sonic-preview",
		StartedAt: s.base.Add(4 * time.Second), Success: false, ErrorCode: "start_failed",
	}))
	// Another agent is not this call.
	s.Require().NoError(insertRequest(s.ctx, s.store, &Request{
		Modality: "stt", CustomerID: "acme", AgentID: "agent-2",
		Provider: "grok", Model: "grok-stt",
		StartedAt: s.base.Add(2 * time.Second), Success: true,
	}))

	used, err := s.store.Calls.UsedModels(s.ctx, "acme", "agent-1", s.base, nil)
	s.Require().NoError(err)
	s.Equal([]UsedModel{
		{Modality: "llm", Provider: "openai", Model: "gpt-5.6-sol"},
		{Modality: "llm", Provider: "gemini", Model: "gemini-3.5-flash-lite"},
		{Modality: "stt", Provider: "deepgram", Model: "flux-general-en"},
	}, used)
}

func (s *StoreSuite) TestACallAddsUpWhatEveryOneOfItsRequestsSpent() {
	s.Require().NoError(insertRequest(s.ctx, s.store, &Request{
		Modality: "llm", CustomerID: "acme", AgentID: "agent-1",
		Provider: "openai", Model: "gpt-5.6-sol", StartedAt: s.base.Add(time.Second),
		InputTokens: 800, CachedInputTokens: 500, OutputTokens: 120,
		CostMicros: 1400, Success: true,
	}))
	// A turn the model read and then failed on is still billed for the reading.
	s.Require().NoError(insertRequest(s.ctx, s.store, &Request{
		Modality: "llm", CustomerID: "acme", AgentID: "agent-1",
		Provider: "openai", Model: "gpt-5.6-sol", StartedAt: s.base.Add(2 * time.Second),
		InputTokens: 200, OutputTokens: 0, CostMicros: 100,
		Success: false, ErrorCode: "overloaded",
	}))
	// Another agent's spend is not this call's.
	s.Require().NoError(insertRequest(s.ctx, s.store, &Request{
		Modality: "llm", CustomerID: "acme", AgentID: "agent-2",
		Provider: "openai", Model: "gpt-5.6-sol", StartedAt: s.base.Add(time.Second),
		InputTokens: 9000, OutputTokens: 9000, CostMicros: 90000, Success: true,
	}))

	spent, err := s.store.Calls.Usage(s.ctx, "acme", "agent-1", "session-1", s.base, nil)

	s.Require().NoError(err)
	s.Equal(CallUsage{
		InputTokens: 1000, CachedInputTokens: 500, OutputTokens: 120,
		CostMicros: 1500, Requests: 2,
	}, spent)
}

func (s *StoreSuite) TestACallsRuntimeIsItsOwnSessionsAndNoRequest() {
	// Two sessions of one agent at once: the call's time is its own session's.
	for _, row := range []Request{
		{Modality: "llm", SessionID: "session-1", Provider: "openai", Model: "gpt-5.6-sol", InputTokens: 100, CostMicros: 50},
		{Modality: RuntimeModality, SessionID: "session-1", Provider: "stream", Model: "voice", CostMicros: 600},
		{Modality: RuntimeModality, SessionID: "session-2", Provider: "stream", Model: "voice", CostMicros: 900},
	} {
		row.CustomerID, row.AgentID, row.StartedAt, row.Success = "acme", "agent-1", s.base.Add(time.Second), true
		s.Require().NoError(insertRequest(s.ctx, s.store, &row))
	}

	spent, err := s.store.Calls.Usage(s.ctx, "acme", "agent-1", "session-1", s.base, nil)
	s.Require().NoError(err)
	s.EqualValues(650, spent.CostMicros, "the model's and this session's time, not the other session's")
	s.EqualValues(1, spent.Requests, "runtime is no request")

	used, err := s.store.Calls.Tokens(s.ctx, "acme", "agent-1", "session-1", s.base, nil)
	s.Require().NoError(err)
	var cost, requests int64
	for _, model := range used {
		cost += model.CostMicros
		requests += model.Requests
	}
	s.EqualValues(650, cost)
	s.EqualValues(1, requests)
}

func (s *StoreSuite) TestACallDoesNotCountAnotherSessionOfItsAgent() {
	// A held session resumed days later: everything the agent's other sessions spent since it
	// began is in its window, and none of it is its own.
	for _, row := range []Request{
		{Modality: "llm", SessionID: "session-1", Provider: "anthropic", Model: "claude-opus-5-5", InputTokens: 100, CostMicros: 50},
		{Modality: "llm", SessionID: "session-2", Provider: "anthropic", Model: "claude-opus-5-5", InputTokens: 90000, CostMicros: 200000},
		// From before requests named their session: only agent and window can match it.
		{Modality: "llm", Provider: "anthropic", Model: "claude-opus-5-5", InputTokens: 7, CostMicros: 3},
	} {
		row.CustomerID, row.AgentID, row.StartedAt, row.Success = "acme", "agent-1", s.base.Add(time.Second), true
		s.Require().NoError(insertRequest(s.ctx, s.store, &row))
	}

	spent, err := s.store.Calls.Usage(s.ctx, "acme", "agent-1", "session-1", s.base, nil)
	s.Require().NoError(err)
	s.Equal(CallUsage{InputTokens: 107, CostMicros: 53, Requests: 2}, spent)

	used, err := s.store.Calls.Tokens(s.ctx, "acme", "agent-1", "session-1", s.base, nil)
	s.Require().NoError(err)
	s.Require().Len(used, 1)
	s.EqualValues(107, used[0].InputTokens)
	s.EqualValues(53, used[0].CostMicros)
	s.EqualValues(2, used[0].Requests)
}

func (s *StoreSuite) TestACallThatMadeNoRequestsSpentNothing() {
	spent, err := s.store.Calls.Usage(s.ctx, "acme", "agent-with-no-work", "session-1", s.base, nil)

	s.Require().NoError(err)
	s.Equal(CallUsage{}, spent)
}

func (s *StoreSuite) TestWhatWasSpentAfterACallEndedIsNotTheCallsToPayFor() {
	ended := s.base.Add(10 * time.Second)
	s.Require().NoError(insertRequest(s.ctx, s.store, &Request{
		Modality: "llm", CustomerID: "acme", AgentID: "agent-1",
		Provider: "openai", Model: "gpt-5.6-sol", StartedAt: s.base.Add(time.Second),
		InputTokens: 300, CostMicros: 400, Success: true,
	}))
	s.Require().NoError(insertRequest(s.ctx, s.store, &Request{
		Modality: "llm", CustomerID: "acme", AgentID: "agent-1",
		Provider: "openai", Model: "gpt-5.6-sol", StartedAt: ended.Add(time.Minute),
		InputTokens: 7000, CostMicros: 8000, Success: true,
	}))

	spent, err := s.store.Calls.Usage(s.ctx, "acme", "agent-1", "session-1", s.base, &ended)

	s.Require().NoError(err)
	s.Equal(int64(300), spent.InputTokens)
	s.Equal(int64(1), spent.Requests)
}

func (s *StoreSuite) TestAnArrivingCallNamesTheCustomerWhoseNumberWasRung() {
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &PhoneNumber{
		E164: "+15125551234", Vendor: "twilio", Country: "US",
		CustomerID: "acme", PurchasedAt: s.base,
	}))
	s.Require().NoError(s.store.PhoneNumbers.Attach(
		s.ctx, "acme", "+15125551234", NumberAttachment{TrunkID: "trunk-7", CallType: "support", CallID: "the-support-line"}))

	number, err := s.store.NumberByCallInApp(s.ctx, AppScope{Unpinned: true}, "support", "the-support-line")

	s.Require().NoError(err)
	s.Equal("acme", number.CustomerID)
	s.Equal("+15125551234", number.E164)
}

func (s *StoreSuite) TestANumberFoundByItsCallOrRingCarriesItsTrunksAddress() {
	group := TrunkGroup{CustomerID: "acme", Vendor: "twilio"}
	s.savedTrunk(group, "trunk-7")
	s.savedTrunk(TrunkGroup{CustomerID: "globex", Vendor: "twilio"}, "trunk-8")
	s.lined(group, "+15125551234", "trunk-7")
	s.Require().NoError(s.store.PhoneNumbers.Attach(
		s.ctx, "acme", "+15125551234", NumberAttachment{TrunkID: "trunk-7", CallType: "support", CallID: "the-support-line"}))
	s.lined(group, "+15125551235", "trunk-8")
	s.Require().NoError(s.store.PhoneNumbers.Attach(
		s.ctx, "acme", "+15125551235", NumberAttachment{TrunkID: "trunk-8", CallType: "support", CallID: "another-line"}))

	var byCall, rung, theirs PhoneNumber
	var errs [3]error
	s.Equal(3, s.countQueries(func() {
		byCall, errs[0] = s.store.NumberByCallInApp(s.ctx, AppScope{Unpinned: true}, "support", "the-support-line")
		rung, errs[1] = s.store.NumberRungInApp(s.ctx, AppScope{Unpinned: true}, "+15125551234")
		theirs, errs[2] = s.store.NumberByCallInApp(s.ctx, AppScope{Unpinned: true}, "support", "another-line")
	}), "one query each, the trunk's address included")

	for _, err := range errs {
		s.Require().NoError(err)
	}
	s.Equal("sip:trunk-7@sip.example.test", byCall.BridgeURI)
	s.Equal("sip:trunk-7@sip.example.test", rung.BridgeURI)
	s.Empty(theirs.BridgeURI, "another customer's trunk of that id is not the number's")
}

func (s *StoreSuite) TestANumberAttachedBeforeItsCallWasRecordedIsStillFound() {
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &PhoneNumber{
		E164: "+15125551234", Vendor: "twilio", Country: "US",
		CustomerID: "acme", PurchasedAt: s.base,
	}))
	// What an attach looked like before the call was recorded: a trunk and nothing else.
	_, err := s.store.global.NewUpdate().Model((*PhoneNumber)(nil)).
		Set("stream_trunk_id = ?", "trunk-7").
		Where("e164 = ?", "+15125551234").
		Exec(s.ctx)
	s.Require().NoError(err)

	number, err := s.store.NumberByCallInApp(s.ctx, AppScope{Unpinned: true}, "default", "phone-+15125551234")

	s.Require().NoError(err)
	s.Equal("acme", number.CustomerID)
}

// Another customer's row for the number on its own trunk, lined in the same app to place calls
// from it, as one added before a number was held by one customer at a time can be.
func (s *StoreSuite) TestANumberOnAnotherCustomersOwnTrunkDoesNotMakeACallNamedForTheNumberNobodys() {
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &PhoneNumber{
		E164: "+15125551234", Vendor: "twilio", Country: "US",
		CustomerID: "acme", PurchasedAt: s.base,
	}))
	_, err := s.store.global.NewUpdate().Model((*PhoneNumber)(nil)).
		Set("stream_trunk_id = ?", "trunk-7").
		Where("e164 = ?", "+15125551234").
		Exec(s.ctx)
	s.Require().NoError(err)
	s.lined(TrunkGroup{CustomerID: "globex", Vendor: sipTrunkVendor}, "+15125551234", "trunk-9")

	number, err := s.store.NumberByCallInApp(s.ctx, AppScope{Unpinned: true}, "default", "phone-+15125551234")

	s.Require().NoError(err)
	s.Equal("acme", number.CustomerID)
}

func (s *StoreSuite) TestACallNoNumberReachesIsNotAttributedToAnybody() {
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &PhoneNumber{
		E164: "+15125551234", Vendor: "twilio", Country: "US",
		CustomerID: "acme", PurchasedAt: s.base,
	}))

	_, err := s.store.NumberByCallInApp(s.ctx, AppScope{Unpinned: true}, "default", "some-video-call")

	s.ErrorContains(err, "no number reaches call default:some-video-call")
	s.ErrorIs(err, ErrNoNumber, "a video call is told from a call to a number")
}

func (s *StoreSuite) TestAReleasedNumbersCallIsNotAttributedToItsFormerHolder() {
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &PhoneNumber{
		E164: "+15125551234", Vendor: "twilio", Country: "US",
		CustomerID: "acme", PurchasedAt: s.base,
	}))
	s.Require().NoError(s.store.PhoneNumbers.Attach(
		s.ctx, "acme", "+15125551234", NumberAttachment{TrunkID: "trunk-7", CallType: "default", CallID: "phone-+15125551234"}))
	s.Require().NoError(giveBack(s.ctx, s.store, "acme", "+15125551234", s.base.Add(time.Hour)))

	_, err := s.store.NumberByCallInApp(s.ctx, AppScope{Unpinned: true}, "default", "phone-+15125551234")

	s.ErrorContains(err, "no number reaches call")
}

// onShard runs a statement on this shard's database, failing the test if it fails.
func (s *StoreSuite) onShard(query string, args ...any) {
	_, err := s.store.ShardDB().ExecContext(s.ctx, query, args...)
	s.Require().NoError(err)
}

// usageIndex reports whether requests_usage_id_idx is in this shard's database, and if it
// is, whether it is valid.
func (s *StoreSuite) usageIndex() (valid, found bool) {
	var state sql.NullBool
	s.Require().NoError(s.store.ShardDB().QueryRowContext(s.ctx,
		"SELECT (SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass('requests_usage_id_idx'))").Scan(&state))
	return state.Bool, state.Valid
}

// restoreUsageIndex puts requests_usage_id_idx back as the migration leaves it once the test
// ends, whatever the test got to.
func (s *StoreSuite) restoreUsageIndex() {
	s.T().Cleanup(func() {
		s.onShard("TRUNCATE requests")
		s.onShard("DROP INDEX CONCURRENTLY IF EXISTS requests_usage_id_idx")
		s.onShard("CREATE UNIQUE INDEX CONCURRENTLY requests_usage_id_idx ON requests (usage_id)")
	})
}

func (s *StoreSuite) TestMigrateBuildsAUsageIndexSomebodyDropped() {
	s.restoreUsageIndex()
	s.onShard("DROP INDEX CONCURRENTLY requests_usage_id_idx")
	row := s.usageRow("dropped-index", nil)
	_, err := s.store.Requests.InsertMany(s.ctx, []Request{row})
	s.Require().ErrorContains(err, "ON CONFLICT", "nothing to infer the conflict from without the index")

	s.Require().NoError(s.store.Migrate(s.ctx), "the migration is applied already, and Migrate builds the index itself")

	valid, found := s.usageIndex()
	s.True(found)
	s.True(valid)
	written, err := s.store.Requests.InsertMany(s.ctx, []Request{row, row})
	s.Require().NoError(err)
	s.Equal([]uuid.UUID{row.UsageID}, written)
}

func (s *StoreSuite) TestMigrateRebuildsAUsageIndexThatDidNotBuild() {
	s.restoreUsageIndex()
	s.onShard("DROP INDEX CONCURRENTLY requests_usage_id_idx")
	twice := s.usageRow("invalid-index", nil)
	_, err := s.store.ShardDB().NewInsert().Model(&[]Request{twice, twice}).Exec(s.ctx)
	s.Require().NoError(err)
	_, err = s.store.ShardDB().ExecContext(s.ctx, "CREATE UNIQUE INDEX CONCURRENTLY requests_usage_id_idx ON requests (usage_id)")
	s.Require().Error(err, "two rows with one usage id fail the build")
	valid, found := s.usageIndex()
	s.Require().True(found, "a failed concurrent build leaves its index behind")
	s.Require().False(valid)

	err = s.store.Migrate(s.ctx)
	s.Require().Error(err, "the router does not start while the index cannot be built")
	s.Contains(err.Error(), "requests_usage_id_idx")

	s.onShard("DELETE FROM requests WHERE id = (SELECT max(id) FROM requests WHERE usage_id = ?)", twice.UsageID)
	s.Require().NoError(s.store.Migrate(s.ctx))

	valid, found = s.usageIndex()
	s.True(found)
	s.True(valid, "the invalid index is dropped and built again")
}

func (s *StoreSuite) TestMigrateRebuildsAGrantsIndexLeftInvalid() {
	// As a CONCURRENTLY build that failed leaves it.
	s.onShard("UPDATE pg_index SET indisvalid = false WHERE indexrelid = to_regclass('requests_limit_grants_started_idx')")
	var valid bool
	read := func() {
		s.Require().NoError(s.store.ShardDB().QueryRowContext(s.ctx,
			"SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass('requests_limit_grants_started_idx')").Scan(&valid))
	}
	read()
	s.Require().False(valid)

	s.Require().NoError(s.store.Migrate(s.ctx))

	read()
	s.True(valid, "dropped and built again")
}

func (s *StoreSuite) TestMigrateRebuildsTheContactIndexLeftInvalid() {
	// As a CONCURRENTLY build that failed leaves it: the migration's IF NOT EXISTS skips it.
	_, err := s.store.GlobalDB().ExecContext(s.ctx, "UPDATE pg_index SET indisvalid = false WHERE indexrelid = to_regclass('contact_map_conversation')")
	s.Require().NoError(err)
	var valid bool
	read := func() {
		s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx,
			"SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass('contact_map_conversation')").Scan(&valid))
	}
	read()
	s.Require().False(valid)

	s.Require().NoError(s.store.Migrate(s.ctx))

	read()
	s.True(valid, "dropped and built again")
}

func (s *StoreSuite) TestMigrateRebuildsTheSessionLogIndexLeftInvalid() {
	// As a CONCURRENTLY build that failed leaves it: the migration's IF NOT EXISTS skips it.
	_, err := s.store.GlobalDB().ExecContext(s.ctx, "UPDATE pg_index SET indisvalid = false WHERE indexrelid = to_regclass('agent_logs_session_order')")
	s.Require().NoError(err)
	var valid bool
	read := func() {
		s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx,
			"SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass('agent_logs_session_order')").Scan(&valid))
	}
	read()
	s.Require().False(valid)

	s.Require().NoError(s.store.Migrate(s.ctx))

	read()
	s.True(valid, "dropped and built again")
	var definition string
	s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx,
		"SELECT pg_get_indexdef(to_regclass('agent_logs_session_order'))").Scan(&definition))
	s.Equal("CREATE INDEX agent_logs_session_order ON public.agent_logs USING btree (customer_id, session_id, id DESC)", definition)
}

func (s *StoreSuite) TestMigrateRebuildsTheTaskChangesIndexLeftInvalid() {
	built := func() (valid bool, definition string) {
		s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx, "SELECT indisvalid, pg_get_indexdef(indexrelid) FROM pg_index "+
			"WHERE indexrelid = to_regclass('data_changes_tasks_idx')").Scan(&valid, &definition))
		return valid, definition
	}
	const want = "CREATE INDEX data_changes_tasks_idx ON public.data_changes USING btree (customer_id, " +
		"COALESCE((payload ->> 'task_id'::text), (payload ->> 'id'::text)), ((payload ->> 'id'::text))) " +
		"WHERE ((table_name = ANY (ARRAY['scheduled_tasks'::text, 'scheduled_task_runs'::text])) AND (op <> 'delete'::text))"
	_, definition := built()
	s.Equal(want, definition, "as the migration builds it")
	// As a CONCURRENTLY build that failed leaves it.
	_, err := s.store.GlobalDB().ExecContext(s.ctx, "UPDATE pg_index SET indisvalid = false WHERE indexrelid = to_regclass('data_changes_tasks_idx')")
	s.Require().NoError(err)

	s.Require().NoError(s.store.Migrate(s.ctx))

	valid, definition := built()
	s.True(valid, "dropped and built again")
	s.Equal(want, definition, "built again as the migration builds it")
}

func (s *StoreSuite) TestMigrateRebuildsTheHeldNumberIndexLeftInvalid() {
	built := func() (valid bool, definition string) {
		s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx, "SELECT indisvalid, pg_get_indexdef(indexrelid) FROM pg_index "+
			"WHERE indexrelid = to_regclass('phone_numbers_e164_held_idx')").Scan(&valid, &definition))
		return valid, definition
	}
	const want = "CREATE INDEX phone_numbers_e164_held_idx ON public.phone_numbers USING btree (e164) WHERE (released_at IS NULL)"
	_, definition := built()
	s.Equal(want, definition, "as the migration builds it")
	// As a CONCURRENTLY build that failed leaves it.
	_, err := s.store.GlobalDB().ExecContext(s.ctx, "UPDATE pg_index SET indisvalid = false WHERE indexrelid = to_regclass('phone_numbers_e164_held_idx')")
	s.Require().NoError(err)

	s.Require().NoError(s.store.Migrate(s.ctx))

	valid, definition := built()
	s.True(valid, "dropped and built again")
	s.Equal(want, definition, "built again as the migration builds it")
}

func (s *StoreSuite) TestMigrateRebuildsTheCampaignAttemptIndexLeftInvalid() {
	// As a CONCURRENTLY build that failed leaves it: the migration's IF NOT EXISTS skips it.
	_, err := s.store.GlobalDB().ExecContext(s.ctx, "UPDATE pg_index SET indisvalid = false WHERE indexrelid = to_regclass('campaign_attempts_campaign_idx')")
	s.Require().NoError(err)
	var valid bool
	read := func() {
		s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx,
			"SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass('campaign_attempts_campaign_idx')").Scan(&valid))
	}
	read()
	s.Require().False(valid)

	s.Require().NoError(s.store.Migrate(s.ctx))

	read()
	s.True(valid, "dropped and built again")
}

func (s *StoreSuite) TestMigrateRebuildsTheResponseParentIndexLeftInvalid() {
	// As a CONCURRENTLY build that failed leaves it: the migration's IF NOT EXISTS skips it.
	s.onShard("UPDATE pg_index SET indisvalid = false WHERE indexrelid = to_regclass('agent_responses_parent_idx')")
	var valid bool
	read := func() {
		s.Require().NoError(s.store.ShardDB().QueryRowContext(s.ctx,
			"SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass('agent_responses_parent_idx')").Scan(&valid))
	}
	read()
	s.Require().False(valid)

	s.Require().NoError(s.store.Migrate(s.ctx))

	read()
	s.True(valid, "dropped and built again")
}

func (s *StoreSuite) TestMigrateRebuildsTheCampaignIndexLeftInvalid() {
	s.onShard("UPDATE pg_index SET indisvalid = false WHERE indexrelid = to_regclass('calls_customer_campaign_idx')")
	var valid bool
	read := func() {
		s.Require().NoError(s.store.ShardDB().QueryRowContext(s.ctx,
			"SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass('calls_customer_campaign_idx')").Scan(&valid))
	}
	read()
	s.Require().False(valid)

	s.Require().NoError(s.store.Migrate(s.ctx))

	read()
	s.True(valid, "dropped and built again")
}

// The reconcile reads every shard every few minutes, so it reads only the rows naming a grant,
// by the partial index, rather than every request.
func (s *StoreSuite) TestTheGrantReconcileReadsByTheGrantsIndex() {
	var plan []string
	err := s.store.ShardDB().RunInTx(s.ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		// A table this small costs any index read and a sort about the same, so which one the
		// planner takes moved with whatever rows other tests had left. The grants index is the
		// one that reads in the order the reconcile pages by, so with no sort to fall back on
		// the plan says whether the query can be served by it at all.
		if _, err := tx.ExecContext(ctx, "SET LOCAL enable_seqscan = off; SET LOCAL enable_sort = off; SET LOCAL enable_incremental_sort = off"); err != nil {
			return err
		}
		return tx.NewRaw("EXPLAIN "+reconcileGrantsQuery, time.Now(), 0, grantReconcilePage).Scan(ctx, &plan)
	})

	s.Require().NoError(err)
	s.Contains(strings.Join(plan, "\n"), "requests_limit_grants_started_idx")
}

func (s *StoreSuite) TestARouterWaitingToMigrateDoesNotHoldUpTheOneMigrating() {
	// The router migrating holds the lock on a connection of its own, as Migrate does.
	holder, err := s.store.GlobalDB().DB.Conn(s.ctx)
	s.Require().NoError(err)
	defer holder.Close()
	_, err = holder.ExecContext(s.ctx, "SELECT pg_advisory_lock(hashtextextended('router.migrate', 0))")
	s.Require().NoError(err)
	unlock := sync.OnceFunc(func() {
		_, err := holder.ExecContext(s.ctx, "SELECT pg_advisory_unlock(hashtextextended('router.migrate', 0))")
		s.Require().NoError(err)
	})
	defer unlock()

	waiter, err := s.openRouter()
	s.Require().NoError(err)
	defer waiter.Close()
	migrated := make(chan error, 1)
	go func() { migrated <- waiter.Migrate(s.ctx) }()
	select {
	case err := <-migrated:
		s.FailNow("migrated while another router held the lock", "%v", err)
	case <-time.After(300 * time.Millisecond):
	}

	// A concurrent build waits for every snapshot older than its own to end. goose runs the
	// usage migration's on another connection of the pool than the one holding the lock, so
	// Postgres sees no deadlock to break.
	s.T().Cleanup(func() {
		_, err := s.store.GlobalDB().ExecContext(s.ctx, "DROP INDEX CONCURRENTLY IF EXISTS agent_logs_lock_wait_idx")
		s.Require().NoError(err)
	})
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()
	_, err = s.store.GlobalDB().ExecContext(ctx, "CREATE INDEX CONCURRENTLY agent_logs_lock_wait_idx ON agent_logs (customer_id)")
	s.Require().NoError(err, "a router waiting for the lock holds no snapshot the build waits on")
	unlock()

	select {
	case err := <-migrated:
		s.NoError(err)
	case <-time.After(30 * time.Second):
		s.FailNow("the waiting router never migrated once the lock was free")
	}
}

// shortLimit is the statement and idle limit of a limitedStore's connections.
const shortLimit = 300 * time.Millisecond

// limitedDSN is dsn with a statement and idle limit of shortLimit on every connection, as a
// DSN may set them, so a test can show what outlasts the serving limits without waiting for
// them.
func (s *StoreSuite) limitedDSN(dsn string) string {
	parsed, err := url.Parse(dsn)
	s.Require().NoError(err)
	query := parsed.Query()
	limit := fmt.Sprintf("%dms", shortLimit.Milliseconds())
	query.Set("statement_timeout", limit)
	query.Set("idle_in_transaction_session_timeout", limit)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

// limitedStore is another router's store on the suite's databases, every connection of it
// held to shortLimit.
func (s *StoreSuite) limitedStore() *Store {
	store, err := OpenShards(Options{DSN: s.limitedDSN(s.dsn), ShardDSN: s.limitedDSN(s.shardDSN)})
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.NoError(store.Close()) })
	return store
}

// postgresCode is the SQLSTATE Postgres answered err with, empty for an error of another kind.
func postgresCode(err error) string {
	var refused pgdriver.Error
	if errors.As(err, &refused) {
		return refused.Field('C')
	}
	return ""
}

func (s *StoreSuite) TestAStatementPastTheLimitIsEndedOnTheServer() {
	started := time.Now()
	_, err := s.store.GlobalDB().ExecContext(s.ctx, "SELECT pg_sleep(20)")
	took := time.Since(started)

	s.Equal("57014", postgresCode(err), "the server cancelled it, not the driver: %v", err)
	s.InDelta(8, took.Seconds(), 1)
	var running int
	s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx, `
SELECT count(*) FROM pg_stat_activity
WHERE datname = current_database() AND state = 'active' AND query LIKE '%pg_sleep(20)%' AND pid <> pg_backend_pid()`).Scan(&running))
	s.Zero(running, "nothing is left running on the server")
}

func (s *StoreSuite) TestAWaitOnAnotherSessionsLockIsEnded() {
	config := s.boundConfig("app")
	holder, err := s.store.GlobalDB().BeginTx(s.ctx, nil)
	s.Require().NoError(err)
	defer holder.Rollback() //nolint:errcheck // only ever holds the lock
	_, err = holder.ExecContext(s.ctx, "SELECT 1 FROM agent_configs WHERE id = ? FOR UPDATE", config.ID)
	s.Require().NoError(err)

	started := time.Now()
	_, err = s.store.GlobalDB().ExecContext(s.ctx, "UPDATE agent_configs SET name = name WHERE id = ?", config.ID)

	s.Equal("55P03", postgresCode(err), "the wait was ended as a lock timeout: %v", err)
	s.InDelta(3, time.Since(started).Seconds(), 1)
}

func (s *StoreSuite) TestAServingConnectionBoundsAnIdleTransaction() {
	var limit string
	s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx, "SHOW idle_in_transaction_session_timeout").Scan(&limit))
	s.Equal("30s", limit)
}

func (s *StoreSuite) TestDaysAreCutInUTCWhateverTheDatabaseSays() {
	var database string
	s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx, "SELECT current_database()").Scan(&database))
	_, err := s.store.GlobalDB().ExecContext(s.ctx, "ALTER DATABASE ? SET timezone = 'Pacific/Kiritimati'", bun.Ident(database))
	s.Require().NoError(err)
	s.T().Cleanup(func() {
		_, err := s.store.GlobalDB().ExecContext(s.ctx, "ALTER DATABASE ? RESET timezone", bun.Ident(database))
		s.NoError(err)
	})
	fresh, err := OpenShards(Options{DSN: s.dsn, ShardDSN: s.shardDSN})
	s.Require().NoError(err)
	defer fresh.Close()
	// One in the morning in UTC is the afternoon of the same day at UTC+14, whose day began
	// the evening before in UTC.
	morning := time.Date(2026, 3, 1, 1, 0, 0, 0, time.UTC)
	s.opened("one", "acme", morning, nil)

	buckets, err := fresh.CustomerActivity(s.ctx, "acme", ActivityDaily, morning.Add(-48*time.Hour), morning.Add(48*time.Hour))

	s.Require().NoError(err)
	s.Require().Len(buckets, 1)
	s.Equal(time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC), buckets[0].Bucket.UTC())
}

func (s *StoreSuite) TestALimitTheDSNSetsIsKeptOverTheDefault() {
	limited := s.limitedStore()
	tx, err := limited.GlobalDB().BeginTx(s.ctx, nil)
	s.Require().NoError(err)
	defer tx.Rollback() //nolint:errcheck // ended by the server
	_, err = tx.ExecContext(s.ctx, "SELECT 1")
	s.Require().NoError(err)

	time.Sleep(3 * shortLimit)
	_, err = tx.ExecContext(s.ctx, "SELECT 1")

	s.Error(err, "the transaction was ended at the DSN's idle limit, not the default's")
	_ = tx.Rollback()
	for range 3 {
		_, err := limited.GlobalDB().ExecContext(s.ctx, "SELECT 1")
		s.Require().NoError(err, "the pool hands out no connection the server ended")
	}
}

// inSmallBatches makes every batched prune and rename take one row per statement for the
// test, so a backlog of a few rows is more than one statement's worth.
func (s *StoreSuite) inSmallBatches() {
	kept := writeBatch
	writeBatch = 1
	s.T().Cleanup(func() { writeBatch = kept })
}
