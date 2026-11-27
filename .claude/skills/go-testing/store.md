# DB model and state testing

The models and repositories in `internal/store`, tested against a real Postgres and Redis: rows in, rows and aggregates out. How the layer is built is in the `go` skill's `store.md`.

## Rules

- Put a test next to the file it tests: `session.go` is tested in `session_test.go`.
- Every integration test is a method on the one `StoreSuite` (`store_test.go`), so the schema is migrated once and every file shares its helpers.
- Assert on what a method returns, never on the SQL it ran. Reach for `s.global` or `s.shard` only to set up a state the repositories cannot make, such as backdating `updated_at`.
- Pin time to `s.base`, the start of a fixed hour, so hourly and daily buckets fall where the test expects.

## The store suite

`SetupSuite` opens a database of the suite's own (`testenv.Database`), because dropping the schema and truncating tables is not something to do to a database another package's suite is reading while `go test` runs them side by side. It refuses any database whose name does not end in `_test`, drops the schema and runs both embedded migration sets, so the migrations are what the tests exercise. `SetupTest` truncates the tables and flushes the suite's Redis database, so each test starts empty and fixed ids such as `"app"` and `"one"` are safe here, unlike in controller tests.

The truncate list is read from the catalogue, so a new table needs nothing there.

`ShardedStoreSuite` runs every test again with the shared tables in one database, this shard's (`here`) in another and a second shard's router beside it (`s.other`, shard `there`). Neither opens the other's database. A row written through `s.other` is not read through `s.store`: a test of a cross-region page ships it first (`ShipIndex`, `ShipStats`) and reads the global copy, or runs the shard job, and one about the shard itself asserts the other's row is absent.

## Helpers

A helper makes one row, defaulting the fields a test does not care about, and takes a function to change the ones it does:

```go
func (s *StoreSuite) TestSavingASessionTwiceIsOneConversation() {
	s.opened("one", "app", s.base, func(session *AgentSession) { session.Title = "first" })
	s.opened("one", "app", s.base, func(session *AgentSession) { session.Title = "renamed" })

	found, err := s.store.Sessions.Get(s.ctx, "app", "one")
	s.Require().NoError(err)
	s.Equal("renamed", found.Title)
}
```

`opened` (sessions, in `session_test.go`) and `record` (requests, in `store_test.go`) are the ones to copy.

## Scoping

Every query that takes a customer gets a test that another customer's rows are invisible to it, both fetched by id and listed (`TestASessionIsOnlyItsOwnCustomers`).

## Validation

Each model's `Validate` gets a table test in `<file>_unit_test.go` naming the field each bad row is refused for, and one suite test that `Insert` of an invalid row returns a `*ValidationError` and writes nothing.

## Budgets

A state method's cost is part of its behaviour, so its test pins it:

```go
queries := s.countQueries(func() {
	_, err := s.store.Sessions.Select(s.ctx, "app", SessionFilter{Limit: 20})
	s.Require().NoError(err)
})
s.Equal(1, queries)
```

`s.countRedis` does the same for Redis round trips (a `DoMulti` is one). Seed more rows than one page before counting, so a query per row shows up. `store_explain_test.go` seeds a few thousand rows and asserts the plan of each hot query uses an index.

## Without Postgres

Logic decided before any SQL runs, such as validation or which query a call picks, goes in `<file>_unit_test.go` with no build tag and plain `require`: see `session_unit_test.go`.

## Run

```bash
cd acceleration
go test ./internal/store                    # unit
go test -tags integration -run TestStoreSuite ./internal/store
go test -tags integration -run '^$' -bench . ./internal/store   # benchmarks
```

The DSNs come from `internal/config/testing.yaml`; see the `parallel-agents` skill for running beside other agents.
