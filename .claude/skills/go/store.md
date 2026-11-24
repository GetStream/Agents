# The store layer

`acceleration/internal/store` is the router's one state layer. It owns every Postgres table and
every Redis key that holds a model's state, so no other package imports bun, writes SQL or keeps
model state in Redis. Read this before you add a table, a column, a query or a Redis key.

## One repository per model

- A model is a Go struct for one table (or one Redis key family). Its file is named after it in
  the singular: `Session` lives in `session.go`, `CustomModel` in `custom_model.go`.
- The file holds, in this order: the model struct, its `Validate() error`, its errors
  (`ErrNoSession`), the repository type named in the plural (`Sessions`) and the repository's
  methods. Its tests are in `session_test.go` (Postgres, `StoreSuite`) and
  `session_unit_test.go` (no database).
- The repository is a field of `Store`, declared in `repos.go`, so a caller writes
  `s.store.Sessions.Get(ctx, customerID, id)`. Nothing outside the package holds a `*bun.DB`.

## Verbs

Use these names, with the customer first after `ctx` for every tenant-scoped model:

| Method | Does |
| --- | --- |
| `Get(ctx, customerID, id)` | One row by primary key; `ErrNoX` when absent. Other lookups are `GetByName`, `GetByAddress`. |
| `Select(ctx, customerID, filter)` | A page of rows. The filter is a struct named `XFilter` with `Limit` and the cursor (see the `pagination` skill). |
| `Insert(ctx, *X)` | A new row. Fills id and timestamps on the model. |
| `Update(ctx, *X)` | Replaces the mutable columns; `ErrNoX` when no row matched. |
| `Upsert(ctx, *X)` | Insert or update on the natural key. |
| `Delete(ctx, customerID, id)` | Removes one row; `ErrNoX` when absent. |

A state change that is more than a column write gets a domain verb on the same repository:
`Calls.Finish`, `CampaignContacts.Lease`, `Episodes.Close`. Batch versions take a slice
(`InsertMany`, `GetMany`) and run one statement, never a loop of single ones.

## Validation

- Every model has `func (x *X) Validate() error`. It checks what the table's NOT NULL and
  CHECK constraints check, plus what the column means (an E.164 number, a known enum value, a
  positive limit), and returns a `*ValidationError` naming the field.
- `Insert`, `Update` and `Upsert` call `Validate` before they touch the database, so invalid
  data never reaches Postgres. The API maps `*ValidationError` to a 400 with
  `invalidRequest` (`internal/api/apierror.go`).
- Keep the database constraints as well: they guard writers outside the store (migrations, a
  psql session).

## Which database

- `global` is the database every shard shares: tenants, configs, connectors, numbers, limits.
  `shard` is this router's own: sessions, responses, calls, call legs, requests and their
  rollups. A deployment of one database has both handles pointing at it.
- A repository holds only the handle its table lives in, set in `repos.go`. Reading across
  every shard goes through `everyShard` / `whicheverShard` (`shards.go`), never a loop over
  peers.
- Migrations live in `acceleration/migrations/global` or `acceleration/migrations/shard`,
  matching where the table lives. Each set has its own goose version table. A table never
  moves between the sets in place: add it to the new set, copy, then drop it.
- Every migration run in a transaction starts with `SET LOCAL lock_timeout = '5s';`, so one
  waiting behind a long query fails instead of holding every write queued behind it
  (`TestEveryTransactionalMigrationAfterTheInitialOneSetsALockTimeout`).
- The change log (`data_changes`, `data_change_capture`, `record_data_change()`) is in both
  sets, written idempotently, because a row's change is recorded in the database it is
  written to. A migration that changes it goes in both sets.
- `schema/global.sql` and `schema/shard.sql` describe what each set leaves. Regenerate them
  with `go run ./cmd/schema`; `TestTheSchemaFilesAreCurrent` fails while they are stale.

## Redis

- Redis state that belongs to a model (a call's turns and events, a user's last-seen time, a
  cached copy of a row) is a repository too, in the same file as the model if it has a table.
  It takes the store's `rueidis.Client`.
- Any method touching more than one key sends one round trip: `DoMulti`, a `MULTI`, or a Lua
  script. A loop issuing one command per item is a bug.
- Each Redis command of a cached read (`cachedFor`) waits `cacheWait` (250ms) for its answer,
  and so does any command on the shared client sent with no deadline, rueidisaside's own
  among them (`cacheReadBound`); then the row is read
  from Postgres. A Redis that is down slows a request and does not stop it. The load itself
  runs on the caller's context, and the readers of a cold key on one router share it.
- Every key has a TTL or a named owner that deletes it. Name keys `<family>:{<customer>}:...`
  so a customer's keys share a cluster slot when a script needs several.
- Rate limiters, admission counters and pub/sub stay in their own packages (`policy`,
  `quota`, `concurrency`, `relay`): they are protocols, not model state. The same one-round-trip
  rule applies to them.

## Queries

- Use bun's builders (see the `go` skill for when raw SQL is allowed).
- Every method has a query budget, asserted in its test with `s.countQueries`: a Get is one
  query, a Select one (two with a count), an Insert one. A method that reads related rows
  joins or batches them; it never queries once per row.
- Every Select and Get filters on columns an index covers, customer first. A new query on a
  large table (requests, sessions, calls, call legs, agent logs) gets an `EXPLAIN` check in
  `store_explain_test.go`: no sequential scan on seeded data.
- Opening the database with `bun.WithDiscardUnknownColumns()` means a release that does not
  know a new column still reads the row. So add a column to the table that owns it, nullable
  or with a default; do not create a 1:1 side table to dodge a rollback.
- A table of work routers claim (inbound messages, deliveries, jobs) is a `queue`
  (`queue.go`): `claim` leases due rows with `FOR UPDATE SKIP LOCKED` in one query,
  `nextDue` says when to look again, and every write after the claim goes through `heldBy`,
  so a claim whose lease ran out cannot overwrite the next one. `image_jobs.go` is the
  example. Do not write the claim by hand.
- Every serving connection runs with `statement_timeout` 8s, `lock_timeout` 3s,
  `idle_in_transaction_session_timeout` 30s and `timezone` UTC (`sessionDefaults` in
  `store.go`); a parameter the DSN names wins. Work that legitimately takes longer raises the
  limits for its own transaction with `boundTransaction` (the export, the grant reconcile,
  a trunk group), or a `SET LOCAL` of its own (`VendorCostGaps`). A prune or rename over
  many rows goes `writeBatch` rows per statement (`deleteInBatches`), and a rollup a bucket at
  a time: the driver gives up after 10 s, so no statement may outgrow that.

## Naming

- `customer_id` is the app id on every tenant table. `stream_app_pk` names a Stream app
  pinned to a row. `config_id` names an agent config.
- A lease is `lease_until` and `leased_by`. A queue row retries with `attempts` and
  `next_attempt_at`. A sealed secret column ends in `_sealed` with its key version in
  `<column>_kek_version` (`kek_version` when the table has one secret).

## Adding a model

1. Write the migration in the set the table belongs to.
2. Add `x.go` with the model, `Validate`, the repository and its methods, and wire the
   repository in `repos.go`.
3. If a data move must carry it, add it to `dataTables` (`datamove.go`).
4. Test it in `x_test.go` on `StoreSuite`: behaviour, the other customer's rows staying
   invisible, `Validate` refusing bad rows, and the query budget.
5. Regenerate `acceleration/schema/*.sql` with `go run ./cmd/schema`.

## Other SDKs

Changing what the store keeps does not change an SDK by itself. When it changes the HTTP API,
follow "SDK changes" in AGENTS.md.
