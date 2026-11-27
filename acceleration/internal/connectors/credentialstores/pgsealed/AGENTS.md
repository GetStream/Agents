# internal/connectors/credentialstores/pgsealed

The `core.CredentialStore` that keeps a connection's credential state in Postgres: the `connector_connections` row, with `core.StoredCredentials` sealed by `auth.Sealer` (AES-GCM, KEK keyring) into `credentials_sealed`. The SQL is the store's (`internal/store/connector_connection.go`: `ConnectorConnections.WithLocked`, `ConnectorConnections.SaveAtRevision`); this package turns the row into a `core.CredentialState` and back, and owns sealing, the revision and the rewrap. The resolver (`internal/connectors/resolver`) takes it directly, built in `cmd/router/resolver.go`: a connection names no credential store, so `core.Registry.CredentialStores` stays empty until a second one exists.

## Flow

```
Update(ref, fn)
  1. lock     pg_try_advisory_lock(839, key(customer, id)) on a lock pool connection, kept if it
              got the lock; otherwise the connection goes back, pause 5ms doubling to 100ms, retry
              until ctx is done
  2. load     the live row, on that connection
  3. open     AAD = customer, id, revision, under either key
              credentials_data_key_version NULL: credentials_sealed under credentials_kek_version
                opened -> router_sealed_kek_opens_total{record="connector_credentials"} + 1
                key version not in the keyring -> return ErrKeyUnavailable, write nothing
              credentials_data_key_version N: credentials_sealed under version N of the app's
              data key (appkeys), which KMS unwraps
                router without data keys, or KMS did not unwrap -> return ErrKeyUnavailable,
                write nothing
                the app has no data key N -> as does not authenticate
              does not authenticate -> state is needs_reauthorization with empty Credentials, committed now
  4. fn       fn(state, checkpoint)
  5. checkpoint (any number of times, inside fn)
              seal if Credentials changed (revision + 1) or the key is old (same revision)
              auth.app_keys_seal on: the seal is under the app's data key (credentials_kek_version 0,
              credentials_data_key_version = the key's version)
                KMS cannot give the key -> the seal is under the router's current key, with
                credentials_data_key_version NULL, so the write still succeeds
              auth.app_keys_seal off: the seal is under the router's current key, with
              credentials_data_key_version NULL
              the blob, the key version and the data key version are written in one statement
              credentials under a data key are never old, so they are never rewrapped
              UPDATE ... WHERE id AND customer AND revision = <last committed> AND deleted_at IS NULL
              detached from ctx (2s), commits at once: the lock is session-level, so it stays held
  6. final    changed -> one more CAS like 5 with fn's state
              unchanged but credentials under an older key -> CAS like 5 with the state as stored
              unchanged, auth.app_keys_seal on, credentials opened under the router's current key
                -> reseal: the stored credentials under the app's data key, CAS at the same revision
                KMS cannot give the key, or the CAS finds a newer row -> log a warning, write
                nothing, and the use still succeeds
  7. unlock   pg_advisory_unlock, then the connection goes back to the lock pool
```

Nothing waits inside Postgres at 1, so a caller that gives up just stops trying: no wait stays queued on the server and no connection is held while it pauses. A blocking `pg_advisory_lock` cannot promise that, because pgdriver bounds every read by its `ReadTimeout` (10s by default, `newDefaultConfig` in `uptrace/bun/driver/pgdriver` v1.2.18) or the ctx deadline, drops the socket when it passes, and the server "will detect the loss of the connection only at the next interaction with the socket" ([client_connection_check_interval](https://www.postgresql.org/docs/current/runtime-config-connection.html#GUC-CLIENT-CONNECTION-CHECK-INTERVAL)). The cost of trying is that waiters are not served in order. Writes at 5 and 6 run detached from ctx for the same driver reason: a passed deadline is a passed socket deadline, which would drop tokens a provider already rotated.

## Why session-level here and xact-level in connector_definition.go

```
connector_definition.go (T6, revisions)       credential_lock.go (T8, this store)
BEGIN                                          pg_try_advisory_lock        <- held
  pg_advisory_xact_lock    <- held             UPDATE  status = needs_reauthorization   (checkpoint, autocommit)
  SELECT latest, INSERT next                   POST refresh_token to the provider        <- side effect
COMMIT                     <- released         UPDATE  credentials at revision + 1        (final CAS)
                                               pg_advisory_unlock          <- released
```

T6 does all its work in one transaction, so the lock can end with it. Here the checkpoint has to be visible to every other router before the refresh token leaves, which means it commits while the lock is still needed: a transaction-level lock would be released by that commit. "Once acquired at session level, an advisory lock is held until explicitly released or the session ends", and session-level requests "do not honor transaction semantics" ([explicit-locking, Advisory Locks](https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS)). Because it outlives transactions, it is held on one dedicated `db.Conn` and released in a defer; an unlock that fails closes the connection, which ends the session and the lock.

The key is the two-integer form `(839, first 4 bytes of SHA-256(customer NUL id))`. Postgres keeps the one-bigint and two-integer key spaces apart ("these two key spaces do not overlap", [functions-admin, Advisory Lock Functions](https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-ADVISORY-LOCKS)), and every other advisory lock in the repository takes one bigint, so no credential lock can be a T6 or agent-log lock.

The second key is 32 bits because the two-integer form is what buys that separation, and its first half is the namespace. So two connections can share a key, which costs contention, never correctness: the lock only orders callbacks, and each callback writes its own row under its own compare-and-swap. With keys uniform over 2^32 values, a lock taken while k others are held shares a key with one of them with probability about (k-1)/2^32, 2.3e-7 for k = 1000; a table of a million connections has about 116 colliding pairs (n(n-1)/2 / 2^32), and a pair waits only when both are locked at once (`credentialLockKey`).

## Rules

- **The credential store owns the revision.** New `StoredCredentials` (any byte of `Scheme`, `Version`, `Payload` differs) are sealed for revision + 1; anything else keeps the revision. fn changing `CredentialState.Revision` gets `errRevisionIsTheStores`. Check: `go test -tags integration -run 'TestPGSealedSuite/(TestNewCredentials|TestTheRevisionIsNot|TestAStatusChange)' ./internal/connectors/credentialstores/pgsealed`.
- **Every write is a compare-and-swap on the revision last committed.** A stale write, or one onto a deleted row, is `store.ErrConnectorConnectionChanged`. Check: `go test -tags integration -run 'TestStoreSuite/(TestASaveAtAStale|TestASaveOntoADeleted|TestAWriteThatSkipped|TestALockedCallbackCannotMove)' ./internal/store`.
- **`ConnectorConnections.SaveAtRevision` is new credentials at the next revision, nothing else.** A checkpoint can commit a status without moving the revision, so a same-revision save from an older snapshot would put back the credentials the checkpoint retired. Check: `go test -tags integration -run 'TestStoreSuite/(TestASaveIsNewCredentials|TestASaveCannotPutBack)' ./internal/store`.
- **A commit lands even after the caller's deadline**, within 2s of its own. Check: `go test -tags integration -run TestStoreSuite/TestACommitAfter ./internal/store`.
- **One locked callback per connection at a time, across routers.** Check: `go test -tags integration -run TestStoreSuite/TestLockedCallbacks ./internal/store` and `go test -tags integration -run TestPGSealedSuite/TestConcurrent ./internal/connectors/credentialstores/pgsealed`.
- **Checkpoint before a side effect that cannot be taken back.** A refresh whose outcome is never learned leaves `needs_reauthorization` and the refresh token is never sent again. Check: `go test -tags integration -run TestPGSealedSuite/TestRefreshOutcomeSurvives ./internal/connectors/credentialstores/pgsealed`.
- **A waiter waits as long as its ctx allows, and one that gives up leaves no lock, no queued wait and no borrowed connection.** `TestAWaiterOutlastsTheDriversReadTimeout` takes 12s by design. Check: `go test -tags integration -run TestStoreSuite/TestAWaiter ./internal/store`.
- **Stored credentials are sealed for one tenant, connection and revision.** The AAD is `accelerate:connector-material:v1:<len>:<customer>:<len>:<id>:<revision>`, a label that keeps the old word "material" so blobs sealed before the rename still open; a blob from another row, customer or revision does not open and the connection becomes `needs_reauthorization`, with the blob left for a reconnect to replace. Check: `go test -tags integration -run 'TestPGSealedSuite/TestABlobFrom' ./internal/connectors/credentialstores/pgsealed` and `go test -run TestSealSuite ./internal/connectors/credentialstores/pgsealed`.
- **A key version the keyring lacks is the keyring's fault, not the connection's.** `Update` returns `auth.ErrKeyVersionUnavailable`, runs no fn and writes nothing, so putting the key back restores the connection. Check: `go test -tags integration -run TestPGSealedSuite/TestAKeyVersionMissing ./internal/connectors/credentialstores/pgsealed`.
- **Stored credentials under an older key are rewrapped on the next use that returns no error**, at the same revision, and a use that returned changed false has its rewrap written from what was stored, never from fn's edits. Check: `go test -tags integration -run 'TestPGSealedSuite/(TestCredentialsSealedUnder|TestAFailedUse|TestARewrapKeeps)' ./internal/connectors/credentialstores/pgsealed`.
- **With sealing on, credentials under the router's key move to the app's data key on their next use that returns no error**, at the same revision, and that move never fails the use: when KMS cannot give the key or another writer saved first, the row stays as it is. With sealing off nothing moves, and every open under the router's key is counted. Check: `go test -tags integration -run 'TestPGSealedSuite/(TestWithSealingOnARowSealedOnTheBase|TestWithSealingOff|TestWithSealingOnAKMSFailure|TestAResealThatLoses|TestWithSealingOnAnUnreadable|TestTwoRoutersResealing|TestCredentialsSealedOnTheBase)' ./internal/connectors/credentialstores/pgsealed`.
- **`connected_at` is the caller's.** A commit writes the `ConnectedAt` fn leaves; the store stamps nothing. Whatever begins a grant names when it began: a consent (`api/authorizations.go`), and a credentials write that connects a connection that was not connected (`api/connection_tools.go`). A refresh keeps it, even past its own checkpoint, and so does the resolver's `saveAgain`, which saves a renewal again in an Update that loads the checkpoint's `needs_reauthorization` (AI-3504). `Resolver.Revoke` reads it to leave a grant newer than a provider's signal, and the tool pins of a grant are keyed by it. Check: `go test -tags integration -run 'TestPGSealedSuite/(TestAConnectionCommittedConnected|TestARefreshPastItsCheckpoint|TestWhenAConsentConnected)' ./internal/connectors/credentialstores/pgsealed`.
- **A lock is held on a connection of the store's lock pool, never of the serving pool.** The lock keeps its connection across the provider round trip, and fn's other statements (the resolver's definition and broken-revision reads, the OAuth client lookup) each borrow a serving connection for one statement. Locks stalled at a provider fill only the lock pool (16, `lockConns` in `internal/store/credential_lock.go`), and a lock past it waits for a lock connection until its ctx ends. One customer's locks, credential and provider app ones together, hold at most 4 of the 16 (`lockConnsPerCustomer`, counted per router process), so a customer that stalls its own refreshes leaves the rest to the others; its fifth waits holding no connection until its ctx ends. Check: `go test -tags integration -run 'TestStoreSuite/(TestCredentialLocksHeldAtOnce|TestClosingTheStoreClosesItsLockPool|TestOneCustomersStalledLocks|TestACustomersLockSlotIsGivenBack)' ./internal/store` and `go test -tags integration -run TestResolverSuite/TestRefreshesStalled ./internal/connectors/resolver`.
- **fn does not lock the same connection again** (`Update`, `ConnectorConnections.SaveAtRevision`): that is a second pool connection waiting on the first.
- **Secrets never print.** Stored credentials go into an error or a log only as `core.StoredCredentials`, whose `String`, `GoString` and `LogValue` redact the payload; tests compare blobs with `bytes.Equal`, not `Equal`, so a failure prints no ciphertext.
- **Every hardcoded value says where it comes from**, beside it.

## Tests

From `acceleration/`, with Postgres on `:55432`:

```bash
go test ./internal/connectors/credentialstores/...                                   # unit: seal, open, AAD
go test -tags integration -count=1 -race ./internal/store ./internal/connectors/credentialstores/...
```

`PGSealedSuite` drops and migrates a database of its own (`testenv.Database(dsn, "pgsealed")`), as `StoreSuite` does. A concurrent caller is a router with a pool of its own (`router()`), or the race never reaches Postgres. The token endpoint and the resolver in the tests are written there, not taken from `fakeprovider` or a scheme: the credential store is under test, not refresh.
