# internal/connectors/resolver

The `core.Resolver` the router uses: the one door to a connection's access credential. `Resolve` reads the connection, hands out a cached credential while the row still allows it, and otherwise gets one through the connection's scheme under the credential store's lock, with the checkpoint before any refresh. It moves the connection's status by what the scheme answered. `Invalidate` is the caller's way to say a provider refused the credential; `Revoke` is a verified provider event saying the grant ended. It replaces the prototype's `connectors.ResolveCredentials` (`internal/connectors/runtime.go:26-153` on `codex/connector-support` at `cf62af0d`), which refreshed on the tool call's context. `cmd/router/resolver.go` builds it over `pgsealed`. Request wrapping is `core.Transports` (T13, `core/AGENTS.md`, «Transport»), which `TransportSuite` here runs over this resolver; rate limiting (T28) is not here.

## Flow

```
Resolve(ref, req)
  row         store.ConnectorConnection, every call, no lock      deleted -> store.ErrNoConnectorConnection
  fast path   connected, cache[ref].revision == row.revision,     -> cached credential
              younger than maxAge, ExpiresAt after req.Deadline
              never when req.Refused is set
  slow path   on a goroutine; the caller's ctx ending returns ctx's error at once
    lock      CredentialStore.Update(ctx, ...)                    ctx bounds only the wait
    status    not connected -> ErrNotConnected
    load      definition at state.DefinitionRevision (inside the lock: a consent moves it)
    broken    a later revision marked it broken -> ErrNotConnected, nothing written
    Retrieve  scheme.Retrieve(detached, ..., opts)                context.WithoutCancel + retrieveTimeout
      ValidUntil  req.Deadline: the scheme renews a credential that expires before it
      Refused     req.Refused is still the stored revision: renew whatever the expiry
      Checkpoint  called by the scheme right before a refresh:
                  commit needs_reauthorization + lostRefresh, lock still held
    outcome   nil              -> connected, last_error "", expiry, new credentials (revision + 1)
              invalid_grant    -> needs_reauthorization, rejectedGrant       ErrNotConnected
              scope_required   -> needs_reauthorization, missingScope        ErrNotConnected
              uncertain        -> needs_reauthorization, lostRefresh         ErrNotConnected
              transient, other -> connected, temporarilyUnavailable          ErrTemporarilyUnavailable
              a credential beside the error -> handed out for this call, never cached
    CAS       the credential store's final commit at the revision it loaded
    save fail after a checkpoint, an error a retry can fix: saveAgain, a new Update
              each time, up to saveAttempts saves; writes only while the row is as the
              checkpoint left it; gives up with one error line, the checkpoint's status stays
    cache     only after a nil Retrieve, keyed by ref, at the revision the store left in state
    return    the credential, its Revision set to that revision

Invalidate(ref, rejected, why)    why is invalid_grant or scope_required, else refused
  drop the cache entry
  rejected had expired                     -> nothing more
  under the lock, connected and the stored revision still rejected.Revision
                                           -> needs_reauthorization; otherwise nothing more
    last_error                             invalid_grant of a core.Static scheme (bearer,
                                           api_key): rejectedStatic, replace it; else the
                                           why's: rejectedGrant or missingScope

Revoke(ref, why, endedAt)         why is revoked, uninstalled or rotated, else refused
  drop the cache entry
  under the lock, connected                -> needs_reauthorization, the why's last_error
                                              (any revision); otherwise nothing more
  under the lock, a renewal's checkpoint   -> the why's last_error, status stays: a retried
    (needs_reauthorization, lostRefresh)      save (saveAgain) finds it moved and leaves it
    connected_at after endedAt + 1 s       -> nothing more: a newer grant than the
                                              signal's (endedAt zero: never)
```

## Cache

- **Key**: the connection, holding the revision of the stored credentials the credential came from. A new revision (a refresh on any router, a reconnect) is a miss.
- **The row is read on every call.** A delete, a disconnect or an `Invalidate` on any router reaches every other router's next `Resolve`, with no window. A soft delete (`store.ConnectorConnections.Delete`, #746) sets `deleted_at` and `disconnected` and moves no revision, so a revision check alone would miss it. A maximum age alone would leave a window of that age on every router. An `Invalidate` from the delete handler would reach only the router that served the delete. Example: Alice deletes her connection through router A while her session runs on router B, which cached her token a second earlier. B's next `Resolve` reads no live row and fails, so the deleted connection's token never leaves B again.
- **A status other than connected is never served from the cache.** It goes to the lock, which is where a refresh in flight on another router (status `needs_reauthorization` by its checkpoint) ends: the waiter then sees the final state.
- **maxAge (30 s) bounds what the row does not show**: a credential the scheme would now renew, and stored credentials under an old key that `pgsealed` rewraps on its next use.
- **A credential that expires before `req.Deadline` is not served**: the call goes to the lock, and the scheme renews it for the deadline (`RetrieveOptions.ValidUntil`). The renewed one is cached, so the next call inside the window takes no lock. One exception: a credential this router renewed within `maxAge` is served while it has not expired, even when it expires before the deadline. It is as long as the provider issues them, so renewing again gives one no longer, and asking for that on every call would be a refresh per call. Then a call does start with a credential that dies during it; nothing can give it a longer one. Check: `go test -tags integration -run 'TestResolverSuite/(TestACachedCredentialThatExpiresBefore|TestACredentialThatExpiresBefore|TestACredentialRenewedForADeadline)' ./internal/connectors/resolver`.
- Entries past `maxAge` are dropped at most once per `maxAge`, when a new one is put.

## Rules

- **Retrieve runs detached from the caller.** A caller whose ctx ends during a refresh gets ctx's error at once. The refresh finishes, is committed, and the connection stays `connected` with the rotated credentials, never `needs_reauthorization`. Check: `go test -tags integration -run TestResolverSuite/TestCancellingTheCaller ./internal/connectors/resolver`.
- **The checkpoint is committed before the refresh token leaves**, and a lost answer is never followed by the same token. Check: `go test -tags integration -run 'TestResolverSuite/(TestNeedsReauthorizationIsCommitted|TestALostRefresh)' ./internal/connectors/resolver`, and the scheme side: `go test -run 'TestOAuth2CodeSuite/(TestARefreshCheckpoints|TestARefreshWhoseCheckpoint|TestAccessCredentialThatIsNotDueNever)' ./internal/connectors/schemes/oauth2code`.
- **A renewal's save that fails after its checkpoint is tried again.** Until it lands, the connection says `needs_reauthorization` and the tokens the provider may have rotated are only in memory. `saveAgain` saves the same state again, up to `saveAttempts` (3) saves with pauses from `saveFirstPause` (100 ms, then 200 ms), on the refresh's detached context. Each try is a new `CredentialStore.Update`, so it holds the lock again: the lock is the pool connection's session, and the pg driver closes that connection on every failure but a Postgres `ERROR` (`isBadConn` in `pgdriver` v1.2.18), so after a dropped connection no retry on it can work. What bounds the tries is `retrieveTimeout` (30 s from the refresh's lock), which ends the detached context; each try has its own dial, lock, read, save and unlock timeouts, so no fixed sum does. The caller is not held: `Resolve` returns when its ctx ends. Between two tries the lock is free and the row says `needs_reauthorization`, so a `Resolve` on any router gets `ErrNotConnected` (a session reports it as `customer_auth`, `session/dispatcher.go`), where during the refresh it waits on the lock. A `Revoke` between two tries writes its `last_error` on the checkpoint's row, so the next try finds the connection moved and the revocation stands. A try writes only while the row is still as the checkpoint left it: a reconnect, new credentials or a delete that came first stay. A save that landed with its answer lost is found done. `store.ErrConnectorConnectionChanged`, `store.ErrNoConnectorConnection` and what `store.Unwritable` names are not tried again. When it gives up it logs one error line with the connection id and the number of saves, and the connection keeps the checkpoint's `needs_reauthorization`. A renewal it gave up on with an error a retry can fix is held in the resolver's memory, one per connection and at most `unsavedMost` (1000), and the next `Resolve` of that connection on the same router saves it (`saveUnsaved`) and writes its audit row before resolving; on any other router, and after a process stops, the connection still needs a reconnect. A save no checkpoint went before is not tried again: the row still holds what it held. Example: router A renews Alice's token, which rotates it; its save hits a dropped connection; 100 ms later A saves again on a new connection, and Alice's connection reads `connected` with the new refresh token. Check: `go test -tags integration -run TestResolverSuite -testify.m 'TestARefreshWhose|TestASave|TestARetried|TestAFailedSave' ./internal/connectors/resolver`.
- **One refresh across routers.** Check: `go test -tags integration -run TestResolverSuite/TestRoutersResolving ./internal/connectors/resolver`.
- **A deleted connection does not resolve, even from the cache.** Check: `go test -tags integration -run 'TestResolverSuite/(TestADeleted|TestAnotherCustomers)' ./internal/connectors/resolver`.
- **The cache is keyed by revision.** Check: `go test -tags integration -run TestResolverSuite/TestAReconnect ./internal/connectors/resolver`.
- **`Invalidate` writes the status, not only the cache, and only for the grant behind the refused credential.** RFC 6750 §3.1 answers `invalid_token` for a token «expired, revoked, malformed, or invalid for other reasons». A refused credential that had expired, or whose revision the stored credentials have moved past (another router renewed them), leaves the status alone. Example: router B resolves at revision 4; router A renews to 5; the provider refuses B's old token; B's `Invalidate` drops its cache entry and the connection stays `connected`. Check: `go test -tags integration -run TestResolverSuite/TestInvalidate ./internal/connectors/resolver`.
- **`Revoke` is a provider ending the grant, so it checks no revision.** The events endpoint (`internal/api/connector_events.go`) calls it for each connection of the account a verified `core.Signal` names. `Invalidate` with a zero credential is not the same call: its revision check would leave a connected connection connected. Revoke runs under the lock, so a refresh in flight on another router commits first and the revocation is the last write. Example: router A renews Alice's Slack token to revision 5; Slack sends `tokens_revoked` for her; `Revoke` moves the row to `needs_reauthorization` at revision 5, and every router's next `Resolve` reads it and fails with `ErrNotConnected` without asking Slack. A pending, disconnected or already `needs_reauthorization` connection keeps its status. A connection a consent connected more than a second after the signal's `endedAt` (Slack's `event_time`, to the second) keeps its status too: Slack retries an event after 1 and 5 minutes (https://docs.slack.dev/apis/events-api/), and a retry that arrives after the admin reconnected is about the grant that ended. Example: `tokens_revoked` dispatched at 12:00:00 is first answered 500; the admin reconnects at 12:00:40; the retry at 12:01:00 leaves the new grant connected. Check: `go test -tags integration -run TestResolverSuite/TestRevoke ./internal/connectors/resolver`.
- **The revision is the credential store's.** The resolver reads the committed one from `state` after `Update` (`core.CredentialStore`); it never numbers revisions itself.
- **A connection on a revision marked broken gets no credential, and keeps its status.** A later revision's manifest marks it (`core.Manifest.BrokenRevisions`, table `connector_broken_revisions`); `retrieve` answers `ErrNotConnected`, as for a connection that needs a reconnect, and writes nothing. The session offers the binding's login (`needs_reauthorization`), and that consent runs on the latest revision and moves the connection there. The check is on the slow path only: marks change when a router seeds a new build, and a credential cached before that is handed out for at most `maxAge`. Example: Bob's Slack connection was made on revision 4; revision 6 marks 1 to 4 broken; his next `Resolve` fails with `ErrNotConnected`, his chat shows a login, and once it connects his connection reads revision 6. Check: `go test -tags integration -run TestResolverSuite/TestAConnectionOnABrokenRevision ./internal/connectors/resolver`.
- **Outcomes map to statuses as in the flow.** Check: `go test -tags integration -run 'TestResolverSuite/(TestARejected|TestAProvider|TestAPending)' ./internal/connectors/resolver`.
- **Never parse a scheme's error text.** The outcome is read with `errors.As` on `*core.OutcomeError` (`core/AGENTS.md`). Check: `grep -n 'Error()' internal/connectors/resolver/resolver.go` prints nothing.
- **Secrets never print.** Nothing here logs but an audit row that could not be written, with the connection id, the action and the store's error, and a renewal `saveAgain` gave up on, with the connection id, the number of saves and the store's error; none of these holds a credential. The `refresh_failed` line's `error_description` is the scheme's `core.Outcome.Description`, the provider's text with the refresh token and client secret it echoes redacted (`core.LoggedDescription`, AI-1245). The errors it returns wrap the scheme's `*core.OutcomeError`, which carries no secret (`core/contracttest` checks every scheme). Tests read a token only through `fixture.token` and compare two with `==`, not `Equal`, so a failure prints no token. Check: `grep -n 'slog\|log\.' internal/connectors/resolver/resolver.go` prints only the import, the logger's field and `New`'s `slog.Default()`.
- **One audit row for each grant change it commits** (T47, `store.ConnectorAuditEvent`). A `Retrieve` that commits a new revision of a connection that was connected writes `grant_refreshed`; one that leaves it anything but connected writes `grant_revoked` with the outcome's kind (`invalid_grant`, `scope_required`, `uncertain`) as the reason. An `Invalidate` or `Revoke` that moved the status writes `grant_revoked` with its outcome or signal kind, and so does a `Revoke` that wrote its `last_error` on a renewal's checkpoint. A `Revoke` of a row the checkpoint left is a change of its own, so an `uncertain` outcome followed by a provider revocation leaves two rows (`uncertain`, then the signal) and the signal's text replaces `last_error`. The fast path, and a renewal that changed nothing, write none. The row is written after the commit, detached from the caller (`auditWriteTimeout`, 5 s), with the request and session ids `core.CorrelationOf(ctx)` holds; a failed write is logged and the change stands. Example: router A refreshes Alice's token during her session's call: one `grant_refreshed` row at revision 5 with the session's id; router B, which waited on the lock and found revision 5, writes none. A grant created at a consent or a credentials write, and one revoked by a delete, are written by the API (`internal/api`). Check: `go test -tags integration -run 'TestResolverSuite/TestAudit' ./internal/connectors/resolver`.
- **Every hardcoded value says where it comes from**, beside it: `retrieveTimeout`, `maxAge`, `auditWriteTimeout`, `saveAttempts` and `saveFirstPause`, the `last_error` texts (`revokedErrors` for `Revoke`). Status strings are the store's constants.

## Tests

From `acceleration/`, with Postgres on `:55432`:

```bash
go test -tags integration -count=1 -race ./internal/connectors/resolver
go test -tags integration -run x -bench . ./internal/connectors/resolver     # p50 and p95, spike 4
```

`ResolverSuite` drops and migrates a database of its own (`testenv.Database(dsn, "resolver")`). Each router is a pool of its own (`fixture.router`), so a race reaches Postgres. The provider is `fakeprovider` and the scheme is the real `oauth2code`; `fixture.interposed` runs a hook when a refresh reaches the transport, which is how a test reads the row mid-refresh or cancels the caller. `fixture.hold` keeps a connection's lock on another router, which is how a test tells the fast path (answers) from the slow one (waits). Benchmarks print p50 and p95 and assert no target (spike 4). No mocks (`.claude/skills/go-testing/SKILL.md`).
