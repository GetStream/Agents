# Errors

Read alongside [SKILL.md](SKILL.md).

## Make and check

- Return `error` last and a literal `nil` on success. Never return a typed nil pointer as an `error`: it is not `nil` to the caller. Check errors before using results that require success. For an `io.Reader`, consume any returned bytes first and preserve a bare `io.EOF` at the end of input.
- Use lowercase messages with the package and operation. Omit "failed to" and trailing punctuation. Write `store: save voice binding: connection refused`.
- Export a sentinel when callers need to recognize a condition, or a type when they need fields. Match with `errors.Is(err, ErrX)` or `errors.AsType[*XError](err)` (Go 1.26+, otherwise `errors.As`). Never compare a wrapped error with `==`, and never branch on error text (`strings.Contains(err.Error(), ...)`).

## Wrap

- In `acceleration/`, call `stack.Wrap` once where the failure occurs or an external call returns it (`internal/stack/stack.go`). Capture a sentinel's stack at the return site: `stack.Wrap(ErrNoVoice)`. Wrapping again is a no-op, so pass the error up unchanged from there.
- Pass errors up unchanged unless you can add useful context. Use `fmt.Errorf("store: save voice binding: %w", err)` when callers may inspect the cause. Translate private causes at package boundaries, e.g. `sql.ErrNoRows` to `store.ErrNoVoice`.
- Use `errors.Join` when multiple failures matter. An `errors.Is` match on one member does not classify the others.

## Handle and log

- Preserve the operation's error. A later `ctx.Err()` can mask a database failure. Stop quietly on caller cancellation. Report dependency deadlines that expire while the request is active. Retry only when safe and time remains.
- Log once where the outcome is decided. Never log an error and also return it. Use structured `slog` fields (`"error", err`) and redact sensitive values. Use error level when an operator must act and warn for degraded service. Explain discarded errors (`_ =`) in a comment.
- Give each goroutine an owner that checks the result of `group.Wait()` or reads its error channel. A worker that handles its own failure owns the log.

## HTTP

At the HTTP layer an error is translated, never wrapped: the caller gets a message we wrote, and the original error goes only to the log. `APIError.Error()` is its `Message`, so wrapping an internal error into one sends its text to the caller.

- Map recognized failures to an `APIError` with a caller-safe message (`invalidRequest`, `notFound`, `conflict`, `unavailable`, ... in `internal/api/apierror.go`; `unknownOr(err, store.ErrNoX, errUnknownX)` for a missing row). Its type sets the status. Use 404 only for a confirmed missing resource and 503 `unavailable` for temporary outages. A database or network failure is never a 404 or a 400.
- Return unexpected errors unchanged. Huma hands them to `answerFailure` (`internal/api/requestid.go`), which logs them through `withRequestLog` with their stack under `X-Request-Id` and answers a generic 500 `internal_error`. A route served by hand answers its own refusals with `writeError(w, apiError)` and every other error with `writeFailure(w, r, err)`.
- Keep internal error text out of responses: no `invalidRequest(err.Error())`, no `fmt.Sprintf("...: %v", err)` into a message, unless the error is one this code wrote for the caller to correct. A store, vendor or network error carries hosts, ports, SQL, constraint names, URLs and bodies. With `CGO_ENABLED=0` a DNS failure names the cluster's DNS server, a `*net.OpError` names local and remote addresses, and a `*url.Error` names the method and full URL.
- An error kept for a later read (a job's error, a binding's error, a turn's error) stores a fixed reason for the caller. The full text goes to the log.
- If streaming has started, signal failure through the protocol (a fixed `error` frame, a close reason) or end the stream. The frame carries the same caller-safe text an HTTP answer would.

## Panic and cleanup

- Reserve `panic` for broken invariants. Recover only where the work can safely end. In a deferred function in the same goroutine, capture `debug.Stack()` and report failure to the owner.
- Close resources on every path. Check `Flush`, `Close`, and `Commit` when success depends on them. If both writing and closing fail, preserve both errors with `errors.Join`. Check `rows.Err()` after SQL iteration.

## Tests

- Test the answer, not the call: assert the status, the `code` and that the body holds none of the cause's text. `TestADatabaseFailureIsAnsweredAsInternalWithNothingOfTheDatabase` (`internal/api/requestid_test.go`) runs a real store against an unreachable Postgres through the `served` harness. In a `RouterSuite`, the `failQueries` hook makes a query fail; a local `httptest` server stands in for a vendor that answers 500 with a body.
- Pair each such test with one showing a refusal the caller can correct still answers its 400 with its text.
