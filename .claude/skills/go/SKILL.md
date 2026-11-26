---
name: go
description: How Go is written in this repo - errors (errors.md), state (store.md), logging (logging.md), spans, context, goroutines, comments. Read before writing or changing Go code.
paths:
  - "**/*.go"
---

# Go

These rules are the choices this repo makes. General Go style is not repeated here.

## Rules

- Check the `go` directive of the nearest `go.mod` before you use a newer feature. The 8 modules pin different versions: `acceleration` is on 1.27, `tui` on 1.26, `sdks/go` on 1.25.
- Errors: read `errors.md` beside this file before you make, wrap, log or answer one. In short: `stack.Wrap` once where it enters, handle it once, and never send its text to a caller.
- Log with `log/slog`. Put the data in key-value fields, not in the message: `logger.Warn("could not store an MCP event subscription", "subscription", sub.ID, "error", err)`.
- An HTTP handler returns its error and does not log it. `withRequestLog` (`internal/api/server.go`) logs the error with its stack, and a 5xx at error level (see `errors.md` and "HTTP API" in AGENTS.md).
- Name a span in lowercase dotted words, the area then the step: `appconfig.read`, `quota.allow`, `turn.llm`.
- Pass `ctx context.Context` as the first parameter. A service may hold a lifetime context made in `New`, with its cancel function, as `channels.Service` does.
- Pass the caller's context on; never start from `context.Background()` where one is reachable. A new root drops the trace span and the attribution, so the work shows up in no trace and bills no one. Work that outlives the caller (a goroutine, a deferred cleanup) takes `context.WithoutCancel(ctx)` and its own timeout. An object that outlives the request that made it keeps `context.WithoutCancel(ctx)` in a `ctx` field, as `agent.Agent` and `session.Session` do; never the request's context itself, which is cancelled. `Background` is right only at a root: `main`, a lifetime context in `New` or `Start`, a queue worker. Tests use `t.Context()`; a `Cleanup` runs after it is cancelled, so it uses `context.WithoutCancel(t.Context())`.
- Start no goroutine in `init()` or in a constructor. Start it in `Start(ctx)` or `Run(ctx)`, as `chatlog.Log.Start` does.
- In the code of a call (the agent, the floor control, the harness, the speech and model providers and their routers), start a goroutine with `goroutine.Go(ctx, fn)` (`internal/goroutine`), not with `go`. Give it the context of the call. Then a panic in the goroutine stops only that call, not the router and all its calls. `TestEveryGoroutineOfACallStartsThroughGo` (`internal/goroutine/guard_test.go`) fails on a `go` statement in these packages.
- Only the goroutine that owns a channel closes it, and it closes it once. A goroutine that can still send after the owner stops does not send on that channel: it selects on the owner's done channel or `ctx.Done()`. A send on a closed channel and a second close both panic. The backend tests run with `-race`, which finds the races that lead to these panics.
- Return a nil slice for "none". A JSON response is the exception: it sends `[]`, not `null`.
- Query Postgres through bun's builders (`NewSelect`, `NewInsert ... On("CONFLICT ...")`, `NewUpdate`, `NewDelete`) on a model, not `NewRaw` or `ExecContext` with a SQL string. A table without a model gets a small unexported one. An insert lists `.Column(...)` so database defaults still apply. Keep SQL raw only where the builder cannot write it or would change what it does: advisory locks, `SET LOCAL`, DDL and catalog reads, `INSERT ... SELECT`, `WITH RECURSIVE`, window and `GROUPING SETS` queries, a statement whose SQL text a test matches, and jsonb that must refuse `\u0000` (bun stores it as text).
- Defensive code (a retry, a fallback, a second check) gets a comment that names the concrete threat: concurrent writers, a network failure the caller retries, malformed input. If you cannot name one, do not add it (see "Code style" in AGENTS.md).

## Comments

- A comment says why, once, in one full sentence: subject first, then verb, the reason joined with "since", "so" or "because". Do not narrate what the code does.
- Short does not mean compressed. Do not open with a participle ("Omitted, ...") or end with a clause that turns the sentence around ("which its minimum refuses"). Write `// A cap that was never set stays out of the response, since a 0 there is below the field's minimum and a client that sends the config back would get a 400.`, not `// A cap never set is left out rather than rendered as 0, which its minimum refuses.`
- Write no new ticket, PR or plan reference in a comment: it belongs in the commit and the PR. Leave the ones already there.
- A link to a public, stable source (an RFC, an engine's documented limits) is fine. The comment still states the fact.

## Other skills

- Errors, wrapping, logging and what reaches a caller: `errors.md` beside this file.
- Postgres and Redis state, models, repositories and validation: `store.md` beside this file.
- When to log and at which level, field names, redaction, and a value a tool reads back from a log: `logging.md` beside this file.
- Tests: the `go-testing` skill (`.claude/skills/go-testing/SKILL.md`).
- Timeouts on outbound calls and streams: the `security` skill (`.claude/skills/security/go.md`, "Abuse limits").
- N+1 queries and batch sizes: the `loop_performance_bench` skill (`.claude/skills/loop_performance_bench/SKILL.md`).
