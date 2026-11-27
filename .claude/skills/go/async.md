# Async Processing & Event Dispatching Guide

This guide explains how background work and event dispatching are structured in
`acceleration/`, when to use `asynq` tasks (`internal/async`) versus other concurrency
primitives, how to add a new task, and how to test tasks and the code that enqueues them.

## 1. Decision Guide: When to Use `asynq` vs. When Not To

Choose the concurrency mechanism based on durability, latency, and coupling:

| Pattern                                         | When to use it                                                                                                                                                                                                                         | When NOT to use it                                                                                                                  | Examples                                                                                                                                                                                         |
|-------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **`asynq` task** (`internal/async`)             | Work triggered off an HTTP request, webhook, or session callback that performs DB writes, external API/LLM/OCR/Stream Chat calls, needs retries/backoff, must survive pod restarts, or can be offloaded to `router async-worker` pods. | Sub-second audio/video streaming, live WebRTC/WebSocket I/O, or high-rate (>1,000/s) telemetry batching.                            | `message.new` dispatch, channel bridge replies, webhook forwarding, image draw/post, knowledge crawl/OCR, call QA review, conversation titling, episode summaries, call-site stats, simulations. |
| **Periodic task** (`async.RegisterPeriodic`)    | Recurring maintenance, pruning, sweeps, or polling external APIs every few minutes/hours. Deduplicated across replicas via unique windows.                                                                                             | Sub-minute liveness heartbeats or process-local cache refreshes.                                                                    | Data-change pruning, tool output redaction, 10DLC poll, Daytona secret sweep, phone sweeps, cost anomaly checks.                                                                                 |
| **Bounded channel + single writer goroutine**   | High-throughput ordered/batched writers where in-memory aggregation is intentional, with fixed buffer capacity, explicit drop/backpressure metrics, and deterministic `Close(ctx)` drain.                                              | Durable user-facing work or arbitrary background fan-out.                                                                           | `routing.Writer`, `session.sessionRecorder`, `session.logRecorder`, `activity.Recorder`, `chatlog` (ephemeral streaming updates).                                                                |
| **Live-connection goroutines** (`goroutine.Go`) | Driving an active real-time media or WebSocket session, or process-local heartbeats/caches tied to this process's lifetime.                                                                                                            | Anything triggered by an HTTP request or webhook that outlives the request.                                                         | `internal/agent`, `internal/stt`, `internal/tts`, `internal/sts`, `node.Directory`, `pricing.Active`, `relay.Bus`.                                                                               |
| **Fire-and-forget `go func()`**                 | **Never** for work with side effects, DB writes, or network I/O triggered from HTTP handlers, webhooks, or session callbacks.                                                                                                          | Always forbidden: drops work on crash, bypasses concurrency limits and shutdown drain, leaks goroutines in tests, and hides errors. | —                                                                                                                                                                                                |

### Rules for Direct Go Channels and Goroutines

1. Channels are only for **intra-process streaming and bounded batching**, never as a
   replacement for a durable work queue.
2. Every channel-backed worker MUST define:
   - a fixed buffer capacity constant,
   - explicit backpressure behavior (drop with Prometheus counter, or context-bounded send),
   - deterministic `Close(ctx)` drain wired into `cmd/router/main.go`.
3. Never spawn an unbounded `go func()` per channel message or HTTP request.
4. Always `recover()` in a deferred function with logging and cleanup.
5. Always prefer `errgroup` and `wg`.

### Rules for `asynq` Task Payloads & Handlers

1. **No secrets or large binary blobs in payloads.** Payloads are stored in Redis and visible
   in dead-letter queues and monitoring tools. Store blobs in object storage and secrets in
   sealed DB columns; pass only IDs / object keys in the task struct.
2. **Forward/backward compatible JSON.** Always tag every exported field with explicit `json`
   tags; newly added fields must have safe zero values during rolling deploys.
3. **Handlers must be idempotent.** `asynq` guarantees at-least-once delivery. Use
   `async.WithTaskID(...)` / `async.WithUnique(...)` plus durable idempotency keys
   (deterministic message IDs, `deliveries_seen`, Redis `SET NX` fences, `ON CONFLICT DO NOTHING`).
4. **Distinguish permanent vs. retryable errors.** Wrap permanent failures (deleted rows,
   invalid input, 4xx HTTP errors) in `async.SkipRetry(err)`. Use `async.RetryAfter(err, d)`
   when you need a custom backoff (e.g. per-destination fairness or per-thread FIFO lock).
5. **Respect shard isolation.** Any task touching shard Postgres or a shard bucket MUST declare
   `Scope() async.Scope { return async.ScopeShard }` and be enqueued on the owning shard's
   Redis (locally or via the internal shard management RPC).

---

## 2. Configuration & Worker Topologies

Background processing is configured under `async:` in `config.Config` (`ROUTER_ASYNC_*`):

```yaml
async:
  backend: asynq           # "asynq" (Redis, staging/prod) or "in_process" (default in local dev)
  scope: both              # "both", "shard", or "global"
  worker:
    enabled: true          # false on HTTP-only pods when all queues run on router async-worker
    queues: ["all"]        # Celery-style: ["all"], ["events", "media"], or ["events:10", "media:4"]
    concurrency: 32        # general pool concurrency
    queue_concurrency:     # dedicated pools with hard per-queue concurrency caps
      knowledge-files: 2
      image-draw: 2
      simulations: 4
    strict_priority: false
    shutdown_timeout: 30s
  scheduler:
    enabled: true          # periodic cron jobs
```

- **Local development (`ROUTER_ENV=local`):** defaults to `backend: in_process` with all
  queues running in-process inside `router serve` (zero extra processes or Redis required).
  Set `ROUTER_ASYNC_BACKEND=asynq` to test Redis-backed queues locally or run a separate
  `router async-worker` process.
- **Staging & production:** `router serve` runs low-latency event queues (`events`) in-process
  on shard Redis, while dedicated `router async-worker` pods run background/heavy queues
  (`webhooks`, `media`, `image-post`, `image-draw`, `knowledge`, `knowledge-files`,
  `simulations`, `maintenance`) with independent CPU/Memory HPA scaling.

---

## 3. Code Organization & Task-as-Struct Pattern

Every domain package that owns async work places its tasks in a dedicated `tasks/` subpackage
(one `.go` file per task plus `register.go`):

```
internal/<domain>/
  service.go              # Enqueues tasks by importing internal/<domain>/tasks
  tasks/
    do_something.go       # Task struct + Handler struct + Handle(ctx, task)
    register.go           # Register(reg *async.Registry, deps Deps)
```

Keep `<domain>/tasks` free of import cycles by defining narrow dependency interfaces in
`tasks/` and wiring them in `cmd/router` via `domaintasks.Register(reg, deps)`.

### Defining a Task (`internal/session/tasks/review_call.go`)

```go
package tasks

import (
    "context"
    "time"

    "github.com/GetStream/Vision-Agents/acceleration/internal/async"
)

type ReviewCall struct {
    CustomerID string `json:"customer_id"`
    CallID     string `json:"call_id"`
    Target     string `json:"target"`
}

func (ReviewCall) TaskType() string   { return "session:review_call" }
func (ReviewCall) Queue() async.Queue { return async.QueueMedia }
func (ReviewCall) Scope() async.Scope { return async.ScopeShard }
func (ReviewCall) TaskOptions() []async.Option {
    return []async.Option{
        async.WithTimeout(90 * time.Second),
        async.WithMaxRetry(3),
    }
}

type ReviewCallHandler struct {
    Reviewer CallReviewer
}

func (h *ReviewCallHandler) Handle(ctx context.Context, task ReviewCall) error {
    score, err := h.Reviewer.Score(ctx, task)
    if err != nil {
        return err
    }
    // Optional: expose structured result for asynctest assertions (no-op in prod).
    async.RecordResult(ctx, score)
    return nil
}
```

### Registering & Enqueueing

```go
// In internal/session/tasks/register.go:
func Register(reg *async.Registry, deps Deps) {
    async.Register(reg, &ReviewCallHandler{Reviewer: deps.Reviewer})
}

// In caller F():
_, err := async.Enqueue(ctx, s.dispatcher, sessiontasks.ReviewCall{
    CustomerID: customerID,
    CallID:     callID,
    Target:     target,
}, async.WithTaskID("review:"+callID))
```

---

## 4. Testing Tasks and Callers (`internal/async/asynctest`)

Use `internal/async/asynctest` instead of spawning Redis servers, polling, or `time.Sleep`.

### Testing Caller `F()` (Awaited by Default)

`asynctest.New(t)` runs in **Eager mode** by default: when `F()` enqueues task `T()`, the
runner JSON-round-trips `T()`, runs its registered handler inline, awaits completion before
`Enqueue` returns, and records the typed arguments, options, error, and optional result.

```go
func TestEndCallEnqueuesAndRunsReview(t *testing.T) {
    tr := asynctest.New(t)
    sessiontasks.Register(tr.Registry(), sessiontasks.Deps{Reviewer: fakeReviewer})

    // F() internally calls async.Enqueue(..., ReviewCall{...}).
    // ReviewCall has already completed when EndCall returns.
    require.NoError(t, svc.EndCall(ctx, "call-1"))

    // Inspect captured args, queue, options, error, and result:
    recs := asynctest.Recorded[sessiontasks.ReviewCall](tr)
    require.Len(t, recs, 1)
    assert.Equal(t, "call-1", recs[0].Args.CallID)
    assert.Equal(t, async.QueueMedia, recs[0].Queue)
    assert.True(t, recs[0].Ran)
    assert.NoError(t, recs[0].Err)
    assert.Equal(t, expectedScore, recs[0].Result)
}
```

### Testing Delays, Retries, and Failure Ordering (`Manual` Mode)

```go
func TestReviewRetriesOnTransientError(t *testing.T) {
    tr := asynctest.New(t, asynctest.Manual())
    sessiontasks.Register(tr.Registry(), sessiontasks.Deps{Reviewer: flakyReviewer})

    require.NoError(t, svc.EndCall(ctx, "call-1"))
    assert.Equal(t, 1, tr.PendingCount())

    rec, ran := tr.RunNext(ctx)
    require.True(t, ran)
    require.Error(t, rec.Err) // re-queued with retry delay

    tr.Advance(30 * time.Second)
    require.NoError(t, tr.Drain(ctx))

    recs := asynctest.Recorded[sessiontasks.ReviewCall](tr)
    require.Len(t, recs, 2)
    assert.NoError(t, recs[1].Err)
}
```

### Isolating Caller Unit Tests (`RecordOnly`)

When a unit test only wants to assert that `F()` enqueued `T()` with the right arguments
without running `T()`:

```go
tr := asynctest.New(t, asynctest.RecordOnly(sessiontasks.ReviewCall{}))
```

### Redis Integration Tests (`asynctest.Redis`)

When testing real `asynq` behavior (Redis Cluster `MOVED`, `TaskID` uniqueness, shutdown drain),
use `asynctest.Redis(t, redisCfg, sharedClient, reg)`. It isolates every test with a unique
queue prefix (`test-<hex>:`) rather than colliding on Redis DB numbers and cleans up all
queues automatically on `t.Cleanup`.
