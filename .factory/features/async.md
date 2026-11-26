# Event dispatching and async processing

Asked for after sprint 17. Not started.

## Asked for

Establish one pattern for all background and event-driven work in `acceleration/`, built on
`asynq`, and replace the SQL-polled `message.new` queue and ad-hoc goroutines with it:

- **Event dispatching via shard RPC + `asynq`.** For inbound events such as Stream Chat's
  `message.new` (and any other flow that currently enqueues into global SQL so another shard
  can pick it up):
  1. Find the router / shard that handles the call or conversation.
  2. If it lives on another shard, send the event over an internal-only management RPC
     (similar to the staff management endpoints used for pricing, never customer-accessible).
  3. The owning shard enqueues the event into its own shard Redis via `asynq`; each shard
     runs an `asynq` runner (with event handling processed by an in-process worker).
- **Centralized async configuration.** A dedicated `async:` configuration section in
  `config.Config`, configurable like Celery: run all queues or a subset of queues, tune
  concurrency, and run all workers in-process inside `router serve` or split selected queues
  into separate `router async-worker` processes.
- **In-process worker by default in development.** Local / development mode runs background
  work in-process with zero extra processes to start.
- **Clear task-as-struct pattern & package layout.** Every async task is a typed Go struct
  living in a `tasks/` subpackage inside its domain package (`internal/<pkg>/tasks/`), registered
  on a shared `internal/async` framework.
- **First-class testing support.** Testing a function `F()` that enqueues task `T()` awaits
  and executes `T()` inline by default, while allowing tests to inspect captured task
  arguments, options, and results — or switch to manual stepping for retry/crash tests.
- **Developer guidance.** Document when to use `asynq`, when *not* to use it, and explicit
  rules against fire-and-forget goroutines and ad-hoc channels.
- **Migrate existing work.** Refactor the two existing `asynq` users (`knowledge/urls`,
  `knowledge/files`), migrate applicable fire-and-forget goroutines, and replace custom
  Postgres-backed queues where `asynq` (and shard RPC where cross-shard) is a better fit.

---

## What exists today

Background processing in `acceleration/` grew across three separate patterns:

1. **Two ad-hoc `asynq` servers** (`github.com/hibiken/asynq v0.26.0` is already in
   `acceleration/go.mod`, wired via `queueRedis()` in `cmd/router/main.go`):
   - `internal/knowledge/urls/urls.go`: private `asynq.Server` + `Client` + `Inspector` on
     **global Redis**, queue `"knowledge"`, hardcoded `concurrency = 4`, plus a `1m` ticker
     goroutine for scheduled refreshes.
   - `internal/knowledge/files/files.go`: private `asynq.Server` + `Client` on **shard Redis**,
     queue `"knowledge-files"`, hardcoded `concurrency = 2` (PDFium takes up to 512 MiB per
     file).
   - Tests (`internal/api/router_suite_test.go`, `internal/knowledge/urls/urls_test.go`) connect
     to real Redis using `DB: int(n)%15 + 1` and poll until completion.

2. **Seven custom Postgres-backed queues** built on `internal/store/queue.go` (`FOR UPDATE SKIP
   LOCKED`, `attempts`, `next_attempt_at`, manual leases) plus duplicated `kick`/`slots`/`NextDue`
   worker loops, all polling **global Postgres**:
   - `chat_inbound` (`internal/chatinbound/worker.go`, `internal/api/chatinbound.go`)
   - `channel_thread_inbound` (`internal/channelbridge/inbound.go`)
   - `connector_event_deliveries` (`internal/eventforward/forwarder.go`)
   - `image_jobs` (`internal/imagejobs/imagejobs.go`)
   - `connection_event_subscriptions` (`internal/mcpevents/mcpevents.go`)
   - `shard_jobs` / `shard_job_runs` (`internal/store/shard_job.go`)
   - `phone_number_orders` (`internal/store/phone_number_order.go`, `cmd/router/phonesweep.go`)
   - plus `notices` (`internal/limitnotice/sender.go`) and `episodes` summary leases
     (`internal/omnichannel/close.go`).

3. **Fire-and-forget goroutines & periodic tickers** spawned directly from HTTP/webhook
   handlers, session callbacks, and `cmd/router/main.go`:
   - `channelbridge.Bridge.Reply` (in-memory retry loop on `go func()`, lost on crash)
   - `mcpevents.Service.Deliver` (`go s.run(...)` after `202`, lost on crash)
   - `session.reviewer.Review` (post-call LLM scoring, abandoned on shutdown)
   - `session.Session.name` / `titler` (conversation auto-titling every 3rd answer)
   - `omnichannel.Closer.EndCall` (post-call episode summarization)
   - `api.Server.logCallSites` (2-minute sleeping goroutine polling Stream call stats)
   - `api.chatEpisode` & `session.callCards` (episode card Stream Chat writes)
   - `simulation.Runner.Start` (in-process eval runs with `ErrBusy` cap of 4)
   - `hookOrigin.answer` / `handOn` / `channels.Service.Receive` (legacy webhook goroutines)
   - Periodic sweepers (`pruneDataChanges`, `pruneScheduledTasks`, `redactToolOutput`,
     `startPhoneSweeper`, `RunLegSweep`, `RunOpenLegsHeal`, `dlc.Service.Run`,
     `daytona.Sweeper`, `limitnotice.Anomalies`).

---

## 1. Centralized async configuration (`config.Async`)

Add a top-level `async:` block to `config.Config` (`acceleration/internal/config/config.go`),
overridable by `ROUTER_ASYNC_*` environment variables, following the same koanf pattern as
every other section.

### Configuration shape

```yaml
async:
  # Backend for task queueing and execution:
  #   "asynq"      Redis-backed asynq queues (production / staging default)
  #   "in_process" In-memory worker pool, no Redis required (local dev default)
  backend: asynq                  # ROUTER_ASYNC_BACKEND

  # Whether this process runs the embedded worker (in-process alongside HTTP).
  # Set worker.enabled: false on HTTP-only pods when offloading all queues to
  # standalone `router async-worker` pods.
  worker:
    enabled: true                 # ROUTER_ASYNC_WORKER_ENABLED
    # Celery-style queue selection:
    #   "all"                     Process every registered queue for this scope
    #   ["events", "webhooks"]    Process only the named queues
    #   {"events": 6, "webhooks": 4}  Named queues with explicit priority weights
    queues: ["all"]               # ROUTER_ASYNC_WORKER_QUEUES (comma-separated, e.g. "events,webhooks" or "events:6,webhooks:4")
    # Global max concurrent tasks for this runner (asynq.Config.Concurrency).
    concurrency: 32               # ROUTER_ASYNC_WORKER_CONCURRENCY
    # Optional per-queue concurrency caps (e.g. keep PDFium at 2 and FAL image draw at 2
    # even when the process concurrency is 32). Enforced via per-queue semaphore / separate
    # asynq pool so heavy queues never starve low-latency queues.
    queue_concurrency:
      knowledge-files: 2          # ROUTER_ASYNC_QUEUE_CONCURRENCY_KNOWLEDGE_FILES
      image-draw: 2               # ROUTER_ASYNC_QUEUE_CONCURRENCY_IMAGE_DRAW
      simulations: 4              # ROUTER_ASYNC_QUEUE_CONCURRENCY_SIMULATIONS
    # Strict priority vs weighted fair scheduling (asynq.Config.StrictPriority).
    strict_priority: false        # ROUTER_ASYNC_WORKER_STRICT_PRIORITY
    # Graceful drain timeout for in-flight tasks on SIGTERM.
    shutdown_timeout: 30s         # ROUTER_ASYNC_WORKER_SHUTDOWN_TIMEOUT

  # Periodic cron scheduler (asynq.Scheduler) for maintenance jobs.
  # Safe to enable on every pod (or only dedicated worker pods); asynq.Scheduler +
  # Unique/TaskID deduplicates periodic enqueues across replicas.
  scheduler:
    enabled: true                 # ROUTER_ASYNC_SCHEDULER_ENABLED

  # Scope of Redis this runner connects to:
  #   "both"   Default in single-process / router serve: runs shard-scoped queues on shard
  #            Redis and global-scoped queues on global Redis.
  #   "shard"  Shard-local queues only (for per-region `router async-worker` deployments).
  #   "global" Global queues only (for global `router async-worker` deployments).
  scope: both                     # ROUTER_ASYNC_SCOPE
```

### Standard queue catalogue

Queues are defined as typed constants in `internal/async/queues.go` so config validation
rejects typos at startup:

| Queue | Scope | Default weight | Intended workload |
| --- | --- | --- | --- |
| `events` | **Shard** | 10 | Inbound `message.new`, channel bridge replies, MCP inbound events, episode card writes. Always kept in-process on router pods for low latency. |
| `webhooks` | **Global** (or Shard) | 6 | Outbound customer webhook deliveries (`eventforward`), Slack limit/anomaly notices. |
| `media` | **Shard** | 4 | Post-call QA reviews, conversation titling, episode summaries, call-site stats fetch, campaign transcript classification. |
| `image-post` | **Shard** | 4 | Upload & post already-drawn images to Stream Chat. |
| `image-draw` | **Shard** | 2 | Async image generation (`imagerouter`, low per-process concurrency = 2). |
| `knowledge` | **Global** | 2 | Knowledge URL crawling via Exa. |
| `knowledge-files` | **Shard** | 1 | 50 MB PDF/DOCX extraction + OCR (memory-heavy, concurrency = 2, prime candidate for dedicated worker pods). |
| `simulations` | **Shard** | 1 | Multi-turn LLM/audio evaluation runs (long-running, up to 30m). |
| `maintenance` | **Shard** / **Global** | 1 | Periodic pruning, tool redaction, 10DLC polling, Daytona secret sweep, phone sweeps, cost anomaly checks, MCP subscription refresh. |

### Environment defaults

- **`local.yaml` (development):** `backend: in_process`, `worker.enabled: true`,
  `worker.queues: ["all"]`, `worker.concurrency: 8`. If a developer explicitly sets
  `ROUTER_ASYNC_BACKEND=asynq` (e.g. to test Redis queueing locally), it connects to
  `redis.addr` / `redis.shard.addr`.
- **`testing.yaml`:** `backend: in_process` (and unit/API suites construct `asynctest.New(t)`
  directly).
- **`staging.yaml` / production:** `backend: asynq`, `worker.enabled: true`,
  `worker.queues: ["events", "media", "image-post", "webhooks"]` (or `["all"]` until
  dedicated worker deployments are rolled out for `knowledge-files`, `image-draw`,
  `simulations`).

### CLI support for standalone workers

Add an `async-worker` subcommand to `cmd/router`:

```
router serve         [--config path]   # Serves HTTP/gRPC API + configured embedded queues (default: all, or events-only)
router async-worker  [--config path]   # Runs ONLY the async worker/scheduler (plus /metrics and /healthz), no public API
```

With this, moving a queue out of the API pods into its own deployment is purely a Helm/config
change — zero Go edits:
- Router pods: `ROUTER_ASYNC_WORKER_QUEUES=events`
- Heavy worker pods: `router async-worker` with `ROUTER_ASYNC_WORKER_QUEUES=knowledge-files,image-draw,simulations` and `ROUTER_ASYNC_WORKER_CONCURRENCY=4`
- Webhook worker pods: `router async-worker` with `ROUTER_ASYNC_WORKER_QUEUES=webhooks` and `ROUTER_ASYNC_WORKER_CONCURRENCY=32`

---

## 2. Developer guide (`acceleration/docs/async.md`)

Write a dedicated guide at `acceleration/docs/async.md` (referenced from `acceleration/README.md`
and `AGENTS.md`) covering four topics:

### A. Decision matrix: when to use `asynq` vs. when NOT to

| Pattern | Use it when… | Never use it when… | Examples in `acceleration/` |
| --- | --- | --- | --- |
| **`asynq` task** (`internal/async`) | Work should run off the request/session path, calls an external API/LLM/OCR/Stream Chat, needs retries/backoff, must survive a pod restart, or may be offloaded to another worker process. | Sub-second streaming audio/video, tightly coupled to a live WebRTC/WebSocket connection, or >1,000 tiny telemetry writes/sec that need in-memory batching. | `message.new` dispatch, webhook forwarding, image draw/post, knowledge crawl/OCR, call review, conversation titling, episode summaries, call-site stats, simulations. |
| **`asynq.Scheduler` (periodic task)** | Periodic maintenance, sweeps, pruning, polling external APIs every N minutes/hours. | Sub-minute live health checks or process-local heartbeats/lease renewals. | DB pruning, tool output redaction, 10DLC poll, Daytona secret sweep, phone sweeps, cost anomaly check. |
| **Bounded in-memory channel + single writer goroutine** | High-throughput, ordered, or batched telemetry/persistence where the process intentionally aggregates in memory before flushing (with explicit queue capacity, drop counter, and `Close()` drain). | Durable business work or user-visible replies that cannot be lost on crash, or arbitrary fan-out jobs. | `routing.Writer`, `session.sessionRecorder`, `session.logRecorder`, `activity.Recorder`, `chatlog` (ephemeral streaming updates), `usage/ledger/mirror`. |
| **Live-connection goroutines** (`goroutine.Go`) | Driving an active real-time stream (WebRTC, STT/TTS/STS, `audiofloor`, `harness`, WebSocket read/write pumps) or process-local cache/directory heartbeat (`node.Directory`, `pricing.Active`, `policy.Enforcer`, `relay.Bus`). | Anything triggered by an HTTP request or webhook that outlives the request/connection. | `internal/agent`, `internal/stt`, `internal/tts`, `internal/sts`, `internal/api/*ws.go`. |
| **Fire-and-forget `go func()`** | **Forbidden** for any work with side effects, DB writes, or network I/O triggered by a request, webhook, or session event. | Always — it drops work on crash, bypasses concurrency limits and drain, leaks across tests, and hides failures. | Everything in Section 6 below is being removed from this pattern. |

### B. Rules for direct Go channels

1. Channels are for **intra-process streaming and bounded batching**, never a substitute for a
   durable job queue.
2. Every channel-backed worker MUST declare:
   - a fixed buffer capacity constant,
   - explicit backpressure behavior (drop + Prometheus counter, or block with context timeout),
   - deterministic `Close(ctx)` draining joined into shutdown order in `cmd/router/main.go`.
3. Never spawn an unbounded `go func()` per item read from a channel or per HTTP request.

### C. Rules for `asynq` payloads & handlers

- **Never put secrets, tokens, or large binary blobs in task payloads.** Payloads live in Redis
  and appear in dead-letter queues and inspector UIs. Pass IDs (row ID, bucket object key,
  sealed credential reference) and open secrets inside the handler.
- **Payloads must be forward/backward compatible.** JSON-encoded structs with explicit `json`
  tags; adding fields must default safely during rolling deploys.
- **Handlers must be idempotent.** `asynq` provides at-least-once delivery. Deduplicate side
  effects with `async.WithTaskID(...)` / `async.WithUnique(...)` plus durable idempotency keys
  (e.g. deterministic Stream Chat message IDs, `deliveries_seen`, `ClaimTurn`).
- **Distinguish permanent vs. retryable errors.** Return `async.SkipRetry(err)` (wrapping
  `asynq.SkipRetry`) for 4xx / deleted rows / permanently invalid state so dead tasks do not
  burn retries.
- **Respect shard isolation.** A task that reads or writes shard Postgres or a shard bucket
  MUST be declared with `Scope: async.ScopeShard` and enqueued on that shard's Redis (via
  local enqueue or the shard management RPC).

### D. Cookbook: adding a task and testing code that uses it

Walk through a complete end-to-end example (struct definition in `internal/<pkg>/tasks/`,
handler + `Register`, enqueueing from `F()`, unit test verifying `F()` awaits `T()` and
inspects args/results, plus a manual-mode test for retry behavior).

---

## 3. Task struct pattern & code organization (`tasks/` subpackages)

### Shared framework: `acceleration/internal/async/`

```
acceleration/internal/async/
  async.go          Task interface, Scope (Shard | Global), Queue constants, Enqueue[T], Option
  registry.go       Registry, typed Handler[T], Register[T], middleware chain
  middleware.go     Logging (slog + task_type/queue/task_id/attempt), OTel spans, Sentry, Prometheus metrics, attribution propagation
  asynq.go          AsynqDispatcher + AsynqRunner (wraps asynq.Client, asynq.Server, asynq.Inspector, asynq.Scheduler, cluster detection)
  inproc.go         InProcRunner (in-memory Dispatcher + worker pool with concurrency, delays, retries, graceful drain)
  queues.go         Canonical queue names, default weights, config parser/validator ("all" vs list vs weighted map)
  asynctest/
    runner.go       TestRunner: Eager (default) & Manual modes, JSON round-trip, Recorded[T], Wait/Drain, fake clock
    redis.go        Redis integration helper with per-test queue prefix & WaitIdle
```

### Code organization convention

Every domain package that owns background tasks keeps them in a dedicated `tasks/` subpackage
(one file per task, plus `register.go` wiring the package's tasks):

```
acceleration/internal/
  chatinbound/tasks/
    dispatch_message.go      # DispatchMessage task struct + handler
    register.go              # Register(reg, deps)
  channelbridge/tasks/
    ingest_inbound.go        # IngestInbound task
    send_reply.go            # SendReply task
    register.go
  eventforward/tasks/
    deliver_webhook.go       # DeliverWebhook task
    register.go
  imagejobs/tasks/
    draw_image.go            # DrawImage task (queue: image-draw)
    post_image.go            # PostImage task (queue: image-post)
    register.go
  knowledge/urls/tasks/
    index_url.go             # IndexURL task + periodic RefreshDueURLs
    register.go
  knowledge/files/tasks/
    read_file.go             # ReadFile task (queue: knowledge-files)
    register.go
  session/tasks/
    review_call.go           # ReviewCall task
    title_conversation.go    # TitleConversation task
    register.go
  omnichannel/tasks/
    write_episode_card.go    # WriteEpisodeCard task
    summarize_episode.go     # SummarizeEpisode task
    close_idle_episodes.go   # Periodic sweep task
    register.go
  mcpevents/tasks/
    reconcile_subscription.go
    handle_event.go
    register.go
  limitnotice/tasks/
    post_notice.go
    check_anomalies.go
    register.go
  simulation/tasks/
    run_simulation.go
    register.go
  phone/tasks/
    fetch_call_sites.go
    settle_order.go
    sweep.go
    register.go
  campaign/tasks/
    classify_call.go
    register.go
```

*(Note on avoiding import cycles: `<pkg>/tasks` defines narrow dependency interfaces or takes
the parent service's concrete types via `Deps`, and `cmd/router` calls `<pkg>tasks.Register(...)`
at startup. Parent packages that need to enqueue only import `<pkg>/tasks` for the payload
struct — never the reverse — so `<pkg>/tasks` has zero circular imports with `<pkg>`.)*

### Task-as-struct contract

```go
package async

// Task is implemented by every typed task payload struct.
type Task interface {
    // TaskType is the stable wire name, e.g. "session:review_call".
    TaskType() string
    // Queue is the default queue, e.g. async.QueueMedia.
    Queue() Queue
    // Scope says whether this task lives on shard Redis or global Redis.
    Scope() Scope
}

// Configurable is optionally implemented by tasks that declare default options
// (timeout, max retries, retention, uniqueness).
type Configurable interface {
    TaskOptions() []Option
}

// Handler processes one typed task.
type Handler[T Task] interface {
    Handle(ctx context.Context, task T) error
}
```

Example task in `internal/session/tasks/review_call.go`:

```go
package tasks

import (
    "context"
    "time"

    "github.com/GetStream/Vision-Agents/acceleration/internal/async"
    "github.com/GetStream/Vision-Agents/acceleration/internal/routing"
)

type ReviewCall struct {
    CustomerID  string              `json:"customer_id"`
    CallID      string              `json:"call_id"`
    Region      string              `json:"region"`
    Target      string              `json:"target"`
    Guide       string              `json:"guide,omitempty"`
    Attribution routing.Attribution `json:"attribution"`
    Lines       []SpokenLine        `json:"lines"`
}

func (ReviewCall) TaskType() string  { return "session:review_call" }
func (ReviewCall) Queue() async.Queue { return async.QueueMedia }
func (ReviewCall) Scope() async.Scope { return async.ScopeShard }
func (ReviewCall) TaskOptions() []async.Option {
    return []async.Option{
        async.WithTimeout(90 * time.Second),
        async.WithMaxRetry(3),
    }
}

type ReviewCallHandler struct {
    LLM   LLMCaller
    Store CallReviewStore
}

func (h *ReviewCallHandler) Handle(ctx context.Context, task ReviewCall) error {
    // Attribution, tracing span, structured logger are already on ctx via middleware.
    // Return async.SkipRetry(err) for non-retryable errors.
    ...
}
```

Enqueueing from caller code `F()`:

```go
if _, err := async.Enqueue(ctx, s.tasks, sessiontasks.ReviewCall{
    CustomerID:  call.CustomerID,
    CallID:      call.ID,
    Region:      region,
    Target:      target,
    Guide:       guide,
    Attribution: attribution,
    Lines:       lines,
}, async.WithTaskID("review:"+call.ID)); err != nil {
    return err
}
```

---

## 4. Testing setup (`internal/async/asynctest`) & test migration

### Goals

Today, testing code that spawns background work either sleeps/polls on real Redis (`DB: int(n)%15 + 1`
in `router_suite_test.go`), leaks goroutines across tests, or requires bespoke `Replying()` /
`ChatCardsRunning()` / `turnsEnded()` hooks.

`internal/async/asynctest` makes async work deterministic and inspectable by default:

### A. Eager mode (default for unit & API suite tests)

```go
func TestEndCallReviewsAndLogsSites(t *testing.T) {
    tasks := asynctest.New(t) // Eager mode by default; no Redis required
    sessiontasks.Register(tasks.Registry(), sessiontasks.Deps{LLM: fakeLLM, Store: st})
    phonetasks.Register(tasks.Registry(), phonetasks.Deps{Stream: fakeStream, Store: st})

    // F() internally enqueues ReviewCall and FetchCallSites.
    // In Eager mode, Enqueue JSON-round-trips the payload, runs the registered handler
    // immediately, awaits completion, and records args, options, and error.
    err := svc.EndCall(ctx, callID)
    require.NoError(t, err)

    // Inspect typed task args and execution results:
    reviews := asynctest.Recorded[sessiontasks.ReviewCall](tasks)
    require.Len(t, reviews, 1)
    assert.Equal(t, callID, reviews[0].Args.CallID)
    assert.Equal(t, async.QueueMedia, reviews[0].Queue)
    assert.NoError(t, reviews[0].Err)
    assert.True(t, reviews[0].Ran)
}
```

Key properties of `asynctest.New(t)`:
- **Awaited by default:** when `F()` calls `async.Enqueue(ctx, dispatcher, task)`, `T()` runs
  and completes before `Enqueue` returns (including nested tasks enqueued by `T()` itself, with
  loop/depth guard), so assertions immediately after `F()` see the side effects without polling
  or `time.Sleep`.
- **Full JSON round-trip:** payloads are always marshaled and unmarshaled even in-memory, so
  missing `json` tags, unexported fields, or broken custom marshalers fail immediately in unit
  tests.
- **Unregistered handler safety:** if a test only wants to assert that `F()` enqueued `T()`
  without running `T()`, `asynctest.WithRecordOnly(sessiontasks.ReviewCall{})` (or leaving a
  handler unregistered with `asynctest.AllowUnregistered()`) records the call and returns `nil`.
  By default, enqueueing an unregistered task fails the test loudly so typos are caught.
- **Result & error inspection:** every `Record[T]` exposes:
  - `Args T` (post-JSON-round-trip payload)
  - `Queue async.Queue`, `Scope async.Scope`, `Options async.EnqueueOptions` (`TaskID`, `ProcessIn`, `ProcessAt`, `MaxRetry`, `UniqueTTL`)
  - `Attempt int`, `Ran bool`, `Err error`, `SkippedRetry bool`, `Duration time.Duration`
  - Optional typed handler output capture via `async.RecordResult(ctx, value)` for tasks whose
    tests want to inspect computed results without re-querying a store.

### B. Manual / deferred mode (retries, delays, ordering, crash recovery)

```go
tasks := asynctest.New(t, asynctest.Manual())
// F() enqueues into an in-memory priority queue; nothing runs yet.
require.NoError(t, svc.EndCall(ctx, callID))
assert.Equal(t, 1, tasks.PendingCount())

// Advance fake clock for ProcessIn / ProcessAt tasks and step deterministically:
tasks.Advance(10 * time.Second)
res := tasks.RunNext(ctx)          // runs one due task; returns Record
require.Error(t, res.Err)          // re-queued with retry backoff
tasks.Advance(30 * time.Second)
require.NoError(t, tasks.Drain(ctx)) // runs all due tasks until queue is empty
```

### C. Integration mode against real Redis (`asynctest.Redis`)

For integration tests that specifically verify Redis Cluster compatibility, `asynq`
uniqueness/TaskID dedup, or real `asynq.Server` shutdown/drain:
- `asynctest.Redis(t, addr)` creates a unique per-test queue prefix (`test-<8hex>:` instead of
  the colliding `DB: int(n)%15 + 1` trick, which also breaks on Redis Cluster where only DB 0
  exists!).
- Provides `WaitIdle(ctx)` (polls `asynq.Inspector.GetQueueInfo` until `Pending == 0 && Active == 0`)
  and deletes all test queues automatically in `t.Cleanup`.

### D. Existing tests to refactor onto `asynctest`

- `internal/api/router_suite_test.go`: replace `knowledgeRedis` (`DB: int(n)%15 + 1`) and
  bespoke `turnsEnded()` / `ChatCardsRunning()` polling with `asynctest.New(t)` for fast suite
  tests and `asynctest.Redis(t, addr)` where real Redis queueing is under test.
- `internal/knowledge/urls/urls_test.go`: remove hardcoded `const redisDB = 13` and polling
  loops; run unit logic on `asynctest.New(t)` and keep a focused `asynctest.Redis` test.
- `internal/api/knowledgefiles_test.go` and `cmd/router/queueredis_integration_test.go`: adopt
  `asynctest.Redis` (and keep the Redis Cluster `MOVED` regression tests).
- Migrated package suites (`chatinbound`, `channelbridge`, `eventforward`, `imagejobs`,
  `mcpevents`, `omnichannel`, `limitnotice`, `simulation`, `session` review/title, `api`
  callsites): replace `time.Sleep` / ticker-shortening hacks with `asynctest` eager & manual
  modes.

---

## 5. In-process worker & standalone worker topology

The framework supports two orthogonal deployment knobs:

1. **Where the `asynq` worker runs (same process vs. separate process):**
   - **Embedded in `router serve` (default for `events`):** `cmd/router` starts an `async.Runner`
     inside the router process for the queues listed in `async.worker.queues`. Low-latency
     event handling (`events` queue) runs in-process on the shard's router pods, directly next
     to live sessions and dispatch workers, with zero extra network hop after shard routing.
   - **Dedicated `router async-worker` processes:** Any queue (starting with memory-heavy
     `knowledge-files`, rate-limited `image-draw`, long-running `simulations`, or noisy
     `webhooks`) can be removed from `router serve`'s `async.worker.queues` and run in a
     separate `router async-worker` deployment with its own CPU/memory limits and autoscaling.
   - **Graceful shutdown:** `cmd/router`'s `stopServing` stops claiming new `asynq` tasks on
     `SIGTERM` and counts active in-process tasks in `running(...)` so `shutdown.drain` waits
     for in-flight event handlers to finish before exiting.

2. **In-process memory backend (`backend: in_process`) for development:**
   - Default in `local.yaml` (`ROUTER_ENV=local`) and when Redis is not configured.
   - Implements the exact same `async.Dispatcher` and `async.Runner` interfaces as the `asynq`
     backend using a bounded in-memory priority queue + worker goroutine pool, honoring
     `concurrency`, per-queue concurrency caps, `ProcessIn`/`ProcessAt` timers, exponential
     retry backoff, `TaskID` deduplication, JSON round-trip, and graceful `Close(ctx)` drain.
   - Gives local developers the full async architecture with zero Redis or extra worker
     processes required, while keeping behavior identical to `asynq` in staging/production.

---

## 6. Fire-and-forget goroutines to replace with tasks

| # | Current site | What it does | New task struct & package | Queue & scope | Notes / invariants to preserve |
| --- | --- | --- | --- | --- | --- |
| 1 | `internal/channelbridge/bridge.go:515` (`Bridge.Reply`) | `go func()` with in-memory retry loop (`2s/10s/30s`) sending agent replies to Slack/WhatsApp/Telnyx/Linq. | `channelbridgetasks.SendReply` (`internal/channelbridge/tasks/send_reply.go`) | `events`, **Shard** | Fixes crash loss during retries. Keep `channel_thread_messages` claim, `Hold()` test/scheduler bypass, opt-out & `dlc.Gate` checks, and `Resolver.Invalidate` on `invalid_auth`. |
| 2 | `internal/mcpevents/mcpevents.go:676` (`Service.Deliver`) | `go s.run(...)` after `202 Accepted` to open a 15m text conversation for an MCP webhook event. | `mcpeventstasks.HandleEvent` (`internal/mcpevents/tasks/handle_event.go`) | `events`, **Shard** | Fixes crash loss after `deliveries_seen` commit. Preserve shard routing policy placement before enqueue. |
| 3 | `internal/session/review.go:132` (`reviewer.Review`) | `go func()` after call ends to run quality LLM (`90s`) and write call review; abandoned on shutdown. | `sessiontasks.ReviewCall` (`internal/session/tasks/review_call.go`) | `media`, **Shard** | Wait/retry if `sessionRecorder` hasn't flushed the `calls` row yet (`WithProcessIn(2s)` or retry on not-found). Survives shutdown. |
| 4 | `internal/session/title.go:131` (`Session.name`) | `go func()` every 3rd answer calling `llm-fast` (`30s`) to rename conversation. | `sessiontasks.TitleConversation` (`internal/session/tasks/title_conversation.go`) | `media`, **Shard** | Deduplicate in-flight titling per session via `WithUnique("title:"+sessionID, 30s)`. |
| 5 | `internal/omnichannel/close.go:148,200` (`Closer.EndCall` & `Sweep`) | `EndCall` spawns `go func()` off `call.session_ended`; `Sweep` runs 1m ticker + inline LLM summaries. | `omnichanneltasks.SummarizeEpisode` + periodic `CloseIdleEpisodes` (`internal/omnichannel/tasks/`) | `media` / `maintenance`, **Shard** | `EndCall` & `CloseIdle` close rows in Postgres and enqueue `SummarizeEpisode` (`WithTaskID("ep-sum:"+id)`), replacing summary lease polling. |
| 6 | `internal/api/callsites.go:225` (`logCallSites`) | `go func()` off `call.session_ended` sleeping/polling Stream call stats for up to 2m. | `phonetasks.FetchCallSites` (`internal/phone/tasks/fetch_call_sites.go`) | `media`, **Shard** | Enqueue with `WithProcessIn(initialWait)` and retry with backoff instead of holding a sleeping goroutine. |
| 7 | `internal/api/chat_episodes.go:87` & `internal/session/episode_card.go:75` | `go func()` writing episode card message to Stream Chat (`30s`). | `omnichanneltasks.WriteEpisodeCard` (`internal/omnichannel/tasks/write_episode_card.go`) | `events`, **Shard** | Eliminates "card lost on drain timeout is lost for good". Keep `callCards.started`'s live-session `awaitCaller` in session, then enqueue `WriteEpisodeCard` once caller is resolved. |
| 8 | `internal/simulation/simulation.go:154` (`Runner.Start`) | `go func()` per simulation (`runsAtOnce=4`, `30m`), abandons runs on crash. | `simulationtasks.RunSimulation` (`internal/simulation/tasks/run_simulation.go`) | `simulations`, **Shard** | Replaces `ErrBusy` rejection with real queueing; supports cancellation via `simulation_runs` state check / `asynq.Inspector.CancelProcessing`. |
| 9 | `internal/api/hookorigin.go:66` (`hookOrigin.answer`), `messagehooks.go:521` (`handOn`), `internal/channels/service.go:198` | Raw goroutines for webhook `message.new`, voice-note STT, and legacy WhatsApp/SMS/iMessage lines. | Superseded by `chatinboundtasks.DispatchMessage` & `channelbridge` migration (T62). | `events`, **Shard** | Removes remaining raw goroutines on message ingest paths. |

**Explicitly kept off `asynq` (fast-path / live-session signals):**
- `api.refuseArrivingCall` (`internal/api/callhooks.go:462`): caller is actively on the line
  burning vendor seconds; ending the call must happen immediately (bounded goroutine or inline
  with short timeout, not queued behind backlog).
- `api.endUnanswered` (`internal/api/phone_status.go:34`) & `api.tellMachine`
  (`internal/api/phone_machine.go:31`): latency-sensitive signals for a live ringing/connecting
  call; route via the owning-router internal RPC directly (can fall back to a short-TTL high-priority
  task only if peer is transiently unreachable).
- `api.retire` (`internal/api/chat_episodes.go:149`): process-local watcher closing a local
  in-memory `*session.Session` once its in-memory `owedReplies` drain.

---

## 7. Custom Postgres-backed queues to migrate

| Current Postgres queue | Table(s) | Migration verdict | Target `asynq` design & invariants |
| --- | --- | --- | --- |
| **`chatinbound`** (`internal/chatinbound/worker.go`) | `chat_inbound` (global) | **Replace fully** (Section 9) | Replaced by shard routing -> internal management RPC -> shard `asynq` (`events` queue, `chatinboundtasks.DispatchMessage`). Keep `deliveries_seen` (or Redis dedup) and at-most-once settlement / `ClaimTurn`. |
| **`eventforward`** (`internal/eventforward/forwarder.go`) | `connector_event_deliveries` (global) | **Migrate fully** | Enqueue `eventforwardtasks.DeliverWebhook` (`webhooks` queue) with retries `5s/5m/30m/2h` and `SkipRetry` on 3xx/4xx (except 429). Preserve `perDestination = 2` fairness using a short-TTL Redis slot (`concurrency.Slots`) that returns a short retry delay when a destination is saturated; preserve dual-secret rotation and `HeadersUntil` stripping. |
| **`imagejobs`** (`internal/imagejobs/imagejobs.go`) | `image_jobs` (global) | **Migrate queue; keep lightweight status row or move drawn bytes to blob** | Split into two tasks: `imagejobstasks.DrawImage` (`image-draw` queue, concurrency = 2, `maxDraws = 2`) and `imagejobstasks.PostImage` (`image-post` queue, concurrency = 16, 10-step backoff). Store drawn image bytes in blob storage (like `knowledge-files`) instead of 10 MiB bytea in Postgres or Redis payload. |
| **`mcpevents` subscriptions** (`internal/mcpevents/mcpevents.go`) | `connection_event_subscriptions` (global) | **Migrate worker; keep table as metadata store** | Keep `connection_event_subscriptions` for subscription records & sealed secrets. Replace polling loop with `mcpeventstasks.ReconcileSubscription` enqueued immediately on `Reconcile()` and scheduled via `WithProcessAt(refreshBefore - 10m)` on success (and exponential backoff up to 24h on refusal). |
| **`limitnotice.Sender`** (`internal/limitnotice/sender.go`) | `notices` (global) | **Migrate delivery; keep `notices` for dedup/audit** | Insert dedup row into `notices` (`ON CONFLICT DO NOTHING`), then enqueue `limitnoticetasks.PostNotice` (`webhooks` queue) instead of inline Slack HTTP + DB sweep loop. |
| **`channelbridge` inbound** (`internal/channelbridge/inbound.go`) | `channel_thread_inbound` (global) | **Migrate with per-thread ordering** | Preserve strict per-thread FIFO ordering (currently enforced by `Claim` taking only the oldest row per `channel_id`) via a per-thread Redis lock or lightweight Postgres head pointer while `asynq` drives worker execution, retries, and media upload retries. Keep `channel_thread_messages` (dedup) and `channel_thread_waiting` (Slack out-of-order replies). On exhaustion (except infinite-retry `STOP`), enqueue `DeliverWebhook` for `unhandled` destinations. |
| **`shard_jobs`** (`internal/store/shard_job.go`) | `shard_jobs`, `shard_job_runs` (global) | **Replace polling with RPC + `asynq`; keep ledger table** | Keep `shard_jobs`/`shard_job_runs` as the durable fan-out & result-aggregation record (so callers can `Wait` and down shards still recover on startup), but trigger each shard immediately via shard management RPC -> shard `asynq` task instead of 5s global Postgres polling (Section 9). |
| **`phone_number_orders`** (`internal/store/phone_number_order.go`) | `phone_number_orders` (global) | **Event-driven task + periodic fallback** | Enqueue delayed `phonetasks.SettleOrder` (`ProcessIn(30s)`) when an order is inserted; keep a low-frequency periodic sweep (`maintenance`) as crash safety net. |
| **`campaign` post-call classification** (`internal/campaign/campaign.go`) | `campaign_contacts` (global) | **Hybrid: migrate classification only** | Keep campaign dial placement, calling hours, and per-campaign concurrency caps in Postgres. Move post-call transcript polling (`10s` delay + 6×`30s` retries) and LLM outcome classification to `campaigntasks.ClassifyCall` (`media` queue). |
| **`scheduling`** (`internal/scheduling/scheduler.go`) | `scheduled_tasks`, `scheduled_task_runs` (global) | **Hybrid / optional Phase 2** | Keep RRULE, pause/edit cancellation, quiet-hours `Postponed`, and missed-run tracking in Postgres. Optionally replace the 1s poll/claim + peer-forward loop with shard RPC + `asynq` delivery once the core framework is proven (careful with mid-run `errClaimLost` lease renewals). |
| **Periodic maintenance sweepers** (`cmd/router/main.go`, `phonesweep.go`, `phone/legs.go`, `dlc/service.go`, `daytona/sweep.go`, `limitnotice/anomaly.go`) | Various | **Migrate to `asynq.Scheduler`** | Register periodic cron entries on the `maintenance` queue (`pruneDataChanges`, `pruneScheduledTasks`, `redactToolOutput`, `SweepCallResources`/`SweepBridges`/`SweepStatusTokens`, `RunLegSweep`/`RunOpenLegsHeal`, `dlc.Poll`, `daytona.Sweep`, `limitnotice.Anomalies`), deduplicated via `TaskID`/unique windows rather than every pod running its own ticker. |

---

## 8. Refactoring existing `asynq` code to the new pattern

Both existing `asynq` users are refactored to drop their private `asynq.Server` / `asynq.Client`
instances and join the unified `internal/async` framework:

1. **`internal/knowledge/urls` -> `internal/knowledge/urls/tasks/`**
   - Extract `IndexURL` struct (`TaskType() = "knowledge:url:index"`, `Queue() = async.QueueKnowledge`,
     `Scope() = async.ScopeGlobal`, timeout `2m`, max retry `3`) and `RefreshDueURLs` periodic task.
   - Replace `urls.Service`'s private `asynq.Client`, `asynq.Server`, and `asynq.Inspector` with
     `async.Dispatcher` (exposing `Cancel` / `Exists` helpers for the existing `:again` dedup
     and deletion cancellation behavior).
   - Remove the manual `go s.refresh()` `1m` ticker goroutine; schedule `RefreshDueURLs` via
     `asynq.Scheduler` on `maintenance`.
   - Keep `urls.Service` as the HTTP control plane (validate, store subscription, enqueue via
     `async.Enqueue`).

2. **`internal/knowledge/files` -> `internal/knowledge/files/tasks/`**
   - Extract `ReadFile` struct (`TaskType() = "knowledge:file:read"`,
     `Queue() = async.QueueKnowledgeFiles`, `Scope() = async.ScopeShard`, timeout `30m`, max
     retry `3`).
   - Remove `files.Service`'s private `asynq.Client` and `asynq.Server`; inject shard
     `async.Dispatcher`.
   - Concurrency (default `2`) moves from a hardcoded package constant to
     `async.worker.queue_concurrency.knowledge-files`, allowing operators to move PDF/OCR
     ingestion onto dedicated high-memory `router async-worker` pods with a config flip.

3. **`cmd/router/main.go` cleanup**
   - Delete standalone `queueRedis()` / `buildKnowledgeURLs` / `buildKnowledgeFiles` server
     lifecycle wiring; construct one shard `async.Runtime` and one global `async.Runtime` from
     `settings.Async` + `settings.Redis`, register all domain `tasks.Register(...)` handlers,
     and start/close them in one place.

---

## 9. Exploded change list: replacing SQL-enqueued flows & `message.new` with shard RPC + `asynq`

### Why this matters for multi-region

Today, flows like Stream Chat `message.new` (`chat_inbound`), external channel webhooks
(`channel_thread_inbound`), image jobs (`image_jobs`), and cross-shard jobs (`shard_jobs`)
write rows into **global Postgres**. Any router in any region then polls global Postgres,
claims rows across regions, and often has to perform a second hop over peer gRPC to reach the
shard or node that actually owns the conversation. This adds cross-region Postgres load,
latency, and failure coupling between shards.

### Target architecture for event dispatching & cross-shard enqueueing

```
Caller (Stream Chat / provider webhook / another shard's router)
  │
  ▼
Receiving Router (any shard)
  │ 1. Authenticate & validate payload (signature / app ownership / addressed / stale check)
  │ 2. Resolve owning target:
  │      a. Live session/conversation -> node.Directory (returns node + shard)
  │      b. Existing session/call     -> store.SessionHomes / store.CallHomes (returns home shard)
  │      c. New conversation / call   -> shardmap.Map.Place(policy, closestShard) (returns target shard)
  │
  ├── Target shard == this shard:
  │     Enqueue directly into THIS shard's Redis via asynq (queue: events)
  │     Return 200 OK
  │
  └── Target shard != this shard:
        Send via internal management RPC to owning router / target shard
        (POST /v1/manage/internal/events/dispatch or /v1/manage/internal/tasks/enqueue,
         authenticated with internal-only ManageKey / node credential, excluded from public
         gateway & SDKs, matching /v1/manage/price-lists isolation)
          │
          ▼
        Owning Shard Router
          Verify internal auth -> Enqueue into ITS shard Redis via asynq -> Return 200 OK
          │
          ▼
        Owning Shard In-Process asynq Worker (queue: events)
          Processes event on the owning shard:
            - Local live session: answer directly in-process
            - Live session on sibling pod in SAME shard: single intra-shard peer RPC
            - No live session: open thread/chat session or assign dispatch worker in-shard
```

### Exploded list of changes (for review)

#### A. Internal management RPC & shard dispatch layer
1. **`acceleration/internal/api/manage.go` & `manage_dispatch.go` (new):**
   - Add internal-only management endpoints excluded from customer-facing SDKs (`staffTag = "Staff"`,
     never proxied by `stream-accelerate` gateway, protected by NetworkPolicy + internal auth):
     - `POST /v1/manage/internal/events/dispatch` — accepts a verified, pre-classified event
       envelope (`EventType`, `Origin`, `TargetNode`, `TargetShard`, `DedupeKey`, `Payload`) and
       enqueues it into this shard's `asynq` Redis (`events` queue) with `TaskID = DedupeKey`.
     - `POST /v1/manage/internal/tasks/enqueue` — generic internal RPC allowing one shard (or
       internal Stream service) to enqueue a registered shard-scoped task onto a specific
       target shard's Redis (`TaskType`, `Queue`, `TaskID`, `ProcessIn`, `Payload`).
   - Authentication: validate internal management credential (dedicated `ROUTER_AUTH_INTERNAL_KEY`
     or `X-Manage-Key` / keyring-derived `node.CredentialPurpose` accepted on peer/internal
     listener; never accepted from customer auth modes `api_key`/`proxy`).
   - Guard against cross-shard ping-pong: carry a `forwarded_to_shard` marker so a receiving
     shard enqueues locally and never re-forwards to another shard.
   - Return `200 OK` only once `asynq.Client.EnqueueContext` commits to shard Redis (or detects
     `asynq.ErrTaskIDConflict` / `asynq.ErrDuplicateTask`, which is idempotently `200 OK`);
     return `503` if Redis is unavailable so the caller retries.
2. **`acceleration/internal/node/directory.go`:**
   - Ensure `Directory` lookup helpers expose `(address, shard)` together for
     `NodeByConversation`, `NodeByAgent`, and `Node`, so step 1 resolves both owning router and
     owning shard in one Redis lookup.
3. **`acceleration/internal/api/shard_dispatch.go` (new):**
   - Central `dispatchToShard(ctx, targetShard, targetNode, task)` helper:
     - If `targetShard == "" || targetShard == s.directory.Shard()`: enqueue directly into local
       shard `async.Dispatcher`.
     - Else: resolve target router (`targetNode` if live and in `targetShard`, else
       `s.directory.NodeInShard(ctx, targetShard)`), send via internal management RPC.
     - Honor residency policies (`place.strict`): if target geography has no healthy shard,
       return `503 errResidencyDown` (never enqueue locally outside residency); if non-strict
       (`closest` / `us_east`), fall back to local shard enqueue and increment `shardFallbacks`.

#### B. Stream Chat inbound `message.new` queue (`chat_inbound`)
4. **`acceleration/internal/chatinbound/tasks/dispatch_message.go` (new):**
   - Define `DispatchMessage` task struct (`StreamAppPK`, `MessageID`, `CustomerID`,
     `Deployment`, `APIKey`, `Body`, `TargetNode`, `HeardText`), `Queue() = async.QueueEvents`,
     `Scope() = async.ScopeShard`, timeout `5m` (matching current `chatinbound.Lease`), retries
     `2s, 10s, 30s, 2m, 10m`.
   - Move `Heard` caching across retries from `chat_inbound.heard` SQL column to a short-TTL
     shard Redis key (`chatinbound:heard:<app>:<msg_id>`, TTL `30m`) so retries never re-bill
     voice-note STT.
   - Replace `chat_inbound.StartAnswer` / `ReleaseAnswer` (at-most-once settlement for sessions
     and dispatch workers) with an atomic shard Redis fence (`SET NX` on
     `chatinbound:settled:<app>:<msg_id>`, TTL `24h`, deleted only on proven-untaken
     `unanswered` errors — never on `node.ErrNoReply`), while thread channels keep their
     existing `store.ChannelThreadMessages.Claim(ClaimTurn)`.
5. **`acceleration/internal/api/chatinbound.go`:**
   - Rewrite `receiveChatInbound` (`POST /v1/chat/internal/messages`):
     1. Keep signature verification (`chatinbound.Verify`), `chatInboundOrigin`, `addressedIn`,
        and `stale` checks.
     2. Check/record `deliveries_seen` (or shard Redis + `deliveries_seen` while legacy webhook
        coexists) so legacy webhook and internal delivery still share one dedup key.
     3. Resolve owning router/shard:
        - Linked thread channel -> thread's shard (`threadElsewhere` / `placed`)
        - Running session on channel -> `answeringNode` (gives owning node + shard)
        - No running session -> `ownerOf` + `placed(customerID, configID, configShard(...))`
     4. Call `dispatchToShard(ctx, targetShard, targetNode, chatinboundtasks.DispatchMessage{...})`
        with `async.WithTaskID(fmt.Sprintf("chatinbound:%d:%s", origin.app, delivery.Message.ID))`.
     5. Return `200 OK` on enqueue/duplicate, `503` on Redis/RPC/residency failure.
   - Update `receiveMessageEvent` (legacy `message.new` webhook) to use the **exact same**
     `dispatchToShard` + `DispatchMessage` path instead of `go answer()` goroutines, unifying
     both entry points.
   - Simplify `/internal/chat-inbound/answer` (`answerChatInboundForPeer`): no longer needs to
     re-read `chat_inbound` rows from global Postgres; intra-shard forwarding to the pod holding
     a live session passes the verified `DispatchMessage` payload directly over the authenticated
     peer path.
6. **Removals (after dual-drain rollout):**
   - Remove `internal/chatinbound/worker.go` (custom polling worker) and `Server.SetChatInboundKick`.
   - Remove `store.ChatInbounds` (`internal/store/chat_inbound.go`) and drop `chat_inbound` table
     in a follow-up expand/contract migration once all pods are upgraded and `chat_inbound` has
     zero queued rows.

#### C. External channel bridge inbound (`channel_thread_inbound`)
7. **`acceleration/internal/channelbridge/tasks/ingest_inbound.go` (new):**
   - On `POST /v1/connectors/events/{connector}/{app}` (`Bridge.Deliver`):
     - Keep `channel_thread_messages` dedup, `channel_threads.Link`, and `channel_thread_waiting`
       reply collection in Postgres.
     - Determine target shard for the thread (`placed` by agent config routing policy / residency).
     - Enqueue `channelbridgetasks.IngestInbound` onto the target shard via `dispatchToShard`
       (instead of inserting into global `channel_thread_inbound` and polling from every shard).
     - Enforce per-thread FIFO ordering using a shard-local Redis mutex/chain per `channel_id`
       (re-enqueue with short delay if an earlier sequence for the same thread is in flight).
     - On handler completion, invoke `AnswerQueued` locally on the owning shard (eliminating the
       cross-shard hop `threadElsewhere` currently performs after polling global SQL).
     - On retry exhaustion (except `STOP` opt-outs which keep retrying), enqueue
       `eventforwardtasks.DeliverWebhook` for `unhandled` destinations.
   - Retire `internal/channelbridge/inbound.go` polling loop and `channel_thread_inbound` table
     after dual-drain.

#### D. Outbound webhook forwarding (`connector_event_deliveries`)
8. **`acceleration/internal/eventforward/tasks/deliver_webhook.go` (new):**
   - `Forwarder.Forward` & `Forwarder.ForwardUnanswered` look up matching destinations and
     enqueue one `DeliverWebhook` task per destination (`webhooks` queue, `WithTaskID` keyed by
     destination + delivery ID) instead of inserting into `connector_event_deliveries`.
   - `policy` balance notices (`store.Notices.InsertQueued`) split into: insert `notices` dedup
     row -> enqueue `DeliverWebhook` for each `stream` destination + enqueue `PostNotice` for
     Slack.
   - Replace `perDestination = 2` SQL ranking with a Redis slot (`concurrency.Slots`) per
     destination ID; when full, return a short retry delay so other destinations never block.
   - Retire `internal/eventforward/forwarder.go` polling loop and `connector_event_deliveries`
     table after dual-drain.

#### E. Async image generation (`image_jobs`)
9. **`acceleration/internal/imagejobs/tasks/draw_image.go` & `post_image.go` (new):**
   - `generate_image` tool runs inside a session on its owning shard; instead of inserting into
     global `image_jobs`, it enqueues `DrawImage` directly onto the **local shard** `image-draw`
     queue (`WithTaskID("img-draw:"+requestKey)`).
   - `DrawImage` draws the image, writes the drawn bytes to a short-lived blob key (instead of
     10 MiB bytea in global Postgres), and enqueues `PostImage` on the local shard `image-post`
     queue.
   - `PostImage` uploads to Stream Chat, posts the message (deterministic message ID for
     idempotency), and deletes the temporary blob.
   - Retire `internal/imagejobs/imagejobs.go` polling loop and `image_jobs` table after
     dual-drain.

#### F. Cross-shard fan-out jobs (`shard_jobs` / `shard_job_runs`)
10. **`acceleration/internal/store/tasks/run_shard_job.go` (new):**
    - Keep `shard_jobs` and `shard_job_runs` in global Postgres as the durable fan-out record
      and counts ledger (needed by `ForgetUserOnShards`, `ClaimGuest`, `LegacyStreamPins`,
      `DeleteBranches`, `DeleteInvocations`).
    - When a job is created in global Postgres, immediately fan out to every shard via
      `dispatchToShard(ctx, shard, "", RunShardJob{JobID, RunID})` (local enqueue for this
      shard, internal management RPC for remote shards).
    - Replace the `5s` `ShardJobWorker` polling loop with a low-frequency (`5m`) `maintenance`
      reconciliation task on each shard that picks up any `shard_job_runs` missed while a shard
      was down or unreachable during fan-out.

#### G. MCP event subscriptions (`connection_event_subscriptions`)
11. **`acceleration/internal/mcpevents/tasks/reconcile_subscription.go` (new):**
    - `Reconcile` writes desired subscriptions to `connection_event_subscriptions` and enqueues
      `ReconcileSubscription` (`WithTaskID("mcp-sub:"+subID)`).
    - Handler subscribes/unsubscribes and schedules the next renewal via
      `WithProcessAt(refreshBefore - 10m)` (or backoff on failure).
    - Remove polling loop in `internal/mcpevents/mcpevents.go` and `Claim`/`NextDue` SQL
      methods.

#### H. Episode closing & summarization (`episodes`), Notices (`notices`), Phone orders (`phone_number_orders`)
12. **`omnichannel.Closer`:** `EndCall` (already running on the call's home shard via
    `handedHome`) and `CloseIdleEpisodes` (periodic `maintenance` task) enqueue
    `SummarizeEpisode` directly onto the owning shard's `media` queue (`dispatchToShard` when
    triggered globally) instead of polling `ClaimSummaries` leases in global Postgres.
13. **`limitnotice.Sender`:** `Note` inserts into `notices` for dedup and enqueues
    `PostNotice` onto `webhooks` instead of inline HTTP + DB sweep loop.
14. **`phone_number_orders`:** insert order row and enqueue delayed `SettleOrder`
    (`ProcessIn(30s)`) on `maintenance`, keeping a periodic sweep only as crash fallback.

#### I. Scheduled tasks & Campaigns (hybrid)
15. **`campaign.Runner`:** keep dialer claim loop in Postgres (campaign concurrency & calling
    hours), but once a call ends on its shard, enqueue `campaigntasks.ClassifyCall`
    (`ProcessIn(10s)`) directly onto that shard's `media` queue — removing the `classifying`
    polling branch from `campaign.Runner`.
16. **`scheduling.Scheduler` (optional follow-up):** keep RRULE/pause state machine in global
    Postgres; when an occurrence is due, dispatch via `dispatchToShard` (using `ShardClaim`
    target shard) onto the target shard's `events` queue instead of cross-shard claim polling +
    `/internal/scheduled-tasks/deliver`.

#### J. Rollout & migration safety (expand -> dual-drain -> contract)
17. **Zero-loss upgrade across rolling deploys:**
    - **Phase A (Expand):** Deploy `internal/async`, the new internal management RPC endpoints,
      and the `asynq` handlers alongside the old SQL workers (feature-flagged off for new
      enqueues, or draining existing SQL rows while new work goes to `asynq`).
    - **Phase B (Cutover + Drain):** Switch producers (`receiveChatInbound`, `Bridge.Deliver`,
      `Forwarder`, `ImageJobs.Queue`, etc.) to enqueue via shard RPC + `asynq`. Keep old SQL
      workers running for one release so any rows queued by older pods before cutover finish
      cleanly.
    - **Phase C (Contract):** Once SQL queue tables are confirmed empty in production, delete
      old polling workers, `store.queue` methods, and drop retired tables in a separate
      migration.

---

## Order of work

Each step ships as an independent, reviewable PR:

1. **Framework, config & testing (`internal/async`, `config.Async`, `internal/async/asynctest`, `router async-worker`).**
   No behavior changes to existing features yet; unit + integration tests prove both `in_process`
   and `asynq` (standalone + Redis Cluster) backends and Celery-style queue selection.
2. **Developer documentation (`acceleration/docs/async.md`).**
   Publish the decision guide, task-as-struct pattern, anti-patterns, and testing cookbook
   before migrating features so all contributors follow one standard.
3. **Refactor existing `asynq` users (`knowledge/urls`, `knowledge/files`) + test migration.**
   Validates both global-scoped and shard-scoped `asynq` queues, `asynq.Scheduler`, and
   `asynctest` on real production workloads; retires `cmd/router/main.go:queueRedis`.
4. **Internal management dispatch RPC + Stream Chat `message.new` (`chatinbound`).**
   Introduces `/v1/manage/internal/events/dispatch` and `/v1/manage/internal/tasks/enqueue`,
   moves `receiveChatInbound` and `receiveMessageEvent` onto shard RPC + shard `asynq`
   (`events` queue), and adds dual-drain for `chat_inbound`.
5. **Migrate fire-and-forget goroutines.**
   `channelbridge.Reply`, `mcpevents.Deliver`, `session.reviewer`, `session.titler`,
   `omnichannel` episode cards & summaries, `api.logCallSites`, `simulation.Runner`.
6. **Migrate remaining Postgres queues & hybrid workers.**
   `eventforward`, `imagejobs`, `channelbridge` inbound, `mcpevents` reconciliation,
   `limitnotice`, `shard_jobs` fan-out, `phone_number_orders`, `campaign` classification, and
   periodic maintenance sweepers on `asynq.Scheduler`.
7. **Contract cleanup.**
   Remove retired SQL workers, `internal/store/queue.go` (if no callers remain), and drop
   empty queue tables after one full release cycle.

---

## Decided & open questions

### Decided
- **`asynq` is the standard async queue** for `acceleration/`; raw fire-and-forget `go func()`
  from handlers/webhooks/callbacks is retired.
- **Tasks are typed structs in `<pkg>/tasks/`** with JSON payloads and zero secrets.
- **Tests await tasks inline by default** (`asynctest.New(t)` eager mode) with full args and
  result inspection, plus an explicit manual mode for retry/delay tests.
- **Development mode defaults to the in-process worker** (`backend: in_process`, all queues)
  with no extra process required.
- **Event handling runs in-process on router pods** via shard Redis (`events` queue); any queue
  can be split out to `router async-worker` pods via Celery-style `queues` and `concurrency` config.
- **Cross-shard enqueueing goes through an internal-only management RPC**, never by writing
  into another shard's Redis or polling global Postgres from every shard.

### Open questions to confirm before implementation
1. **Auth scheme for the internal dispatch RPC:** reuse `X-Manage-Key` (`manageSecurity`, same
   as `/v1/manage/price-lists`), use a dedicated `ROUTER_AUTH_INTERNAL_KEY`, or reuse the
   keyring-derived peer credential (`internal/node/forward.go`) over the peer listener?
   *(Recommendation: accept a dedicated internal/manage key on `/v1/manage/internal/*` for
   external Stream services such as Stream Chat/gateway, and allow keyring-derived peer
   credentials for router-to-router shard hops so routers need no extra secret.)*
2. **Per-queue concurrency implementation:** `asynq.Server` has a single process-wide
   `Concurrency` setting with weighted queues. For hard caps like `knowledge-files: 2` and
   `image-draw: 2` when running all queues in one process, should `internal/async` run a
   dedicated `asynq.Server` per capped queue group, or enforce per-queue concurrency in
   middleware? *(Recommendation: separate `asynq.Server` per capped group — avoids holding
   general worker slots while a capped queue waits, and matches how `router async-worker` splits
   queues.)*
3. **Should `scheduling` (reminders) stay on Postgres claim-polling in Phase 1** or also move
   its delivery dispatch to shard RPC + `asynq`? *(Recommendation: keep RRULE/pause/quiet-hours
   claim in Postgres initially and only dispatch due deliveries via `asynq` in a later PR once
   `events` and `channelbridge` are live.)*
4. **Drawn image storage for `imagejobs`:** confirm reusing `knowledge_bucket_url` (or adding a
   general `media_bucket_url` / ephemeral bucket prefix) so drawn PNG/JPEG bytes move out of
   Postgres and never enter Redis.
