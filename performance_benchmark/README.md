# performance_benchmark

How many cascaded voice agents does one router hold at once?

Each agent in a run is a real conversation. The Go SDK opens a session, the router joins
the call and runs the whole loop, and a caller here joins the same call over WebRTC and
talks to it. What is not real is the three models: the run uses the router's own `mock/*`
speech, voice and language providers, which reach no vendor and bill nothing. So a run
measures the router — its audio path, its turn taking and its database — and nothing else.

```bash
go run -tags webrtc ./cmd/perfbench run -target benchmark -max 64 -out out/before
go run -tags webrtc ./cmd/perfbench compare out/before out/after
```

It needs `STREAM_API_KEY` and `STREAM_API_SECRET`, and a router started with
`ROUTER_MOCK_PROVIDERS=true`. Read `.claude/skills/performance_benchmark/SKILL.md` before
running one.

The ramp doubles the agents each step and stops at the first step where more than 5% of
conversations never get the agent onto the call, more than 10% of turns go unanswered, a
reply takes over three seconds at p95, or a router pod restarts. A step that saturated
this machine is reported as the caller's ceiling rather than the router's, because at that
point the measurement is the laptop's.

## Two shards, with a region between them

`compose.shards.yaml` runs shard us on :8080 and shard eu on :8081, with Toxiproxy on every
link that crosses a region in production: the EU router's global Postgres and Redis, and
each router's reads of the other shard. Put `ROUTER_MOCK_PROVIDERS=true` and
`ROUTER_AUTH_MODE=noauth` in the root `.env`, then, from the repo root:

```bash
acceleration/dev/shards.sh up            # builds the router and starts both
acceleration/dev/shards.sh latency 100   # 100 ms ±10 on each cross-region answer
(cd performance_benchmark && STREAM_ACCELERATION_CUSTOMER_ID=perfbench \
  go run -tags webrtc ./cmd/perfbench run -target http://localhost:8081 -namespace "" -out out/eu-100ms)
acceleration/dev/shards.sh clear         # back to 0 ms
```

The toxics live in Toxiproxy (API on :8474), so they survive a router restart and are gone
when Toxiproxy restarts.
