# Multi-region

## Asked for

Run the router in several regions at once (us-east4, europe-west4, more later), each region
serving the people near it, without one region's outage or distance slowing the others.
Keep a session, a call and its rows in the region where it started. Make the design hold
for many regions, not two.

## The rule: a shard never reads another shard

A region is a **shard**: its routers, its own Postgres database and its own Redis. Every
shard also reaches the **global** Postgres and Redis, which live in one region.

A router talks to four stores and no others:

| Store            | Holds                                                                 |
| ---------------- | --------------------------------------------------------------------- |
| its shard's Postgres | sessions, responses, calls and legs, requests, agent logs: what the region runs |
| its shard's Redis    | the session event relay, the store's own keys, knowledge uploads in flight |
| global Postgres      | customers, configs, numbers, users, and the copies shards ship (below) |
| global Redis         | the node directory (which router runs what, and each shard's routers), spend and rate limits, open-leg counts |

**Shard A never opens a connection to shard B's database.** Reading another shard directly
does not scale:

- Every router holds a pool to every shard, so N regions means N² cross-region pools.
- One request pays a cross-region round trip for each shard it reads.
- One shard down fails or empties the pages of every customer in every region.

When code needs something that lives in another shard, it does one of three things instead:

1. **Forward the request** to a router of the shard that holds it (`directory.NodeInShard`).
   This is how a single session or call is read, answered or stopped.
2. **Read a copy shipped to global.** Each shard ships what cross-region pages need to
   global tables a few seconds behind. The session and call lists read `session_index` and
   `call_index`. Stats and spend read the hourly `shard_*_hourly` rollups.
3. **Queue a job** for that shard to run on its own rows, for writes that touch every
   shard, such as forgetting a user.

The usage service gets every shard's requests, so spend limits never read a shard.

When you add code, check each query: if it reads rows a session or call made, it must run
on `store.shard` and be about something this shard holds. If the answer must cover every
region, read a global copy or forward the request; do not loop over shards.

## How work finds its shard

| What arrives                          | Which shard does it                                                  |
| ------------------------------------- | -------------------------------------------------------------------- |
| A client or SDK request               | The nearest healthy region: the SDKs default to `https://accelerate-auto.gcp.stream-io-api.com`, a load balancer that picks it. `accelerate-<region>` pins a region. |
| A new conversation from a chat message | The agent's region when it is kept in one (residency), else the shard that last served the user's own client (`users.shard`) |
| An inbound phone call                 | The shard of the called number's country (`shard_routing.countries`, else `shard_routing.default`) |
| Anything about an existing session or call | The shard it lives in, always (below)                           |

`shardmap.Map` holds that routing, and every router has the same copy, so whichever router a
hook lands on picks the same shard.

## Edge cases

### The dashboard opens a session or a call

The dashboard talks to one URL, which may be in any region.

- **Lists** (`GET /v1/agents/sessions`, `GET /v1/agents/calls`) read `session_index` and
  `call_index` in global, so every region's rows are listed from any region, and a region
  that is down still has its rows listed. Rows another shard wrote appear about 5 seconds
  late (`IndexShipper`). Rows this shard wrote in the last minute are merged in from the
  shard itself, so the page you just created is on the list.
- **Every row carries `region`**, the shard it lives in. The dashboard can send the next
  request for that session to `accelerate-<region>` directly and skip a hop.
- **Opening one** (`GET /v1/agents/sessions/{id}`, its responses, its logs, `GET
  /v1/agents/calls/{id}`) works from any region. The router looks up where it lives
  (`session_homes`, `call_index`) and forwards the request to a router of that shard, which
  reads its own database. The answer is the same as asking that region.
- **The home region is down:** the request is answered 503 `shard_unavailable`, "the region
  this session lives in could not be reached". No other shard has its rows, so none
  pretends to.
- A running session is found through the node directory first, so a request reaches the
  exact router running it.
- **Agent logs and a connection's tool calls** are not shipped. Logs filtered to a `session_id`
  are read in the session's shard. Any other read is of this shard, or of the shard `region`
  names, and `X-Shard` says which answered.

### An incoming phone call

1. Stream sends `call.session_started` to whichever router the hook lands on.
2. That router finds the number in global Postgres. The number's country picks the shard
   (`ForCountry`).
3. If it is another shard, the router records the hook as delivered and hands it to a router
   of that shard (`handedToShard`). The leg is opened there, and `call_homes` records which
   shard holds the call.
4. Every later event for the call (`handedHome`) goes to that shard, wherever Stream sends it.
5. If no router of the chosen shard answers, the call is taken where it landed, not dropped,
   and counted in `shardFallbacks` for an alert. A caller on the line matters more than
   residency.

### Vendor callbacks for a call this shard placed

A call's bearer token starts with the shard that minted it (`us-east4.<uuid>`,
`Store.MintToken`). A vendor callback that lands in another region is forwarded to that
shard (`SplitToken`). That shard holds the call's legs and bridge.

### Carrying a session on

A resumed or carried-on session runs in its home shard, wherever the request arrived. Its
history stays in one database, so a session never splits across shards.

### Stats, spend and limits

- Stats, spend, activity, tool usage, turns and concurrency pages read the hourly rollups
  every shard ships to global (`ShipStats`). A down region keeps its last shipped numbers
  rather than failing the page.
- A sharded deployment must have the usage service (`usage.url`). It receives every shard's
  requests and backs spend limits; the router refuses to start without it.
- Open phone legs are counted per shard in Redis and summed for concurrency limits.
- Usage that more than one shard could bill (a number's monthly rent) is claimed once in the
  global `usage_claims`.

### Scheduled work

A reminder or a dial meant for a shard is claimed there. If it is unclaimed after 5 minutes
(`claimOverdue`), any shard takes it, so a down region does not hold up reminders.

### A region is down

| What                                     | What happens                         |
| ---------------------------------------- | ------------------------------------ |
| Lists, stats, spend                      | Answered from global, with the down region's last shipped rows |
| A session or call that lives there       | 503 `shard_unavailable`              |
| A new call to a number routed there      | Answered in the region it landed in  |
| Requests in every other region           | Unaffected                           |

Global Postgres being down still stops every region: configs, numbers and customers live
there.

## How to run the benchmark

Two benchmarks matter: the load ramp of real voice agents (`performance_benchmark/`, read
`.claude/skills/performance_benchmark/SKILL.md`) and the API round trips across regions.

### Two shards with Docker

`compose.shards.yaml` runs shard `us` on :8080 and shard `eu` on :8081 against one Postgres
server and separate Redis. Toxiproxy sits on the links that cross a region: the EU router's
global Postgres and Redis. Put `ROUTER_MOCK_PROVIDERS=true` and `ROUTER_AUTH_MODE=noauth`
in the root `.env`, then from the repo root:

```bash
acceleration/dev/shards.sh up            # build the router, start both shards
acceleration/dev/shards.sh latency 100   # 100 ms ±10 on each cross-region answer
(cd performance_benchmark && STREAM_ACCELERATION_CUSTOMER_ID=perfbench \
  go run -tags webrtc ./cmd/perfbench run -target http://localhost:8081 -namespace "" -out out/eu-100ms)
acceleration/dev/shards.sh clear         # back to 0 ms
```

### Two shards without Docker

`acceleration/dev/shards-local.sh` runs the same two shards on a machine without Docker (a
cloud agent), on the local Postgres (:5432) and Redis (:6379). It starts Toxiproxy
(`toxiproxy-server` on `PATH`), a Redis per shard, the usage service and both routers from a
router binary you give it. Toxiproxy also sits on each router's forwarding address, so a
request forwarded to the other region pays the cross-region trip too, which compose does not
model.

The routers get the mock models and the Stream keys and nothing else from the environment, so
no request can reach a paid model vendor.

```bash
go build -o /tmp/router ./acceleration/cmd/router
acceleration/dev/shards-local.sh up /tmp/router
acceleration/dev/shards-local.sh seed      # 20000 sessions and calls a shard
acceleration/dev/shards.sh latency 100
acceleration/dev/shards-api-bench.sh       # the dashboard's reads, from each region
acceleration/dev/shards-local.sh down
```

### What to measure

- **One request from each region**: 50 sequential GETs of `configs`, the session and call
  lists, and one session read from the region that does not hold it. Each request should
  pay at most one cross-region round trip, and the US router's lists none.
- **A region down**: stop the EU router and its database link, then list sessions and calls
  from the US. They must still answer, with the EU rows.
- **The ramp** from the EU router at 0 and 100 ms: session create p95 and reply p95 should
  move by about one round trip, not grow with load.

Follow the benchmark rules in `AGENTS.md`: same code and config in both runs, only the
toxic differs, and report base and candidate commits.

## Results

API reads, 2026-11-25, with `shards-local.sh` and `shards-api-bench.sh` on one 4-core VM.
Base `f2f4191ee` reads the other shard's database directly. Candidate `23d979b80` reads the
index and forwards. The same script and config ran both, with 100 ms ±10 on every
cross-region link (global Postgres and Redis from the EU, each shard's database from the
other, each router's forwarding address). Figures are p50 in ms, the middle of three rounds of
50 requests, and each round agreed within 6 ms.

| from | request                         | base | candidate |
| ---- | ------------------------------- | ---- | --------- |
| us   | `GET configs` (control)         | 0.9  | 0.8       |
| us   | session list                    | 201  | 2.4       |
| us   | call list                       | 103  | 1.4       |
| us   | an EU session                   | 505  | 310       |
| us   | an EU call                      | 305  | 108       |
| eu   | `GET configs` (control)         | 101  | 102       |
| eu   | session list                    | 302  | 204       |
| eu   | call list                       | 100  | 103       |
| eu   | a US session                    | 603  | 412       |
| eu   | a US call                       | 302  | 309       |

- **The US region reads no other region** to list sessions and calls: 201 ms to 2.4 ms. With
  more regions, the base pays for the slowest of them on every list.
- **The EU pays one trip, to global,** for a list, as before. Its session list is one trip
  shorter: the list no longer looks itself up as a session (`23d979b80`).
- **A session or call in another region** costs the lookups of where it lives (global
  Postgres and Redis) and one forward. The router that holds it then reads its own database.
- **With 0 ms between regions**, the lists are 0.2-0.9 ms slower than the base: two queries,
  the shard's and the index's.

**EU down** (its router stopped, its database unreachable), asked of the US router:

| request             | base | candidate                          |
| ------------------- | ---- | ---------------------------------- |
| call list           | 200, US calls only | 200, both regions' calls  |
| session list        | 500  | 200, both regions' sessions        |
| a US session        | 500  | 200                                |
| an EU session or call | 500 | 503 `shard_unavailable`           |

The voice ramp (`perfbench`) did not run: the Stream key on that VM is not a Stream app
(`GetApp` answers `api_key not found`), and an agent cannot join a call without one. It needs
a working `STREAM_API_KEY` and `STREAM_API_SECRET`.

## Not done

- **Terraform** in `chat/infra/accelerate` still sets `ROUTER_POSTGRES_SHARDS`, which the router
  now refuses at start. Each region's router needs `ROUTER_SHARDS` with every shard's name
  instead (`us-east4,europe-west4`), the same on every router, and keeps `ROUTER_SHARD` and
  `ROUTER_POSTGRES_SHARD_DSN`.
- **Exporting a customer** from a deployment of shards is still refused (`not_configured`): an
  export reads its own shard alone, and a shard's tables record no changes.
- **Rollups are whole hours.** A stats window that starts mid-hour counts that hour whole;
  tool p99 is averaged across hours, and the concurrency peak is per minute.
- **Lists from other regions lag** by about 5 seconds, and a row deleted in another region
  stays listed until the next ship.
- **The Swift client** is not regenerated with `region`.
- **Finding where a session lives costs three trips to global** before the forward. A session
  id that names its shard, as call tokens do, would make it one.
- **The voice ramp** across regions has not run (see Results).
