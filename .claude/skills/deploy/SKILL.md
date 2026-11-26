---
name: deploy
description: Ship acceleration/cmd/router to Stream's hosted environments. Read before deploying the router or asking why the hosted one behaves differently.
---

# Deploy the router

The hosted router behind `accelerate-<region>.gcp.stream-io-api.com` (production) and
`accelerate-staging.gcp.stream-io-api.com` is `acceleration/cmd/router`
built from this repo. **The deploy lives in Stream's private repos.** GetStream/agents-platform
holds the Deploy staging button (`cmd/quickdeploy`). GetStream/chat holds the rest:
`infra/docs/accelerate_launch.md` is the runbook, `infra/charts/accelerate/README.md` the
chart, and the CLI (rocky), cluster names and credentials are all there. This repo is public —
name the runbook, not the infrastructure.

Kubernetes owns the image, a version registry owns the binary: a pod fetches
`router-linux-<arch>` at start, so shipping a router is publishing a binary, moving that
pointer and restarting. Not an image build, not a chart deploy.

## Release

A release is one version, `vX.Y.Z`, tagged `accelerate-vX.Y.Z`, and a
published version is immutable. Only the Deploy staging button cuts one (AI-3915): never build or upload a release by hand. Staging runs
it first, then prod gets the same binaries, one shard after another. To check only that the
cgo build of a branch pushed to origin (not the working tree) passes, run this in
GetStream/agents-platform; it tags, uploads and deploys nothing:
`GOWORK=off go run ./cmd/quickdeploy -build-only -ref <branch> -src <this checkout>`.

1. **Release to staging.** Merge to `main`, then press the button as the `quickdeploy` skill
   says (`.claude/skills/quickdeploy/SKILL.md`). Note the two versions its success line names:
   the new one and the one to roll back to.
2. **Test it live on staging, with a control.** A green run is not a test. Run what the change
   changed (a request, a call, a tool) as a real app on staging. Then run the same check where
   the change must make no difference, or on a prod shard that still runs the previous version,
   so the pair shows that the test tells the new version from the old. On a failure, roll staging
   back (step 5); prod does not start.
3. **Get a go.** Prod starts only after a person says go for this version. Ask with the version,
   the staging result and its control, and wait. A passing test is not a go.
4. **Promote the same version to prod, one shard at a time,** as «Production» below says.
5. **Roll back.** Staging: press the button with the previous version («Roll back or redeploy»
   in the `quickdeploy` skill). Prod, per shard: `rocky launch deploy` as in step 4, with
   `--version` set to what `launch inspect` showed before the promotion, then the same checks.
   `kubectl rollout undo` does not change the version. The floors in «Before and after» hold.

## Overlap deploys (staging and the benchmark release)

Staging and the benchmark release (`perfbench -target benchmark`, nobody else's traffic) ship
a router without dropping a call. The runbook is `infra/docs/accelerate_overlap_deploy.md` in
the private repo; `rocky fastdeploy --service accelerate-staging` (or `accelerate-benchmark`)
builds this checkout and swaps it in, in 11-20 s warm.

- It needs `source ~/.config/rocky/gcp.env`, Docker, Go and gcloud, and an arm64 machine:
  the build runs natively and these releases' pods run arm64 only. It builds the working tree,
  uncommitted changes included, so the merge-first rule below is for tagged releases.
- `--status` prints what each pod runs. The fetch initContainer's log only says what a pod
  started on, which a swap has since replaced.
- On staging a fastdeploy and a tagged release move the same pointer, and a restarted pod
  comes back on whichever was last. Pull before a fastdeploy, or it rolls back a teammate's
  release. Load-test on benchmark, not staging.

- A supervisor (devrun) owns the public socket and hands it to each router as fd 3
  (`listen_fd`), with a peer port of its own (`node.listen`). A swap starts the new router on
  the same socket, waits for `/health`, then SIGTERMs the old one.
- The old router stops accepting, keeps every call until it ends, bounded by
  `shutdown.drain` (1 h), still answering its peers, then exits. A second signal ends its
  calls now. At most three generations run at once.
- A draining router exits on `session.Manager.Len()` reaching zero, which counts a session
  until its `Close` returns. Anything new that holds a call open must be counted there, or
  the old router exits under it.
- A call only ends when the client's `close` reaches the router. A client that stops reading
  its session socket, or closes the connection straight after `close`, leaves the session
  running and holds the old router for the whole drain. `Pipeline.Leave` in the Go SDK waits
  for the router to hang up.
- To check a deploy, run perfbench against the benchmark release during the swap and confirm
  every call held, nothing answered 5xx, and devrun logged `pid N (sha) drained in ...,
  exit 0` for each old router once its last call ended.

## Sandbox proxy

Staging and every prod shard run the router with `broker: proxy`: code in a sandbox reaches a
connector's API through the router's egress proxy (`acceleration/internal/sandbox/egress/AGENTS.md`),
not through Daytona Secrets.

| Where | On since | GetStream/chat PR |
|---|---|---|
| staging | AI-2035 | |
| prod us-east4 | AI-2461 | #20044 |
| prod us-west1, europe-west4 | AI-2464 | #20103 |

AI-2462 proved it live on prod us-east4: `run_code` `GET api.github.com/user` through the proxy
answered 200, and the token never reached the sandbox.

- The values are in GetStream/chat on `master`. Staging's file
  is `infra/shards/us-east1-acceleration/accelerate-staging.yaml`. Each prod shard has its own
  file, `infra/shards/<region>-acceleration/accelerate-prod.yaml`.
- `router.sandboxProxy` holds `enabled`, `broker: proxy`, the public URL (it holds the proxy port)
  and the LoadBalancer's reserved address. The accelerate chart (`infra/charts/accelerate`, same
  repo) renders the Service from them.
- `router.overlapDeploy.holdSandboxProxy: true` makes devrun hold the proxy port :8091.
  `router.overlapDeploy.devrunImage` pins the devrun that can hold it (chat #19840).
- The CA certificate and key are two secrets, `ROUTER_CODE_SANDBOX_PROXY_CA_CERT` and
  `ROUTER_CODE_SANDBOX_PROXY_CA_KEY`. They reach the pods through `accelerate-secrets`, which is
  synced from Secret Manager. Staging has its own CA. Each prod shard has its own P-256 CA
  (AI-2460). The comment above `sandboxProxy` in each values file names the CA's expiry.
- Rotate each CA before it expires. Staging: the steps are in Linear AI-2069. Prod: AI-2663
  (us-east4), AI-2664 (us-west1) and AI-2665 (europe-west4), each due 2027-10-02.
- This repo is public. Read addresses and secret paths in the values file, and do not copy them here.

Verify after an apply, on each shard (AI-2461, AI-2464):

1. The rollout finishes, and each router logs `serving the sandbox egress proxy` with `tls=true`.
2. TLS verifies against that shard's CA: `openssl s_client -connect <address>:8091 -CAfile
   <shard CA cert> -verify_ip <address> -verify_return_error` prints `Verification: OK`.
3. A CONNECT without a credential gets `407 Proxy Authentication Required`.
4. One `run_code` `GET api.github.com/user` through a binding with `sandbox: true` answers 200.

Every prod region has a public hostname, `https://accelerate-<region>.gcp.stream-io-api.com`: all
three are behind the global load balancer (`accelerate_prod_regions` in
`infra/dns/gcp-stream-io-api-com/accelerate_global_lb.tf`). To test a region, call its hostname as
the app. Prod runs `authMode: proxy` with `proxyDeclaresKind: true`, so send `X-Stream-App-Id:
<app>` and `Stream-Auth-Type: server` (`acceleration/internal/auth/auth.go`,
`proxied.Authenticate`). Also send `X-Stream-Api-Key-Id: <the app's public key>` (`APIKeyIDHeader`,
`auth.go:112-118`) and `X-Stream-Actor-Name: <your name>`. Without the key header, the router
records the run's usage with no API key and raises the `unattributed` usage alert. This happened on
prod us-west1 and europe-west4 on 2026-11-03 21:28Z (AI-2464 E2E). The sandbox still reaches the
shard's proxy on its public address.

Datadog (AI-2463, chat #20097, `infra/shards/us-east4-acceleration/datadog/sandbox_egress.tf`):

- The dashboard "Accelerate sandbox egress proxy" shows the proxy's requests per cluster, decision
  and host.
- The monitor `sandbox_egress_5xx` fires on 3 or more 5xx in 30 minutes in `accelerate-prod`, all
  clusters together. A refused Upgrade (`denied_request`, 501) does not count. It notifies
  `@oncall-agents` and pages nobody.
- Datadog scrapes the router in us-west1 and europe-west4 since #20103 (`monitoring.datadog.enabled`).

To turn the proxy off on one shard:

1. In that shard's values file, set `router.overlapDeploy.holdSandboxProxy: false` and
   `router.sandboxProxy.enabled: false`.
2. Run `rocky accelerate up --region <region>-acceleration --env prod`.
3. Wait for the rollout to finish. An edit alone changes nothing.

With the proxy off, prod refuses a session that binds a sandbox again. The steps come from the
AI-2461 and AI-2464 apply plan. The chart renders the hold only while `sandboxProxy.enabled` is on.

## Production

Production is three shards, us-east4, us-west1 and europe-west4. Each region has its own hostname,
`accelerate-<region>.gcp.stream-io-api.com`, that reaches only that shard. All three are published
(`local.accelerate_prod_regions`). The SDKs' default, `accelerate.gcp.stream-io-api.com`
(`DefaultURL`, `sdks/go/stream/backend.go:75`), reaches the nearest one. Any shard serves any
app. Each runs the router with its own Postgres and Redis beside the global ones in us-east4
(`acceleration/README.md`, "Shards"). The runbook is `infra/docs/accelerate_launch.md` in
GetStream/chat, step 8 and «Day two».

You need rocky built from GetStream/chat `master` at or after #21792 (AI-3953), with
`make -C infra/cli install`: it takes one deploy lock per shard and env, the lock the button
takes on staging; an older rocky takes one lock that neither sees. You also need
`source ~/.config/rocky/gcp.env` and a kube context for each prod cluster (runbook step 4). Run
rocky from inside the private repo, or set `ROCKY_DIR` to that repo's `infra/` directory (a
checkout of its `master` branch). Without either, it stops with "could not locate infra/
directory" before it takes the lock, so nothing moves.

Start only after the go of «Release» step 3. Note what each shard runs now, its rollback
version, then promote the shards one at a time, in any order:

```bash
rocky launch inspect --env prod
VERSION=vX.Y.Z   # the version staging passed
REGION=us-west1   # first shard; any shard may go first
rocky launch deploy --shard "$REGION-acceleration" --env prod --service acceleration --version "$VERSION"
rocky accelerate status --region "$REGION-acceleration" --env prod
curl -fsS "https://accelerate-$REGION.gcp.stream-io-api.com/health"
```

Once the shard passes the checks below, run the last three commands again with the next
`REGION` (`us-west1`, `europe-west4`, `us-east4`).

`launch deploy` refuses a version not published for every architecture, moves the shard's
pointer, restarts and waits for each Deployment to roll out (10 min, `--timeout`). Start the
next shard only when this one passes all four: `launch deploy` exits 0; `accelerate status`
shows every router pod `Running` and a fetch log naming `$VERSION`; `/health` answers 200; the
step 2 test passes as an app against this shard's hostname (headers in «Sandbox proxy»
above). On a failure, stop: the shards not yet promoted keep the previous version. A failure of
this shard's environment rolls back this shard only. A failure that comes from the version
itself rolls back every shard already promoted, one at a time, as «Release» step 5 says.

- Migrations run on start and take an advisory lock, so shards starting together are safe.
  A deployment that sets `postgres.migrate_dsn` runs `router migrate` per shard first instead,
  and its routers refuse to start until it has (`acceleration/README.md`, «A serving role that
  owns no table»).
  A migration has to be right for both databases: the global tables and the shard tables
  share one migration set.
- Prod runs with the overlap deploy on, so a restarted pod drains its calls (up to 1 h),
  but `rocky fastdeploy` does not reach the prod clusters yet: deploy with `launch deploy`.

## The key encryption key

A hosted router unwraps its key encryption key with Cloud KMS at startup: `ROUTER_AUTH_KMS_KEY`
and `ROUTER_AUTH_KEK_V<n>_WRAPPED`. The steps (IAM role, the `gcloud kms encrypt` line, the two
variables) are in `acceleration/README.md`, «The key encryption key in Cloud KMS». The router
refuses to start with a plain `ROUTER_AUTH_KEK` or `ROUTER_AUTH_KEK_V<n>` in its environment, and
`ROUTER_DEV_KEK` (laptops and CI) beside a KMS setting. So a deployment that still holds a plain
key switches its values on the build it runs (set the two variables, remove the plain ones,
restart) before it takes a build that refuses them. Keep a copy of the plain key outside the
deployment, and check that the pods start and open an existing connection. To roll back a new
wrapped value, set `ROUTER_AUTH_KEK_V1_WRAPPED` back to the previous one and restart.

## Before and after

- Merge first: the button releases only a commit on `main`, so the version names a commit anybody can check out.
- Values and router are one deployment. Rolling the auth work also meant moving `ROUTER_AUTH_MODE` to `proxy`, because `noauth` had changed meaning underneath it. Render the chart, diff it against the live release, explain every line.
- Migrations run on start, or in `router migrate` where `postgres.migrate_dsn` is set. Additive
  is fine; anything else wants a plan first.
- Never roll back below `accelerate-v0.6.135`, the first tag with f38926d2 (#1182): an older
  router ignores `require_approval` on connector bindings, so their write tools run without
  the person's approval. This floor holds in every environment.
- Staging and every prod shard have a second, higher floor: never roll back below
  `accelerate-v0.6.191` (#1433). Since 2026-10-29 staging devrun holds the sandbox proxy port
  :8091 and passes it to the router as fd 4 (`router.overlapDeploy.holdSandboxProxy`, GetStream/chat
  #19810, #19840). Prod devrun does the same since AI-2461 (us-east4) and AI-2464 (us-west1,
  europe-west4). A router older than v0.6.191 ignores that socket and retries :8091 until it
  exits: the API serves, the sandbox proxy does not. To go below it on one shard, in this order:
  1. Set `router.overlapDeploy.holdSandboxProxy: false` in that shard's values file.
  2. Run `rocky accelerate up --region us-east1-acceleration --env staging`, or on prod
     `rocky accelerate up --region <region>-acceleration --env prod`.
  3. Wait for the rollout.
  4. Roll back.

  The steps are in GetStream/chat `infra/docs/accelerate_overlap_deploy.md`, paragraph "Revert" under
  "The sandbox proxy socket". If the only target is older than v0.6.135, stop and ask.
  A router older than v0.6.191 already stuck behind devrun's socket needs only steps 1 to 3 (set false, `rocky accelerate up`, wait); the new pods start without it.
- The version is deliberately not in the pod spec. The fetch initContainer's log says which binary a pod started on; after a fastdeploy, ask `--status` instead.
- A model with no key in that environment drops to the next candidate, and when a strict term like `diarize` leaves none the STT socket just closes on the caller. Check the key before reading that as a bug.
- Verify from outside: `examples/routers/stt_realtime_example -hosted` streams a file through the hosted router and prints which model answered.
