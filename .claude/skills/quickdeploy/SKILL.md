---
name: quickdeploy
description: Release the router to staging, roll staging back or redeploy a version, by pressing the Deploy staging button (a GitHub workflow in GetStream/agents-platform). Read when asked to release main to staging, roll staging back, redeploy, or press the deploy button. Not for prod.
---

# Quickdeploy: release the router to staging

The script is `cmd/quickdeploy` in the private repo `GetStream/agents-platform`. Its `Deploy staging`
workflow (`.github/workflows/deploy-staging.yml`) runs it. This skill holds no deploy logic: it says
which command to run and how to read the result. This repo is public: name that repo and its files,
never the infrastructure behind them.

## Use it for / not

- Use it to release `main` to staging, to roll staging back, or to redeploy a version.
- Prod is the `deploy` skill. A load test or benchmark is `performance_benchmark`.
- The press needs `gh` logged in with write access to `GetStream/agents-platform`.

## Release main

```bash
url=$(gh workflow run deploy-staging.yml -R GetStream/agents-platform --ref main 2>&1 | grep -o 'https://[^ ]*runs/[0-9]*')
gh run watch "${url##*/}" -R GetStream/agents-platform --exit-status
```

`gh workflow run` prints the new run's URL (gh 2.87 or newer); the last part is the run id. With an
older `gh`, list runs with `gh run list -R GetStream/agents-platform --workflow deploy-staging.yml
--user @me --json databaseId,status,createdAt --limit 1` and take the run only if `createdAt` is
after the press: right after it the list can still show the previous, finished run, and watching that
one exits 0 at once.

A release is always cut from the head of this repo's `main`; the workflow has no branch input.
A run, in order: checks its tools, the staging cluster rights and the version it was given; picks
the head of `main`; refuses a commit that is not merged or that adds an out-of-order migration;
builds both architectures; pushes the tag `accelerate-vX.Y.Z` here; publishes the binaries; restarts
staging one pod at a time; verifies every pod and the health URL. After the script, the workflow
attests the release manifest.

Read these log lines (`gh run view <run id> -R GetStream/agents-platform --log`):

- `staging runs vA (set by ...)`: what ran before the press.
- `staging runs vX.Y.Z; roll back with: quickdeploy -version vA`: success. `vX.Y.Z` is the new
  release, `vA` the one to go back to. The log names the script's flag; with the button pass it as
  `-f version=vA`.
- `done in ...`: the script finished.

## Roll back or redeploy

```bash
gh workflow run deploy-staging.yml -R GetStream/agents-platform --ref main -f version=vX.Y.Z
```

This builds nothing and cuts no tag; it points staging at a version that is already published and
restarts the pods. Use the `vA` of the success line to roll back, or the current version to redeploy.
A version that is not `vX.Y.Z`, or is not fully published, is refused. A version below `v0.6.135` is refused: older routers ignore `require_approval`, so connector write
tools would run unasked.

## Refusals

The run fails with `quickdeploy: <message>`; the message tells the cause.

| Message starts or contains | What it means and what to do |
| --- | --- |
| `the deploy lock is held by` | Another deploy runs on staging. Wait for it; never force the lock. The stuck-lock runbook is in the `AGENTS.md` of `GetStream/agents-platform`. |
| `is older than ... goose would refuse it` | The release adds a migration older than one staging already applied. Rename the migration past it, merge, press again. |
| `is not on origin/main` | Laptop `-ref` only; the button always releases the head of `main`. Only merged commits are released. |
| `is already released as vX.Y.Z; deploy it with -version` | The head of `main` has a release already. Redeploy it with `-f version=vX.Y.Z`. |
| `cannot ... fix cluster access` | The deploy account lacks a cluster right. Stop and tell the owner of `GetStream/agents-platform`. |
| `is below v0.6.135` | Rollback floor, see above. |
| `is not vX.Y.Z` or `is not fully published` | The `-f version` is malformed, or that version was never fully published. Use a version from a success line. |
| Red only at the step `Attest the release manifest` | Staging is up, but this release can never be attested, so prod will not take it. Do not rerun: it stops at "already released". The next `main` commit gives a release prod can promote. See the `AGENTS.md` of `GetStream/agents-platform`. |
| `roll back with: quickdeploy -version vA` after an error | The pods did not come up or `/health` failed. Run the rollback to `vA`. |

## Laptop fallback

Use it only when the button cannot be pressed: a laptop release carries a manifest nobody attested,
so prod will not take it and it stays on staging. From a `GetStream/agents-platform` checkout with
Docker, `gcloud` and `kubectl` set up for staging:

```bash
GOWORK=off go run ./cmd/quickdeploy -dry-run -src <path to this checkout>
GOWORK=off go run ./cmd/quickdeploy -src <path to this checkout>
GOWORK=off go run ./cmd/quickdeploy -src <path to this checkout> -version vX.Y.Z
```

The flags, the stuck-lock runbook and how to verify an attestation are in the `AGENTS.md` of
`GetStream/agents-platform`. Read them there; do not copy them here.

## After a deploy

Test the feature live on staging, with a control, before saying it shipped. A green run is not a test.
