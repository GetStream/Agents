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
gh workflow run deploy-staging.yml -R GetStream/agents-platform --ref main
gh run list -R GetStream/agents-platform --workflow deploy-staging.yml --limit 1   # the run id
gh run watch <run id> -R GetStream/agents-platform --exit-status
```

A release is always cut from the head of this repo's `main`; the workflow has no branch input.
A run, in order: checks its tools, the staging cluster rights and the version it was given; picks
the head of `main`; refuses a commit that is not merged or that adds an out-of-order migration;
builds both architectures; pushes the tag `accelerate-vX.Y.Z` here; publishes the binaries; attests
the release manifest; restarts staging one pod at a time; verifies every pod and the health URL.

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
A version below `v0.6.135` is refused: older routers ignore `require_approval`, so connector write
tools would run unasked.

## Refusals

The run fails with `quickdeploy: <message>`; the message tells the cause.

| Message starts or contains | What it means and what to do |
| --- | --- |
| `the deploy lock is held by` | Another deploy runs on staging. Wait for it; never force the lock. The stuck-lock runbook is in the `AGENTS.md` of `GetStream/agents-platform`. |
| `is older than ... goose would refuse it` | The release adds a migration older than one staging already applied. Rename the migration past it, merge, press again. |
| `is not on origin/main` | Only merged commits are released. Merge the change first. |
| `is already released as vX.Y.Z; deploy it with -version` | The head of `main` has a release already. Redeploy it with `-f version=vX.Y.Z`. |
| `cannot ... fix cluster access` | The deploy account lacks a cluster right. Stop and tell the owner of `GetStream/agents-platform`. |
| `is below v0.6.135` | Rollback floor, see above. |
| `roll back with: quickdeploy -version vA` after an error | The pods did not come up or `/health` failed. Run the rollback to `vA`. |

## Laptop fallback

Use it only when the button cannot be pressed. From a `GetStream/agents-platform` checkout with
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
