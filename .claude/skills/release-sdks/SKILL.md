---
name: release-sdks
description: How the SDKs are published - what a release of the whole repo publishes by itself, the Release all SDKs workflow for doing it by hand or for one SDK, the one-time registry setup each SDK needs, and what to check when a publish fails. Read before publishing an SDK, adding a release workflow for one, or after publishing a GitHub release. Cutting the release itself (CHANGELOG, tag, GitHub release) is the `release` skill.
---

# Releasing the SDKs

A release is one version of the whole repo, `vX.Y.Z` on `main` (the `release` skill). Publishing
its GitHub release publishes the SDKs as `X.Y.Z`. Each SDK still has its own tag family and its
own workflow, so one can also be released alone, between releases of the repo.

| SDK | Package | Registry | Workflow | On a release `vX.Y.Z` | Tag |
| --- | --- | --- | --- | --- | --- |
| Python | `vision-agents` and the plugins | PyPI | `release.yml` | `X.Y.Z`, from the tag | `v*` |
| Swift | `stream-agents` (SwiftPM) | this repository | none | the tag is the version | `v*` |
| Kotlin | `io.getstream:stream-agents-android-core` | stream-io-repo.com | `publish-sdk-kotlin.yml` | `X.Y.Z` | `sdk-kotlin-v*` |
| .NET | `getstream-calypso` | NuGet | `release-sdk-dotnet.yml` | `X.Y.Z` | `sdk-dotnet-v*` |
| Ruby | `getstream-vision-agents` | RubyGems | `release-sdk-ruby.yml` | `X.Y.Z` | `sdk-ruby-v*` |
| Rust | `vision-agents` | crates.io | `release-sdk-rust.yml` | `X.Y.Z` | `sdk-rust-v*` |
| PHP | `getstream/vision-agents` | Packagist, through a split repository | `release-sdk-php.yml` | `X.Y.Z` | `sdk-php-v*` |
| JS | `@stream-io/stream-agents` | npm | `release-sdk-js.yml` | its own, from `sdk-js` commits | `sdk-js-v*` |
| Dart | `stream_agents` | pub.dev | `release-sdk-dart.yml` | the one in `pubspec.yaml` | `sdk-dart-v*` |
| Go | `github.com/GetStream/Vision-Agents/sdks/go` | the Go proxy | `release-sdk-go.yml` (verifies) | nothing: tagged by hand | `sdks/go/v*` |

## What publishing a GitHub release starts

Three workflows run on the `release` event of a `vX.Y.Z` tag on `main`:

- `release.yml`: the Python packages.
- `publish-sdk-kotlin.yml`: Kotlin `X.Y.Z`.
- `release-sdks.yml` ("Release all SDKs"): starts `release-sdk-<sdk>.yml` on `main` for .NET,
  Ruby, Rust, PHP, JS and Dart, for real, waits and reports one row per SDK with its run.

`release-sdks.yml` publishes nothing itself. npm, NuGet, RubyGems and crates.io trust a workflow
by its file name, so every release stays a run of `release-sdk-<sdk>.yml`. Do not turn those
into reusable workflows called from one file, and do not rename them.

A pre-release, a release for another tag (`sdk-js-v*`, `sdks/go/v*`, `accelerate-v*`) or one whose
commit is not on `main` starts no SDK; the run's summary says which.

What the version does not move:

- **JS** takes its version from commits scoped `sdk-js` since the last `sdk-js-v*` tag, and
  releases nothing when there are none (the `sdk-js` skill).
- **Dart** releases the version committed in `sdks/dart/stream_agents/pubspec.yaml`, if it has
  no `sdk-dart-v*` tag yet. To have a release of the repo include Dart, its CHANGELOG pull
  request also sets `version` in `pubspec.yaml` and `sdkVersion` in `lib/src/core/backend.dart`
  to `X.Y.Z` and adds the package's `CHANGELOG.md` entry. pub.dev takes a package only from a
  run on a tag, so the run from `main` tags the commit and starts a second run of
  `release-sdk-dart.yml` on that tag, which publishes it. If that run fails, run the workflow
  again from the tag.
- **Go** is tagged by hand, with `Version` in `stream/backend.go` set first (the `sdk-go` skill).

After publishing a release, check the "Release all SDKs" run beside the checks in step 6 of the
`release` skill:

```bash
gh run list -R GetStream/Agents --workflow release-sdks.yml --event release --limit 1
```

A failed SDK fails that run and leaves the others as they were. Fix it and release that SDK
alone, as below, at the same version.

## By hand: a dry run, one SDK, or a release between releases

Actions, "Release all SDKs", "Use workflow from" `main`. Or:

```bash
gh workflow run release-sdks.yml -R GetStream/Agents --ref main -f sdks=all -f dry_run=true
```

- `sdks`: `all`, or a list such as `ruby,rust`. The names are `js`, `dotnet`, `ruby`, `rust`,
  `php`, `dart`.
- `version`: the exact version for every SDK named, such as `0.8.0`. Leave it empty to raise
  each SDK's own last version by `bump` (patch, minor or major) instead.
- `dry_run`: on by default. A dry run works out every version, tests and packages, and
  publishes and tags nothing. Each run's summary lists the version and the commits going out.

Before a release of the repo, do a dry run with `version` set to the release's: it is the only
rehearsal of what the GitHub release will start.

One SDK's own workflow takes the same inputs and can be run directly.

## Rules

- Never create a GitHub release for one SDK's tag. JS's release is made as a draft by
  `GITHUB_TOKEN`, which starts nothing.
- A tag is pushed after the publish succeeds, so a failed release leaves no tag and the next
  run works out the same version. Dart is the exception, as above. Never move or delete a
  released tag.
- The version in `version.rb`, `Cargo.toml` and `package.json` is not maintained. The release
  writes the one it publishes into the package and commits nothing back. Dart, Go and Swift do
  name their version in a file, as above.
- A job that publishes runs in the `registry-publish` environment and only from `main`, and a
  job that holds a registry's OIDC token restores no dependency cache.
- A new SDK gets `release-sdk-<sdk>.yml` with `bump`, `version` and `dry_run` inputs, the
  `.github/actions/sdk-version` step for its version, the `refs/heads/main` guard, the
  `registry-publish` environment, and its name in `release-sdks.yml`'s list.
  `release-sdk-ruby.yml` is the one to copy.

## One-time setup

Each registry has to trust its workflow before the first real release. A dry run needs none of
this. Where a registry asks, the repository is `GetStream/Agents` and the environment
`registry-publish`.

- **npm, NuGet**: done.
- **RubyGems**: on rubygems.org add a trusted publisher for the gem `getstream-vision-agents`
  (a pending one, while the gem does not exist): workflow `release-sdk-ruby.yml`.
- **crates.io**: publish the first version by hand with a token (`cargo publish`, then
  `git tag sdk-rust-v<version>`), then add a trusted publisher in the crate's settings:
  workflow `release-sdk-rust.yml`.
- **Packagist**: create the read-only split repository, submit it to Packagist as
  `getstream/vision-agents`, and set the repository variable `SDK_PHP_SPLIT_REPOSITORY`
  (`owner/name`) and, in the `registry-publish` environment, the secret `SDK_PHP_SPLIT_TOKEN`
  (a token that may push to that repository).
- **pub.dev**: on `stream_agents`' admin page enable publishing from GitHub Actions: tag
  pattern `sdk-dart-v{{version}}`, `workflow_dispatch` events allowed.

## When a publish fails

- **"did not start"** in the summary: the SDK's workflow is disabled or not on `main`. The
  "Start each SDK's release" step's log has the refusal.
- **`sdk-<sdk>-v<version> already exists`**: that version is already released for this SDK.
- **A publish step is refused** (401, 403, "no trusted publisher"): the registry's trust is
  missing, or names another file, repository or environment than the run's.
- **Published but not tagged**: the tag step failed after the publish. Tag that run's commit
  `sdk-<sdk>-v<version>` by hand, or the next run tries the same version and the registry
  refuses it.
- **Dart is "already released"**: the pubspec version has a tag. Raise it in a pull request.
- **A GitHub release started no SDK**: the run's summary says why (a pre-release, not
  `vX.Y.Z`, or the tag is not on `main`).
