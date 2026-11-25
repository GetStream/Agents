---
name: release
description: Cut a release of the whole repo - write its section of the root CHANGELOG.md, tag `vX.Y.Z` on main and publish the GitHub release, which publishes the Python packages. Read before tagging a release, or when asked to update CHANGELOG.md. Not for router deploy tags (`accelerate-v*`, the `deploy` skill) or SDK tags (each `sdk-<lang>` skill).
---

# Release

A release is one version of the whole repo: the router, the SDKs, the Python packages and the
apps. It is a tag `vX.Y.Z` on a `main` commit and a GitHub release for that tag. The GitHub
release starts `.github/workflows/release.yml`, which publishes the Python packages to PyPI, and
`.github/workflows/publish-sdk-kotlin.yml`, which publishes the Kotlin SDK as `X.Y.Z` and tags
`sdk-kotlin-vX.Y.Z` when `X.Y.Z` is newer than the last `sdk-kotlin-v*` tag. It also starts
`.github/workflows/release-sdks.yml`, which runs the release workflow of the .NET, Ruby, Rust,
PHP, JS and Dart SDKs (the `release-sdks` skill).
Router deploy tags (`accelerate-v*`) and SDK tags (`sdk-js-v*`, `sdk-kotlin-v*`, `sdk-dotnet-v*`,
`sdk-dart-v*`, `sdks/go/v*`) are not releases here and get no CHANGELOG section.

A version uploaded to PyPI can never be uploaded again. Ask the human before step 5.

## 1. Find the previous release and choose the version

```bash
gh release list -R GetStream/Agents --limit 50 --json tagName,publishedAt \
  --jq '[.[] | select(.tagName | test("^v?[0-9]+\\.[0-9]+\\.[0-9]+$"))] | sort_by(.publishedAt) | last | .tagName'
```

This prints the newest release whose tag is a bare version. Tags are `vX.Y.Z`; `0.7.0` is the
one exception, with no `v`. While the version is 0.x, a breaking change bumps the minor and
anything else bumps the patch. No file holds the version: the Python packages read it from the
tag (`sdks/python/pyproject.toml`, `[tool.hatch.version] source = "vcs"`).

## 2. Write the CHANGELOG section in a PR

On a branch from `origin/main`, list what merged since the previous release:

```bash
git log <previous tag>..origin/main --oneline | grep -E '\(#[0-9]+\)$|Merge pull request #[0-9]+ '
```

Do not add `--first-parent`: a PR that reached `main` inside a merge commit (the #2030 accelerate
merge) is not on the first-parent chain, and the list would miss it (#2053). The second pattern
lists a PR merged by a `Merge pull request #N` commit (#2088).

In the root `CHANGELOG.md`:

- Rename `# Unreleased` to `# vX.Y.Z`, and add a new, empty `# Unreleased` above it. For the first
  release after `0.7.0` only, add one line under the heading: the section also covers `0.7.0`,
  which shipped without one. No later release needs the line: since AI-3557, `# Unreleased`
  holds only breaking-change lines between releases. For the first release after `0.7.0`, run the
  listing above from `v0.6.9` (the release before `0.7.0`) instead of `0.7.0`, so 0.7.0's PRs are
  listed too.
- Keep three sections, in this order: `## Breaking Changes`, `## New Features`, `## Bug Fixes`.
  Merge sections that appear twice or under another name (`## Features`). The `# Unreleased`
  written before AI-3557 has both problems.
- Add an entry for each user-facing PR that has none: a `###` title and one or two sentences,
  ending with the PR number, e.g. `(#2207)`. Since AI-3557, PRs add only breaking-change lines,
  so most of the entries are written here. Skip docs-only, CI-only, test-only and skill-only PRs.

In `sdks/swift/core/Sources/StreamAgents/Backend.swift`, set `version` to `X.Y.Z`. The tag is the
Swift SDK's release, and SwiftPM gives a package no way to read it, so this literal is what an iOS
app names itself with in `X-Stream-Client`.

For the release to publish the Dart SDK too, set `version` in
`sdks/dart/stream_agents/pubspec.yaml` and `sdkVersion` in its `lib/src/core/backend.dart` to
`X.Y.Z`, and add the entry to its `CHANGELOG.md`: Dart is published at the version its pubspec
names. Before tagging, run "Release all SDKs" as a dry run with `version` `X.Y.Z` (the
`release-sdks` skill).

Open the PR with the title `Release vX.Y.Z: CHANGELOG`. It needs one approving review. Merge it
before you tag.

## 3. Tag the merge commit

```bash
set -euo pipefail
git fetch origin --tags
SHA=<merge commit of the CHANGELOG PR>
git merge-base --is-ancestor "$SHA" origin/main
test -z "$(git tag --points-at "$SHA")"
git tag vX.Y.Z "$SHA"
```

Do not push the tag yet: step 4 checks it first.

The tag must start with `v`: `release.yml` runs its jobs only when the tag starts with `v`.
`0.7.0` had no `v`, so nothing ran (AI-3561). The commit must carry no other tag. The Python
version is taken from the nearest tag of any kind, and on `main` that is usually a router deploy
tag: between releases, `hatch version` prints a version like `0.6.372.dev14`, from
`accelerate-v0.6.371`.

## 4. Check the version the packages will get

```bash
git worktree add --detach <scratch dir> vX.Y.Z
(cd <scratch dir>/sdks/python && uvx -q --from 'hatch==1.18.2' hatch version)
git worktree remove <scratch dir>
```

It must print `X.Y.Z` exactly. If it does not, stop: delete the local tag (`git tag -d vX.Y.Z`)
and do not publish. If it does, push the tag: `git push origin vX.Y.Z`.

`release.yml` repeats the check on the runner, where the checkout holds only the release's tag:
its build job runs `.github/scripts/check-release-version.sh`, which fails before the upload
unless every wheel and sdist carries the version of the tag. The `Release check` workflow
(Actions > Release check, also run on a pull request that changes the release files) builds every
package at a local tag and runs that script, without the `pypi` environment, so it cannot publish.

## 5. Publish the GitHub release (ask first)

```bash
gh release create vX.Y.Z -R GetStream/Agents --verify-tag --title vX.Y.Z \
  --generate-notes --notes-start-tag <previous tag> \
  --notes "Changelog: https://github.com/GetStream/Agents/blob/vX.Y.Z/CHANGELOG.md (section \`# vX.Y.Z\`)"
```

The body is GitHub's generated list of merged PRs, with the `--notes` line before it
(`gh release create --help`: "Additional release notes can be prepended to automatically
generated notes by using the `--notes` flag"). The CHANGELOG section is not copied into the body.

## 6. Check what was published

- `gh run list -R GetStream/Agents --workflow release.yml --limit 1` shows the run of the new tag.
  Do not read its conclusion: the `integration` job is in the same run and does not gate the
  publish (a provider outage must not hold back a release), so a good publish can end `failure`.
  `gh run view <run id> -R GetStream/Agents --json jobs --jq '.jobs[] | [.name, .conclusion]'` must
  show `success` for `Build & Test Core SDK` (build, version check and upload) and for
  `Verify PIP install works`.
- `curl -s https://pypi.org/pypi/vision-agents/json | jq -r .info.version` must print `X.Y.Z`.
- `gh run list -R GetStream/Agents --workflow publish-sdk-kotlin.yml --event release --limit 1`
  must show `success`. Its run says whether it published Kotlin `X.Y.Z` or why not.
- `gh run list -R GetStream/Agents --workflow release-sdks.yml --event release --limit 1` must
  show `success`. Its summary has one row per SDK; a failed one is released again alone (the
  `release-sdks` skill).

`release.yml` has never succeeded: of its 41 runs, 34 ended `failure`, 3 `startup_failure`, 3
`cancelled` and 1 `skipped`. The packages of 0.6.5 to 0.6.9 reached PyPI outside it, and 0.6.9
has no Trusted Publishing provenance (AI-3561). The first run of the fixed workflow is the first
check of PyPI's side, which the repo cannot read: every PyPI project of the 41 packages must list
`GetStream/Agents`, `release.yml` and the environment `pypi` as a trusted publisher, and
`vision-agents-plugins-omni` and `vision-agents-plugins-stream` are not on PyPI yet, so each needs a
pending publisher. If the run does not succeed, some packages may be on PyPI and others not. Report
it on AI-3561, and do not upload by hand.
