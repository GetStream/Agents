#!/usr/bin/env bash
# Prints the generated files the PR in <dir> leaves out of step with their source. Drift that is
# already at the merge base with <base-ref> is reported as inherited, not blamed on the PR. Exits 1
# when the PR introduces drift. Runs the code in <dir>: never on a fork PR without asking.
set -euo pipefail

if [ $# -ne 2 ]; then
  echo "usage: generated.sh <dir> <base-ref>" >&2
  exit 2
fi
DIR=$(cd "$1" && pwd)
BASE=$2
docker info >/dev/null

# The generated code of each SDK that CI checks, except Swift. The Swift generator is in a private
# repository.
GENERATED=(
  sdks/go/acceleration/generated.go
  plugins/stream/vision_agents/plugins/stream/_generated
  sdks/kotlin/core/src/main/kotlin/io/getstream/agents/android/core/generated
  sdks/dotnet/src/GetStream.VisionAgents/Generated
  sdks/php/src/Generated
  sdks/rust/src/types.rs
  sdks/rust/src/operations.rs
  sdks/ruby/lib/getstream/vision_agents/generated
  sdks/dart/stream_agents/lib/src/core/generated
)

# Prints each generated file that is out of step with its source in checkout $1.
stale() {
  local co=$1 out
  # cmd/openapi writes api/openapi.yaml in place: compare, then put the committed file back.
  (cd "$co/acceleration" && go run ./cmd/openapi)
  if ! git -C "$co" diff --quiet -- acceleration/api/openapi.yaml; then
    echo acceleration/api/openapi.yaml
  fi
  git -C "$co" restore -- acceleration/api/openapi.yaml
  # The check runs openapi-typescript from node_modules. --ignore-scripts installs it without
  # running any package's install scripts.
  if [ ! -d "$co/sdks/js/node_modules" ]; then
    (cd "$co/sdks/js" && npm ci --ignore-scripts --no-audit --no-fund >/dev/null)
  fi
  if out=$(cd "$co/sdks/js" && npm run --silent types -- --check 2>&1); then
    :
  elif grep -q "out of step" <<<"$out"; then
    echo sdks/js/src/generated/api.ts
  else
    echo "$out" >&2
    return 1
  fi
  # These are the commands of acceleration/README.md, "Regenerate the HTTP layer". Each generator
  # prints a "wrote ..." line, and the caller reads stdout as the list of stale files.
  {
    local REPO=$co
    (cd "$co/sdks/go" && go generate ./...)
    uv run "$co/plugins/stream/generate.py"
    uv run "$co/sdks/kotlin/generate.py"
    docker run --rm -v "$REPO":/repo -v va-nuget:/root/.nuget -w /repo/sdks/dotnet \
      mcr.microsoft.com/dotnet/sdk:10.0 dotnet run --project tools/Generate
    docker run --rm -v "$REPO":/repo -v "$REPO/sdks/php/build/composer-cache":/tmp/composer-cache \
      -e COMPOSER_CACHE_DIR=/tmp/composer-cache -w /repo/sdks/php composer:2 install
    docker run --rm -v "$REPO":/repo -w /repo/sdks/php php:8.4-cli \
      php bin/generate.php ../../acceleration/api/openapi.yaml
    docker run --rm -v "$REPO":/repo -v va-rust-cargo:/usr/local/cargo/registry \
      -v va-rust-rustup:/usr/local/rustup -v va-rust-target:/repo/sdks/rust/target \
      -w /repo/sdks/rust rust:1 cargo run -q -p generate
    docker run --rm -v "$REPO":/repo -w /repo/sdks/ruby ruby:4.0 ruby script/generate
    docker run --rm -v "$REPO":/repo -v va-dart-pub:/pub-cache -e PUB_CACHE=/pub-cache \
      -w /repo/sdks/dart/stream_agents ghcr.io/cirruslabs/flutter:3.44.0 \
      sh -c 'flutter pub get && dart run tool/generate.dart'
  } >/dev/null
  git -C "$co" status --porcelain --untracked-files=all -- "${GENERATED[@]}" | cut -c4-
  git -C "$co" restore -- "${GENERATED[@]}"
  git -C "$co" clean -fdq -- "${GENERATED[@]}"
}

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

stale "$DIR" >"$WORK/head"
if [ ! -s "$WORK/head" ]; then
  echo "in step: every generated file matches its source"
  exit 0
fi

git -C "$DIR" fetch --quiet origin "$BASE"
MB=$(git -C "$DIR" merge-base "origin/$BASE" HEAD)
trap 'git -C "$DIR" worktree remove --force "$WORK/base" 2>/dev/null || true; rm -rf "$WORK"' EXIT
git -C "$DIR" worktree add --quiet --detach "$WORK/base" "$MB"
stale "$WORK/base" >"$WORK/base.txt"

INTRODUCED=$(grep -vxFf "$WORK/base.txt" "$WORK/head" || true)
INHERITED=$(grep -xFf "$WORK/base.txt" "$WORK/head" || true)
for f in $INHERITED; do echo "inherited from $BASE at ${MB:0:7}: $f"; done
for f in $INTRODUCED; do echo "introduced by the PR: $f"; done
[ -z "$INTRODUCED" ]
