#!/usr/bin/env bash
# shards.sh up: starts the two-shard stack of compose.shards.yaml (us on :8080, eu on :8081).
# shards.sh latency <ms> [jitter]: delays every cross-region link by <ms>, each way's answer.
# shards.sh clear: takes the delay off again.
set -euo pipefail
cd "$(dirname "$0")/../.."
api=${TOXIPROXY_URL:-http://localhost:8474}

# links is every link Toxiproxy holds: compose.shards.yaml's two, and shards-local.sh's
# forwards too.
links() {
  proxies=$(curl -sSf "$api/proxies" | grep -oE '"[A-Za-z0-9_]+":\{"name"' | cut -d'"' -f2 | sort | tr '\n' ' ')
}

clear() {
  for proxy in $proxies; do
    curl -sS -o /dev/null -X DELETE "$api/proxies/$proxy/toxics/latency"
  done
}

case ${1:-} in
up)
  docker compose -f compose.yaml -f compose.shards.yaml up -d --build --wait router router-eu
  ;;
latency)
  ms=${2:?say the latency in milliseconds}; jitter=${3:-10}
  links
  clear
  for proxy in $proxies; do
    curl -sSf -o /dev/null -X POST "$api/proxies/$proxy/toxics" -H 'Content-Type: application/json' \
      -d "{\"name\":\"latency\",\"type\":\"latency\",\"stream\":\"downstream\",\"attributes\":{\"latency\":$ms,\"jitter\":$jitter}}"
  done
  echo "$proxies: ${ms}ms ±${jitter}ms"
  ;;
clear)
  links
  clear
  echo "$proxies: no latency"
  ;;
*)
  echo "usage: $0 up | latency <ms> [jitter] | clear" >&2
  exit 2
  ;;
esac
