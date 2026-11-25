#!/usr/bin/env bash
# shards-local.sh up <router binary>: the two-shard stack of compose.shards.yaml without Docker,
# on the Postgres at localhost:5432 and the Redis at localhost:6379 (shard us on :8080, eu on
# :8081). Toxiproxy (toxiproxy-server on PATH) sits on the same links and on each router's
# address its peers forward to, so shards.sh latency and shards.sh clear work against it.
# shards-local.sh seed: 20000 closed sessions and calls in each shard, 2000 of them customer
# perfbench's (us-s-1, eu-c-1 and so on), for shards-api-bench.sh.
# shards-local.sh down: stops what up started. Logs are in $SHARDS_DIR (/tmp/shards-local).
set -euo pipefail
cd "$(dirname "$0")/../.."
dir=${SHARDS_DIR:-/tmp/shards-local}
pg=postgres://postgres:postgres@localhost
mkdir -p "$dir"

started() {
  "$@" >>"$dir/$name.log" 2>&1 &
  echo $! >"$dir/$name.pid"
}

healthy() {
  for _ in $(seq 120); do
    curl -sf -o /dev/null "$1" && return 0
    sleep 1
  done
  echo "$1 never answered; see $dir" >&2
  return 1
}

up() {
  router=$(realpath "${1:?say which router binary to run}")
  for db in bench_global bench_us bench_eu bench_usage; do
    psql "$pg:5432/postgres" -tAc "SELECT 1 FROM pg_database WHERE datname = '$db'" | grep -q 1 ||
      psql "$pg:5432/postgres" -qc "CREATE DATABASE $db"
  done
  redis-server --port 6380 --save '' --daemonize yes --pidfile "$dir/redis-us.pid" >/dev/null
  redis-server --port 6381 --save '' --daemonize yes --pidfile "$dir/redis-eu.pid" >/dev/null

  cat >"$dir/toxiproxy.json" <<'EOF'
[
  {"name": "pg_global", "listen": "127.0.0.1:15432", "upstream": "127.0.0.1:5432", "enabled": true},
  {"name": "redis_global", "listen": "127.0.0.1:16379", "upstream": "127.0.0.1:6379", "enabled": true},
  {"name": "node_us", "listen": "127.0.0.1:28080", "upstream": "127.0.0.1:8080", "enabled": true},
  {"name": "node_eu", "listen": "127.0.0.1:28081", "upstream": "127.0.0.1:8081", "enabled": true}
]
EOF
  name=toxiproxy started toxiproxy-server -host 127.0.0.1 -config "$dir/toxiproxy.json"

  (cd acceleration && go build -o "$dir/usage" ./cmd/usage)
  name=usage started env USAGE_ADDR=127.0.0.1:8070 USAGE_POSTGRES_DSN="$pg:5432/bench_usage?sslmode=disable" \
    USAGE_REDIS_URL=redis://localhost:6379 USAGE_INGEST_TOKEN=local-usage-token \
    USAGE_MANAGE_KEY=local-usage-manage-key USAGE_TIERS_FILE=acceleration/tiers/dev.json "$dir/usage"
  healthy http://127.0.0.1:8070/healthz

  # Only the mock models and the Stream keys: no vendor key reaches a router, so a run bills nothing.
  shared=(-i PATH="$PATH" HOME="$HOME" STREAM_API_KEY="${STREAM_API_KEY:-}" STREAM_API_SECRET="${STREAM_API_SECRET:-}"
    ROUTER_MOCK_PROVIDERS=true ROUTER_AUTH_MODE=noauth ROUTER_LIMITS_MODE=off
    # Peers forward to each other only with a key encryption key, the same on both.
    ROUTER_DEV_KEK=6c6f63616c2d7368617264732d6b656b6c6f63616c2d7368617264732d6b656b
    ROUTER_USAGE_URL=http://127.0.0.1:8070 ROUTER_USAGE_TOKEN=local-usage-token ROUTER_SHARDS=us,eu)
  name=router-us started env "${shared[@]}" ROUTER_ADDR=127.0.0.1:8080 ROUTER_PUBLIC_URL=http://localhost:8080 \
    ROUTER_SHARD=us ROUTER_NODE_ADVERTISE=127.0.0.1:28080 \
    ROUTER_POSTGRES_DSN="$pg:5432/bench_global?sslmode=disable" \
    ROUTER_POSTGRES_SHARD_DSN="$pg:5432/bench_us?sslmode=disable" \
    ROUTER_REDIS_ADDR=localhost:6379 ROUTER_REDIS_SHARD_ADDR=localhost:6380 \
    ROUTER_USAGE_REDIS_URL=redis://localhost:6379 "$router"
  healthy http://127.0.0.1:8080/health
  name=router-eu started env "${shared[@]}" ROUTER_ADDR=127.0.0.1:8081 ROUTER_PUBLIC_URL=http://localhost:8081 \
    ROUTER_SHARD=eu ROUTER_NODE_ADVERTISE=127.0.0.1:28081 \
    ROUTER_POSTGRES_DSN="$pg:15432/bench_global?sslmode=disable" \
    ROUTER_POSTGRES_SHARD_DSN="$pg:5432/bench_eu?sslmode=disable" \
    ROUTER_REDIS_ADDR=localhost:16379 ROUTER_REDIS_SHARD_ADDR=localhost:6381 \
    ROUTER_USAGE_REDIS_URL=redis://localhost:16379 "$router"
  healthy http://127.0.0.1:8081/health
  echo "shard us on :8080, shard eu on :8081; logs in $dir"
}

seed() {
  for shard in us eu; do
    psql "$pg:5432/bench_$shard" -q <<EOF
INSERT INTO agent_sessions (id, customer_id, agent_id, state, title, closed_at, created_at, updated_at)
SELECT '$shard-s-' || g, CASE WHEN g <= 2000 THEN 'perfbench' ELSE 'other-' || (g % 50) END,
  'agent-' || (g % 10), 'closed', 'session ' || g, now(), now() - g * interval '1 minute', now() - g * interval '1 minute'
FROM generate_series(1, 20000) g ON CONFLICT DO NOTHING;
INSERT INTO calls (id, customer_id, agent_id, call_id, session_id, started_at, ended_at)
SELECT '$shard-c-' || g, CASE WHEN g <= 2000 THEN 'perfbench' ELSE 'other-' || (g % 50) END,
  'agent-' || (g % 10), '$shard-call-' || g, '$shard-s-' || g, now() - g * interval '1 minute', now()
FROM generate_series(1, 20000) g ON CONFLICT DO NOTHING;
ANALYZE agent_sessions; ANALYZE calls;
EOF
  done
  echo "20000 sessions and calls in each shard, 2000 of them perfbench's"
}

down() {
  for name in router-eu router-us usage toxiproxy redis-us redis-eu; do
    [ -f "$dir/$name.pid" ] || continue
    kill "$(cat "$dir/$name.pid")" 2>/dev/null || true
    rm -f "$dir/$name.pid"
  done
}

case ${1:-} in
up) up "${2:-}" ;;
seed) seed ;;
down) down ;;
*)
  echo "usage: $0 up <router binary> | seed | down" >&2
  exit 2
  ;;
esac
