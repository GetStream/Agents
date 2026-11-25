#!/usr/bin/env bash
# shards-api-bench.sh: times the reads a dashboard makes against both shards of shards.sh or
# shards-local.sh, as customer $CUSTOMER (perfbench). Each endpoint is 3 rounds of 5 warmups and
# 50 sequential requests; a line per endpoint gives each round's p50 and p95 in ms, and the
# status codes seen. US_SESSION/EU_SESSION and US_CALL/EU_CALL name a session and a call each
# shard holds, read from the other shard.
set -euo pipefail
customer=${CUSTOMER:-perfbench}
rounds=${ROUNDS:-3}
runs=${RUNS:-50}

timed() {
  local method=$1 url=$2 body=${3:-}
  local args=(-s -o /dev/null -w '%{http_code} %{time_total}\n' -X "$method" -H "X-Customer-Id: $customer")
  [ -n "$body" ] && args+=(-H 'Content-Type: application/json' -d "$body")
  curl "${args[@]}" "$url"
}

percentiles() {
  sort -n | awk '{a[NR]=$1} END {p50=a[int(NR*0.5+0.5)]; p95=a[int(NR*0.95+0.5)]; printf "%.1f/%.1f", p50*1000, p95*1000}'
}

bench() {
  local label=$1 method=$2 url=$3 body=${4:-}
  local line="$label"
  local codes=""
  for _ in $(seq "$rounds"); do
    for _ in $(seq 5); do timed "$method" "$url" "$body" >/dev/null; done
    local out
    out=$(for _ in $(seq "$runs"); do timed "$method" "$url" "$body"; done)
    codes+=$(awk '{print $1}' <<<"$out")$'\n'
    line+=" | $(awk '{print $2}' <<<"$out" | percentiles)"
  done
  echo "$line | $(sort <<<"$codes" | uniq -c | awk 'NF==2 {printf "%s×%s ", $2, $1}')"
}

echo "endpoint | round 1 p50/p95 ms | round 2 | round 3 | statuses"
for port in 8080 8081; do
  shard=us other_session=${EU_SESSION:-eu-s-1} other_call=${EU_CALL:-eu-c-1}
  if [ "$port" = 8081 ]; then shard=eu other_session=${US_SESSION:-us-s-1} other_call=${US_CALL:-us-c-1}; fi
  base=http://localhost:$port
  bench "$shard GET configs" GET "$base/v1/agents/configs"
  bench "$shard POST sessions/query" POST "$base/v1/agents/sessions/query" '{"limit":20}'
  bench "$shard GET calls" GET "$base/v1/agents/calls?limit=20"
  bench "$shard GET session of the other shard" GET "$base/v1/agents/sessions/$other_session"
  bench "$shard GET call of the other shard" GET "$base/v1/agents/calls/$other_call"
done
