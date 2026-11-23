#!/usr/bin/env bash
# Runs cleanup.sql on one router database from a short-lived psql pod in the router's namespace,
# with the DSN read from accelerate-secrets, then deletes the pod. A dry run unless --apply.
#
#   run.sh <region> <namespace> <dsn-key> <set> [--apply] [--drop-strays] [--show]
#
# Each database, once every router using it runs a release with the squashed migrations:
#
#   run.sh us-east1     accelerate-staging ROUTER_POSTGRES_DSN       both     staging, one database
#   run.sh us-east4     accelerate-prod    ROUTER_POSTGRES_DSN       global   the shared database
#   run.sh us-east4     accelerate-prod    ROUTER_POSTGRES_SHARD_DSN shard    each region's own
#   run.sh us-west1     accelerate-prod    ROUTER_POSTGRES_SHARD_DSN shard
#   run.sh europe-west4 accelerate-prod    ROUTER_POSTGRES_SHARD_DSN shard
set -euo pipefail

if [[ $# -lt 4 ]]; then
  sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//'
  exit 2
fi
region=$1 namespace=$2 key=$3 set=$4
shift 4
flags=""
for flag in "$@"; do
  case $flag in
    --apply) flags+=" -v apply=1" ;;
    --drop-strays) flags+=" -v drop_strays=1" ;;
    --show) flags+=" -v show=1" ;;
    *) echo "unknown flag $flag" >&2; exit 2 ;;
  esac
done
case $set in global | shard | both) ;; *) echo "set must be global, shard or both" >&2; exit 2 ;; esac

context="gke_stream-gcp-2026-v2_${region}_${region}-acceleration"
pod="squash-cleanup-$(date +%s)-$RANDOM"
script=$(base64 < "$(dirname "$0")/cleanup.sql" | tr -d '\n')

kubectl --context "$context" -n "$namespace" apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${pod}
  labels: {app: squash-cleanup}
spec:
  restartPolicy: Never
  automountServiceAccountToken: false
  securityContext:
    runAsNonRoot: true
    runAsUser: 70
    runAsGroup: 70
    seccompProfile: {type: RuntimeDefault}
  containers:
    - name: psql
      image: postgres:18-alpine
      command: ["sh", "-c"]
      args:
        - echo "\$SCRIPT" | base64 -d > /tmp/cleanup.sql && psql "\$DSN" -X -v set=${set}${flags} -f /tmp/cleanup.sql
      securityContext:
        allowPrivilegeEscalation: false
        capabilities: {drop: [ALL]}
      resources:
        requests: {cpu: 100m, memory: 128Mi}
        limits: {memory: 128Mi}
      env:
        - name: SCRIPT
          value: "${script}"
        - name: DSN
          valueFrom:
            secretKeyRef: {name: accelerate-secrets, key: ${key}}
EOF

trap 'kubectl --context "$context" -n "$namespace" delete pod "$pod" --wait=false >/dev/null 2>&1 || true' EXIT
phase=Pending
for _ in $(seq 1 150); do
  phase=$(kubectl --context "$context" -n "$namespace" get pod "$pod" -o jsonpath='{.status.phase}')
  [[ $phase == Succeeded || $phase == Failed ]] && break
  sleep 2
done
echo "== $region $namespace $key ($set):$flags"
kubectl --context "$context" -n "$namespace" logs "$pod"
[[ $phase == Succeeded ]]
