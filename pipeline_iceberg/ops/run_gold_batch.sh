#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  bash pipeline_iceberg/ops/run_gold_batch.sh YEAR MONTH

Example:
  bash pipeline_iceberg/ops/run_gold_batch.sh 2020 03

Optional environment variables:
  NAMESPACE=dt
  CONFIGMAP=ocean-pipeline-config
  LOG_ROOT=/opt/zfs/project/logs
  AOI_IDS=taiwan,northwest_pacific
  GOLD_TIMEOUT=12h
  SERVING_TIMEOUT=6h

What it does:
  1. Patch ConfigMap with BATCH_ID, START_DATE, END_DATE, DASHBOARD_START_DATE, SERVING_RELEASE_ID
  2. Run existing 04-gold-job.yaml
  3. Wait for Gold job completion
  4. Run existing 05-serving-job.yaml
  5. Wait for Serving export completion
  6. Verify Iceberg and HDFS serving batch outputs
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -ne 2 ]]; then
  usage
  exit 2
fi

YEAR="$1"
MONTH="$2"

if ! [[ "$YEAR" =~ ^[0-9]{4}$ ]]; then
  echo "ERROR invalid YEAR: $YEAR" >&2
  exit 2
fi

if ! [[ "$MONTH" =~ ^[0-9]{1,2}$ ]]; then
  echo "ERROR invalid MONTH: $MONTH" >&2
  exit 2
fi

MONTH="$(printf '%02d' "$((10#$MONTH))")"
if (( 10#$MONTH < 1 || 10#$MONTH > 12 )); then
  echo "ERROR invalid MONTH: $MONTH" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIPELINE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$PIPELINE_ROOT/.." && pwd)}"
K8S_DIR="$PIPELINE_ROOT/deploy/kubernetes"

NAMESPACE="${NAMESPACE:-dt}"
CONFIGMAP="${CONFIGMAP:-ocean-pipeline-config}"
LOG_ROOT="${LOG_ROOT:-/opt/zfs/project/logs}"
GOLD_TIMEOUT="${GOLD_TIMEOUT:-12h}"
SERVING_TIMEOUT="${SERVING_TIMEOUT:-6h}"

START_DATE="${YEAR}-${MONTH}-01"
END_DATE="$(date -d "$START_DATE +1 month -1 day" +%F)"
BATCH_ID="${YEAR}_${MONTH}"
SERVING_RELEASE_ID="${SERVING_RELEASE_ID:-$BATCH_ID}"
DASHBOARD_START_DATE="${DASHBOARD_START_DATE:-$START_DATE}"

mkdir -p "$LOG_ROOT"
RUN_TS="$(date -u +%Y%m%dT%H%M%SZ)"
LOG_FILE="$LOG_ROOT/gold_batch_${BATCH_ID}_${RUN_TS}.log"

log() {
  printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" | tee -a "$LOG_FILE"
}

run_logged() {
  log "RUN $*"
  "$@" 2>&1 | tee -a "$LOG_FILE"
}

wait_job() {
  local job_name="$1"
  local timeout="$2"

  run_logged kubectl wait -n "$NAMESPACE" \
    --for=condition=complete \
    "job/$job_name" \
    "--timeout=$timeout"
}

wait_job_pod_started() {
  local job_name="$1"
  local timeout="${2:-10m}"

  log "WAIT job=$job_name pod=created"
  run_logged kubectl wait -n "$NAMESPACE" \
    --for=create \
    "pod" \
    -l "job-name=$job_name" \
    "--timeout=$timeout"

  log "WAIT job=$job_name pod=ready"
  if ! kubectl wait -n "$NAMESPACE" \
    --for=condition=Ready \
    "pod" \
    -l "job-name=$job_name" \
    "--timeout=$timeout" 2>&1 | tee -a "$LOG_FILE"; then
    log "WAIT job=$job_name pod did not become Ready within $timeout; trying logs anyway"
  fi
}

stream_job_logs() {
  local job_name="$1"
  wait_job_pod_started "$job_name" "10m"
  kubectl logs -n "$NAMESPACE" -f "job/$job_name" 2>&1 | tee -a "$LOG_FILE" || true
}

show_job_failure_hint() {
  local job_name="$1"
  log "FAILED job=$job_name. Last logs:"
  kubectl logs -n "$NAMESPACE" "job/$job_name" --tail=200 2>&1 | tee -a "$LOG_FILE" || true
  log "Inspect with: kubectl describe job $job_name -n $NAMESPACE"
}

patch_configmap() {
  local patch

  if [[ -n "${AOI_IDS:-}" ]]; then
    patch="$(printf '{"data":{"BATCH_ID":"%s","START_DATE":"%s","END_DATE":"%s","DASHBOARD_START_DATE":"%s","SERVING_RELEASE_ID":"%s","AOI_IDS":"%s"}}' \
      "$BATCH_ID" "$START_DATE" "$END_DATE" "$DASHBOARD_START_DATE" "$SERVING_RELEASE_ID" "$AOI_IDS")"
  else
    patch="$(printf '{"data":{"BATCH_ID":"%s","START_DATE":"%s","END_DATE":"%s","DASHBOARD_START_DATE":"%s","SERVING_RELEASE_ID":"%s"}}' \
      "$BATCH_ID" "$START_DATE" "$END_DATE" "$DASHBOARD_START_DATE" "$SERVING_RELEASE_ID")"
  fi

  run_logged kubectl patch configmap "$CONFIGMAP" -n "$NAMESPACE" --type merge -p "$patch"
  run_logged kubectl get configmap "$CONFIGMAP" -n "$NAMESPACE" \
    -o "jsonpath={.data.BATCH_ID}{' '}{.data.START_DATE}{' '}{.data.END_DATE}{' '}{.data.DASHBOARD_START_DATE}{' '}{.data.SERVING_RELEASE_ID}{' '}{.data.AOI_IDS}{'\n'}"
}

run_gold() {
  log "GOLD status=starting batch=$BATCH_ID start=$START_DATE end=$END_DATE"
  run_logged kubectl delete job ocean-gold -n "$NAMESPACE" --ignore-not-found
  run_logged kubectl apply -f "$K8S_DIR/04-gold-job.yaml"
  stream_job_logs ocean-gold

  if ! wait_job ocean-gold "$GOLD_TIMEOUT"; then
    show_job_failure_hint ocean-gold
    exit 1
  fi
  log "GOLD status=success batch=$BATCH_ID"
}

run_serving() {
  log "SERVING status=starting batch=$BATCH_ID release=$SERVING_RELEASE_ID"
  run_logged kubectl delete job ocean-serving-export -n "$NAMESPACE" --ignore-not-found
  run_logged kubectl apply -f "$K8S_DIR/05-serving-job.yaml"
  stream_job_logs ocean-serving-export

  if ! wait_job ocean-serving-export "$SERVING_TIMEOUT"; then
    show_job_failure_hint ocean-serving-export
    exit 1
  fi
  log "SERVING status=success batch=$BATCH_ID"
}

verify_batch() {
  run_logged bash "$SCRIPT_DIR/verify_gold_batch.sh" "$YEAR" "$MONTH"
}

cd "$PROJECT_ROOT"

log "BATCH status=starting batch=$BATCH_ID project_root=$PROJECT_ROOT"
patch_configmap
run_gold
run_serving
verify_batch
if grep -q "GOLD_DEGRADED" "$LOG_FILE"; then
  log "BATCH status=degraded batch=$BATCH_ID log=$LOG_FILE"
else
  log "BATCH status=success batch=$BATCH_ID log=$LOG_FILE"
fi
