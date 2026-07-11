#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  bash pipeline_iceberg/ops/verify_gold_batch.sh YEAR MONTH

Example:
  bash pipeline_iceberg/ops/verify_gold_batch.sh 2020 03
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
MONTH="$(printf '%02d' "$((10#$2))")"
BATCH_ID="${YEAR}_${MONTH}"

WAREHOUSE_ROOT="${WAREHOUSE_ROOT:-/dataset/ocean/warehouse}"
ICEBERG_DB_PATH="${ICEBERG_DB_PATH:-/dataset/ocean/warehouse/ocean}"
SERVING_BATCH_ROOT="${SERVING_BATCH_ROOT:-/dataset/ocean/serving/batches/$BATCH_ID}"

log() {
  printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

require_hdfs_path() {
  local path="$1"
  if hdfs dfs -test -e "$path"; then
    log "VERIFY exists path=$path"
  else
    log "VERIFY failed missing path=$path"
    exit 1
  fi
}

count_hdfs_path() {
  local path="$1"
  require_hdfs_path "$path"
  hdfs dfs -count -h "$path"
}

log "VERIFY status=starting batch=$BATCH_ID"

require_hdfs_path "$ICEBERG_DB_PATH"
hdfs dfs -ls "$ICEBERG_DB_PATH"

log "VERIFY iceberg metadata sample"
hdfs dfs -find "$WAREHOUSE_ROOT" -path '*/metadata/*.metadata.json' | head || true

log "VERIFY serving batch outputs"
count_hdfs_path "$SERVING_BATCH_ROOT/gold_map_metric"
count_hdfs_path "$SERVING_BATCH_ROOT/gold_dashboard_daily_metrics"
count_hdfs_path "$SERVING_BATCH_ROOT/gold_dashboard_status_distribution"

log "VERIFY status=success batch=$BATCH_ID"
