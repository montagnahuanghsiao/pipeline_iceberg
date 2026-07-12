#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  bash pipeline_iceberg/ops/run_gold_monthly.sh YEAR START_MONTH END_MONTH

Example:
  bash pipeline_iceberg/ops/run_gold_monthly.sh 2020 01 12
  bash pipeline_iceberg/ops/run_gold_monthly.sh 2020 03 06

Optional environment variables are passed to run_gold_batch.sh:
  NAMESPACE=dt
  CONFIGMAP=ocean-pipeline-config
  LOG_ROOT=/opt/zfs/project/logs
  AOI_IDS=taiwan,northwest_pacific
  GOLD_TIMEOUT=12h
  SERVING_TIMEOUT=6h
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -ne 3 ]]; then
  usage
  exit 2
fi

YEAR="$1"
START_MONTH="$2"
END_MONTH="$3"

if ! [[ "$YEAR" =~ ^[0-9]{4}$ ]]; then
  echo "ERROR invalid YEAR: $YEAR" >&2
  exit 2
fi

START_MONTH="$(printf '%02d' "$((10#$START_MONTH))")"
END_MONTH="$(printf '%02d' "$((10#$END_MONTH))")"

if (( 10#$START_MONTH < 1 || 10#$START_MONTH > 12 || 10#$END_MONTH < 1 || 10#$END_MONTH > 12 )); then
  echo "ERROR month range must be 01..12" >&2
  exit 2
fi

if (( 10#$START_MONTH > 10#$END_MONTH )); then
  echo "ERROR START_MONTH must be <= END_MONTH" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_ROOT="${LOG_ROOT:-/opt/zfs/project/logs}"
mkdir -p "$LOG_ROOT"

RUN_TS="$(date -u +%Y%m%dT%H%M%SZ)"
LOG_FILE="$LOG_ROOT/gold_monthly_${YEAR}_${START_MONTH}_${END_MONTH}_${RUN_TS}.log"

log() {
  printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" | tee -a "$LOG_FILE"
}

log "MONTHLY status=starting year=$YEAR start_month=$START_MONTH end_month=$END_MONTH"

for month in $(seq -w "$((10#$START_MONTH))" "$((10#$END_MONTH))"); do
  month="$(printf '%02d' "$((10#$month))")"
  batch_id="${YEAR}_${month}"
  log "MONTHLY batch=$batch_id status=starting"

  if bash "$SCRIPT_DIR/run_gold_batch.sh" "$YEAR" "$month" 2>&1 | tee -a "$LOG_FILE"; then
    if grep -q "BATCH status=degraded batch=$batch_id" "$LOG_FILE"; then
      log "MONTHLY batch=$batch_id status=degraded"
    else
      log "MONTHLY batch=$batch_id status=success"
    fi
  else
    log "MONTHLY batch=$batch_id status=failed"
    log "MONTHLY stopped. Resume after fixing with: bash pipeline_iceberg/ops/run_gold_monthly.sh $YEAR $month $END_MONTH"
    exit 1
  fi
done

log "MONTHLY status=success year=$YEAR start_month=$START_MONTH end_month=$END_MONTH log=$LOG_FILE"
