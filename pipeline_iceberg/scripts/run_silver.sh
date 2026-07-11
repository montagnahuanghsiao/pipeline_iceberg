#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/spark_common.sh"
: "${AOI_IDS:?}" "${HDFS_BRONZE_ROOT:?}" "${HDFS_SILVER_ROOT:?}"
: "${HDFS_METADATA_ROOT:?}"
: "${START_DATE:?}" "${END_DATE:?}" "${SILVER_WRITE_SHARDS:?}" "${MAX_RECORDS_PER_FILE:?}"
RUN_ID="${PIPELINE_RUN_ID:-silver_$(date -u +%Y%m%dT%H%M%SZ)}"
SILVER_STRICT_QUALITY="${SILVER_STRICT_QUALITY:-true}"
IFS=',' read -r -a aoi_ids <<< "${AOI_IDS}"
for aoi_id in "${aoi_ids[@]}"; do
  aoi_id="$(echo "${aoi_id}" | xargs)"
  if [[ -z "${aoi_id}" ]]; then
    continue
  fi
  echo "SILVER run_id=${RUN_ID} aoi=${aoi_id} status=starting"
  "${SPARK_HOME}/bin/spark-submit" "${SPARK_COMMON[@]}" \
    "${SCRIPT_DIR}/../jobs/silver.py" \
    --bronze-root "${HDFS_BRONZE_ROOT}" \
    --silver-root "${HDFS_SILVER_ROOT}" \
    --aoi-id "${aoi_id}" \
    --aoi-config "${SCRIPT_DIR}/../configs/aoi_presets.json" \
    --start-date "${START_DATE}" \
    --end-date "${END_DATE}" \
    --metadata-root "${HDFS_METADATA_ROOT}" \
    --run-id "${RUN_ID}" \
    --write-shards "${SILVER_WRITE_SHARDS}" \
    --max-records-per-file "${MAX_RECORDS_PER_FILE}" \
    --strict-quality "${SILVER_STRICT_QUALITY}"
  echo "SILVER run_id=${RUN_ID} aoi=${aoi_id} status=success"
done
