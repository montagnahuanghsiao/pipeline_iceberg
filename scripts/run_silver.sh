#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/spark_common.sh"
: "${AOI_IDS:?}" "${HDFS_BRONZE_ROOT:?}" "${HDFS_SILVER_ROOT:?}"
: "${START_DATE:?}" "${END_DATE:?}" "${SILVER_WRITE_SHARDS:?}" "${MAX_RECORDS_PER_FILE:?}"
IFS=',' read -r -a aoi_ids <<< "${AOI_IDS}"
for aoi_id in "${aoi_ids[@]}"; do
  echo "SILVER aoi=${aoi_id} status=starting"
  "${SPARK_HOME}/bin/spark-submit" "${SPARK_COMMON[@]}" \
    "${SCRIPT_DIR}/../jobs/silver.py" \
    --bronze-root "${HDFS_BRONZE_ROOT}" \
    --silver-root "${HDFS_SILVER_ROOT}" \
    --aoi-id "${aoi_id}" \
    --aoi-config "${SCRIPT_DIR}/../configs/aoi_presets.json" \
    --start-date "${START_DATE}" \
    --end-date "${END_DATE}" \
    --write-shards "${SILVER_WRITE_SHARDS}" \
    --max-records-per-file "${MAX_RECORDS_PER_FILE}"
  echo "SILVER aoi=${aoi_id} status=success"
done
