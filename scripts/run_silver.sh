#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/spark_common.sh"
"${SPARK_HOME}/bin/spark-submit" "${SPARK_COMMON[@]}" \
  "${SCRIPT_DIR}/../jobs/silver.py" \
  --bronze-root "${HDFS_BRONZE_ROOT}" \
  --silver-root "${HDFS_SILVER_ROOT}" \
  --aoi-id "${AOI_ID}" \
  --aoi-config "${SCRIPT_DIR}/../configs/aoi_presets.json" \
  --start-date "${START_DATE}" \
  --end-date "${END_DATE}" \
  --write-shards "${SILVER_WRITE_SHARDS}" \
  --max-records-per-file "${MAX_RECORDS_PER_FILE}"
