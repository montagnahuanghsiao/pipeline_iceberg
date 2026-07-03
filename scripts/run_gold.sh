#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/spark_common.sh"
"${SPARK_HOME}/bin/spark-submit" "${SPARK_COMMON[@]}" \
  "${SCRIPT_DIR}/../jobs/gold.py" \
  --silver-root "${HDFS_SILVER_ROOT}" \
  --catalog "${ICEBERG_CATALOG}" \
  --namespace "${ICEBERG_NAMESPACE}" \
  --aoi-id "${AOI_ID}" \
  --start-date "${START_DATE}" \
  --end-date "${END_DATE}"
