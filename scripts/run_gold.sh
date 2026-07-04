#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/spark_common.sh"
: "${AOI_IDS:?}" "${HDFS_SILVER_ROOT:?}" "${ICEBERG_NAMESPACE:?}"
: "${START_DATE:?}" "${END_DATE:?}"
IFS=',' read -r -a aoi_ids <<< "${AOI_IDS}"
for aoi_id in "${aoi_ids[@]}"; do
  echo "GOLD aoi=${aoi_id} status=starting"
  "${SPARK_HOME}/bin/spark-submit" "${SPARK_COMMON[@]}" \
    "${SCRIPT_DIR}/../jobs/gold.py" \
    --silver-root "${HDFS_SILVER_ROOT}" \
    --catalog "${ICEBERG_CATALOG}" \
    --namespace "${ICEBERG_NAMESPACE}" \
    --aoi-id "${aoi_id}" \
    --start-date "${START_DATE}" \
    --end-date "${END_DATE}"
  echo "GOLD aoi=${aoi_id} status=success"
done
