#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/spark_common.sh"
: "${AOI_IDS:?}" "${HDFS_SILVER_ROOT:?}" "${ICEBERG_NAMESPACE:?}"
: "${START_DATE:?}" "${END_DATE:?}"
FILL_WINDOW_DAYS="${FILL_WINDOW_DAYS:-5}"
DASHBOARD_START_DATE="${DASHBOARD_START_DATE:-2024-01-01}"
IFS=',' read -r -a aoi_ids <<< "${AOI_IDS}"
for aoi_id in "${aoi_ids[@]}"; do
  aoi_id="$(echo "${aoi_id}" | xargs)"
  if [[ -z "${aoi_id}" ]]; then
    continue
  fi
  echo "GOLD aoi=${aoi_id} status=starting"
  "${SPARK_HOME}/bin/spark-submit" "${SPARK_COMMON[@]}" \
    "${SCRIPT_DIR}/../jobs/gold.py" \
    --silver-root "${HDFS_SILVER_ROOT}" \
    --catalog "${ICEBERG_CATALOG}" \
    --namespace "${ICEBERG_NAMESPACE}" \
    --aoi-id "${aoi_id}" \
    --aoi-config "${SCRIPT_DIR}/../configs/aoi_presets.json" \
    --start-date "${START_DATE}" \
    --end-date "${END_DATE}" \
    --fill-window-days "${FILL_WINDOW_DAYS}"
  echo "GOLD aoi=${aoi_id} status=success"
done
echo "GOLD_DASHBOARD status=starting start=${DASHBOARD_START_DATE} end=${END_DATE}"
"${SPARK_HOME}/bin/spark-submit" "${SPARK_COMMON[@]}" \
  "${SCRIPT_DIR}/../jobs/gold_dashboard.py" \
  --catalog "${ICEBERG_CATALOG}" \
  --namespace "${ICEBERG_NAMESPACE}" \
  --start-date "${DASHBOARD_START_DATE}" \
  --end-date "${END_DATE}"
echo "GOLD_DASHBOARD status=success"
