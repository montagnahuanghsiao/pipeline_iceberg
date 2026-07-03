#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/spark_common.sh"
"${SPARK_HOME}/bin/spark-submit" "${SPARK_COMMON[@]}" \
  "${SCRIPT_DIR}/../jobs/maintenance.py" \
  --catalog "${ICEBERG_CATALOG}" \
  --namespace "${ICEBERG_NAMESPACE}" \
  --retain-hours "${ICEBERG_RETAIN_HOURS:-168}"
