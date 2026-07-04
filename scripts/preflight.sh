#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/load_runtime.sh"
: "${LOCAL_BRONZE_ROOT:?}" "${HADOOP_HOME:?}" "${SPARK_HOME:?}" "${ICEBERG_JAR:?}"
: "${HDFS_BRONZE_ROOT:?}" "${HDFS_SILVER_ROOT:?}" "${HDFS_SERVING_ROOT:?}"
: "${HDFS_METADATA_ROOT:?}" "${ICEBERG_WAREHOUSE:?}"

for command in \
  "${HADOOP_HOME}/bin/hdfs" \
  "${HADOOP_HOME}/bin/yarn" \
  "${SPARK_HOME}/bin/spark-submit"; do
  if [[ ! -x "${command}" ]]; then
    echo "ERROR: executable not found: ${command}" >&2
    exit 2
  fi
done
if [[ ! -f "${ICEBERG_JAR}" ]]; then
  echo "ERROR: Iceberg runtime JAR not found: ${ICEBERG_JAR}" >&2
  exit 2
fi
for product in CHL NFLH POC SST NSST SST4 GFW; do
  count="$(find "${LOCAL_BRONZE_ROOT}/${product}" -type f -name '*.parquet' | wc -l)"
  if (( count == 0 )); then
    echo "ERROR: no Parquet files found for ${product}" >&2
    exit 3
  fi
  echo "BRONZE product=${product} parquet_files=${count}"
done

"${HADOOP_HOME}/bin/hdfs" dfs -ls /
"${HADOOP_HOME}/bin/yarn" node -list -all
"${HADOOP_HOME}/bin/hdfs" dfs -mkdir -p \
  "${HDFS_BRONZE_ROOT}" \
  "${HDFS_SILVER_ROOT}" \
  "${HDFS_SERVING_ROOT}" \
  "${HDFS_METADATA_ROOT}" \
  "${ICEBERG_WAREHOUSE}"
probe="${HDFS_METADATA_ROOT}/.write_probe_$(date -u +%Y%m%dT%H%M%SZ)_$$"
"${HADOOP_HOME}/bin/hdfs" dfs -touchz "${probe}"
"${HADOOP_HOME}/bin/hdfs" dfs -rm "${probe}"

echo "PREFLIGHT status=success"
