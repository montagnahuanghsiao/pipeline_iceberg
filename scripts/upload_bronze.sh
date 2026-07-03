#!/usr/bin/env bash
set -euo pipefail
source "${RUNTIME_ENV:-$(dirname "$0")/../configs/runtime.env}"
: "${LOCAL_BRONZE_ROOT:?}" "${HDFS_BRONZE_ROOT:?}" "${HADOOP_HOME:?}"
"${HADOOP_HOME}/bin/hdfs" dfs -mkdir -p "${HDFS_BRONZE_ROOT}"
for product in CHL NFLH POC SST NSST SST4 GFW; do
  test -d "${LOCAL_BRONZE_ROOT}/${product}"
  "${HADOOP_HOME}/bin/hdfs" dfs -mkdir -p "${HDFS_BRONZE_ROOT}/${product}"
  "${HADOOP_HOME}/bin/hdfs" dfs -put -f "${LOCAL_BRONZE_ROOT}/${product}"/* "${HDFS_BRONZE_ROOT}/${product}/"
done
