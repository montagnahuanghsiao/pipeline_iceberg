#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/spark_common.sh"
: "${HDFS_SERVING_ROOT:?}" "${LOCAL_SERVING_ROOT:?}" "${SERVING_RELEASE_ID:?}"
: "${START_DATE:?}" "${END_DATE:?}" "${MAX_RECORDS_PER_FILE:?}"

echo "SERVING_EXPORT release=${SERVING_RELEASE_ID} status=starting"
"${SPARK_HOME}/bin/spark-submit" "${SPARK_COMMON[@]}" \
  "${SCRIPT_DIR}/../jobs/export_serving.py" \
  --catalog "${ICEBERG_CATALOG}" \
  --namespace "${ICEBERG_NAMESPACE}" \
  --serving-root "${HDFS_SERVING_ROOT}" \
  --start-date "${START_DATE}" \
  --end-date "${END_DATE}" \
  --max-records-per-file "${MAX_RECORDS_PER_FILE}"

case "${LOCAL_SERVING_ROOT}" in
  /opt/zfs/project/data/serving|/opt/zfs/project/data/serving/*) ;;
  *)
    echo "ERROR: unsafe LOCAL_SERVING_ROOT: ${LOCAL_SERVING_ROOT}" >&2
    exit 2
    ;;
esac

release_root="${LOCAL_SERVING_ROOT}/releases"
release_dir="${release_root}/${SERVING_RELEASE_ID}"
staging_dir="${LOCAL_SERVING_ROOT}/.staging_${SERVING_RELEASE_ID}_$$"
mkdir -p "${release_root}"
rm -rf "${staging_dir}"
mkdir -p "${staging_dir}"
trap 'rm -rf "${staging_dir}"' EXIT

"${HADOOP_HOME}/bin/hdfs" dfs -get \
  "${HDFS_SERVING_ROOT}/gold_map_metric" "${staging_dir}/"
"${HADOOP_HOME}/bin/hdfs" dfs -get \
  "${HDFS_SERVING_ROOT}/gold_daily_metric_summary" "${staging_dir}/"

test -n "$(find "${staging_dir}/gold_map_metric" -type f -name '*.parquet' -print -quit)"
test -n "$(find "${staging_dir}/gold_daily_metric_summary" -type f -name '*.parquet' -print -quit)"

rm -rf "${release_dir}"
mv "${staging_dir}" "${release_dir}"
trap - EXIT
ln -sfn "${release_dir}" "${LOCAL_SERVING_ROOT}/current"
echo "SERVING_EXPORT release=${SERVING_RELEASE_ID} current=${release_dir} status=success"
