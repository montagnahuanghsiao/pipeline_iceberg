#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/spark_common.sh"
: "${HDFS_SERVING_ROOT:?}" "${LOCAL_SERVING_ROOT:?}" "${SERVING_RELEASE_ID:?}"
: "${START_DATE:?}" "${END_DATE:?}" "${MAX_RECORDS_PER_FILE:?}"

echo "SERVING_EXPORT release=${SERVING_RELEASE_ID} status=starting"
batch_hdfs_root="${HDFS_SERVING_ROOT%/}/batches/${SERVING_RELEASE_ID}"
"${SPARK_HOME}/bin/spark-submit" "${SPARK_COMMON[@]}" \
  "${SCRIPT_DIR}/../jobs/export_serving.py" \
  --catalog "${ICEBERG_CATALOG}" \
  --namespace "${ICEBERG_NAMESPACE}" \
  --serving-root "${batch_hdfs_root}" \
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
batch_dir="${LOCAL_SERVING_ROOT}/.batch_${SERVING_RELEASE_ID}_$$"
mkdir -p "${release_root}"
rm -rf "${staging_dir}"
mkdir -p "${staging_dir}"
rm -rf "${batch_dir}"
mkdir -p "${batch_dir}"
trap 'rm -rf "${staging_dir}" "${batch_dir}"' EXIT

if [[ -d "${LOCAL_SERVING_ROOT}/current" ]]; then
  cp -a "${LOCAL_SERVING_ROOT}/current/." "${staging_dir}/"
fi

"${HADOOP_HOME}/bin/hdfs" dfs -get \
  "${batch_hdfs_root}/gold_map_metric" "${batch_dir}/"
"${HADOOP_HOME}/bin/hdfs" dfs -get \
  "${batch_hdfs_root}/gold_dashboard_daily_metrics" "${batch_dir}/"
"${HADOOP_HOME}/bin/hdfs" dfs -get \
  "${batch_hdfs_root}/gold_dashboard_status_distribution" "${batch_dir}/"

test -n "$(find "${batch_dir}/gold_map_metric" -type f -name '*.parquet' -print -quit)"
test -n "$(find "${batch_dir}/gold_dashboard_daily_metrics" -type f -name '*.parquet' -print -quit)"
test -n "$(find "${batch_dir}/gold_dashboard_status_distribution" -type f -name '*.parquet' -print -quit)"

merge_dataset() {
  local dataset="$1"
  local source_root="${batch_dir}/${dataset}"
  local target_root="${staging_dir}/${dataset}"
  mkdir -p "${target_root}"
  while IFS= read -r -d '' partition_dir; do
    local relative="${partition_dir#${source_root}/}"
    local target_partition="${target_root}/${relative}"
    rm -rf "${target_partition}"
    mkdir -p "$(dirname "${target_partition}")"
    cp -a "${partition_dir}" "${target_partition}"
    echo "SERVING_MERGE dataset=${dataset} partition=${relative} status=merged"
  done < <(find "${source_root}" -type d -name 'resolution_km=*' -print0)
}

merge_dataset "gold_map_metric"
merge_dataset "gold_dashboard_daily_metrics"
merge_dataset "gold_dashboard_status_distribution"

test -n "$(find "${staging_dir}/gold_map_metric" -type f -name '*.parquet' -print -quit)"
test -n "$(find "${staging_dir}/gold_dashboard_daily_metrics" -type f -name '*.parquet' -print -quit)"
test -n "$(find "${staging_dir}/gold_dashboard_status_distribution" -type f -name '*.parquet' -print -quit)"

rm -rf "${release_dir}"
mv "${staging_dir}" "${release_dir}"
trap - EXIT
ln -sfn "${release_dir}" "${LOCAL_SERVING_ROOT}/current"
rm -rf "${batch_dir}"
echo "SERVING_EXPORT release=${SERVING_RELEASE_ID} batch_hdfs=${batch_hdfs_root} current=${release_dir} status=success"
