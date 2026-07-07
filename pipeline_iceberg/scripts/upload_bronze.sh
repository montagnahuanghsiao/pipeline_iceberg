#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/load_runtime.sh"
: "${LOCAL_BRONZE_ROOT:?}" "${HDFS_BRONZE_ROOT:?}" "${HDFS_METADATA_ROOT:?}"
: "${HADOOP_HOME:?}" "${START_DATE:?}" "${END_DATE:?}"

HDFS="${HADOOP_HOME}/bin/hdfs"
if ! start_epoch="$(date -u -d "${START_DATE}" +%s 2>/dev/null)" \
  || ! end_epoch="$(date -u -d "${END_DATE}" +%s 2>/dev/null)"; then
  echo "ERROR: START_DATE and END_DATE must use YYYY-MM-DD." >&2
  exit 2
fi
if (( end_epoch < start_epoch )); then
  echo "ERROR: END_DATE must not precede START_DATE." >&2
  exit 2
fi

run_id="bronze_upload_${BATCH_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
manifest="$(mktemp)"
trap 'rm -f "${manifest}"' EXIT
printf 'run_id\tproduct\tevent_date\tlocal_file\thdfs_file\tbytes\tsha256\n' > "${manifest}"

"${HDFS}" dfs -mkdir -p "${HDFS_BRONZE_ROOT}" "${HDFS_METADATA_ROOT}/bronze"
total_files=0
for product in CHL NFLH POC SST NSST SST4 GFW; do
  product_root="${LOCAL_BRONZE_ROOT}/${product}"
  if [[ ! -d "${product_root}" ]]; then
    echo "ERROR: Bronze product directory not found: ${product_root}" >&2
    exit 3
  fi

  product_files=0
  current_epoch="${start_epoch}"
  while (( current_epoch <= end_epoch )); do
    day="$(date -u -d "@${current_epoch}" +%F)"
    compact_day="${day//-/}"
    year="${day:0:4}"
    month="${day:5:2}"
    source_dir="${product_root}/year=${year}/month=${month}"
    target_dir="${HDFS_BRONZE_ROOT}/${product}/year=${year}/month=${month}"
    "${HDFS}" dfs -mkdir -p "${target_dir}"

    if [[ ! -d "${source_dir}" ]]; then
      echo "ERROR: Bronze month directory not found: ${source_dir}" >&2
      exit 4
    fi

    mapfile -d '' day_files < <(
      find "${source_dir}" -maxdepth 1 -type f \
        \( -name "*${compact_day}*.parquet" -o -name "*${day}*.parquet" \) \
        -print0
    )
    if (( ${#day_files[@]} != 1 )); then
      echo "ERROR: expected exactly one ${product} file for ${day}; found ${#day_files[@]}" >&2
      printf '  %s\n' "${day_files[@]:-<none>}" >&2
      exit 4
    fi

    file="${day_files[0]}"
    name="$(basename "${file}")"
    target="${target_dir}/${name}"
    bytes="$(stat -c %s "${file}")"
    sha256="$(sha256sum "${file}" | awk '{print $1}')"
    "${HDFS}" dfs -put -f "${file}" "${target}"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "${run_id}" "${product}" "${day}" "${file}" "${target}" "${bytes}" "${sha256}" >> "${manifest}"
    product_files=$((product_files + 1))
    total_files=$((total_files + 1))
    echo "UPLOAD product=${product} date=${day} file=${name} status=success"
    current_epoch=$((current_epoch + 86400))
  done

  echo "PRODUCT product=${product} files=${product_files} status=complete"
done

"${HDFS}" dfs -put -f "${manifest}" "${HDFS_METADATA_ROOT}/bronze/${run_id}.tsv"
echo "COMPLETE run_id=${run_id} files=${total_files} manifest=${HDFS_METADATA_ROOT}/bronze/${run_id}.tsv"
