# 前端 Serving 資料跨電腦搬移與合併流程

## 1. 適用情境

兩臺電腦具有相同 Pipeline、API、前端及 Kubernetes 環境，但分別處理不同年份。完成 Gold 與 Serving 後，需要把其中一臺電腦產出的「前端所需資料」搬到主要展示機，與展示機既有年份合併。

本流程不搬移 Docker／Podman 映像、前端原始碼、Spark 環境、Iceberg warehouse 或完整 HDFS，只搬移 Flask API 實際查詢的 Serving Parquet。

## 2. 前端實際需要的三個資料集

Flask API 從 `/opt/zfs/project/data/serving/current` 讀取：

| Serving 資料集                       | 前端用途                             |
| ------------------------------------ | ------------------------------------ |
| `gold_map_metric`                    | 地圖網格、熱力圖、指標數值與顯示等級 |
| `gold_dashboard_daily_metrics`       | KPI、摘要卡與趨勢圖                  |
| `gold_dashboard_status_distribution` | 狀態分布圓餅圖與分類統計             |

其分區結構為：

```text
資料集/
  event_date=YYYY-MM-DD/
    aoi_id=.../
      resolution_km=.../
        *.parquet
```

因此可以依年份搬移，並以完整的 `resolution_km=*` 分區為最小覆蓋單位。

## 3. 來源機：先確認 Serving 資料

以下以搬移 `2024` 年為例：

```bash
YEAR=2024
SERVING_ROOT=/opt/zfs/project/data/serving
CURRENT=$(readlink -f "${SERVING_ROOT}/current")

echo "CURRENT=${CURRENT}"
test -d "${CURRENT}" || {
  echo "ERROR: serving current 不存在"
  exit 1
}
```

確認三個資料集都有該年份分區：

```bash
for dataset in \
  gold_map_metric \
  gold_dashboard_daily_metrics \
  gold_dashboard_status_distribution; do

  count=$(find "${CURRENT}/${dataset}" \
    -type d \
    -name "event_date=${YEAR}-*" |
    wc -l)

  echo "dataset=${dataset} year=${YEAR} date_partitions=${count}"
done
```

如果某個資料集結果為 `0`，先不要打包，應先確認該年份是否已成功執行 Serving Export。

## 4. 來源機：只打包指定年份的前端資料

這是主要打包指令。它只包含三個 Serving 資料集內指定年份的日期分區：

```bash
set -euo pipefail

YEAR=2024
SERVING_ROOT=/opt/zfs/project/data/serving
CURRENT=$(readlink -f "${SERVING_ROOT}/current")
TRANSFER_DIR=/opt/zfs/project/data/serving-transfer
BUNDLE="${TRANSFER_DIR}/ocean-serving-${YEAR}-$(date -u +%Y%m%dT%H%M%SZ).tar.gz"

mkdir -p "${TRANSFER_DIR}"
test -d "${CURRENT}"

cd "${CURRENT}"

find \
  gold_map_metric \
  gold_dashboard_daily_metrics \
  gold_dashboard_status_distribution \
  -type d \
  -name "event_date=${YEAR}-*" \
  -print0 |
tar \
  --null \
  --files-from=- \
  --create \
  --gzip \
  --file="${BUNDLE}"

test -s "${BUNDLE}"
sha256sum "${BUNDLE}" > "${BUNDLE}.sha256"

echo "BUNDLE=${BUNDLE}"
ls -lh "${BUNDLE}" "${BUNDLE}.sha256"
```

這個壓縮檔會保留：

```text
gold_map_metric/event_date=2024-.../...
gold_dashboard_daily_metrics/event_date=2024-.../...
gold_dashboard_status_distribution/event_date=2024-.../...
```

不會包含其他年份，也不會包含 `releases` 或 `current` symlink。

## 5. 來源機：驗證壓縮檔

設定剛才輸出的實際檔名：

```bash
BUNDLE=/opt/zfs/project/data/serving-transfer/ocean-serving-2024-實際時間戳記.tar.gz
```

確認壓縮檔只含指定年份：

```bash
tar -tzf "${BUNDLE}" | head -30

if tar -tzf "${BUNDLE}" |
  grep -E 'event_date=' |
  grep -v "event_date=2024-"; then
  echo "ERROR: 壓縮檔混入其他年份"
  exit 1
else
  echo "YEAR_CHECK=OK"
fi
```

統計三個資料集的 Parquet 數量：

```bash
for dataset in \
  gold_map_metric \
  gold_dashboard_daily_metrics \
  gold_dashboard_status_distribution; do

  count=$(tar -tzf "${BUNDLE}" |
    grep -c "^${dataset}/.*\.parquet$")

  echo "dataset=${dataset} parquet_files=${count}"
done
```

三個結果都必須大於 `0`。

## 6. 搬移到主要展示機

使用 SCP：

```bash
scp \
  "${BUNDLE}" \
  "${BUNDLE}.sha256" \
  TARGET_USER@TARGET_IP:/opt/zfs/project/data/serving-transfer/
```

如果 SSH 使用自訂連接埠，例如 `22101`：

```bash
scp -P 22101 \
  "${BUNDLE}" \
  "${BUNDLE}.sha256" \
  TARGET_USER@TARGET_IP:/opt/zfs/project/data/serving-transfer/
```

也可以使用共用資料夾或隨身碟，但 `.tar.gz` 與 `.sha256` 必須一起搬移。

## 7. 展示機：檢查收到的檔案

```bash
cd /opt/zfs/project/data/serving-transfer

BUNDLE=ocean-serving-2024-實際時間戳記.tar.gz

sha256sum -c "${BUNDLE}.sha256"
```

結果必須為：

```text
ocean-serving-2024-....tar.gz: OK
```

確認內容：

```bash
tar -tzf "${BUNDLE}" | head -30
```

## 8. 展示機：安全合併至新的 Serving release

不要直接把壓縮檔解到 `current`，也不要刪除既有 release。

以下流程會：

1. 複製展示機目前的完整 `current`。
2. 解壓縮外來年份至暫存目錄。
3. 依日期、AOI、解析度分區合併。
4. 若有同一分區，使用搬入版本完整覆蓋該分區。
5. 驗證三個資料集後建立新 release。
6. 最後才原子切換 `current` symlink。

執行：

```bash
set -euo pipefail

SERVING_ROOT=/opt/zfs/project/data/serving
TRANSFER_DIR=/opt/zfs/project/data/serving-transfer
BUNDLE=ocean-serving-2020-20260712T105843Z.tar.gz
RELEASE_ID="combined-$(date -u +%Y%m%dT%H%M%SZ)"

CURRENT=$(readlink -f "${SERVING_ROOT}/current")
RELEASE_DIR="${SERVING_ROOT}/releases/${RELEASE_ID}"
STAGING_DIR="${SERVING_ROOT}/.staging_${RELEASE_ID}"
IMPORT_DIR="${SERVING_ROOT}/.import_${RELEASE_ID}"

test -d "${CURRENT}"
test -f "${TRANSFER_DIR}/${BUNDLE}"
test ! -e "${RELEASE_DIR}"

mkdir -p "${STAGING_DIR}" "${IMPORT_DIR}"
trap 'rm -rf "${STAGING_DIR}" "${IMPORT_DIR}"' EXIT

# 保留展示機既有的所有年份。
cp -a "${CURRENT}/." "${STAGING_DIR}/"

# 外來資料先解到獨立暫存目錄。
tar -xzf "${TRANSFER_DIR}/${BUNDLE}" \
  -C "${IMPORT_DIR}"

merge_dataset() {
  local dataset="$1"
  local source_root="${IMPORT_DIR}/${dataset}"
  local target_root="${STAGING_DIR}/${dataset}"

  test -d "${source_root}"
  mkdir -p "${target_root}"

  while IFS= read -r -d '' partition_dir; do
    relative="${partition_dir#${source_root}/}"
    target_partition="${target_root}/${relative}"

    rm -rf "${target_partition}"
    mkdir -p "$(dirname "${target_partition}")"
    cp -a "${partition_dir}" "${target_partition}"

    echo "MERGE dataset=${dataset} partition=${relative} status=success"
  done < <(
    find "${source_root}" \
      -type d \
      -name 'resolution_km=*' \
      -print0
  )
}

merge_dataset gold_map_metric
merge_dataset gold_dashboard_daily_metrics
merge_dataset gold_dashboard_status_distribution

# 基本驗證：三個合併後資料集都必須含有 Parquet。
for dataset in \
  gold_map_metric \
  gold_dashboard_daily_metrics \
  gold_dashboard_status_distribution; do

  test -n "$(find "${STAGING_DIR}/${dataset}" \
    -type f \
    -name '*.parquet' \
    -print -quit)"

  echo "VERIFY dataset=${dataset} status=success"
done

mv "${STAGING_DIR}" "${RELEASE_DIR}"
ln -sfn "${RELEASE_DIR}" "${SERVING_ROOT}/current"

rm -rf "${IMPORT_DIR}"
trap - EXIT

echo "CURRENT=$(readlink -f "${SERVING_ROOT}/current")"
echo "MERGE_RELEASE=${RELEASE_DIR} status=success"
```

### 合併規則

如果兩臺電腦處理不同年份，不會發生衝突，展示機既有年份與搬入年份會同時保留。

如果兩邊包含相同的：

```text
event_date + aoi_id + resolution_km
```

搬入壓縮檔中的該完整分區會覆蓋展示機原分區。這可以避免把兩組 Parquet 疊在一起造成 API 重複列。

## 9. 合併後驗證年份與數量

因為 `current` 是 symlink，`find` 必須加上 `-L` 才會跟隨連結：

```bash
CURRENT=/opt/zfs/project/data/serving/current

readlink -f "${CURRENT}"

for dataset in \
  gold_map_metric \
  gold_dashboard_daily_metrics \
  gold_dashboard_status_distribution; do

  echo "=== ${dataset} ==="

  find -L "${CURRENT}/${dataset}" \
    -type d \
    -name 'event_date=*' \
    -printf '%f\n' |
    sort -u |
    awk -F= '{print substr($2,1,4)}' |
    sort |
    uniq -c

  parquet_count=$(find -L "${CURRENT}/${dataset}" \
    -type f \
    -name '*.parquet' |
    wc -l)

  echo "parquet_files=${parquet_count}"
done
```

確認容量：

```bash
du -shL /opt/zfs/project/data/serving/current
```

## 10. API 與前端驗證

API 每次查詢會從 `LOCAL_SERVING_CURRENT` 指向的 `current` 讀取 Parquet。只切換資料 symlink 時，通常不需要重建前端或 API 映像。

先確認 API 健康狀態：

```bash
curl -sS http://127.0.0.1:30800/healthz
```

依實際存在日期測試 availability：

```bash
curl -sS \
  'http://127.0.0.1:30800/api/v1/availability?aoi=taiwan&product=CHL&metric=chlor_a&resolution=4'
```

測試搬入年份的地圖 API：

```bash
curl -i \
  'http://127.0.0.1:30800/api/v1/gold/daily-grid?date=2024-01-01&aoi=taiwan&product=CHL&metric=chlor_a&resolution=4'
```

測試趨勢 API：

```bash
curl -i \
  'http://127.0.0.1:30800/api/v1/gold/trend?date=2024-01-15&aoi=taiwan&product=CHL&metric=chlor_a&resolution=4&trend_window_days=30'
```

如果 API Pod 因掛載或程序狀態沒有看到新 `current`，再執行：

```bash
kubectl rollout restart -n dt deployment/ocean-flask-api
kubectl rollout status -n dt deployment/ocean-flask-api --timeout=5m
```

不需要重新執行 Gold，也不需要重新建置前端映像。

## 11. 發生問題時回復上一版

合併流程不會刪除展示機原本 release。先查看：

```bash
ls -lah /opt/zfs/project/data/serving/releases
readlink -f /opt/zfs/project/data/serving/current
```

將 `原本release完整路徑` 換成切換前 `readlink -f` 顯示的路徑：

```bash
ln -sfn \
  /opt/zfs/project/data/serving/releases/原本release \
  /opt/zfs/project/data/serving/current
```

再次確認：

```bash
readlink -f /opt/zfs/project/data/serving/current
curl -sS http://127.0.0.1:30800/healthz
```

## 12. 不應採用的搬移方式

不要直接執行：

```bash
cp -a 另一臺電腦的current /opt/zfs/project/data/serving/current
```

也不要直接把壓縮檔解進：

```text
/opt/zfs/project/data/serving/current
```

原因是：

- `current` 是 symlink，不是一般資料夾。
- 直接覆蓋可能使展示機原有年份消失。
- 相同分區若只是追加 Parquet，可能造成重複資料。
- 搬移中斷時，API 可能讀到不完整資料。

正確方式是先建立完整 staging、驗證，再切換到新的 versioned release。
