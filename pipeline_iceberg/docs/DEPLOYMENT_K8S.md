# OceanGrid pipeline_iceberg Kubernetes 部署 SOP

本文件記錄目前在 tkdt / Kubernetes 環境部署 `pipeline_iceberg` 的實際流程。

目前資料流：

```text
本機 Bronze Parquet
  -> HDFS Bronze
  -> Spark on YARN Silver
  -> Spark on YARN Gold Iceberg
  -> Spark Serving Export
  -> 本機 Serving Parquet Snapshot
  -> Flask API
  -> Nginx Frontend
  -> Windows browser via SSH tunnel
```

目前不使用 Trino，也不使用 Hive Metastore。Gold 層使用 Iceberg HadoopCatalog：

```text
catalog: lake
namespace: ocean
warehouse: hdfs:///dataset/ocean/warehouse

tables:
  lake.ocean.gold_daily_grid_features
  lake.ocean.gold_map_metric
  lake.ocean.gold_dashboard_daily_metrics
  lake.ocean.gold_dashboard_status_distribution
```

舊的 `lake.ocean.gold_daily_metric_summary` 已淘汰，不再產出，也不再作為前端 dashboard 的資料來源。

## 1. 環境與路徑

目前假設：

```text
Kubernetes namespace: dt
專案目錄: /opt/zfs/project
本機 Bronze: /opt/zfs/project/data/bronze
本機 Serving current: /opt/zfs/project/data/serving/current
ConfigMap: pipeline_iceberg/deploy/kubernetes/00-configmap.yaml
ConfigMap name: ocean-pipeline-config
```

HDFS / Iceberg 主要路徑：

```text
HDFS_BRONZE_ROOT=hdfs:///raw/ocean/bronze
HDFS_SILVER_ROOT=hdfs:///elt/ocean/silver
HDFS_SERVING_ROOT=hdfs:///dataset/ocean/serving
HDFS_METADATA_ROOT=hdfs:///metadata/ocean
ICEBERG_WAREHOUSE=hdfs:///dataset/ocean/warehouse
ICEBERG_CATALOG=lake
ICEBERG_NAMESPACE=ocean
```

Bronze 上傳後會依產品與月份放在：

```text
/raw/ocean/bronze/<PRODUCT>/year=YYYY/month=MM
```

例如：

```text
/raw/ocean/bronze/CHL/year=2020/month=01
/raw/ocean/bronze/GFW/year=2020/month=01
```

## 2. 每次 batch 必改設定

每次換月份或換批次，主要修改 `00-configmap.yaml`：

```yaml
BATCH_ID: "2020_01"
START_DATE: "2020-01-01"
END_DATE: "2020-01-31"
AOI_IDS: taiwan,northwest_pacific
SERVING_RELEASE_ID: "2020_01"
DASHBOARD_START_DATE: "2020-01-01"
FILL_WINDOW_DAYS: "5"
```

欄位說明：

| 欄位                   | 何時改               | 說明                                                  |
| ---------------------- | -------------------- | ----------------------------------------------------- |
| `BATCH_ID`             | 每次 batch           | 批次識別碼，建議用 `YYYY_MM`                          |
| `START_DATE`           | 每次 batch           | 本次處理開始日期                                      |
| `END_DATE`             | 每次 batch           | 本次處理結束日期                                      |
| `AOI_IDS`              | 視需求               | 目前常用 `taiwan,northwest_pacific`                   |
| `SERVING_RELEASE_ID`   | 每次 batch           | Serving release 名稱，通常與 `BATCH_ID` 相同          |
| `DASHBOARD_START_DATE` | dashboard 範圍改變時 | Dashboard 指標表起算日期，現階段建議保留 `2020-01-01` |
| `FILL_WINDOW_DAYS`     | 補值策略調整時       | Gold 網格補值回看天數，目前為 `5`，降低計算壓力       |

月批次範例：

```yaml
BATCH_ID: "2020_02"
START_DATE: "2020-02-01"
END_DATE: "2020-02-29"
AOI_IDS: taiwan,northwest_pacific
SERVING_RELEASE_ID: "2020_02"
DASHBOARD_START_DATE: "2020-01-01"
FILL_WINDOW_DAYS: "5"
```

## 3. 什麼時候需要重建 image

只跑下一個月份 batch 時，通常只需要改 ConfigMap 並重跑 Job，不一定要重建 image。

| 變更內容                                          | 是否重建 image | 需要處理                                                                |
| ------------------------------------------------- | -------------: | ----------------------------------------------------------------------- |
| 只改 `BATCH_ID` / 日期 / AOI / `FILL_WINDOW_DAYS` |             否 | `kubectl apply` ConfigMap，重跑 upload / silver / gold / serving Job    |
| 改 `pipeline_iceberg/jobs/*.py`                   |             是 | 重建並 push `ocean-spark-client:3.5.8`                                  |
| 改 `pipeline_iceberg/scripts/*.sh`                |             是 | 重建並 push `ocean-spark-client:3.5.8`                                  |
| 改 Flask API                                      |             是 | 重建並 push `ocean-flask-api:0.5.0`                                     |
| 改前端 HTML / CSS / JS                            |             是 | 重建並 push `ocean-frontend:0.5.0`，建議 `sudo podman build --no-cache` |
| 只改 Kubernetes YAML image tag / env / resource   |             否 | `kubectl apply` 對應 YAML                                               |

## 4. 建立與推送映像

在可以執行 sudo podman 並推送私有 Registry 的機器：

```bash
cd /opt/zfs/project

sudo podman build \
  -f pipeline_iceberg/deploy/docker/Dockerfile.spark-client \
  -t dkreg.taroko:5000/ocean-spark-client:3.5.8 \
  pipeline_iceberg

sudo podman push --creds bigred:bigred --tls-verify=false dkreg.taroko:5000/ocean-spark-client:3.5.8

sudo podman build \
  -f pipeline_iceberg/deploy/docker/Dockerfile.api \
  -t dkreg.taroko:5000/ocean-flask-api:0.5.0 \
  pipeline_iceberg

sudo podman push --creds bigred:bigred --tls-verify=false dkreg.taroko:5000/ocean-flask-api:0.5.0

sudo podman build --no-cache \
  -f pipeline_iceberg/deploy/docker/Dockerfile.frontend \
  -t dkreg.taroko:5000/ocean-frontend:0.5.0 \
  .

sudo podman push --creds bigred:bigred --tls-verify=false dkreg.taroko:5000/ocean-frontend:0.5.0
```

注意：

- `sudo podman push` 需要保留 `--creds bigred:bigred --tls-verify=false`。
- Frontend 建議保留 `sudo podman build --no-cache`，避免舊靜態檔或舊 JS cache 造成畫面沒有更新。
- Kubernetes 目前使用 `ocean-flask-api:0.5.0` 與 `ocean-frontend:0.5.0`。

`0.5.0` 前端 / API 包含：

- Leaflet + 本機 GeoJSON 地圖。
- Nginx `/api/` proxy 到 Flask。
- API availability，讓前端只使用 serving/current 內實際存在的日期。
- Dashboard summary / trend / status distribution API。
- 前端 module query string 更新，避免瀏覽器繼續吃舊 JS。
- Dashboard 專用圖卡，不再依賴舊 summary 表。

## 5. 套用 ConfigMap

```bash
cd /opt/zfs/project

kubectl apply -f pipeline_iceberg/deploy/kubernetes/00-configmap.yaml
kubectl get configmap ocean-pipeline-config -n dt -o yaml
```

確認至少包含：

```text
BATCH_ID=2020_01
START_DATE=2020-01-01
END_DATE=2020-01-31
AOI_IDS=taiwan,northwest_pacific
SERVING_RELEASE_ID=2020_01
DASHBOARD_START_DATE=2020-01-01
FILL_WINDOW_DAYS=5
HDFS_BRONZE_ROOT=hdfs:///raw/ocean/bronze
HDFS_SILVER_ROOT=hdfs:///elt/ocean/silver
HDFS_SERVING_ROOT=hdfs:///dataset/ocean/serving
HDFS_METADATA_ROOT=hdfs:///metadata/ocean
ICEBERG_WAREHOUSE=hdfs:///dataset/ocean/warehouse
LOCAL_SERVING_CURRENT=/opt/zfs/project/data/serving/current
```

## 6. 安全的月批次重跑流程

標準流程：

```bash
cd /opt/zfs/project

# 1. 修改 ConfigMap 後套用
kubectl apply -f pipeline_iceberg/deploy/kubernetes/00-configmap.yaml

# 2. 重新建立本次 batch 需要的 Job
kubectl delete job ocean-bronze-upload -n dt --ignore-not-found
kubectl apply -f pipeline_iceberg/deploy/kubernetes/02-upload-job.yaml

kubectl delete job ocean-silver -n dt --ignore-not-found
kubectl apply -f pipeline_iceberg/deploy/kubernetes/03-silver-job.yaml

kubectl delete job ocean-gold -n dt --ignore-not-found
kubectl apply -f pipeline_iceberg/deploy/kubernetes/04-gold-job.yaml

kubectl delete job ocean-serving-export -n dt --ignore-not-found
kubectl apply -f pipeline_iceberg/deploy/kubernetes/05-serving-job.yaml
```

如果只要重跑某一層，可以只刪除並重建該層 Job。但不要在上游失敗時直接跳到下游；下游成功不代表上游資料正確。

## 7. Preflight

```bash
kubectl delete job ocean-pipeline-preflight -n dt --ignore-not-found
kubectl apply -f pipeline_iceberg/deploy/kubernetes/00-preflight-job.yaml
kubectl logs -n dt -f job/ocean-pipeline-preflight
kubectl wait -n dt --for=condition=complete \
  job/ocean-pipeline-preflight --timeout=30m
```

必須看到：

```text
PREFLIGHT status=success
```

## 8. Bronze：上傳到 HDFS

如果 Bronze 已經在 `/opt/zfs/project/data/bronze`，且已整理為 `product/year=YYYY/month=MM`：

```bash
kubectl delete job ocean-bronze-upload -n dt --ignore-not-found
kubectl apply -f pipeline_iceberg/deploy/kubernetes/02-upload-job.yaml
kubectl logs -n dt -f job/ocean-bronze-upload
kubectl wait -n dt --for=condition=complete \
  job/ocean-bronze-upload --timeout=6h
```

驗證：

```bash
hdfs dfs -count -h /raw/ocean/bronze/CHL/year=2020/month=01
hdfs dfs -count -h /raw/ocean/bronze/GFW/year=2020/month=01
hdfs dfs -ls /metadata/ocean/bronze | tail
```

`02-upload-job.yaml` 會依 `HDFS_BRONZE_ROOT` 建立目標目錄；HDFS 不會自動照本機完整路徑建立，而是由腳本依產品與年月建立目標分區。

## 9. Silver

```bash
kubectl delete job ocean-silver -n dt --ignore-not-found
kubectl apply -f pipeline_iceberg/deploy/kubernetes/03-silver-job.yaml
kubectl logs -n dt -f job/ocean-silver
kubectl wait -n dt --for=condition=complete \
  job/ocean-silver --timeout=12h
```

驗證：

```bash
hdfs dfs -count -h /elt/ocean/silver/taiwan/nasa_daily_grid
hdfs dfs -count -h /elt/ocean/silver/taiwan/gfw_daily_grid
hdfs dfs -count -h /elt/ocean/silver/northwest_pacific/nasa_daily_grid
hdfs dfs -count -h /elt/ocean/silver/northwest_pacific/gfw_daily_grid
```

Silver 以前的管線目前不為 dashboard 改動；Gold dashboard 專用表會從 Silver / Gold 結果往後產出。

## 10. Gold Iceberg

```bash
kubectl delete job ocean-gold -n dt --ignore-not-found
kubectl apply -f pipeline_iceberg/deploy/kubernetes/04-gold-job.yaml
kubectl logs -n dt -f job/ocean-gold
kubectl wait -n dt --for=condition=complete \
  job/ocean-gold --timeout=12h
```

Gold 目前產出：

```text
lake.ocean.gold_daily_grid_features
lake.ocean.gold_map_metric
lake.ocean.gold_dashboard_daily_metrics
lake.ocean.gold_dashboard_status_distribution
```

驗證：

```bash
hdfs dfs -ls /dataset/ocean/warehouse/ocean
hdfs dfs -find /dataset/ocean/warehouse \
  -path '*/metadata/*.metadata.json' | head
```

應看到下列 table 的 metadata：

```text
gold_daily_grid_features
gold_map_metric
gold_dashboard_daily_metrics
gold_dashboard_status_distribution
```

如果 HadoopCatalog 實際目錄不是 `/dataset/ocean/warehouse/ocean`，以 `hdfs dfs -find /dataset/ocean/warehouse -path '*/metadata/*.metadata.json'` 為準。

## 11. Serving Export

Serving job 會從 Gold Iceberg 匯出前端查詢用 Parquet，並在 local serving 端執行 partition merge：

```text
舊 /opt/zfs/project/data/serving/current
  + 本次 batch partitions
  -> 新 release staging
  -> 同 event_date/aoi_id/resolution_km partition 以新 batch 覆蓋
  -> 驗證成功後切換 current symlink
```

```bash
kubectl delete job ocean-serving-export -n dt --ignore-not-found
kubectl apply -f pipeline_iceberg/deploy/kubernetes/05-serving-job.yaml
kubectl logs -n dt -f job/ocean-serving-export
kubectl wait -n dt --for=condition=complete \
  job/ocean-serving-export --timeout=6h
```

成功 log 範例：

```text
SERVING_EXPORT release=2020_01 status=starting
SERVING_MERGE dataset=gold_map_metric partition=event_date=2020-01-01/aoi_id=taiwan/resolution_km=4 status=merged
SERVING_EXPORT release=2020_01 batch_hdfs=hdfs:///dataset/ocean/serving/batches/2020_01 current=/opt/zfs/project/data/serving/releases/2020_01 status=success
```

驗證 HDFS batch：

```bash
hdfs dfs -count -h /dataset/ocean/serving/batches/2020_01/gold_map_metric
hdfs dfs -count -h /dataset/ocean/serving/batches/2020_01/gold_dashboard_daily_metrics
hdfs dfs -count -h /dataset/ocean/serving/batches/2020_01/gold_dashboard_status_distribution
```

驗證本機 current snapshot：

```bash
find -L /opt/zfs/project/data/serving/current \
  -type f \
  -name '*.parquet' |
head

# 統計 Parquet 數量：
find -L /opt/zfs/project/data/serving/current \
  -type f \
  -name '*.parquet' |
wc -l

# 統計容量：
du -shL \
  /opt/zfs/project/data/serving/current
```

## 12. Flask API

```bash
kubectl apply -f pipeline_iceberg/deploy/kubernetes/07-flask-api.yaml
kubectl rollout status -n dt deployment/ocean-flask-api --timeout=5m
kubectl get pod,svc -n dt -l app=ocean-flask-api
kubectl logs -n dt deployment/ocean-flask-api
```

叢集內驗證：

```bash
kubectl run ocean-api-curl -n dt --rm -it --restart=Never \
  --image=curlimages/curl:8.12.1 -- \
  curl -sS http://ocean-flask-api:8000/healthz
```

應回傳：

```json
{ "status": "ok" }
```

如果叢集不能拉 `curlimages/curl`，也可在 `dtadm` 直接測：

```bash
curl -sS http://ocean-flask-api:8000/healthz
```

## 13. Frontend

```bash
kubectl apply -f pipeline_iceberg/deploy/kubernetes/08-frontend.yaml
kubectl rollout status -n dt deployment/ocean-frontend --timeout=5m
kubectl get svc ocean-frontend -n dt
kubectl get pod -n dt -l app=ocean-frontend -o wide
```

目前 frontend Service 是 NodePort：

```text
ocean-frontend  NodePort  8080:30801/TCP
```

在 `dtadm` 驗證：

```bash
curl -I http://ocean-frontend:8080/
curl -I http://172.22.136.4:30801/
curl -sS http://ocean-frontend:8080/runtime-config.js

curl -sS -o /tmp/dashboard.css \
  -w 'status=%{http_code} type=%{content_type} size=%{size_download}\n' \
  http://ocean-frontend:8080/src/styles/dashboard.css
head -3 /tmp/dashboard.css
```

API proxy 驗證：

```bash
curl -sS http://ocean-frontend:8080/api/v1/catalog
curl -sS 'http://ocean-frontend:8080/api/v1/availability?aoi=taiwan&product=CHL&metric=chlor_a&resolution=4'
curl -sS 'http://ocean-frontend:8080/api/v1/gold/summary?aoi=taiwan&resolution=4'
curl -sS 'http://ocean-frontend:8080/api/v1/gold/trend?aoi=taiwan&resolution=4'
curl -sS 'http://ocean-frontend:8080/api/v1/gold/status-distribution?aoi=taiwan&resolution=4'
curl -i 'http://ocean-frontend:8080/api/v1/gold/daily-grid?date=2020-01-03&aoi=taiwan&product=CHL&metric=chlor_a&resolution=4'
```

前端會用 availability 自動限制日期，避免選到沒有資料的分區。Dashboard 圖卡使用 `gold_dashboard_daily_metrics` 與 `gold_dashboard_status_distribution`。

## 14. Windows 瀏覽器連線方式：SSH tunnel

Windows 已可連到 VM：

```text
192.168.44.139:22
```

但 Windows 不能直接連：

```text
192.168.44.139:30801
172.22.136.x:30801
```

因此用 SSH tunnel。

在 Windows PowerShell 開一個新視窗，不要關：

```powershell
ssh -L 18081:172.22.136.4:30801 bigred@192.168.44.139
```

登入後保持 SSH 視窗開著。

Windows 瀏覽器開：

```text
http://localhost:18081/
```

流量路徑：

```text
Windows browser
  -> localhost:18081
  -> SSH tunnel
  -> 192.168.44.139
  -> 172.22.136.4:30801
  -> ocean-frontend NodePort
  -> frontend Nginx
  -> /api/v1 proxy
  -> ocean-flask-api:8000
```

如果 frontend Pod 換到不同 worker，先查：

```bash
kubectl get pod -n dt -l app=ocean-frontend -o wide
kubectl get node -o wide
```

再把 tunnel 目的地換成該 Pod 所在 Node 的 `INTERNAL-IP:30801`。例如：

```powershell
ssh -L 18081:172.22.136.3:30801 bigred@192.168.44.139
ssh -L 18081:172.22.136.4:30801 bigred@192.168.44.139
ssh -L 18081:172.22.136.5:30801 bigred@192.168.44.139
```

NodePort 理論上任一 K8S Node IP 都可用；實務上使用已驗證可通的 Node IP 即可。

## 15. 常見排查

### 前端顯示 `Failed to construct 'URL': Invalid URL`

代表前端 JavaScript 組 URL 失敗，API request 還沒送到 Flask。新版前端應使用：

```js
new URL(`${APP_CONFIG.apiBaseUrl}${path}`, window.location.origin);
```

部署後驗證：

```bash
curl -sS http://ocean-frontend:8080/runtime-config.js
curl -sS http://ocean-frontend:8080/api/v1/catalog
curl -sS 'http://ocean-frontend:8080/api/v1/availability?aoi=taiwan&product=CHL&metric=chlor_a&resolution=4'
```

### API 回 `404 no matching grid partition`

代表前端已經能打 API，下一步檢查該日期、AOI、產品、指標、解析度在 serving snapshot 裡是否有資料：

```bash
find /opt/zfs/project/data/serving/current -type f -name '*.parquet' | head
hdfs dfs -count -h /dataset/ocean/serving/batches/2020_01/gold_map_metric
```

### HDFS 目錄看起來沒有變

先確認是否看對目錄。upload 寫入目標是：

```text
/raw/ocean/bronze/<PRODUCT>/year=YYYY/month=MM
```

不是本機完整目錄層級，也不是 `/raw/` 底下直接多出本機資料夾名稱。

可用：

```bash
hdfs dfs -ls /raw/ocean/bronze
hdfs dfs -find /raw/ocean/bronze -path '*/year=2020/month=01/*.parquet' | head
hdfs dfs -ls /metadata/ocean/bronze | tail
```

如果 Job 中斷但 HDFS 沒看到新檔案，可能是：

- Job 尚未跑到實際 `put`。
- 寫入的是不同月份或不同 HDFS root。
- 同檔名以 `put -f` 覆蓋，檔案數量看起來不變。
- 你查的是 `/raw/` 或舊路徑，而不是 `/raw/ocean/bronze/<PRODUCT>/year=YYYY/month=MM`。

## 16. Iceberg Maintenance

完整驗證一個月資料後，再啟用 Iceberg maintenance：

```bash
kubectl apply -f pipeline_iceberg/deploy/kubernetes/06-maintenance-cronjob.yaml
```

Demo 驗證前不建議先開 maintenance，避免除錯時多一個變因。也不要在仍需回溯除錯時過早刪除 Iceberg snapshot。

## 17. 資源設定速查

目前規格假設：

```text
14G RAM / 8C CPU × 3 Worker
```

YARN 建議：

```xml
<property>
    <name>yarn.nodemanager.resource.memory-mb</name>
    <value>12288</value>
</property>
<property>
    <name>yarn.nodemanager.resource.cpu-vcores</name>
    <value>8</value>
</property>
<property>
    <name>yarn.scheduler.minimum-allocation-mb</name>
    <value>1024</value>
</property>
<property>
    <name>yarn.scheduler.maximum-allocation-mb</name>
    <value>6144</value>
</property>
<property>
    <name>yarn.scheduler.maximum-allocation-vcores</name>
    <value>3</value>
</property>
```

Spark ConfigMap 建議：

```yaml
SPARK_EXECUTOR_INSTANCES: "6"
SPARK_EXECUTOR_CORES: "3"
SPARK_EXECUTOR_MEMORY: 5g
SPARK_EXECUTOR_MEMORY_OVERHEAD: 1g
SPARK_DRIVER_MEMORY: 2g
SPARK_SHUFFLE_PARTITIONS: "72"
SPARK_ADVISORY_PARTITION_SIZE: "134217728"
SILVER_WRITE_SHARDS: "16"
MAX_RECORDS_PER_FILE: "2000000"
```

OOM 速查：

| 症狀                      | 調整項目                         | 建議值                   |
| ------------------------- | -------------------------------- | ------------------------ |
| Executor OOM              | `SPARK_EXECUTOR_MEMORY`          | `6g`                     |
| YARN container killed     | `SPARK_EXECUTOR_MEMORY_OVERHEAD` | `1500m`                  |
| Driver OOM                | `SPARK_DRIVER_MEMORY`            | `3g`                     |
| Shuffle 單 partition 過重 | `SPARK_SHUFFLE_PARTITIONS`       | `120`                    |
| 西北太平洋跑太慢          | `SPARK_EXECUTOR_INSTANCES`       | `"8"`，需確認 vCore 上限 |
