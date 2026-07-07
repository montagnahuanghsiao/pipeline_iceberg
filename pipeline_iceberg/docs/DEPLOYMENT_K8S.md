# Pipeline 部署與資源設定手冊

> 規格：14G RAM / 8C CPU × 3 Worker
> ConfigMap 位置：`pipeline_iceberg/deploy/kubernetes/00-configmap.yaml`

---

## 快速查閱：欄位分類總覽

```
00-configmap.yaml
│
├── 每次 batch 必改
│   ├── BATCH_ID
│   ├── START_DATE / END_DATE
│   ├── AOI_IDS
│   └── SERVING_RELEASE_ID
│
├── 換環境才改
│   ├── HDFS_*_ROOT（4 個路徑）
│   ├── ICEBERG_WAREHOUSE / CATALOG / NAMESPACE
│   └── yarn-site.xml（NM 層 + Scheduler 層）
│
└── 效能調優才改
    ├── SPARK_EXECUTOR_INSTANCES / CORES / MEMORY
    ├── SPARK_EXECUTOR_MEMORY_OVERHEAD
    ├── SPARK_DRIVER_MEMORY
    ├── SPARK_SHUFFLE_PARTITIONS
    ├── SILVER_WRITE_SHARDS
    └── MAX_RECORDS_PER_FILE
```

---

## 1. 每次 Batch 必改欄位

```yaml
BATCH_ID: "2024_01"
START_DATE: "2024-01-01"
END_DATE: "2024-01-31"
AOI_IDS: taiwan,northwest_pacific
SERVING_RELEASE_ID: "2024_01"
```

| 欄位                 | 說明                                 | 何時改             |
| -------------------- | ------------------------------------ | ------------------ |
| `BATCH_ID`           | 批次識別碼                           | 每次換月份、換批次 |
| `START_DATE`         | 本次處理開始日期                     | 每次 batch         |
| `END_DATE`           | 本次處理結束日期                     | 每次 batch         |
| `AOI_IDS`            | 處理區域（台灣 / 西北太平洋 / 兩者） | 視需求調整         |
| `SERVING_RELEASE_ID` | 通常與 `BATCH_ID` 相同，方便追蹤     | 每次 batch         |

### 月批次範例（2024-02）

```yaml
BATCH_ID: "2024_02"
START_DATE: "2024-02-01"
END_DATE: "2024-02-29"
AOI_IDS: taiwan,northwest_pacific
SERVING_RELEASE_ID: "2024_02"
```

---

## 2. HDFS / Iceberg 路徑設定

> 換環境才動，月批次不改。

```yaml
HDFS_BRONZE_ROOT: hdfs:///raw/ocean/bronze
HDFS_SILVER_ROOT: hdfs:///elt/ocean/silver
HDFS_SERVING_ROOT: hdfs:///dataset/ocean/serving
HDFS_METADATA_ROOT: hdfs:///metadata/ocean
ICEBERG_WAREHOUSE: hdfs:///dataset/ocean/warehouse
ICEBERG_CATALOG: lake
ICEBERG_NAMESPACE: ocean
```

| 設定                 | 用途                                   |
| -------------------- | -------------------------------------- |
| `HDFS_BRONZE_ROOT`   | Bronze 原始 Parquet 上傳位置           |
| `HDFS_SILVER_ROOT`   | Silver 清洗後 Parquet                  |
| `HDFS_SERVING_ROOT`  | Serving batch Parquet                  |
| `HDFS_METADATA_ROOT` | Pipeline manifest / quality report     |
| `ICEBERG_WAREHOUSE`  | Iceberg Gold 表 warehouse 根目錄       |
| `ICEBERG_CATALOG`    | Spark SQL catalog 名稱（目前：`lake`） |
| `ICEBERG_NAMESPACE`  | Iceberg namespace（目前：`ocean`）     |

---

## 3. YARN 資源設定

### 計算過程

```
實體記憶體  14G = 14336 MB
  ├─ 保留 OS / DataNode / NM daemon  ~2048 MB
  └─ 給 YARN                         12288 MB  ✅

實體核心  8C
  └─ CPU 可超賣，全部報給 YARN        8 vCore  ✅

全叢集資源池（× 3 台 Worker）
  記憶體池  12288 × 3 = 36864 MB
  vCore 池      8 × 3 =    24 cores
```

### yarn-site.xml

```xml
<!-- ── NodeManager 層：每台撥多少給 YARN ── -->
<property>
    <name>yarn.nodemanager.resource.memory-mb</name>
    <value>12288</value>
</property>
<property>
    <name>yarn.nodemanager.resource.cpu-vcores</name>
    <value>8</value>
</property>

<!-- ── Container 層：單一 Container 大小範圍 ── -->
<property>
    <name>yarn.scheduler.minimum-allocation-mb</name>
    <value>1024</value>
</property>
<property>
    <name>yarn.scheduler.maximum-allocation-mb</name>
    <value>6144</value>
</property>
<property>
    <name>yarn.scheduler.minimum-allocation-vcores</name>
    <value>1</value>
</property>
<property>
    <name>yarn.scheduler.maximum-allocation-vcores</name>
    <value>3</value>
</property>
```

### 套用流程

```bash
stopyarn
nano ~/wulin/wk/dt/conf/hadoop-3.4.3/yarn-site.xml
dtconf
startyarn

# 等 20-30 秒讓 NodeManager 重新註冊
curl -s http://dtm-1:8088/ws/v1/cluster/metrics \
  | grep -E "totalMB|totalVirtualCores|activeNodes"

# 預期結果
# "activeNodes": 3
# "totalMB": 36864
# "totalVirtualCores": 24
```

---

## 4. Spark 資源設定（ConfigMap）

### Executor 設計

```
每個 Executor：3 core + 5g memory + 1g overhead = 6g container

驗算（每台）：
  記憶體：12288 ÷ 6144 = 2 個  ✅
  vCore：     8 ÷ 3    = 2 個（剩 2 core 給 OS / daemon）  ✅

全叢集：2 × 3 = 6 個 Executor
Task 槽：6 executor × 3 core = 18 個平行 Task
Shuffle Partitions：18 × 4 = 72

並行度驗算：
  記憶體瓶頸：36864 ÷ 1024 = 36 個 container
  vCore 瓶頸：   24 ÷    3 =  8 個 container
  min(36, 8) = 8  → vCore 是瓶頸，記憶體仍有餘裕 ✅
```

### 00-configmap.yaml（Spark 資源區塊）

```yaml
SPARK_EXECUTOR_INSTANCES: "6" # 2 個/台 × 3 台
SPARK_EXECUTOR_CORES: "3" # 每個 executor 拿 3 core
SPARK_EXECUTOR_MEMORY: 5g # container = 5g + 1g overhead = 6g
SPARK_EXECUTOR_MEMORY_OVERHEAD: 1g # 6g ≤ max-allocation 6144 MB ✅
SPARK_DRIVER_MEMORY: 2g
SPARK_SHUFFLE_PARTITIONS: "72" # 18 task 槽 × 4
SPARK_ADVISORY_PARTITION_SIZE: "134217728" # 128 MB
SILVER_WRITE_SHARDS: "16"
MAX_RECORDS_PER_FILE: "2000000"
```

### 調整時機

| 設定                             | 調整時機                                  |
| -------------------------------- | ----------------------------------------- |
| `SPARK_EXECUTOR_INSTANCES`       | 資料變多、叢集資源足夠時增加（上限 8）    |
| `SPARK_EXECUTOR_CORES`           | 每個 executor 可用 CPU 增加時調           |
| `SPARK_EXECUTOR_MEMORY`          | shuffle / join / groupBy OOM 時增加       |
| `SPARK_EXECUTOR_MEMORY_OVERHEAD` | YARN container 被 kill 時增加             |
| `SPARK_DRIVER_MEMORY`            | driver 收 metadata 或 planning OOM 時增加 |
| `SPARK_SHUFFLE_PARTITIONS`       | 資料變多時增加，太小會單 partition 過重   |
| `SILVER_WRITE_SHARDS`            | Silver 輸出太集中或小檔太多時調           |
| `MAX_RECORDS_PER_FILE`           | 控制單一 Parquet 檔案大小                 |

### OOM 速查

| 症狀                      | 調整項目                         | 建議值              |
| ------------------------- | -------------------------------- | ------------------- |
| Executor OOM              | `SPARK_EXECUTOR_MEMORY`          | `6g`                |
| YARN container killed     | `SPARK_EXECUTOR_MEMORY_OVERHEAD` | `1500m`             |
| Driver OOM                | `SPARK_DRIVER_MEMORY`            | `3g`                |
| Shuffle 單 partition 過重 | `SPARK_SHUFFLE_PARTITIONS`       | `120`               |
| 西北太平洋跑太慢          | `SPARK_EXECUTOR_INSTANCES`       | `"8"`（vCore 上限） |

---

## 5. 換月份標準流程

```bash
# 1. 改 ConfigMap（batch 必改欄位）
nano pipeline_iceberg/deploy/kubernetes/00-configmap.yaml

# 2. 套用到 K8s
kubectl apply -f pipeline_iceberg/deploy/kubernetes/00-configmap.yaml

# 3. 確認套用成功
kubectl get configmap -n <namespace>
kubectl describe configmap <configmap-name> -n <namespace>

# 4. 重新部署 Job（configmap 變更不會自動觸發 Job 重跑）
kubectl delete -f pipeline_iceberg/deploy/kubernetes/
kubectl apply -f pipeline_iceberg/deploy/kubernetes/
```

---

## 6. 設定總覽

```
規格：14G 8C × 3 Worker
│
├── YARN 層（yarn-site.xml）
│   ├── NM memory-mb          12288   （14G - 2G OS）
│   ├── NM cpu-vcores             8   （全部，CPU 可超賣）
│   ├── scheduler min-mb       1024
│   ├── scheduler max-mb       6144
│   └── scheduler max-vcores      3
│
├── Spark 層（00-configmap.yaml）
│   ├── executor instances        6   （2/台 × 3 台，上限可到 8）
│   ├── executor cores            3
│   ├── executor memory          5g
│   ├── executor overhead        1g   → container = 6g
│   ├── driver memory            2g
│   └── shuffle partitions       72   （18 task × 4）
│
├── 資料湖路徑（00-configmap.yaml）
│   ├── HDFS Bronze / Silver / Serving / Metadata
│   └── Iceberg Warehouse / Catalog / Namespace
│
└── 瓶頸分析
    └── vCore（24 ÷ 3 = 8），記憶體還有餘裕
        未來可加 SPARK_EXECUTOR_INSTANCES 至 "8"
```

# OceanGrid pipeline_iceberg Kubernetes 部署 SOP

本文件記錄目前在 tkdt / Kubernetes 環境部署 `pipeline_iceberg` 的實際流程。

目前採用：

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
  lake.ocean.gold_daily_metric_summary
```

Iceberg 管理 table metadata、snapshot、manifest、partition 與覆寫一致性；底層資料檔案仍是 Parquet。

## 1. 目前連線限制

已驗證 Kubernetes 內部可通：

```bash
curl -I http://172.22.136.4:30801/
curl -I http://10.244.137.141:8080/
curl -I http://ocean-frontend:8080/
```

但 Windows 不能直接連：

```text
http://192.168.44.139:30801/
http://172.22.136.x:30801/
```

因此 demo 使用 SSH tunnel。這不是前端或 Service 壞掉，而是 tkdt 內部 NodePort 沒有直接暴露到 Windows 可達的 VM 網卡。

## 2. 確認 Bronze

在 `dtadm`：

```bash
cd /opt/zfs/project

for p in CHL POC NFLH SST NSST SST4 GFW; do
  printf '%-5s ' "$p"
  find "data/bronze/$p" -type f -name '*.parquet' | wc -l
done
```

本機 Bronze 建議先整理成月分區。這不是重新爬蟲，只是把既有每日 Parquet 移到 `year=YYYY/month=MM` 目錄：

```text
/opt/zfs/project/data/bronze/CHL/year=2024/month=01/*.parquet
/opt/zfs/project/data/bronze/POC/year=2024/month=01/*.parquet
/opt/zfs/project/data/bronze/GFW/year=2024/month=01/*.parquet
```

若先在 Windows 端整理本機專案，可執行：

```powershell
cd C:\Users\yah51\Desktop\project

# 先預演，不會搬檔
.\pipeline_iceberg\scripts\Convert-LocalBronzeToMonthlyLayout.ps1 -WhatIf

# 確認輸出正確後再搬檔
.\pipeline_iceberg\scripts\Convert-LocalBronzeToMonthlyLayout.ps1
```

如果想先複製、不移動原檔：

```powershell
.\pipeline_iceberg\scripts\Convert-LocalBronzeToMonthlyLayout.ps1 -Mode Copy
```

## 3. 建立與推送映像

在可以執行 Docker 並推送私有 Registry 的機器：

```bash
cd /opt/zfs/project

docker build \
  -f pipeline_iceberg/deploy/docker/Dockerfile.spark-client \
  -t dkreg.taroko:5000/ocean-spark-client:3.5.8 \
  pipeline_iceberg

docker push dkreg.taroko:5000/ocean-spark-client:3.5.8

docker build \
  -f pipeline_iceberg/deploy/docker/Dockerfile.api \
  -t dkreg.taroko:5000/ocean-flask-api:0.3.1 \
  pipeline_iceberg

docker push dkreg.taroko:5000/ocean-flask-api:0.3.1

docker build --no-cache \
  -f pipeline_iceberg/deploy/docker/Dockerfile.frontend \
  -t dkreg.taroko:5000/ocean-frontend:0.4.6 \
  .

docker push dkreg.taroko:5000/ocean-frontend:0.4.6
```

前端請使用 `ocean-frontend:0.4.6`。

`0.4.6` 包含：

- Leaflet + 本機 GeoJSON 地圖。
- Nginx `/api/` proxy 到 Flask。
- 靜態 CSS / JS 檔案正確回傳，不會被 SPA fallback 成 HTML。
- 修正 `apiBaseUrl: "/api/v1"` 時，瀏覽器 `new URL()` 產生 `Invalid URL` 的問題。
- 前端 module query string 更新，避免瀏覽器繼續吃舊 JS。
- 透過 API availability 只使用 serving/current 裡實際存在的日期。

不建議前端繼續使用 `0.3.0`。`0.3.0` 是舊版前端 image，通常不包含上述修正。即使重新 build 同一個 `0.3.0` tag，Kubernetes 的 `imagePullPolicy: IfNotPresent` 和瀏覽器快取也可能讓你繼續看到舊內容。

Flask API 請使用 `ocean-flask-api:0.3.1`，此版本新增 `/api/v1/availability`，讓前端只使用 serving/current 內實際存在的日期。

## 4. 套用 ConfigMap

```bash
cd /opt/zfs/project

kubectl apply -f pipeline_iceberg/deploy/kubernetes/00-configmap.yaml
kubectl get configmap ocean-pipeline-config -n dt -o yaml
```

重要設定：

```text
BATCH_ID=2024_01
START_DATE=2024-01-01
END_DATE=2024-01-31
AOI_IDS=taiwan,northwest_pacific
HDFS_BRONZE_ROOT=hdfs:///raw/ocean/bronze
HDFS_SILVER_ROOT=hdfs:///elt/ocean/silver
ICEBERG_WAREHOUSE=hdfs:///dataset/ocean/warehouse
HDFS_SERVING_ROOT=hdfs:///dataset/ocean/serving
LOCAL_SERVING_CURRENT=/opt/zfs/project/data/serving/current
```

## 5. Preflight

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

## 6. 上傳 Bronze 到 HDFS

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
hdfs dfs -count -h /raw/ocean/bronze/CHL/year=2024/month=01
hdfs dfs -count -h /raw/ocean/bronze/GFW/year=2024/month=01
hdfs dfs -ls /metadata/ocean/bronze | tail
```

## 7. Silver

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

## 8. Gold Iceberg

```bash
kubectl delete job ocean-gold -n dt --ignore-not-found
kubectl apply -f pipeline_iceberg/deploy/kubernetes/04-gold-job.yaml
kubectl logs -n dt -f job/ocean-gold
kubectl wait -n dt --for=condition=complete \
  job/ocean-gold --timeout=12h
```

驗證：

```bash
hdfs dfs -ls /dataset/ocean/warehouse/ocean
hdfs dfs -find /dataset/ocean/warehouse/ocean \
  -path '*/metadata/*.metadata.json' | head
```

應出現：

```text
gold_daily_grid_features
gold_map_metric
gold_daily_metric_summary
```

## 9. Serving Export

Serving job 會從 Gold Iceberg 匯出本次 batch 的前端查詢用窄欄位 Parquet，然後在 local serving 端執行 partition merge：

```text
舊 /opt/zfs/project/data/serving/current
  + 本次 batch partitions
  -> 新 release staging
  -> 同 event_date/aoi_id/resolution_km partition 以新 batch 覆蓋
  -> 驗證成功後切換 current symlink
```

這樣逐月補資料時，前端仍可同時查既有月份、台灣周邊與西北太平洋。

```bash
kubectl delete job ocean-serving-export -n dt --ignore-not-found
kubectl apply -f pipeline_iceberg/deploy/kubernetes/05-serving-job.yaml
kubectl logs -n dt -f job/ocean-serving-export
kubectl wait -n dt --for=condition=complete \
  job/ocean-serving-export --timeout=6h
```

成功 log 範例：

```text
SERVING_EXPORT release=2024_01 status=starting
SERVING_MERGE dataset=gold_map_metric partition=event_date=2024-01-01/aoi_id=taiwan/resolution_km=4 status=merged
SERVING_EXPORT release=2024_01 batch_hdfs=hdfs:///dataset/ocean/serving/batches/2024_01 current=/opt/zfs/project/data/serving/releases/2024_01 status=success
```

驗證：

```bash
hdfs dfs -count -h /dataset/ocean/serving/batches/2024_01/gold_map_metric
hdfs dfs -count -h /dataset/ocean/serving/batches/2024_01/gold_daily_metric_summary

find /opt/zfs/project/data/serving/current \
  -type f -name '*.parquet' | head
```

## 10. Flask API

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

也可在 `dtadm` 直接測：

```bash
curl -sS http://ocean-flask-api:8000/healthz
```

## 11. Frontend

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

curl -sS -o /tmp/base.css \
  -w 'status=%{http_code} type=%{content_type} size=%{size_download}\n' \
  http://ocean-frontend:8080/src/styles/base.css
head -3 /tmp/base.css
```

CSS 應該看到：

```text
status=200 type=text/css ...
:root {
  color-scheme: dark;
```

API proxy 驗證：

```bash
curl -sS http://ocean-frontend:8080/api/v1/catalog
curl -sS 'http://ocean-frontend:8080/api/v1/availability?aoi=taiwan&product=CHL&metric=chlor_a&resolution=4'
```

第一個應回傳 AOI、metrics、resolutions；第二個應回傳 serving/current 裡可查詢的日期。前端會用 availability 自動限制日期，避免選到沒有資料的分區。

## 12. Windows 瀏覽器連線方式：SSH tunnel

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

## 13. 查詢失敗排查

如果畫面顯示：

```text
Failed to construct 'URL': Invalid URL
```

代表前端 JavaScript 組 URL 失敗，API request 還沒送到 Flask。新版前端應使用：

```js
new URL(`${APP_CONFIG.apiBaseUrl}${path}`, window.location.origin);
```

部署後驗證：

```bash
curl -sS http://ocean-frontend:8080/runtime-config.js
curl -sS http://ocean-frontend:8080/api/v1/catalog
curl -sS 'http://ocean-frontend:8080/api/v1/availability?aoi=taiwan&product=CHL&metric=chlor_a&resolution=4'
curl -i 'http://ocean-frontend:8080/api/v1/gold/daily-grid?date=2024-01-03&aoi=taiwan&product=CHL&metric=chlor_a&resolution=4'
```

如果 API 回 `404 no matching grid partition`，代表前端已經能打 API，下一步才是檢查該日期、AOI、產品、指標、解析度在 serving snapshot 裡是否有資料。

## 14. Iceberg Maintenance

完整驗證一個月資料後，再啟用 Iceberg maintenance：

```bash
kubectl apply -f pipeline_iceberg/deploy/kubernetes/06-maintenance-cronjob.yaml
```

Demo 驗證前不建議先開 maintenance，避免除錯時多一個變因。
