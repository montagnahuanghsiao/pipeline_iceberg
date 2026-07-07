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
{"status":"ok"}
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
new URL(`${APP_CONFIG.apiBaseUrl}${path}`, window.location.origin)
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
