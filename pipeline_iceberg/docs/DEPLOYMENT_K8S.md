# tkdt：從本機 Bronze 部署到 Flask 地圖

## 1. 本次起點與完成條件

本次不重跑 NASA/GFW 下載。起點是 `tkdt-worker1` 已有：

```text
/opt/zfs/project/data/bronze/
├── CHL/
├── POC/
├── NFLH/
├── SST/
├── NSST/
├── SST4/
└── GFW/
```

完成後資料流為：

```text
local Bronze
  -> HDFS Bronze
  -> Spark on YARN Silver (taiwan MVP)
  -> Gold Iceberg HadoopCatalog
  -> Spark serving export
  -> local versioned Parquet snapshot
  -> Flask + DuckDB
  -> Nginx frontend
```

不使用 Trino，也不使用 Hive Metastore。Iceberg 的正式表名是
`lake.ocean.gold_*`；HadoopCatalog 直接以 HDFS warehouse 管理 metadata。

## 2. 節點責任

本專案有兩種巢狀叢集資源規格：

| Pipeline | control-plane | worker |
|---|---:|---:|
| `pipeline_iceberg` | 5 GiB / 4 cores | 6 GiB / 4 cores |
| `pipeline_ispan` | 5 GiB / 8 cores | 14 GiB / 8 cores |

本文件與本目錄設定只套用 `pipeline_iceberg`。目前 repository 中沒有
`pipeline_ispan/`，因此不共用或推測其 Spark 參數。

| 位置 | 執行內容 |
|---|---|
| `tkadm`（或可操作 Docker、kubectl 的管理節點） | build/push image、套用 YAML、看 Pod log |
| `tkdt-worker1` | 保存 `/opt/zfs/project` 與 `/opt/zfs/sys`；Kubernetes driver Pod 固定在此 |
| YARN ResourceManager/NodeManager 節點 | 分配 Spark executor container；不是 Kubernetes Pod |
| `dtadm`（若它是 Hadoop client 節點） | 用 `hdfs dfs`、`yarn` 做叢集外驗證 |

Kubernetes Node 是既有機器；Job controller 建立 Pod，scheduler 再把 Pod 排到
Node。Spark driver 在 Pod 內呼叫 YARN，YARN 另外建立 executor container。

## 3. 放置專案與先核對實機版本

在保存 ZFS 專案的節點：

```bash
cd /opt/zfs/project
find data/bronze -type f -name '*.parquet' | head
for p in CHL POC NFLH SST NSST SST4 GFW; do
  printf '%-5s ' "$p"
  find "data/bronze/$p" -type f -name '*.parquet' | wc -l
done

ls -ld /opt/zfs/sys/hadoop-* /opt/zfs/sys/spark-*
find /opt/zfs/sys/spark-3.5.8-bin-hadoop3/jars \
  -name 'iceberg-spark-runtime-3.5_2.12-*.jar'
```

本版 YAML 預設：

```text
HADOOP_HOME=/opt/zfs/sys/hadoop-3.5.0
SPARK_HOME=/opt/zfs/sys/spark-3.5.8-bin-hadoop3
Iceberg runtime=3.5_2.12-1.11.0
Java=17（由 spark-client image 提供 /opt/java/openjdk）
```

若實機目錄不同，先改
`pipeline_iceberg/deploy/kubernetes/00-configmap.yaml` 的 `HADOOP_HOME`、
`HADOOP_CONF_DIR`、`YARN_CONF_DIR`、`SPARK_HOME`、`ICEBERG_JAR`，不可只改其中
一個路徑。

確認 YARN 容器限制：

```bash
/opt/zfs/sys/hadoop-3.5.0/bin/yarn getconf \
  -confKey yarn.scheduler.maximum-allocation-mb
/opt/zfs/sys/hadoop-3.5.0/bin/yarn getconf \
  -confKey yarn.scheduler.maximum-allocation-vcores
```

`pipeline_iceberg` 的 MVP 設定為 2 個 executor，每個 `1g + 512m overhead`、
1 core；Driver heap 為 `1g`，Kubernetes Driver Pod limit 為 `2Gi`。Silver、
Gold與Iceberg維護Driver排在`dt=admin` control-plane；YARN executor仍由
ResourceManager分配到worker。Serving為了把本機快照放到API所在的hostPath，
仍固定在`tkdt-worker1`。

## 4. 建置並推送三個映像

在 `/opt/zfs/project` 執行：

```bash
docker build \
  -f pipeline_iceberg/deploy/docker/Dockerfile.spark-client \
  -t dkreg.taroko:5000/ocean-spark-client:3.5.8 \
  pipeline_iceberg

docker build \
  -f pipeline_iceberg/deploy/docker/Dockerfile.api \
  -t dkreg.taroko:5000/ocean-flask-api:0.3.0 \
  pipeline_iceberg

docker build \
  -f pipeline_iceberg/deploy/docker/Dockerfile.frontend \
  -t dkreg.taroko:5000/ocean-frontend:0.3.0 \
  .

docker push dkreg.taroko:5000/ocean-spark-client:3.5.8
docker push dkreg.taroko:5000/ocean-flask-api:0.3.0
docker push dkreg.taroko:5000/ocean-frontend:0.3.0
```

`ocean-collector` 這次不需建置，因為 Bronze 已存在。未來要重新下載才使用
`01-bronze-job.yaml`。

## 5. 套用設定並跑 preflight

在可操作 `kubectl` 的管理節點：

```bash
cd /opt/zfs/project
kubectl get node tkdt-worker1
kubectl apply -f pipeline_iceberg/deploy/kubernetes/00-configmap.yaml

kubectl delete job ocean-pipeline-preflight -n dt --ignore-not-found
kubectl apply -f pipeline_iceberg/deploy/kubernetes/00-preflight-job.yaml
kubectl logs -n dt -f job/ocean-pipeline-preflight
kubectl wait -n dt --for=condition=complete \
  job/ocean-pipeline-preflight --timeout=30m
```

成功訊號為 `PREFLIGHT status=success`。它會檢查七個 Bronze 產品、Hadoop/Spark
執行檔、Iceberg JAR、YARN Node，並實際建立 HDFS 目錄與寫入/刪除 probe。

若 Pod 一直 Pending：

```bash
kubectl describe pod -n dt \
  -l job-name=ocean-pipeline-preflight
kubectl get nodes --show-labels
```

先確認 `tkdt-worker1` 名稱與 nodeSelector 完全一致。

## 6. Bronze：本機上傳 HDFS

執行的 YAML：
`pipeline_iceberg/deploy/kubernetes/02-upload-job.yaml`

```bash
kubectl delete job ocean-bronze-upload -n dt --ignore-not-found
kubectl apply -f pipeline_iceberg/deploy/kubernetes/02-upload-job.yaml
kubectl logs -n dt -f job/ocean-bronze-upload
kubectl wait -n dt --for=condition=complete \
  job/ocean-bronze-upload --timeout=6h
```

腳本只比對 ConfigMap 的 `START_DATE`～`END_DATE`，並寫成：

```text
hdfs:///raw/ocean/bronze/<PRODUCT>/<YEAR>/<file>.parquet
hdfs:///metadata/ocean/bronze/bronze_upload_<run_id>.tsv
```

同檔名採 `put -f`，所以重跑不會累積重複檔案。若某產品在日期範圍內一個檔案
都沒有，Job 會非零失敗。

在 Hadoop client 節點驗證：

```bash
hdfs dfs -count -h /raw/ocean/bronze/CHL/2024
hdfs dfs -count -h /raw/ocean/bronze/GFW/2024
hdfs dfs -ls /metadata/ocean/bronze | tail
```

## 7. Silver：台灣 AOI 清洗

執行的 YAML：
`pipeline_iceberg/deploy/kubernetes/03-silver-job.yaml`

```bash
kubectl delete job ocean-silver -n dt --ignore-not-found
kubectl apply -f pipeline_iceberg/deploy/kubernetes/03-silver-job.yaml
kubectl logs -n dt -f job/ocean-silver
kubectl wait -n dt --for=condition=complete job/ocean-silver --timeout=12h
```

目前`AOI_IDS=taiwan`會建立：

```text
hdfs:///elt/ocean/silver/taiwan/nasa_daily_grid/
hdfs:///elt/ocean/silver/taiwan/gfw_daily_grid/
```

MVP ConfigMap只啟用7日範圍。每次Silver執行另寫：

```text
hdfs:///metadata/ocean/silver/<run_id>/<aoi_id>/nasa_daily_grid/
hdfs:///metadata/ocean/silver/<run_id>/<aoi_id>/gfw_daily_grid/
```

GFW Silver保留`presence_hours`、`fishing_hours`及
`vessel_presence_count`。最後一項是來源`mmsi_present`加總代理值，可能因船籍或
漁具分類重複，不能當成精確去重船數。

驗證：

```bash
hdfs dfs -count -h /elt/ocean/silver/taiwan/nasa_daily_grid
hdfs dfs -count -h /elt/ocean/silver/taiwan/gfw_daily_grid
```

## 8. Gold：寫入 Iceberg

執行的 YAML：
`pipeline_iceberg/deploy/kubernetes/04-gold-job.yaml`

```bash
kubectl delete job ocean-gold -n dt --ignore-not-found
kubectl apply -f pipeline_iceberg/deploy/kubernetes/04-gold-job.yaml
kubectl logs -n dt -f job/ocean-gold
kubectl wait -n dt --for=condition=complete job/ocean-gold --timeout=12h
```

Gold 表：

```text
lake.ocean.gold_daily_grid_features
lake.ocean.gold_map_metric
lake.ocean.gold_daily_metric_summary
```

HadoopCatalog 實體根目錄是：

```text
hdfs:///dataset/ocean/warehouse/ocean/
```

不保證會出現 Hive 風格的 `ocean.db/`，因此程式不得依賴該字串。

快速確認：

```bash
hdfs dfs -ls /dataset/ocean/warehouse/ocean
hdfs dfs -find /dataset/ocean/warehouse/ocean \
  -path '*/metadata/*.metadata.json' | head
```

## 9. Serving：從 Gold 匯出 Flask 快照

執行的 YAML：
`pipeline_iceberg/deploy/kubernetes/05-serving-job.yaml`

```bash
kubectl delete job ocean-serving-export -n dt --ignore-not-found
kubectl apply -f pipeline_iceberg/deploy/kubernetes/05-serving-job.yaml
kubectl logs -n dt -f job/ocean-serving-export
kubectl wait -n dt --for=condition=complete \
  job/ocean-serving-export --timeout=6h
```

Spark 先寫 HDFS：

```text
hdfs:///dataset/ocean/serving/gold_map_metric/
hdfs:///dataset/ocean/serving/gold_daily_metric_summary/
```

再下載到固定執行節點：

```text
/opt/zfs/project/data/serving/releases/2024_12_01_07/
/opt/zfs/project/data/serving/current -> releases/2024_12_01_07
```

快照完整下載並驗證有 Parquet 後才切換 `current`，避免 Flask 看見半成品。

## 10. 部署 Flask API

執行的 YAML：
`pipeline_iceberg/deploy/kubernetes/07-flask-api.yaml`

```bash
kubectl apply -f pipeline_iceberg/deploy/kubernetes/07-flask-api.yaml
kubectl rollout status -n dt deployment/ocean-flask-api --timeout=5m
kubectl get pod,svc -n dt -l app=ocean-flask-api
kubectl logs -n dt deployment/ocean-flask-api
```

叢集內測試：

```bash
kubectl run ocean-api-curl -n dt --rm -it --restart=Never \
  --image=curlimages/curl:8.12.1 -- \
  curl -sS http://ocean-flask-api:8000/healthz
```

若叢集不能拉 `curlimages/curl`，從可連到 NodePort 的機器測：

```bash
curl http://<任一可達K8S_NODE_IP>:30800/healthz
curl 'http://<任一可達K8S_NODE_IP>:30800/api/v1/gold/daily-grid?date=2024-12-12&aoi=taiwan&product=CHL&metric=chlor_a&resolution=4'
```

API Pod 固定在 `tkdt-worker1`，因為 serving snapshot 目前是該 Node 的 hostPath。
若要多副本或跨 Node，下一階段應把 serving snapshot 改成 RWX PVC 或物件儲存。

## 11. 部署前端

執行的 YAML：
`pipeline_iceberg/deploy/kubernetes/08-frontend.yaml`

```bash
kubectl apply -f pipeline_iceberg/deploy/kubernetes/08-frontend.yaml
kubectl rollout status -n dt deployment/ocean-frontend --timeout=5m
kubectl get svc ocean-frontend -n dt
```

瀏覽：

```text
http://<任一可達K8S_NODE_IP>:30801/
```

前端 Nginx 將 `/api/` 代理到 `ocean-flask-api.dt.svc.cluster.local:8000`，
所以瀏覽器不需要知道 Flask Pod IP。原始 `frontend/runtime-config.js` 仍使用
mock；Kubernetes frontend image 會以
`pipeline_iceberg/deploy/nginx/runtime-config.js` 切到 API。

## 12. 定期維護（資料驗證完成後才啟用）

```bash
kubectl apply \
  -f pipeline_iceberg/deploy/kubernetes/06-maintenance-cronjob.yaml
```

先完成一個月端到端測試，再啟用 snapshot expire。不要在仍需回溯除錯時過早
刪除 Iceberg 歷史 snapshot。

## 13. 每次月份重跑順序

1. 修改 `00-configmap.yaml` 的 `START_DATE`、`END_DATE`、
   `SERVING_RELEASE_ID`。
2. `kubectl apply` ConfigMap。
3. 依序刪除並重建 upload、silver、gold、serving Job。
4. API 不需重建；`current` symlink 切換後，新 request 會讀新快照。
5. 驗證筆數、日期分區、API 回傳與地圖後才視為完成。

若 Job 失敗：

```bash
kubectl get pod -n dt -l job-name=<JOB_NAME>
kubectl describe pod -n dt -l job-name=<JOB_NAME>
kubectl logs -n dt job/<JOB_NAME>
```

不要直接跳到下一層；下游成功不代表上游資料正確。
