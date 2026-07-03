# Kubernetes 部署流程

## 前提

- Kubernetes Job 負責協調；Spark 實際以 YARN client mode 執行。
- `/opt/zfs/project` 與 `/opt/zfs/sys` 掛載在執行節點。
- Hadoop 3.5.0、Spark 3.5.8、Java 17 已安裝。
- HDFS、YARN、Hive Metastore、Trino 已可用。
- `nasa-auth` Secret 只由叢集建立，不提交 Cookie。

## 映像

映像至少包含 Python 3.12、專案 requirements 與 Hadoop/Spark client 設定。Iceberg runtime 1.11.0 可預先放入 Spark `jars/`；若使用 `--packages`，Pod 必須能存取 Maven repository。

## 安裝

```bash
kubectl apply -f pipeline_iceberg/deploy/kubernetes/00-configmap.yaml
kubectl create secret generic nasa-auth -n dt --from-file=cookie=/private/nasa_cookie.txt
```

依序執行：

```bash
kubectl apply -f pipeline_iceberg/deploy/kubernetes/01-bronze-job.yaml
kubectl wait -n dt --for=condition=complete job/ocean-bronze --timeout=24h

kubectl apply -f pipeline_iceberg/deploy/kubernetes/02-upload-job.yaml
kubectl wait -n dt --for=condition=complete job/ocean-bronze-upload --timeout=6h

kubectl apply -f pipeline_iceberg/deploy/kubernetes/03-silver-job.yaml
kubectl wait -n dt --for=condition=complete job/ocean-silver --timeout=12h

kubectl apply -f pipeline_iceberg/deploy/kubernetes/04-gold-job.yaml
kubectl wait -n dt --for=condition=complete job/ocean-gold --timeout=12h
```

維護：

```bash
kubectl apply -f pipeline_iceberg/deploy/kubernetes/05-maintenance-cronjob.yaml
kubectl apply -f pipeline_iceberg/deploy/kubernetes/06-api.yaml
```

## Trino catalog

```properties
connector.name=iceberg
iceberg.catalog.type=hive_metastore
hive.metastore.uri=thrift://hive-metastore.dt.svc.cluster.local:9083
fs.hadoop.enabled=true
```

## 驗收

1. HDFS Bronze 每個來源日期有 manifest 與非空 Parquet。
2. Silver 唯一鍵無重複，NASA/GFW 日期涵蓋符合批次。
3. Iceberg 最新 snapshot summary 的 added/deleted records 合理。
4. `gold_map_metric` 每個日期、AOI、metric、resolution 無重複 grid。
5. API 4km 台灣查詢 P95 達專題設定門檻；大型 AOI 使用 16/32km。
6. Spark UI 無單一長尾 task、過量 spill 或數千個小檔案。

## 回滾

Silver 是日期分區覆寫，可重跑受影響日期。Gold 使用 Iceberg snapshot；資料錯誤時先停止下游，再以已驗證 snapshot rollback，修正後重跑該日期。
