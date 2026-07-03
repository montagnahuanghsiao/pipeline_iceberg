# Kubernetes 部署流程

## 支援範圍

正式資料流只包含：

```text
Python collection
  -> local Raw/Bronze Parquet
  -> HDFS Bronze
  -> Spark on YARN Silver Parquet
  -> Spark on YARN Gold Iceberg
  -> HDFS Iceberg warehouse
```

核心版本：

- Python 3.12
- Hadoop 3.5.0（HDFS、YARN）
- Spark 3.5.8、Scala 2.12
- Apache Iceberg 1.11.0
- Java 17

Iceberg 使用 `HadoopCatalog`：

```properties
spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.lake.type=hadoop
spark.sql.catalog.lake.warehouse=hdfs:///dataset/ocean/warehouse
```

## 前置條件

- Kubernetes Job 可連線 Hadoop 叢集。
- `/opt/zfs/project` 與 `/opt/zfs/sys` 已掛載至 Job。
- HDFS、YARN 可用，且執行帳號可寫入 Bronze、Silver 與 Iceberg warehouse。
- Spark client 可取得 Iceberg runtime JAR。
- NASA 認證資料只能放在 Kubernetes Secret，不可提交 Git。

## 建置映像

```bash
docker build \
  -f pipeline_iceberg/deploy/docker/Dockerfile.collector \
  -t dkreg.taroko:5000/ocean-collector:0.3.0 \
  pipeline_iceberg

docker build \
  -f pipeline_iceberg/deploy/docker/Dockerfile.spark-client \
  -t dkreg.taroko:5000/ocean-spark-client:3.5.8 \
  pipeline_iceberg
```

## 部署設定

```bash
kubectl apply -f pipeline_iceberg/deploy/kubernetes/00-configmap.yaml
kubectl create secret generic nasa-auth \
  -n dt \
  --from-file=cookie=/private/nasa_cookie.txt
```

## 執行順序

```bash
kubectl apply -f pipeline_iceberg/deploy/kubernetes/01-bronze-job.yaml
kubectl wait -n dt --for=condition=complete job/ocean-bronze --timeout=24h

kubectl apply -f pipeline_iceberg/deploy/kubernetes/02-upload-job.yaml
kubectl wait -n dt --for=condition=complete job/ocean-bronze-upload --timeout=6h

kubectl apply -f pipeline_iceberg/deploy/kubernetes/03-silver-job.yaml
kubectl wait -n dt --for=condition=complete job/ocean-silver --timeout=12h

kubectl apply -f pipeline_iceberg/deploy/kubernetes/04-gold-job.yaml
kubectl wait -n dt --for=condition=complete job/ocean-gold --timeout=12h

kubectl apply -f pipeline_iceberg/deploy/kubernetes/05-maintenance-cronjob.yaml
```

## 驗收

1. Bronze manifest 的 checksum、筆數與日期範圍正確。
2. Silver NASA/GFW 分區只覆寫指定日期，且唯一鍵沒有重複。
3. Gold Iceberg snapshot 有正確的新增／刪除筆數摘要。
4. `relative_score` 全部介於 0–100。
5. 以 Spark SQL 查詢指定日期、AOI、指標與解析度時能進行分區裁剪。
6. Spark UI 沒有明顯 task skew、shuffle spill 或大量小檔案。

## 重要限制

`HadoopCatalog` 適合目前的單一 Spark/Hadoop 路線。若未來需要正式前端即時
查詢，必須另行設計跨引擎目錄與查詢服務；該擴充不屬於目前結業版本。
