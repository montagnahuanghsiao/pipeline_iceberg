# Gold 月批次與 Serving 匯出部署說明

## 目的

目前 Silver 層可以一次處理整年資料，但 Gold 層在西北太平洋 AOI 與多產品指標聚合時，運算量較大；為了降低 Spark / YARN / Iceberg 寫入壓力，Gold 層採用「每月一批」處理。

本批次工具不改變原本 pipeline 的部署方式，只新增 `pipeline_iceberg/ops/` 下的維運腳本。

## 不需要新增或修改 YAML

Gold 與 Serving 仍然使用原本檔案：

```text
pipeline_iceberg/deploy/kubernetes/04-gold-job.yaml
pipeline_iceberg/deploy/kubernetes/05-serving-job.yaml
```

共用設定仍然來自：

```text
pipeline_iceberg/deploy/kubernetes/00-configmap.yaml
```

差別是不用再手動 `nano 00-configmap.yaml`。腳本會在每個月執行前自動 patch 以下欄位：

```text
BATCH_ID
START_DATE
END_DATE
DASHBOARD_START_DATE
SERVING_RELEASE_ID
```

若有設定環境變數 `AOI_IDS`，腳本也會一併覆蓋 ConfigMap 的 `AOI_IDS`。

## 新增檔案

```text
pipeline_iceberg/
└── ops/
    ├── run_gold_batch.sh
    ├── run_gold_monthly.sh
    ├── verify_gold_batch.sh
    └── README.md

pipeline_iceberg/
└── docs/
    └── GOLD_BATCH_DEPLOYMENT.md
```

## 部署前確認

在 `dtadm` 或可操作 Kubernetes / HDFS 的節點：

```bash
cd /opt/zfs/project

kubectl get configmap ocean-pipeline-config -n dt
kubectl get job -n dt
hdfs dfs -ls /elt/ocean/silver
```

確認 Silver 已完成，並且 Gold Job / Serving Job YAML 都存在：

```bash
ls -l pipeline_iceberg/deploy/kubernetes/04-gold-job.yaml
ls -l pipeline_iceberg/deploy/kubernetes/05-serving-job.yaml
```

第一次從 Windows 複製到 Linux 後，建議補執行權限：

```bash
chmod +x pipeline_iceberg/ops/*.sh
```

## 單月執行

例如執行 2020 年 3 月：

```bash
cd /opt/zfs/project
bash pipeline_iceberg/ops/run_gold_batch.sh 2020 03
```

腳本會自動執行：

```text
1. 計算 START_DATE=2020-03-01
2. 計算 END_DATE=2020-03-31
3. 設定 BATCH_ID=2020_03
4. patch ocean-pipeline-config
5. delete 舊 ocean-gold job
6. apply 04-gold-job.yaml
7. 追蹤 ocean-gold logs
8. wait ocean-gold complete
9. 驗證 Iceberg warehouse
10. delete 舊 ocean-serving-export job
11. apply 05-serving-job.yaml
12. 追蹤 ocean-serving-export logs
13. wait ocean-serving-export complete
14. 驗證 serving batch 三組輸出
```

## 多月批次執行

例如執行 2020 年 1 月到 12 月：

```bash
cd /opt/zfs/project
bash pipeline_iceberg/ops/run_gold_monthly.sh 2020 01 12
```

只補 2020 年 3 月到 6 月：

```bash
cd /opt/zfs/project
bash pipeline_iceberg/ops/run_gold_monthly.sh 2020 03 06
```

如果中間失敗，腳本會停止。修復後可以從失敗月份繼續，例如：

```bash
cd /opt/zfs/project
bash pipeline_iceberg/ops/run_gold_monthly.sh 2020 05 12
```

## 臨時指定 AOI

預設沿用 ConfigMap 裡的 `AOI_IDS`，例如：

```text
taiwan,northwest_pacific
```

如果只想跑台灣：

```bash
cd /opt/zfs/project
AOI_IDS=taiwan bash pipeline_iceberg/ops/run_gold_batch.sh 2020 03
```

如果只想跑西北太平洋：

```bash
cd /opt/zfs/project
AOI_IDS=northwest_pacific bash pipeline_iceberg/ops/run_gold_batch.sh 2020 03
```

## 只做驗證

```bash
cd /opt/zfs/project
bash pipeline_iceberg/ops/verify_gold_batch.sh 2020 03
```

驗證項目：

```text
/dataset/ocean/warehouse/ocean
/dataset/ocean/warehouse/*/metadata/*.metadata.json
/dataset/ocean/serving/batches/2020_03/gold_map_metric
/dataset/ocean/serving/batches/2020_03/gold_dashboard_daily_metrics
/dataset/ocean/serving/batches/2020_03/gold_dashboard_status_distribution
```

## Log 位置

預設輸出到：

```text
/opt/zfs/project/logs/
```

範例：

```text
/opt/zfs/project/logs/gold_batch_2020_03_20260711T090000Z.log
/opt/zfs/project/logs/gold_monthly_2020_01_12_20260711T090000Z.log
```

## 與原本 pipeline 的關係

此批次工具只是把原本手動流程自動化：

```text
手動改 ConfigMap
kubectl apply ConfigMap
kubectl delete/apply Gold Job
kubectl logs Gold
kubectl delete/apply Serving Job
kubectl logs Serving
HDFS 驗證
```

改為：

```text
run_gold_batch.sh YEAR MONTH
```

或：

```text
run_gold_monthly.sh YEAR START_MONTH END_MONTH
```

因此不影響既有部署，也不改變 Gold / Serving 的 Spark job 實作。
