# Gold monthly batch ops

這個目錄是 Gold 層與 Serving 層的批次維運工具。

設計原則：

- 不取代原本的 Kubernetes YAML。
- 不需要手動編輯 `deploy/kubernetes/00-configmap.yaml`。
- 每個月以 `kubectl patch configmap` 動態帶入批次參數。
- Gold 和 Serving 仍然使用原本的 `04-gold-job.yaml`、`05-serving-job.yaml`。
- 失敗即停止，修復後可從失敗月份繼續。

## 單月執行

```bash
cd /opt/zfs/project
bash pipeline_iceberg/ops/run_gold_batch.sh 2020 03
```

## 多月執行

```bash
cd /opt/zfs/project
bash pipeline_iceberg/ops/run_gold_monthly.sh 2020 01 12
```

## 只驗證某月

```bash
cd /opt/zfs/project
bash pipeline_iceberg/ops/verify_gold_batch.sh 2020 03
```

## 臨時覆蓋 AOI

預設沿用 ConfigMap 的 `AOI_IDS`。如果只想跑某個 AOI：

```bash
cd /opt/zfs/project
AOI_IDS=taiwan bash pipeline_iceberg/ops/run_gold_batch.sh 2020 03
```

或：

```bash
cd /opt/zfs/project
AOI_IDS=northwest_pacific bash pipeline_iceberg/ops/run_gold_monthly.sh 2020 03 06
```

## Log

Linux 部署環境預設寫到：

```text
/opt/zfs/project/logs/
```

檔名範例：

```text
gold_batch_2020_03_20260711T090000Z.log
gold_monthly_2020_01_12_20260711T090000Z.log
```

## Missing derived metrics

Gold keeps explained source gaps as `no_data` in the feature table. If a
metric-day cannot provide a complete AOI map, that metric is omitted for the
date at every resolution while other metrics continue. The batch still exits
successfully and logs:

```text
GOLD_METRIC_SKIPPED ... reason=incomplete_metric_scope
GOLD_DEGRADED ...
BATCH status=degraded ...
```

`degraded` is an intentional, auditable partial publication. It is not a zero
value and not a failed batch.
