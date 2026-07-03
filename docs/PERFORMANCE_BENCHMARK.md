# Pipeline 效能驗證規格

本文件只驗證目前唯一支援的資料流：

```text
Raw/Bronze Parquet -> Silver Parquet -> Gold Iceberg
```

## 固定條件

- 使用相同來源檔案、checksum、日期範圍與 AOI。
- 固定 executor 數量、核心、記憶體、shuffle partitions 與 AQE 設定。
- 冷快取與暖快取分開記錄。
- 每個案例至少執行三次，報告中位數；查詢延遲另報 P95。

## 正確性門檻

效能結果必須先通過：

- Bronze、Silver、Gold 每日筆數 reconciliation。
- 唯一鍵重複數為零。
- 必要欄位缺值率及數值範圍符合 contract。
- `relative_score` 介於 0–100。
- 抽樣網格可由 Silver 原始值重新計算並核對。

## 量測指標

| 區域 | 指標 |
|---|---|
| 端到端 | 總執行時間、成功率、失敗重跑時間 |
| Spark | executor CPU、shuffle read/write、spill、peak memory、task skew |
| HDFS | 讀寫 bytes、檔案數、P50/P95 檔案大小 |
| Iceberg Gold | commit 時間、snapshot、manifest、metadata 大小 |
| Serving | Trino scanned bytes、查詢 P50/P95、API payload 大小 |

## 驗收重點

- 增量執行只掃描指定日期與 AOI。
- Gold 地圖查詢必須使用 `event_date`、`aoi_id`、`resolution_km` 分區裁剪。
- Parquet 與 Iceberg 目標檔案大小約 128–256 MiB，避免大量小檔案。
- 不以單次最快結果宣稱效能；資料不一致或產生過量小檔即視為失敗。
