# Pipeline Iceberg 資料流設計

## 目標

前端可依 `date + aoi + product + metric + resolution` 查詢熱力圖。所有顯示指標統一為
0–100 的相對分數，前端只表達「非常少、少、中等、多、非常多」，不顯示科學數值與單位。

## 資料流

```text
NASA / GFW
  -> Python collectors
  -> local immutable Raw + Bronze Parquet
  -> HDFS Bronze staging
  -> Spark incremental Silver Parquet
  -> Spark Gold feature join
  -> Iceberg Gold on HDFS
       ocean.gold_daily_grid_features
       ocean.gold_map_metric
       ocean.gold_daily_metric_summary
  -> Spark export serving Parquet
  -> local versioned serving release
  -> Flask + DuckDB API
  -> Nginx frontend heatmap
```

Iceberg 使用 `HadoopCatalog`，warehouse 位於 HDFS。前端不直接掃描 Iceberg，
也不在 HTTP request 內啟動 Spark。每次 Gold 完成後由 Spark 匯出前端所需的
窄欄位 Parquet，下載成 `/opt/zfs/project/data/serving/releases/<release_id>`，
再以 `current` symlink 原子切換。Flask 以 DuckDB 查詢 `current`；本架構不使用
Trino。

HadoopCatalog 的 namespace 通常對應 warehouse 下的 `ocean/` 目錄，不應把
實體路徑硬寫成 Hive 慣例的 `ocean.db/`。正式識別應使用表名
`lake.ocean.<table>`。

## Gold 表

| 表 | 一列代表 | 主要用途 |
|---|---|---|
| `gold_daily_grid_features` | 日期、AOI、4 km 網格 | 保留清洗後科學值與可追溯特徵 |
| `gold_map_metric` | 日期、AOI、產品、指標、解析度、網格 | 前端地圖 |
| `gold_daily_metric_summary` | 日期、AOI、產品、指標、解析度 | 摘要與趨勢 |

## Flask API

| Endpoint | 用途 |
|---|---|
| `GET /healthz` | 確認 serving snapshot 已掛載 |
| `GET /api/v1/catalog` | AOI、產品指標與解析度契約 |
| `GET /api/v1/gold/daily-grid` | 指定日期的地圖網格 |
| `GET /api/v1/gold/summary` | 指定日期的摘要 |
| `GET /api/v1/gold/trend` | 指定 AOI／指標的日期趨勢 |

查詢參數為 `date`（trend 不需要）、`aoi`、`product`、`metric`、
`resolution`。API 驗證 AOI、產品／指標配對及解析度，並限制單次網格回傳量。

## 相對分數規則

- 比較範圍固定為同一個 `event_date + aoi_id + product_id + metric_id + resolution_km`。
- 依 `raw_metric_value` 由低至高計算百分位排名，輸出 `relative_score` 0–100。
- 只有一個有效網格時分數為 100。
- GFW 無活動或負值網格分數固定為 0；缺值標記為 `no_data`。
- 顯示級距：0–未滿 20 `very_low`、20–未滿 40 `low`、40–未滿 60
  `medium`、60–未滿 80 `high`、80–100 `very_high`。
- `raw_metric_value` 僅供品質檢查與 reconciliation；展示資料只能使用
  `relative_score`、`display_level`。

相對分數只回答所選日期與海域內的高低位置。它不能跨日直接比較絕對變化，也不代表漁獲量或作業建議。

## 分區與重跑

- Silver：按 `event_date` 動態覆寫。
- Gold：Iceberg `overwritePartitions()` 原子覆寫。
- Gold 分區：`event_date, aoi_id`；地圖與摘要另含 `resolution_km`。
- 地圖唯一鍵：
  `event_date + aoi_id + product_id + metric_id + resolution_km + grid_id`。
- 既有 0.2.x Gold 表含舊欄位時，部署 0.3.0 前必須執行 Iceberg schema migration，
  或在可重建的環境刪除 Gold 表後重跑指定日期；不可直接混寫兩種 schema。
