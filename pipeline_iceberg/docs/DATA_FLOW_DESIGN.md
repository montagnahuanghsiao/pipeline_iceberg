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
| `gold_daily_grid_features` | 日期、AOI、4 km NASA 有效網格 | 五項最終產品、缺值與補值來源 |
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
- 依 `metric_value` 由低至高計算百分位排名，輸出 `relative_score` 0–100。
- 只有一個有效網格時分數為 100。
- GFW 沒有活動列時填 0，零活動網格的相對分數固定為 0。
- 顯示級距：0–未滿 20 `very_low`、20–未滿 40 `low`、40–未滿 60
  `medium`、60–未滿 80 `high`、80–100 `very_high`。
- `metric_value` 僅供品質檢查與 reconciliation；展示資料只能使用
  `relative_score`、`display_level`。

相對分數只回答所選日期與海域內的高低位置。它不能跨日直接比較絕對變化，也不代表漁獲量或作業建議。

## 有效海洋網格與展示補值

Gold 從本次處理期間的 NASA Silver 取得曾實際出現的 4 km 網格，再建立
「日期 × 有效網格」骨架，不重新生成 AOI 矩形內的陸地與從未觀測格。NASA
當日觀測缺失時依序採用：

1. 同網格指定窗口的歷史平均；
2. 八個周圍有效網格在同日可用的窗口平均；
3. 無可靠資料時保留 `null`，並標記 `no_data`。

不再使用 AOI 全區平均強制填滿。NaN 或無法由 `value_source` 解釋的 null
會使 Gold Job 失敗；`source=no_data` 的 null 是合法缺值。`value_source`
保留 `observed`、`grid_<window>d_mean`、`neighbor_<window>d_mean`、
`no_data`、`zero_filled` 或衍生補值標記。補值是視覺化展示估計，不代表
當日衛星實際觀測。

## 五項前端產品

| 產品 | Gold 數值 |
|---|---|
| 葉綠素濃度 | `chlor_a`，數值越高百分位越高 |
| 海溫 | 優先 `SST4`，其次 `NSST`，最後 `SST`，再套用補值規則 |
| 海洋生產力分數 | `CHL / AVG(CHL) + POC / AVG(POC) + NFLH / AVG(NFLH)` |
| 永續壓力 | `fishing_hours / ocean_productivity_score` |
| 捕魚時數 | GFW `fishing_hours`，沒有活動列填 0 |

公式中的平均值為同一日期、同一 AOI 有效網格中可用數值的平均。寬表只保留上述五個
產品欄位；POC 與 NFLH 僅作為生產力分數的中間計算值。

## 分區與重跑

- Silver：按 `event_date` 動態覆寫。
- Silver品質報告：
  `hdfs:///metadata/ocean/silver/<run_id>/<aoi_id>/<dataset>/`，記錄來源／輸出
  筆數、唯一鍵、日期範圍、缺值與最小／最大值。
- Gold：Iceberg `overwritePartitions()` 原子覆寫。
- Gold 分區：`event_date, aoi_id`；地圖與摘要另含 `resolution_km`。
- 地圖唯一鍵：
  `event_date + aoi_id + product_id + metric_id + resolution_km + grid_id`。
- 既有 0.4.x Gold 表採完整矩形與強制補值。部署 0.5.0
  前必須執行 Iceberg schema migration，或在可重建的環境刪除三張 Gold 表後
  重跑指定日期；不可直接混寫兩種 schema。
