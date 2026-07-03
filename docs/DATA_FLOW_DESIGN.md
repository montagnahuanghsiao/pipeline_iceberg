# 高吞吐混合式資料流設計

## 目標

支援前端以 `date + aoi + product + metric + resolution` 查詢地圖，同時維持來源可追溯、日期增量、失敗可重跑與合理的小檔案數。

## 資料流

```text
NASA/GFW
  -> Python bounded collectors
  -> local Raw + Bronze Parquet
  -> HDFS Bronze staging
  -> Spark incremental Silver Parquet
       - silver_nasa_daily_grid (六產品一次 pivot)
       - silver_gfw_daily_grid
  -> one Gold join
  -> Iceberg Gold (HiveCatalog)
       - gold_daily_grid_features
       - gold_map_metric (4/16/32 km)
       - gold_daily_metric_summary
  -> Trino -> FastAPI -> Canvas heatmap
```

## 各層 grain 與分區

| Dataset | Grain | Storage | Partition |
|---|---|---|---|
| Bronze NASA | source product pixel/day | local + HDFS Parquet | product/year/month/day |
| Bronze GFW | source cell/flag/gear/day | local + HDFS Parquet | year/month/day |
| Silver NASA | date + AOI + 4km grid | HDFS Parquet | event_date |
| Silver GFW | date + AOI + 4km grid | HDFS Parquet | event_date |
| Gold features | date + AOI + 4km grid | Iceberg | event_date, aoi_id |
| Gold map | date + AOI + product + metric + resolution + grid | Iceberg | event_date, aoi_id, resolution_km |
| Gold summary | date + AOI + product + metric + resolution | Iceberg | event_date, aoi_id, resolution_km |

## 吞吐設計

1. 每次執行必須提供 `start_date/end_date`，只讀與覆寫受影響日期。
2. NASA 六產品先 union 成 long form，再以一次 pivot 形成 Silver 寬表，避免 Gold 六次 full join。
3. Silver 以 `event_date + hash(grid_id)` 分片，允許同一天多個 task 寫入。
4. Gold 只做 NASA/GFW 一次 join，寬表以 `MEMORY_AND_DISK` 重用後再建立長表。
5. Iceberg 目標檔案 256 MiB、range distribution、ZSTD。
6. 前端依 zoom 查 4/16/32 km；API 預設限制 100,000 cells。
7. Summary/Trend 查詢使用預聚合表，不掃描 map cell 明細。

## 一致性與重跑

- Bronze 使用 checksum/manifest 判斷是否需要重新下載。
- Silver 使用 dynamic partition overwrite，只替換輸入日期。
- Gold 使用 Iceberg `overwritePartitions()` 原子提交。
- 唯一鍵：
  - features：`event_date + aoi_id + grid_id`
  - map：`event_date + aoi_id + product_id + metric_id + resolution_km + grid_id`
- 晚到資料以原日期重跑，同一 partition 原子替換。

## Catalog

Spark 與 Trino 必須共用 Hive Metastore：

```text
Spark Iceberg HiveCatalog <-> Hive Metastore <-> Trino Iceberg connector
```

Iceberg data/metadata 位於 HDFS；HMS 僅保存 table catalog entry。

## 維護

- 每日：`rewrite_data_files`、`rewrite_manifests`
- 每週：`expire_snapshots`
- orphan file 清除需保留安全時間，避免刪除仍在進行中的提交。
- 監控：輸入/輸出筆數、shuffle spill、task skew、檔案數、平均檔案大小、API P95。

## 限制

`potential_fishing_score` 是 CHL/POC/NFLH 相對百分位代理指標，不是漁獲預測。GFW `fishing_hours` 是 apparent fishing effort。
