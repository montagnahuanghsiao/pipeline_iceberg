# Hybrid vs Multi-Iceberg benchmark contract

Use the same source files, date range, AOI, Spark resources and metric catalog.
Only the storage design may differ.

## Fixed inputs

- identical HDFS Raw/Bronze file snapshot and checksums
- identical `START_DATE`, `END_DATE`, `AOI_ID`
- identical executor count, cores, memory, shuffle partitions and AQE settings
- cold-cache run first; warm-cache runs reported separately
- three runs per case; report median and P95 where applicable

## Required correctness gate

Before comparing speed, both pipelines must match on:

- Silver and Gold row counts by date
- unique-key duplicate counts
- null counts and min/max/sum for every metric
- `gold_map_metric` schema fingerprint
- sampled grid values within floating-point tolerance

## Metrics

| Area | Metric |
|---|---|
| End-to-end | wall-clock duration, success rate |
| Spark | executor CPU time, shuffle read/write, spill, peak memory |
| HDFS | bytes read/written, file count, mean/P50/P95 file size |
| Iceberg | commit duration, snapshot count, manifest count, metadata size |
| Serving | Trino scanned bytes, query latency P50/P95, API payload bytes |
| Operations | retry duration, late-date replay duration, maintenance duration |

Do not claim a winner from one run or from runtime alone. A faster run that
produces different rows, excessive small files or higher query latency fails.
