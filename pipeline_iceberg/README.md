# OceanGrid Iceberg Pipeline

The only supported production-oriented pipeline for the OceanGrid bootcamp
project.

## Data flow

```text
Python collectors
  -> local Bronze Parquet
  -> HDFS Bronze staging
  -> Spark Silver Parquet
  -> Spark Gold Iceberg
  -> Spark serving snapshot
  -> Flask + DuckDB API
  -> Nginx frontend heatmap
```

Bronze remains reproducible local source data. Before a distributed Silver job,
Bronze must be uploaded to HDFS because YARN executors cannot safely read files
that exist only on the driver host.

## Layout

```text
pipeline_iceberg/
  configs/              AOI, metric and runtime configuration
  contracts/            machine-readable dataset contracts
  jobs/                 Spark Silver, Gold and serving export jobs
  scripts/              operational shell entrypoints
  src/ocean_pipeline/   shared catalog and Flask API
```

## Runtime

- Python 3.12 for collection
- Hadoop 3.5.0
- Spark 3.5.8, Scala 2.12
- Apache Iceberg 1.11.0
- Java 17 (Iceberg 1.11 does not support Java 11)

Install Python dependencies:

```bash
python -m pip install -r pipeline_iceberg/requirements.lock
```

Copy and edit runtime configuration:

```bash
cp pipeline_iceberg/configs/runtime.env.example pipeline_iceberg/configs/runtime.env
```

Run in order:

```bash
bash pipeline_iceberg/scripts/run_bronze.sh
bash pipeline_iceberg/scripts/upload_bronze.sh
bash pipeline_iceberg/scripts/run_silver.sh
bash pipeline_iceberg/scripts/run_gold.sh
bash pipeline_iceberg/scripts/run_serving.sh
```

Gold uses Iceberg `HadoopCatalog` with its warehouse on HDFS. Gold remains the
authoritative analytical layer. A Spark job exports narrow, partitioned Parquet
snapshots to local versioned releases; Flask uses embedded DuckDB to query those
snapshots and never starts Spark inside an HTTP request. Trino is not used.

Gold produces:

- `ocean.gold_daily_grid_features`: daily 4 km complete AOI grid. Missing NASA
  display values are filled with same-grid, neighbor-grid, then AOI-window
  means; GFW missing activity rows are zero-filled.
- `ocean.gold_map_metric`: long frontend-serving table.
- `ocean.gold_dashboard_daily_metrics`: dashboard daily indicators for line/bar/KPI cards.
- `ocean.gold_dashboard_status_distribution`: dashboard status distribution for pie/stacked-bar charts.

Design and deployment:

- `docs/DATA_FLOW_DESIGN.md`
- `docs/DEPLOYMENT_K8S.md`

The serving grain is:

```text
event_date + aoi_id + product_id + metric_id + resolution_km + grid_id
```

Missing NASA display cells use the same-grid trailing mean, eight-neighbor
mean, and AOI-wide trailing mean for the configured window (5 days by
default). If a final display value still cannot be filled, the Gold job fails
instead of writing a partial frontend partition. GFW cells without activity rows
are zero-filled only after the Silver input for the requested dates has passed
its quality checks.

Silver writes one machine-readable quality report per AOI and dataset under:

```text
hdfs:///metadata/ocean/silver/<run_id>/<aoi_id>/<dataset>/
```

GFW Silver keeps `vessel_presence_count` as an activity-density proxy. It is a
sum of source `mmsi_present` counts and is not a globally de-duplicated vessel
count.
