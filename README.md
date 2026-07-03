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
  -> Trino-compatible API
  -> frontend heatmap
```

Bronze remains reproducible local source data. Before a distributed Silver job,
Bronze must be uploaded to HDFS because YARN executors cannot safely read files
that exist only on the driver host.

## Layout

```text
pipeline_iceberg/
  configs/              AOI, metric and runtime configuration
  contracts/            machine-readable dataset contracts
  jobs/                 Spark Silver and Gold jobs
  scripts/              operational shell entrypoints
  src/ocean_pipeline/   shared Python and API modules
  tests/                unit tests independent of a live cluster
```

## Runtime

- Python 3.12 for collection and API
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
uvicorn ocean_pipeline.api:app --app-dir pipeline_iceberg/src --host 0.0.0.0 --port 8000
```

Gold produces:

- `ocean.gold_daily_grid_features`: wide calculation table.
- `ocean.gold_map_metric`: long frontend-serving table.
- `ocean.gold_daily_metric_summary`: daily relative-score summary table.

Design and deployment:

- `docs/DATA_FLOW_DESIGN.md`
- `docs/DEPLOYMENT_K8S.md`

The serving grain is:

```text
event_date + aoi_id + product_id + metric_id + grid_id
```

`potential_fishing_score` is an explainable environmental suitability proxy.
It is not a fish-catch prediction and must not be presented as operational
fishing advice without catch labels and validation.
