# Project Structure

```text
project/
  pipeline_iceberg/       # Bronze → Silver → Gold Iceberg → Flask API
  frontend/               # Date/product/metric/AOI heatmap
  data/                   # Local source data; ignored by Git
  docs/                   # Presentation and project documents
  h3.ipynb                # Spatial exploration notebook
  agents.md               # Collaboration rules
```

## Production pipeline

### `pipeline_iceberg`

```text
collectors/               NASA/GFW collection and Parquet conversion
jobs/                     Bronze orchestration, Silver, Gold, serving, maintenance
src/ocean_pipeline/       Shared catalog and Flask API
configs/                  AOI, metrics and runtime examples
contracts/                Dataset contracts
docs/                     Data-flow and Kubernetes deployment guides
deploy/kubernetes/        ConfigMap, Jobs, Flask/frontend Deployments, CronJob
deploy/nginx/             Frontend reverse proxy and runtime API configuration
scripts/                  Operational entrypoints
```

This is the only supported pipeline. Raw and Bronze remain replayable Parquet,
Silver uses partitioned Parquet for high-throughput transformation, and Gold
uses Iceberg for atomic analytical-table updates. Spark exports a narrow
versioned snapshot that Flask queries through DuckDB; Trino is not used.
