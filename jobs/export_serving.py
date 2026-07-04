"""Export narrow, partitioned Parquet snapshots from Gold Iceberg for Flask."""
from __future__ import annotations

import argparse
from datetime import date

from pyspark.sql import DataFrame, SparkSession, functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="lake")
    parser.add_argument("--namespace", default="ocean")
    parser.add_argument("--serving-root", required=True)
    parser.add_argument("--start-date", required=True, type=date.fromisoformat)
    parser.add_argument("--end-date", required=True, type=date.fromisoformat)
    parser.add_argument("--max-records-per-file", type=int, default=2_000_000)
    return parser.parse_args()


def write_snapshot(df: DataFrame, target: str, max_records_per_file: int) -> None:
    (
        df.repartition("event_date", "aoi_id", "resolution_km")
        .write.mode("overwrite")
        .option("compression", "zstd")
        .option("maxRecordsPerFile", max_records_per_file)
        .partitionBy("event_date", "aoi_id", "resolution_km")
        .parquet(target)
    )


def main() -> None:
    options = parse_args()
    if options.end_date < options.start_date:
        raise ValueError("end-date must not precede start-date")

    spark = (
        SparkSession.builder.appName("ocean-serving-export")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    namespace = f"{options.catalog}.{options.namespace}"
    date_filter = F.col("event_date").between(
        F.lit(options.start_date), F.lit(options.end_date)
    )
    try:
        map_metric = (
            spark.table(f"{namespace}.gold_map_metric")
            .where(date_filter)
            .select(
                "event_date",
                "aoi_id",
                "product_id",
                "metric_id",
                "resolution_km",
                "grid_id",
                "grid_row",
                "grid_col",
                "raw_metric_value",
                "relative_score",
                "display_level",
                "data_coverage",
                "updated_at_utc",
                "pipeline_version",
            )
        )
        summary = (
            spark.table(f"{namespace}.gold_daily_metric_summary")
            .where(date_filter)
            .select(
                "event_date",
                "aoi_id",
                "product_id",
                "metric_id",
                "resolution_km",
                "average_score",
                "maximum_score",
                "cell_count",
                "data_coverage",
            )
        )
        if map_metric.limit(1).count() == 0 or summary.limit(1).count() == 0:
            raise RuntimeError("Gold query returned no rows for the requested date range")

        root = options.serving_root.rstrip("/")
        write_snapshot(
            map_metric,
            f"{root}/gold_map_metric",
            options.max_records_per_file,
        )
        write_snapshot(
            summary,
            f"{root}/gold_daily_metric_summary",
            options.max_records_per_file,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
