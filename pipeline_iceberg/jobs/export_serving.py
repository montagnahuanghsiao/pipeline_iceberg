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
                "metric_value",
                "relative_score",
                "display_level",
                "value_source",
                "data_coverage",
                "updated_at_utc",
                "pipeline_version",
            )
        )
        dashboard_daily = (
            spark.table(f"{namespace}.gold_dashboard_daily_metrics")
            .where(date_filter)
            .select(
                "event_date",
                "aoi_id",
                "resolution_km",
                "cell_count",
                "data_coverage",
                "chlor_a_avg",
                "sea_temperature_avg",
                "ocean_productivity_avg",
                "sustainability_pressure_avg",
                "sustainability_pressure_p90",
                "fishing_hours_total",
                "active_cell_ratio",
                "high_activity_cell_ratio",
                "high_productivity_cell_ratio",
                "high_pressure_cell_ratio",
                "share_of_all_fishing_hours",
                "fishing_hours_7d_avg",
                "sustainability_pressure_7d_avg",
                "chlor_a_score_avg",
                "sea_temperature_score_avg",
                "ocean_productivity_score_avg",
                "sustainability_pressure_score_avg",
                "fishing_hours_score_avg",
                "updated_at_utc",
                "pipeline_version",
            )
        )
        status_distribution = (
            spark.table(f"{namespace}.gold_dashboard_status_distribution")
            .where(date_filter)
            .select(
                "event_date",
                "aoi_id",
                "resolution_km",
                "status_class",
                "cell_count",
                "cell_ratio",
                "fishing_hours_total",
                "productivity_score_avg",
                "fishing_score_avg",
                "updated_at_utc",
                "pipeline_version",
            )
        )
        if (
            map_metric.limit(1).count() == 0
            or dashboard_daily.limit(1).count() == 0
            or status_distribution.limit(1).count() == 0
        ):
            raise RuntimeError("Gold query returned no rows for the requested date range")

        root = options.serving_root.rstrip("/")
        write_snapshot(
            map_metric,
            f"{root}/gold_map_metric",
            options.max_records_per_file,
        )
        write_snapshot(
            dashboard_daily,
            f"{root}/gold_dashboard_daily_metrics",
            options.max_records_per_file,
        )
        write_snapshot(
            status_distribution,
            f"{root}/gold_dashboard_status_distribution",
            options.max_records_per_file,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
