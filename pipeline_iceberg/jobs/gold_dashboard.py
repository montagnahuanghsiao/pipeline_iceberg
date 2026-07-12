"""Build dashboard-oriented Gold aggregate tables from map metrics.

This job intentionally starts from Gold map metrics, not Silver/Bronze, so the
dashboard branch stays separated from the raw/cleaning pipeline.  The output is
small, chart-friendly, and scoped to recent analytical use cases such as
2024-to-latest dashboard cards.
"""
from __future__ import annotations

import argparse
from datetime import date

from pyspark.sql import DataFrame, SparkSession, Window, functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="lake")
    parser.add_argument("--namespace", default="ocean")
    parser.add_argument("--start-date", default="2024-01-01", type=date.fromisoformat)
    parser.add_argument("--end-date", required=True, type=date.fromisoformat)
    return parser.parse_args()


def table_exists(spark: SparkSession, table: str) -> bool:
    try:
        spark.table(table).limit(0).collect()
        return True
    except Exception as exc:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(exc) or "NoSuchTable" in str(exc):
            return False
        raise


def write_iceberg(
    df: DataFrame,
    table: str,
    partitions: list[str],
    overwrite_filter=None,
) -> None:
    ordered = df.sortWithinPartitions(*partitions)
    if table_exists(df.sparkSession, table):
        if overwrite_filter is not None:
            # Replace only the requested date range, including removal of stale
            # status partitions for dates whose derived metrics are unavailable.
            ordered.writeTo(table).overwrite(overwrite_filter)
        else:
            ordered.writeTo(table).overwritePartitions()
    else:
        (
            ordered.writeTo(table)
            .using("iceberg")
            .partitionedBy(*partitions)
            .tableProperty("format-version", "2")
            .tableProperty("write.format.default", "parquet")
            .tableProperty("write.parquet.compression-codec", "zstd")
            .tableProperty("write.target-file-size-bytes", "134217728")
            .tableProperty("write.distribution-mode", "range")
            .create()
        )


def metric_value(metric_id: str, value_column: str = "metric_value"):
    return F.when(F.col("metric_id") == metric_id, F.col(value_column))


def metric_score(metric_id: str):
    return F.when(F.col("metric_id") == metric_id, F.col("relative_score"))


def build_daily_metrics(map_metric: DataFrame) -> DataFrame:
    base = (
        map_metric.groupBy("event_date", "aoi_id", "resolution_km")
        .agg(
            F.countDistinct("grid_id").alias("cell_count"),
            F.avg("data_coverage").alias("data_coverage"),
            F.avg(metric_value("chlor_a")).alias("chlor_a_avg"),
            F.avg(metric_value("sea_temperature")).alias("sea_temperature_avg"),
            F.avg(metric_value("ocean_productivity_score")).alias(
                "ocean_productivity_avg"
            ),
            F.avg(metric_value("sustainability_pressure")).alias(
                "sustainability_pressure_avg"
            ),
            F.expr(
                "percentile_approx("
                "CASE WHEN metric_id = 'sustainability_pressure' "
                "THEN metric_value END, 0.90, 1000)"
            ).alias("sustainability_pressure_p90"),
            F.sum(metric_value("fishing_hours")).alias("fishing_hours_total"),
            F.avg(
                F.when(
                    (F.col("metric_id") == "fishing_hours")
                    & (F.col("metric_value") > 0),
                    F.lit(1.0),
                )
                .when(F.col("metric_id") == "fishing_hours", F.lit(0.0))
            ).alias("active_cell_ratio"),
            F.avg(
                F.when(
                    (F.col("metric_id") == "fishing_hours")
                    & (F.col("relative_score") >= 80),
                    F.lit(1.0),
                )
                .when(F.col("metric_id") == "fishing_hours", F.lit(0.0))
            ).alias("high_activity_cell_ratio"),
            F.avg(
                F.when(
                    (F.col("metric_id") == "ocean_productivity_score")
                    & (F.col("relative_score") >= 80),
                    F.lit(1.0),
                )
                .when(F.col("metric_id") == "ocean_productivity_score", F.lit(0.0))
            ).alias("high_productivity_cell_ratio"),
            F.avg(
                F.when(
                    (F.col("metric_id") == "sustainability_pressure")
                    & (F.col("relative_score") >= 80),
                    F.lit(1.0),
                )
                .when(F.col("metric_id") == "sustainability_pressure", F.lit(0.0))
            ).alias("high_pressure_cell_ratio"),
            F.avg(metric_score("chlor_a")).alias("chlor_a_score_avg"),
            F.avg(metric_score("sea_temperature")).alias("sea_temperature_score_avg"),
            F.avg(metric_score("ocean_productivity_score")).alias(
                "ocean_productivity_score_avg"
            ),
            F.avg(metric_score("sustainability_pressure")).alias(
                "sustainability_pressure_score_avg"
            ),
            F.avg(metric_score("fishing_hours")).alias("fishing_hours_score_avg"),
        )
        .fillna(
            {
                "fishing_hours_total": 0.0,
                "active_cell_ratio": 0.0,
                "high_activity_cell_ratio": 0.0,
            }
        )
    )
    all_aoi = Window.partitionBy("event_date", "resolution_km")
    trend = (
        Window.partitionBy("aoi_id", "resolution_km")
        .orderBy(F.col("event_date").cast("timestamp").cast("long"))
        .rangeBetween(-6 * 86400, 0)
    )
    return (
        base.withColumn(
            "share_of_all_fishing_hours",
            F.when(
                F.sum("fishing_hours_total").over(all_aoi) > 0,
                F.col("fishing_hours_total")
                / F.sum("fishing_hours_total").over(all_aoi),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn("fishing_hours_7d_avg", F.avg("fishing_hours_total").over(trend))
        .withColumn(
            "sustainability_pressure_7d_avg",
            F.avg("sustainability_pressure_avg").over(trend),
        )
        .withColumn(
            "updated_at_utc",
            F.current_timestamp(),
        )
        .withColumn("pipeline_version", F.lit("0.5.1"))
    )


def build_status_distribution(map_metric: DataFrame) -> DataFrame:
    productivity = (
        map_metric.where(F.col("metric_id") == "ocean_productivity_score")
        .select(
            "event_date",
            "aoi_id",
            "resolution_km",
            "grid_id",
            F.col("relative_score").alias("productivity_score"),
        )
    )
    fishing = (
        map_metric.where(F.col("metric_id") == "fishing_hours")
        .select(
            "event_date",
            "aoi_id",
            "resolution_km",
            "grid_id",
            F.col("relative_score").alias("fishing_score"),
            F.col("metric_value").alias("fishing_hours"),
        )
    )
    classified = (
        productivity.join(
            fishing,
            ["event_date", "aoi_id", "resolution_km", "grid_id"],
            "inner",
        )
        .withColumn("is_high_productivity", F.col("productivity_score") >= 60)
        .withColumn(
            "is_high_fishing",
            (F.col("fishing_score") >= 60) & (F.col("fishing_hours") > 0),
        )
        .withColumn(
            "status_class",
            F.when(
                F.col("is_high_productivity") & F.col("is_high_fishing"),
                F.lit("high_productivity_high_fishing"),
            )
            .when(
                F.col("is_high_productivity") & ~F.col("is_high_fishing"),
                F.lit("high_productivity_low_fishing"),
            )
            .when(
                ~F.col("is_high_productivity") & F.col("is_high_fishing"),
                F.lit("low_productivity_high_fishing"),
            )
            .otherwise(F.lit("low_productivity_low_fishing")),
        )
    )
    total = Window.partitionBy("event_date", "aoi_id", "resolution_km")
    return (
        classified.groupBy("event_date", "aoi_id", "resolution_km", "status_class")
        .agg(
            F.count("*").alias("cell_count"),
            F.sum("fishing_hours").alias("fishing_hours_total"),
            F.avg("productivity_score").alias("productivity_score_avg"),
            F.avg("fishing_score").alias("fishing_score_avg"),
        )
        .withColumn("cell_ratio", F.col("cell_count") / F.sum("cell_count").over(total))
        .withColumn("updated_at_utc", F.current_timestamp())
        .withColumn("pipeline_version", F.lit("0.5.1"))
    )


def main() -> None:
    options = parse_args()
    if options.end_date < options.start_date:
        raise ValueError("end-date must not precede start-date")
    spark = SparkSession.builder.appName("ocean-gold-dashboard").getOrCreate()
    namespace = f"{options.catalog}.{options.namespace}"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    try:
        date_filter = F.col("event_date").between(
            F.lit(options.start_date), F.lit(options.end_date)
        )
        map_metric = spark.table(f"{namespace}.gold_map_metric").where(date_filter)
        if map_metric.limit(1).count() == 0:
            raise RuntimeError("gold_map_metric has no rows for dashboard date range")
        daily = build_daily_metrics(map_metric)
        status = build_status_distribution(map_metric)
        write_iceberg(
            daily,
            f"{namespace}.gold_dashboard_daily_metrics",
            ["event_date", "aoi_id", "resolution_km"],
            date_filter,
        )
        write_iceberg(
            status,
            f"{namespace}.gold_dashboard_status_distribution",
            ["event_date", "aoi_id", "resolution_km"],
            date_filter,
        )
        print(
            "GOLD_DASHBOARD "
            f"start={options.start_date} end={options.end_date} status=success"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
