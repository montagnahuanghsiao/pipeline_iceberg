"""Incremental Gold Iceberg features, multi-resolution map cells and summaries."""
from __future__ import annotations

import argparse
from datetime import date
from functools import reduce

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window, functions as F

KEYS = ["event_date", "aoi_id", "grid_id", "grid_row", "grid_col"]
MAP_KEYS = ["event_date", "aoi_id", "product_id", "metric_id", "resolution_km", "grid_id"]


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--silver-root", required=True)
    p.add_argument("--catalog", default="lake")
    p.add_argument("--namespace", default="ocean")
    p.add_argument("--aoi-id", required=True)
    p.add_argument("--start-date", required=True, type=date.fromisoformat)
    p.add_argument("--end-date", required=True, type=date.fromisoformat)
    return p.parse_args()


def percentile(df: DataFrame, source: str, target: str) -> DataFrame:
    rank = Window.partitionBy("event_date", "aoi_id").orderBy(F.col(source))
    ranked = df.where(F.col(source).isNotNull()).select(*KEYS, source).withColumn(target, F.cume_dist().over(rank) * 100).select(*KEYS, target)
    return df.join(ranked, KEYS, "left")


def table_exists(spark: SparkSession, table: str) -> bool:
    try:
        spark.table(table).limit(0).collect()
        return True
    except Exception as exc:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(exc) or "NoSuchTable" in str(exc):
            return False
        raise


def write_iceberg(df: DataFrame, table: str, partitions: list[str]) -> None:
    sort_columns = partitions + [name for name in ("product_id", "metric_id", "grid_id") if name in df.columns]
    ordered = df.sortWithinPartitions(*sort_columns)
    if table_exists(df.sparkSession, table):
        ordered.writeTo(table).overwritePartitions()
    else:
        (
            ordered.writeTo(table).using("iceberg").partitionedBy(*partitions)
            .tableProperty("format-version", "2")
            .tableProperty("write.format.default", "parquet")
            .tableProperty("write.parquet.compression-codec", "zstd")
            .tableProperty("write.target-file-size-bytes", "268435456")
            .tableProperty("write.distribution-mode", "range")
            .create()
        )


def long_metrics(wide: DataFrame) -> DataFrame:
    specs = [
        ("COMBINED", "potential_fishing_score"), ("COMBINED", "productivity_score"),
        ("GFW", "fishing_hours"), ("CHL", "chlor_a"), ("POC", "poc"),
        ("NFLH", "nflh"), ("SST", "precision_sst"),
    ]
    frames = [
        wide.select(*KEYS, F.lit(product).alias("product_id"), F.lit(metric).alias("metric_id"),
                    F.col(metric).alias("raw_metric_value"), "data_coverage", "updated_at_utc", "pipeline_version")
        .where(F.col(metric).isNotNull())
        for product, metric in specs
    ]
    return reduce(DataFrame.unionByName, frames)


def resolution_frame(fine: DataFrame, resolution_km: int) -> DataFrame:
    factor = resolution_km // 4
    staged = (
        fine.withColumn("resolution_km", F.lit(resolution_km))
        .withColumn("grid_row", (F.floor(F.col("grid_row") / factor) * factor).cast("short"))
        .withColumn("grid_col", (F.floor(F.col("grid_col") / factor) * factor).cast("short"))
    )
    aggregated = (
        staged.groupBy("event_date", "aoi_id", "product_id", "metric_id", "resolution_km", "grid_row", "grid_col")
        .agg(
            F.sum("raw_metric_value").alias("_sum_value"), F.avg("raw_metric_value").alias("_avg_value"),
            F.avg("data_coverage").alias("data_coverage"), F.max("updated_at_utc").alias("updated_at_utc"),
            F.max("pipeline_version").alias("pipeline_version"),
        )
        .withColumn("raw_metric_value", F.when(F.col("product_id") == "GFW", F.col("_sum_value")).otherwise(F.col("_avg_value")))
        .drop("_sum_value", "_avg_value")
        .withColumn("grid_id", F.format_string("r%02dkm_%04d_%04d", F.col("resolution_km"), F.col("grid_row"), F.col("grid_col")))
    )
    return aggregated


def add_relative_score(frame: DataFrame) -> DataFrame:
    """Rank every displayed metric inside its date/AOI/metric/resolution scope."""
    scope = ["event_date", "aoi_id", "product_id", "metric_id", "resolution_km"]
    window = Window.partitionBy(*scope).orderBy(F.col("raw_metric_value"))
    count_window = Window.partitionBy(*scope)
    rankable = frame.where(
        F.col("raw_metric_value").isNotNull()
        & ((F.col("product_id") != "GFW") | (F.col("raw_metric_value") > 0))
    )
    ranked = (
        rankable.select(*MAP_KEYS, "raw_metric_value")
        .withColumn(
            "relative_score",
            F.when(F.count("*").over(count_window) == 1, F.lit(100.0))
            .otherwise(F.percent_rank().over(window) * F.lit(100.0)),
        )
        .select(*MAP_KEYS, "relative_score")
    )
    result = frame.join(ranked, MAP_KEYS, "left")
    result = result.withColumn(
        "relative_score",
        F.when(
            (F.col("product_id") == "GFW") & (F.coalesce(F.col("raw_metric_value"), F.lit(0.0)) <= 0),
            F.lit(0.0),
        ).otherwise(F.col("relative_score")),
    )
    return result.withColumn(
        "display_level",
        F.when(F.col("relative_score").isNull(), F.lit("no_data"))
        .when(F.col("relative_score") >= 80, F.lit("very_high"))
        .when(F.col("relative_score") >= 60, F.lit("high"))
        .when(F.col("relative_score") >= 40, F.lit("medium"))
        .when(F.col("relative_score") >= 20, F.lit("low"))
        .otherwise(F.lit("very_low")),
    )


def main():
    options = parse_args()
    spark = SparkSession.builder.appName("ocean-gold-incremental").getOrCreate()
    namespace = f"{options.catalog}.{options.namespace}"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    root = f"{options.silver_root.rstrip('/')}/{options.aoi_id}"
    date_filter = F.col("event_date").between(F.lit(options.start_date), F.lit(options.end_date))
    try:
        nasa = spark.read.parquet(f"{root}/nasa_daily_grid").where(date_filter)
        gfw = spark.read.parquet(f"{root}/gfw_daily_grid").where(date_filter)
        wide = nasa.join(gfw.select(*KEYS, "presence_hours", "fishing_hours"), KEYS, "left")
        wide = wide.withColumn("precision_sst", F.coalesce("sst4", "nsst", "sst"))
        for source, target in (("chlor_a", "chl_pct"), ("poc", "poc_pct"), ("nflh", "nflh_pct")):
            wide = percentile(wide, source, target)
        wide = (
            wide.withColumn("productivity_component_count", sum(F.when(F.col(c).isNotNull(), 1).otherwise(0) for c in ("chl_pct", "poc_pct", "nflh_pct")))
            .withColumn("productivity_score", F.when(F.col("productivity_component_count") >= 2, sum(F.coalesce(F.col(c), F.lit(0.0)) for c in ("chl_pct", "poc_pct", "nflh_pct")) / F.col("productivity_component_count")))
            .withColumn("potential_fishing_score", F.col("productivity_score"))
            .withColumn("data_coverage", sum(F.when(F.col(c).isNotNull(), 1).otherwise(0) for c in ("chlor_a", "poc", "nflh", "precision_sst")) / 4)
            .withColumn("updated_at_utc", F.current_timestamp()).withColumn("pipeline_version", F.lit("0.3.0"))
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        wide.count()
        write_iceberg(wide, f"{namespace}.gold_daily_grid_features", ["event_date", "aoi_id"])
        fine = long_metrics(wide)
        serving = add_relative_score(
            reduce(DataFrame.unionByName, [resolution_frame(fine, km) for km in (4, 16, 32)])
        )
        write_iceberg(serving, f"{namespace}.gold_map_metric", ["event_date", "aoi_id", "resolution_km"])
        summary = serving.groupBy("event_date", "aoi_id", "product_id", "metric_id", "resolution_km").agg(
            F.avg("relative_score").alias("average_score"), F.max("relative_score").alias("maximum_score"),
            F.count("*").alias("cell_count"), F.avg("data_coverage").alias("data_coverage"),
        )
        write_iceberg(summary, f"{namespace}.gold_daily_metric_summary", ["event_date", "aoi_id", "resolution_km"])
        wide.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
