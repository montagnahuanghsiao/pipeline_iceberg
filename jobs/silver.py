"""Incremental high-throughput Silver build on HDFS Parquet."""
from __future__ import annotations

import argparse
import json
from datetime import date
from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F

PRODUCT_COLUMNS = {"CHL": "chlor_a", "NFLH": "nflh", "POC": "poc", "SST": "sst", "NSST": "sst", "SST4": "sst4"}
RANGES = {"CHL": (0.000999, 100.0), "NFLH": (-5.0, 50.0), "POC": (0.0, 5000.0), "SST": (-2.0, 45.0), "NSST": (-2.0, 45.0), "SST4": (-2.0, 45.0)}
KEYS = ["event_date", "aoi_id", "grid_id", "grid_row", "grid_col"]


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--bronze-root", required=True)
    p.add_argument("--silver-root", required=True)
    p.add_argument("--aoi-id", required=True)
    p.add_argument("--aoi-config", required=True)
    p.add_argument("--start-date", required=True, type=date.fromisoformat)
    p.add_argument("--end-date", required=True, type=date.fromisoformat)
    p.add_argument("--write-shards", type=int, default=32)
    p.add_argument("--max-records-per-file", type=int, default=2_000_000)
    return p.parse_args()


def with_grid(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("grid_row", F.floor((F.lit(90.0) - F.col("lat")) * 24).cast("short"))
        .withColumn("grid_col", F.floor((F.col("lon") + F.lit(180.0)) * 24).cast("short"))
        .withColumn("grid_id", F.format_string("nasa4km_r%04d_c%04d", F.col("grid_row"), F.col("grid_col")))
    )


def incremental(df: DataFrame, options) -> DataFrame:
    return df.where(F.col("event_date").between(F.lit(options.start_date), F.lit(options.end_date)))


def write_partitioned(df: DataFrame, target: str, options) -> None:
    staged = df.withColumn("_write_shard", F.pmod(F.xxhash64("grid_id"), F.lit(options.write_shards)))
    (
        staged.repartition(options.write_shards, "event_date", "_write_shard")
        .drop("_write_shard")
        .write.mode("overwrite")
        .option("compression", "zstd")
        .option("maxRecordsPerFile", options.max_records_per_file)
        .partitionBy("event_date")
        .parquet(target)
    )


def main():
    options = parse_args()
    if options.end_date < options.start_date:
        raise ValueError("end-date must not precede start-date")
    spark = (
        SparkSession.builder.appName("ocean-silver-incremental")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    aoi = json.loads(Path(options.aoi_config).read_text(encoding="utf-8"))[options.aoi_id]
    try:
        nasa_long = []
        for product, value_col in PRODUCT_COLUMNS.items():
            raw = spark.read.option("recursiveFileLookup", "true").parquet(f"{options.bronze_root.rstrip('/')}/{product}")
            selected = raw.select(
                F.to_date("date").alias("event_date"), F.col("lat").cast("double").alias("lat"),
                F.col("lon").cast("double").alias("lon"), F.col(value_col).cast("double").alias("metric_value"),
                *([F.col("quality_level").cast("int")] if "quality_level" in raw.columns else []),
            )
            clean = (
                incremental(selected, options)
                .where(F.col("lat").between(aoi["min_lat"], aoi["max_lat"]))
                .where(F.col("lon").between(aoi["min_lon"], aoi["max_lon"]))
                .where(F.col("metric_value").between(*RANGES[product]))
            )
            if "quality_level" in clean.columns and product in {"SST", "NSST", "SST4"}:
                clean = clean.where(F.col("quality_level").isNotNull() & (F.col("quality_level") <= 1))
            nasa_long.append(
                with_grid(clean).select("event_date", "grid_id", "grid_row", "grid_col", F.lit(product).alias("product"), "metric_value")
            )

        conformed = reduce(DataFrame.unionByName, nasa_long)
        nasa = (
            conformed.groupBy("event_date", "grid_id", "grid_row", "grid_col")
            .pivot("product", list(PRODUCT_COLUMNS))
            .agg(F.avg("metric_value"))
            .select(
                "event_date", F.lit(options.aoi_id).alias("aoi_id"), "grid_id", "grid_row", "grid_col",
                F.col("CHL").alias("chlor_a"), F.col("POC").alias("poc"), F.col("NFLH").alias("nflh"),
                F.col("SST").alias("sst"), F.col("NSST").alias("nsst"), F.col("SST4").alias("sst4"),
            )
        )
        write_partitioned(nasa, f"{options.silver_root.rstrip('/')}/{options.aoi_id}/nasa_daily_grid", options)

        raw_gfw = spark.read.option("recursiveFileLookup", "true").parquet(f"{options.bronze_root.rstrip('/')}/GFW")
        effort = incremental(
            raw_gfw.select(
                F.to_date("date").alias("event_date"), (F.col("cell_ll_lat") + .005).alias("lat"),
                (F.col("cell_ll_lon") + .005).alias("lon"), F.col("hours").cast("double"),
                F.col("fishing_hours").cast("double"),
            ),
            options,
        )
        effort = (
            with_grid(
                effort.where(F.col("lat").between(aoi["min_lat"], aoi["max_lat"]))
                .where(F.col("lon").between(aoi["min_lon"], aoi["max_lon"]))
                .where((F.col("hours") >= 0) & (F.col("fishing_hours") >= 0) & (F.col("fishing_hours") <= F.col("hours")))
            )
            .groupBy("event_date", "grid_id", "grid_row", "grid_col")
            .agg(F.sum("hours").alias("presence_hours"), F.sum("fishing_hours").alias("fishing_hours"))
            .withColumn("aoi_id", F.lit(options.aoi_id))
            .select(*KEYS, "presence_hours", "fishing_hours")
        )
        write_partitioned(effort, f"{options.silver_root.rstrip('/')}/{options.aoi_id}/gfw_daily_grid", options)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
