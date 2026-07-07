"""Incremental high-throughput Silver build on HDFS Parquet."""
from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timezone
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
    p.add_argument("--metadata-root", required=True)
    p.add_argument("--run-id", required=True)
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


def write_quality_report(
    df: DataFrame,
    dataset: str,
    metric_columns: list[str],
    required_metric_groups: list[list[str]],
    source_rows: int,
    options,
) -> None:
    """Validate one Silver output and persist a compact machine-readable report."""
    key_null = reduce(
        lambda left, right: left | right,
        [F.col(column).isNull() for column in KEYS],
    )
    aggregations = [
        F.count("*").alias("output_rows"),
        F.countDistinct(F.struct(*[F.col(column) for column in KEYS])).alias(
            "distinct_key_rows"
        ),
        F.sum(F.when(key_null, 1).otherwise(0)).alias("null_key_rows"),
        F.min("event_date").alias("min_event_date"),
        F.max("event_date").alias("max_event_date"),
        F.countDistinct("event_date").alias("observed_day_count"),
    ]
    for column in metric_columns:
        invalid = F.col(column).isNull() | F.isnan(F.col(column).cast("double"))
        aggregations.extend(
            [
                F.sum(F.when(invalid, 1).otherwise(0)).alias(
                    f"{column}_null_or_nan_rows"
                ),
                F.min(column).alias(f"{column}_min"),
                F.max(column).alias(f"{column}_max"),
            ]
        )

    stats = df.agg(*aggregations).first().asDict()
    output_rows = int(stats["output_rows"] or 0)
    distinct_rows = int(stats["distinct_key_rows"] or 0)
    duplicate_rows = output_rows - distinct_rows
    null_key_rows = int(stats["null_key_rows"] or 0)
    expected_day_count = (options.end_date - options.start_date).days + 1
    observed_day_count = int(stats["observed_day_count"] or 0)
    metrics_with_values = {
        column
        for column in metric_columns
        if output_rows - int(stats[f"{column}_null_or_nan_rows"] or 0) > 0
    }
    required_metric_groups_passed = all(
        any(column in metrics_with_values for column in group)
        for group in required_metric_groups
    )
    valid = (
        output_rows > 0
        and duplicate_rows == 0
        and null_key_rows == 0
        and observed_day_count == expected_day_count
        and required_metric_groups_passed
    )
    report = {
        "run_id": options.run_id,
        "dataset": dataset,
        "aoi_id": options.aoi_id,
        "start_date": options.start_date.isoformat(),
        "end_date": options.end_date.isoformat(),
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "passed" if valid else "failed",
        "source_rows_after_cleaning": int(source_rows),
        "output_rows": output_rows,
        "distinct_key_rows": distinct_rows,
        "duplicate_key_rows": duplicate_rows,
        "null_key_rows": null_key_rows,
        "expected_day_count": expected_day_count,
        "observed_day_count": observed_day_count,
        "missing_day_count": expected_day_count - observed_day_count,
        "required_metric_groups": required_metric_groups,
        "required_metric_groups_passed": required_metric_groups_passed,
        "min_event_date": (
            stats["min_event_date"].isoformat() if stats["min_event_date"] else None
        ),
        "max_event_date": (
            stats["max_event_date"].isoformat() if stats["max_event_date"] else None
        ),
        "metrics": {
            column: {
                "null_or_nan_rows": int(
                    stats[f"{column}_null_or_nan_rows"] or 0
                ),
                "min": stats[f"{column}_min"],
                "max": stats[f"{column}_max"],
            }
            for column in metric_columns
        },
    }
    report_json = json.dumps(report, ensure_ascii=False, sort_keys=True)
    report_path = (
        f"{options.metadata_root.rstrip('/')}/silver/{options.run_id}/"
        f"{options.aoi_id}/{dataset}"
    )
    (
        df.sparkSession.createDataFrame([(report_json,)], ["value"])
        .coalesce(1)
        .write.mode("overwrite")
        .text(report_path)
    )
    print(f"QUALITY {report_json}")
    if not valid:
        raise RuntimeError(
            f"Silver quality check failed for {dataset}: "
            f"rows={output_rows}, duplicates={duplicate_rows}, "
            f"null_keys={null_key_rows}, "
            f"days={observed_day_count}/{expected_day_count}, "
            f"required_metrics={required_metric_groups_passed}"
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
        nasa_source_rows = conformed.count()
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
        write_quality_report(
            nasa,
            "nasa_daily_grid",
            ["chlor_a", "poc", "nflh", "sst", "nsst", "sst4"],
            [["chlor_a"], ["poc"], ["nflh"], ["sst", "nsst", "sst4"]],
            nasa_source_rows,
            options,
        )
        write_partitioned(nasa, f"{options.silver_root.rstrip('/')}/{options.aoi_id}/nasa_daily_grid", options)

        raw_gfw = spark.read.option("recursiveFileLookup", "true").parquet(f"{options.bronze_root.rstrip('/')}/GFW")
        effort = incremental(
            raw_gfw.select(
                F.to_date("date").alias("event_date"), (F.col("cell_ll_lat") + .005).alias("lat"),
                (F.col("cell_ll_lon") + .005).alias("lon"), F.col("hours").cast("double"),
                F.col("fishing_hours").cast("double"),
                F.col("mmsi_present").cast("long"),
            ),
            options,
        )
        valid_effort = with_grid(
            effort.where(F.col("lat").between(aoi["min_lat"], aoi["max_lat"]))
                .where(F.col("lon").between(aoi["min_lon"], aoi["max_lon"]))
                .where(
                    (F.col("hours") >= 0)
                    & (F.col("fishing_hours") >= 0)
                    & (F.col("fishing_hours") <= F.col("hours"))
                    & (F.col("mmsi_present") >= 0)
                )
        )
        gfw_source_rows = valid_effort.count()
        effort = (
            valid_effort
            .groupBy("event_date", "grid_id", "grid_row", "grid_col")
            .agg(
                F.sum("hours").alias("presence_hours"),
                F.sum("fishing_hours").alias("fishing_hours"),
                F.sum("mmsi_present").cast("long").alias("vessel_presence_count"),
            )
            .withColumn("aoi_id", F.lit(options.aoi_id))
            .select(
                *KEYS,
                "presence_hours",
                "fishing_hours",
                "vessel_presence_count",
            )
        )
        write_quality_report(
            effort,
            "gfw_daily_grid",
            ["presence_hours", "fishing_hours", "vessel_presence_count"],
            [["presence_hours"], ["fishing_hours"], ["vessel_presence_count"]],
            gfw_source_rows,
            options,
        )
        write_partitioned(effort, f"{options.silver_root.rstrip('/')}/{options.aoi_id}/gfw_daily_grid", options)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
