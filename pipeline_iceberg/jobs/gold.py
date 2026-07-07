"""Build observed-ocean-grid Gold features and percentile heatmaps.

Bronze and Silver keep observed values. This job uses grid cells that occur in
NASA Silver during the processing window and fills missing observations in this
order:

1. observed value for the requested date;
2. trailing mean for the same grid cell;
3. trailing mean of the eight surrounding grid cells;
4. null with an explicit ``no_data`` source.

GFW cells without an activity row are zero-filled.  All five frontend metrics
with values are converted to 0-100 percentile scores within
date/AOI/metric/resolution. Missing NASA-derived metrics remain unscored.
"""
from __future__ import annotations

import argparse
from datetime import date, timedelta
from functools import reduce

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window, functions as F

GRID_KM = 4
KEYS = ["event_date", "aoi_id", "grid_id", "grid_row", "grid_col"]
MAP_KEYS = [
    "event_date",
    "aoi_id",
    "product_id",
    "metric_id",
    "resolution_km",
    "grid_id",
]
PRODUCT_SPECS = (
    ("CHL", "chlor_a", "chlor_a"),
    ("SST", "sea_temperature", "sea_temperature_celsius"),
    ("PRODUCTIVITY", "ocean_productivity_score", "ocean_productivity_score"),
    ("SUSTAINABILITY", "sustainability_pressure", "sustainability_pressure"),
    ("GFW", "fishing_hours", "fishing_hours"),
)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver-root", required=True)
    parser.add_argument("--catalog", default="lake")
    parser.add_argument("--namespace", default="ocean")
    parser.add_argument("--aoi-id", required=True)
    parser.add_argument("--aoi-config", required=True)
    parser.add_argument("--start-date", required=True, type=date.fromisoformat)
    parser.add_argument("--end-date", required=True, type=date.fromisoformat)
    parser.add_argument("--fill-window-days", type=int, default=3)
    return parser.parse_args()


def table_exists(spark: SparkSession, table: str) -> bool:
    try:
        spark.table(table).limit(0).collect()
        return True
    except Exception as exc:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(exc) or "NoSuchTable" in str(exc):
            return False
        raise


def write_iceberg(df: DataFrame, table: str, partitions: list[str]) -> None:
    sort_columns = partitions + [
        name for name in ("product_id", "metric_id", "grid_id") if name in df.columns
    ]
    ordered = df.sortWithinPartitions(*sort_columns)
    if table_exists(df.sparkSession, table):
        ordered.writeTo(table).overwritePartitions()
    else:
        (
            ordered.writeTo(table)
            .using("iceberg")
            .partitionedBy(*partitions)
            .tableProperty("format-version", "2")
            .tableProperty("write.format.default", "parquet")
            .tableProperty("write.parquet.compression-codec", "zstd")
            .tableProperty("write.target-file-size-bytes", "268435456")
            .tableProperty("write.distribution-mode", "range")
            .create()
        )


def observed_grid_backbone(
    spark: SparkSession,
    aoi_id: str,
    observations: DataFrame,
    start_date: date,
    end_date: date,
) -> DataFrame:
    """Return every date x NASA grid cell observed in the processing window."""
    day_count = (end_date - start_date).days + 1
    dates = spark.range(day_count).select(
        F.date_add(F.lit(start_date), F.col("id").cast("int")).alias("event_date")
    )
    valid_grids = observations.select("grid_id", "grid_row", "grid_col").distinct()
    return (
        dates.crossJoin(valid_grids)
        .withColumn("aoi_id", F.lit(aoi_id))
        .select(*KEYS)
    )


def _neighbor_candidates(frame: DataFrame, value_column: str) -> DataFrame:
    """Average available adjacent cells only for targets still missing a value."""
    neighbors = [
        F.struct(
            (F.col("grid_row") + F.lit(row_offset))
            .cast("short")
            .alias("source_grid_row"),
            (F.col("grid_col") + F.lit(col_offset))
            .cast("short")
            .alias("source_grid_col"),
        )
        for row_offset in (-1, 0, 1)
        for col_offset in (-1, 0, 1)
        if (row_offset, col_offset) != (0, 0)
    ]
    missing_targets = (
        frame.where(F.col(value_column).isNull())
        .select(
            "event_date",
            "aoi_id",
            "grid_row",
            "grid_col",
            F.explode(F.array(*neighbors)).alias("source"),
        )
        .select(
            "event_date",
            "aoi_id",
            "grid_row",
            "grid_col",
            F.col("source.source_grid_row").alias("source_grid_row"),
            F.col("source.source_grid_col").alias("source_grid_col"),
        )
    )
    available_sources = frame.where(F.col(value_column).isNotNull()).select(
        "event_date",
        "aoi_id",
        F.col("grid_row").alias("source_grid_row"),
        F.col("grid_col").alias("source_grid_col"),
        value_column,
    )
    return (
        missing_targets.join(
            available_sources,
            [
                "event_date",
                "aoi_id",
                "source_grid_row",
                "source_grid_col",
            ],
            "inner",
        )
        .groupBy("event_date", "aoi_id", "grid_row", "grid_col")
        .agg(F.avg(value_column).alias("_neighbor_window_mean"))
    )


def fill_environment_metric(
    backbone: DataFrame,
    observations: DataFrame,
    source_column: str,
    target_column: str,
    fill_window_days: int,
) -> DataFrame:
    """Fill one NASA metric while retaining its display-value provenance."""
    raw_column = f"_{target_column}_observed"
    selected = observations.select(
        "event_date", "aoi_id", "grid_id", F.col(source_column).alias(raw_column)
    )
    joined = backbone.join(selected, ["event_date", "aoi_id", "grid_id"], "left")
    day_number = F.datediff(F.col("event_date"), F.lit("1970-01-01"))
    cell_window = (
        Window.partitionBy("aoi_id", "grid_id")
        .orderBy(day_number)
        .rangeBetween(-(fill_window_days - 1), 0)
    )
    enriched = joined.withColumn(
        "_grid_window_mean", F.avg(raw_column).over(cell_window)
    ).withColumn(
        "_grid_window_count", F.count(raw_column).over(cell_window)
    )

    neighbors = _neighbor_candidates(enriched, "_grid_window_mean")
    grid_source = f"grid_{fill_window_days}d_mean"
    neighbor_source = f"neighbor_{fill_window_days}d_mean"
    result = (
        enriched.join(
            neighbors,
            ["event_date", "aoi_id", "grid_row", "grid_col"],
            "left",
        )
        .withColumn(
            target_column,
            F.coalesce(
                F.col(raw_column),
                F.col("_grid_window_mean"),
                F.col("_neighbor_window_mean"),
            ),
        )
        .withColumn(
            f"{target_column}_source",
            F.when(F.col(raw_column).isNotNull(), F.lit("observed"))
            .when(F.col("_grid_window_mean").isNotNull(), F.lit(grid_source))
            .when(
                F.col("_neighbor_window_mean").isNotNull(),
                F.lit(neighbor_source),
            )
            .otherwise(F.lit("no_data")),
        )
        .withColumn(
            f"{target_column}_observation_count_window",
            F.coalesce(F.col("_grid_window_count"), F.lit(0)).cast("int"),
        )
        .select(
            *KEYS,
            target_column,
            f"{target_column}_source",
            f"{target_column}_observation_count_window",
        )
    )
    return result


def join_filled_metrics(
    backbone: DataFrame,
    nasa: DataFrame,
    gfw: DataFrame,
    fill_window_days: int,
) -> DataFrame:
    temperature = nasa.withColumn(
        "sea_temperature_celsius", F.coalesce("sst4", "nsst", "sst")
    )
    environmental = (
        ("chlor_a", "chlor_a"),
        ("poc", "poc"),
        ("nflh", "nflh"),
        ("sea_temperature_celsius", "sea_temperature_celsius"),
    )
    filled = [
        fill_environment_metric(
            backbone,
            temperature,
            source,
            target,
            fill_window_days,
        )
        for source, target in environmental
    ]
    wide = reduce(lambda left, right: left.join(right, KEYS, "inner"), filled)

    fishing = (
        backbone.join(
            gfw.select(*KEYS, "fishing_hours"),
            KEYS,
            "left",
        )
        .withColumn(
            "fishing_hours_source",
            F.when(F.col("fishing_hours").isNotNull(), F.lit("observed")).otherwise(
                F.lit("zero_filled")
            ),
        )
        .withColumn("fishing_hours", F.coalesce("fishing_hours", F.lit(0.0)))
        .select(*KEYS, "fishing_hours", "fishing_hours_source")
    )
    return wide.join(fishing, KEYS, "inner")


def add_derived_products(frame: DataFrame) -> DataFrame:
    """Apply the project-defined productivity and pressure formulas."""
    averages = frame.groupBy("event_date", "aoi_id").agg(
        F.avg("chlor_a").alias("_avg_chlor_a"),
        F.avg("poc").alias("_avg_poc"),
        F.avg("nflh").alias("_avg_nflh"),
    )
    joined = frame.join(averages, ["event_date", "aoi_id"], "left")

    def safe_ratio(numerator: str, denominator: str):
        return F.when(
            F.col(numerator).isNotNull()
            & F.col(denominator).isNotNull()
            & (F.abs(F.col(denominator)) > F.lit(1e-12)),
            F.col(numerator) / F.col(denominator),
        )

    productivity = (
        safe_ratio("chlor_a", "_avg_chlor_a")
        + safe_ratio("poc", "_avg_poc")
        + safe_ratio("nflh", "_avg_nflh")
    )
    base_sources = [
        "chlor_a_source",
        "poc_source",
        "nflh_source",
        "sea_temperature_celsius_source",
    ]
    observed_count = sum(
        (F.col(column) == F.lit("observed")).cast("int") for column in base_sources
    )
    return (
        joined.withColumn("ocean_productivity_score", productivity)
        .withColumn(
            "ocean_productivity_score_source",
            F.when(F.col("ocean_productivity_score").isNull(), F.lit("no_data"))
            .when(
                (F.col("chlor_a_source") == "observed")
                & (F.col("poc_source") == "observed")
                & (F.col("nflh_source") == "observed"),
                F.lit("observed"),
            ).otherwise(F.lit("derived_with_fill")),
        )
        .withColumn(
            "sustainability_pressure",
            F.when(F.col("fishing_hours") <= 0, F.lit(0.0)).otherwise(
                F.col("fishing_hours")
                / F.greatest(F.col("ocean_productivity_score"), F.lit(1e-9))
            ),
        )
        .withColumn(
            "sustainability_pressure_source",
            F.when(F.col("sustainability_pressure").isNull(), F.lit("no_data"))
            .when(
                (F.col("fishing_hours_source") == "observed")
                & (F.col("ocean_productivity_score_source") == "observed"),
                F.lit("observed"),
            ).otherwise(F.lit("derived_with_fill")),
        )
        .withColumn("data_coverage", observed_count / F.lit(4.0))
        .drop("_avg_chlor_a", "_avg_poc", "_avg_nflh")
    )


def resolution_wide(fine: DataFrame, resolution_km: int) -> DataFrame:
    factor = resolution_km // GRID_KM
    staged = (
        fine.withColumn("resolution_km", F.lit(resolution_km))
        .withColumn(
            "grid_row",
            (F.floor(F.col("grid_row") / factor) * factor).cast("short"),
        )
        .withColumn(
            "grid_col",
            (F.floor(F.col("grid_col") / factor) * factor).cast("short"),
        )
    )
    return (
        staged.groupBy(
            "event_date", "aoi_id", "resolution_km", "grid_row", "grid_col"
        )
        .agg(
            F.avg("chlor_a").alias("chlor_a"),
            F.avg("sea_temperature_celsius").alias("sea_temperature_celsius"),
            F.avg("ocean_productivity_score").alias("ocean_productivity_score"),
            F.sum("fishing_hours").alias("fishing_hours"),
            F.avg("data_coverage").alias("data_coverage"),
            F.when(
                F.count("chlor_a") == 0,
                F.lit("no_data"),
            )
            .when(
                F.min(
                    (F.col("chlor_a_source") == "observed").cast("int")
                )
                == 1,
                F.lit("observed"),
            )
            .otherwise(F.lit("contains_filled"))
            .alias("chlor_a_source"),
            F.when(
                F.count("sea_temperature_celsius") == 0,
                F.lit("no_data"),
            )
            .when(
                F.min(
                    (F.col("sea_temperature_celsius_source") == "observed").cast(
                        "int"
                    )
                )
                == 1,
                F.lit("observed"),
            )
            .otherwise(F.lit("contains_filled"))
            .alias("sea_temperature_celsius_source"),
            F.when(
                F.count("ocean_productivity_score") == 0,
                F.lit("no_data"),
            )
            .when(
                F.min(
                    (F.col("ocean_productivity_score_source") == "observed").cast(
                        "int"
                    )
                )
                == 1,
                F.lit("observed"),
            )
            .otherwise(F.lit("derived_with_fill"))
            .alias("ocean_productivity_score_source"),
            F.when(
                F.min(
                    (F.col("fishing_hours_source") == "observed").cast("int")
                )
                == 1,
                F.lit("observed"),
            )
            .otherwise(F.lit("zero_filled"))
            .alias("fishing_hours_source"),
        )
        .withColumn(
            "sustainability_pressure",
            F.when(F.col("fishing_hours") <= 0, F.lit(0.0)).otherwise(
                F.col("fishing_hours")
                / F.greatest(F.col("ocean_productivity_score"), F.lit(1e-9))
            ),
        )
        .withColumn(
            "sustainability_pressure_source",
            F.when(F.col("sustainability_pressure").isNull(), F.lit("no_data"))
            .when(
                (F.col("fishing_hours_source") == "observed")
                & (F.col("ocean_productivity_score_source") == "observed"),
                F.lit("observed"),
            ).otherwise(F.lit("derived_with_fill")),
        )
        .withColumn(
            "grid_id",
            F.format_string(
                "r%02dkm_%04d_%04d",
                F.col("resolution_km"),
                F.col("grid_row"),
                F.col("grid_col"),
            ),
        )
    )


def long_metrics(wide: DataFrame) -> DataFrame:
    frames = [
        wide.select(
            "event_date",
            "aoi_id",
            F.lit(product_id).alias("product_id"),
            F.lit(metric_id).alias("metric_id"),
            "resolution_km",
            "grid_id",
            "grid_row",
            "grid_col",
            F.col(value_column).alias("metric_value"),
            F.col(f"{value_column}_source").alias("value_source"),
            "data_coverage",
        )
        for product_id, metric_id, value_column in PRODUCT_SPECS
    ]
    return reduce(DataFrame.unionByName, frames)


def add_relative_score(frame: DataFrame) -> DataFrame:
    """Rank every displayed metric inside its date/AOI/metric/resolution scope."""
    scope = ["event_date", "aoi_id", "product_id", "metric_id", "resolution_km"]
    rank_window = Window.partitionBy(*scope).orderBy(F.col("metric_value"))
    count_window = Window.partitionBy(*scope)
    valid = frame.where(F.col("metric_value").isNotNull())
    ranked = (
        valid.withColumn(
            "relative_score",
            F.when(
                (F.col("product_id") == "GFW") & (F.col("metric_value") <= 0),
                F.lit(0.0),
            )
            .when(F.count("*").over(count_window) == 1, F.lit(100.0))
            .otherwise(F.percent_rank().over(rank_window) * F.lit(100.0)),
        )
        .withColumn(
            "display_level",
            F.when(F.col("relative_score") >= 80, F.lit("very_high"))
            .when(F.col("relative_score") >= 60, F.lit("high"))
            .when(F.col("relative_score") >= 40, F.lit("medium"))
            .when(F.col("relative_score") >= 20, F.lit("low"))
            .otherwise(F.lit("very_low")),
        )
    )
    missing = (
        frame.where(F.col("metric_value").isNull())
        .withColumn("relative_score", F.lit(None).cast("double"))
        .withColumn("display_level", F.lit("no_data"))
    )
    return (
        ranked.unionByName(missing)
        .withColumn("updated_at_utc", F.current_timestamp())
        .withColumn("pipeline_version", F.lit("0.5.0"))
    )


def assert_missing_values_explained(
    frame: DataFrame,
    value_sources: list[tuple[str, str]],
    label: str,
) -> None:
    invalid_condition = reduce(
        lambda left, right: left | right,
        [
            F.isnan(F.col(value))
            | (
                F.col(value).isNull()
                & (
                    F.col(source).isNull()
                    | F.coalesce(F.col(source) != "no_data", F.lit(True))
                )
            )
            | (
                F.col(value).isNotNull()
                & (
                    F.col(source).isNull()
                    | F.coalesce(F.col(source) == "no_data", F.lit(False))
                )
            )
            for value, source in value_sources
        ],
    )
    invalid = frame.where(invalid_condition).limit(1).count()
    if invalid:
        raise RuntimeError(f"{label} contains unexplained or invalid metric values")


def main():
    options = parse_args()
    if options.end_date < options.start_date:
        raise ValueError("end-date must not precede start-date")
    if options.fill_window_days < 1:
        raise ValueError("fill-window-days must be at least 1")

    spark = SparkSession.builder.appName("ocean-gold-observed-grid").getOrCreate()
    namespace = f"{options.catalog}.{options.namespace}"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    root = f"{options.silver_root.rstrip('/')}/{options.aoi_id}"
    history_start = options.start_date - timedelta(days=options.fill_window_days - 1)
    history_filter = F.col("event_date").between(
        F.lit(history_start), F.lit(options.end_date)
    )
    output_filter = F.col("event_date").between(
        F.lit(options.start_date), F.lit(options.end_date)
    )

    try:
        nasa = spark.read.parquet(f"{root}/nasa_daily_grid").where(history_filter)
        gfw = spark.read.parquet(f"{root}/gfw_daily_grid").where(history_filter)
        backbone = observed_grid_backbone(
            spark,
            options.aoi_id,
            nasa,
            history_start,
            options.end_date,
        ).persist(StorageLevel.MEMORY_AND_DISK)
        backbone_rows = backbone.count()
        if backbone_rows == 0:
            raise RuntimeError(
                f"No NASA-observed grids found for {options.aoi_id} "
                f"between {history_start} and {options.end_date}"
            )
        print(
            f"GOLD aoi={options.aoi_id} stage=backbone "
            f"rows={backbone_rows} status=ready"
        )

        filled = join_filled_metrics(
            backbone, nasa, gfw, options.fill_window_days
        )
        wide = (
            add_derived_products(filled)
            .where(output_filter)
            .select(
                *KEYS,
                "chlor_a",
                "sea_temperature_celsius",
                "ocean_productivity_score",
                "sustainability_pressure",
                "fishing_hours",
                "chlor_a_source",
                "sea_temperature_celsius_source",
                "ocean_productivity_score_source",
                "sustainability_pressure_source",
                "fishing_hours_source",
                "data_coverage",
            )
            .withColumn("updated_at_utc", F.current_timestamp())
            .withColumn("pipeline_version", F.lit("0.5.0"))
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        assert_missing_values_explained(
            wide,
            [
                ("chlor_a", "chlor_a_source"),
                ("sea_temperature_celsius", "sea_temperature_celsius_source"),
                ("ocean_productivity_score", "ocean_productivity_score_source"),
                ("sustainability_pressure", "sustainability_pressure_source"),
                ("fishing_hours", "fishing_hours_source"),
            ],
            "gold_daily_grid_features",
        )
        write_iceberg(
            wide,
            f"{namespace}.gold_daily_grid_features",
            ["event_date", "aoi_id"],
        )

        multi_resolution = reduce(
            DataFrame.unionByName,
            [resolution_wide(wide, km) for km in (4, 16, 32)],
        )
        serving = add_relative_score(long_metrics(multi_resolution)).persist(
            StorageLevel.MEMORY_AND_DISK
        )
        assert_missing_values_explained(
            serving,
            [("metric_value", "value_source")],
            "gold_map_metric",
        )
        write_iceberg(
            serving,
            f"{namespace}.gold_map_metric",
            ["event_date", "aoi_id", "resolution_km"],
        )
        summary = serving.groupBy(
            "event_date", "aoi_id", "product_id", "metric_id", "resolution_km"
        ).agg(
            F.avg("relative_score").alias("average_score"),
            F.max("relative_score").alias("maximum_score"),
            F.count("*").alias("cell_count"),
            F.avg("data_coverage").alias("data_coverage"),
        )
        write_iceberg(
            summary,
            f"{namespace}.gold_daily_metric_summary",
            ["event_date", "aoi_id", "resolution_km"],
        )
        serving.unpersist()
        wide.unpersist()
        backbone.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
