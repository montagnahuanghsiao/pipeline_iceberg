"""Build complete-AOI Gold features and percentile heatmaps.

Bronze and Silver keep observed values. This job uses grid cells that occur in
the configured AOI rectangle and fills missing observations in this order:

1. observed value for the requested date;
2. trailing mean for the same grid cell;
3. trailing mean of the eight surrounding grid cells;
4. trailing mean over the whole AOI.

GFW cells without an activity row are zero-filled. All five frontend metrics
with values are converted to 0-100 percentile scores within
date/AOI/metric/resolution. An incomplete metric-day is omitted at every map
resolution while other dates and metrics continue.
"""
from __future__ import annotations

import argparse
import json
from datetime import date, timedelta
from functools import reduce
from pathlib import Path

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


def load_aoi_config(path: str, aoi_id: str) -> dict[str, float]:
    config = json.loads(Path(path).read_text(encoding="utf-8"))
    if aoi_id not in config:
        raise ValueError(f"AOI {aoi_id!r} not found in {path}")
    return config[aoi_id]


def complete_grid_backbone(
    spark: SparkSession,
    aoi_id: str,
    aoi: dict[str, float],
    start_date: date,
    end_date: date,
) -> DataFrame:
    """Return every date x 4 km grid cell inside the configured AOI rectangle."""
    day_count = (end_date - start_date).days + 1
    dates = spark.range(day_count).select(
        F.date_add(F.lit(start_date), F.col("id").cast("int")).alias("event_date")
    )
    row_min = int((90.0 - float(aoi["max_lat"])) * 24)
    row_max = int((90.0 - float(aoi["min_lat"])) * 24) - 1
    col_min = int((float(aoi["min_lon"]) + 180.0) * 24)
    col_max = int((float(aoi["max_lon"]) + 180.0) * 24) - 1
    rows = spark.range(row_min, row_max + 1).select(
        F.col("id").cast("short").alias("grid_row")
    )
    cols = spark.range(col_min, col_max + 1).select(
        F.col("id").cast("short").alias("grid_col")
    )
    grids = (
        rows.crossJoin(cols)
        .withColumn(
            "grid_id",
            F.format_string(
                "nasa4km_r%04d_c%04d",
                F.col("grid_row"),
                F.col("grid_col"),
            ),
        )
        .select("grid_id", "grid_row", "grid_col")
    )
    return (
        dates.crossJoin(grids)
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
    aoi_window = (
        Window.partitionBy("aoi_id")
        .orderBy(day_number)
        .rangeBetween(-(fill_window_days - 1), 0)
    )
    enriched = joined.withColumn(
        "_grid_window_mean", F.avg(raw_column).over(cell_window)
    ).withColumn(
        "_grid_window_count", F.count(raw_column).over(cell_window)
    ).withColumn(
        "_aoi_window_mean", F.avg(raw_column).over(aoi_window)
    )

    neighbors = _neighbor_candidates(enriched, "_grid_window_mean")
    grid_source = f"grid_{fill_window_days}d_mean"
    neighbor_source = f"neighbor_{fill_window_days}d_mean"
    aoi_source = f"aoi_{fill_window_days}d_mean"
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
                F.col("_aoi_window_mean"),
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
            .when(F.col("_aoi_window_mean").isNotNull(), F.lit(aoi_source))
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
            F.when(
                F.col("ocean_productivity_score").isNull(),
                F.lit(None).cast("double"),
            )
            .when(F.col("fishing_hours") <= 0, F.lit(0.0))
            .otherwise(
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
            F.when(
                F.col("ocean_productivity_score").isNull(),
                F.lit(None).cast("double"),
            )
            .when(F.col("fishing_hours") <= 0, F.lit(0.0))
            .otherwise(
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
    """Publish and rank only complete date/AOI/metric scopes.

    A missing cell must not be rendered as a low or zero value.  If any value
    at any resolution is null/NaN, omit that metric-day at every resolution
    while leaving other metrics and dates available.
    """
    scope = ["event_date", "aoi_id", "product_id", "metric_id", "resolution_km"]
    validity_scope = ["event_date", "aoi_id", "product_id", "metric_id"]
    scope_window = Window.partitionBy(*validity_scope)
    rank_window = Window.partitionBy(*scope).orderBy(F.col("metric_value"))
    count_window = Window.partitionBy(*scope)
    valid = (
        frame.withColumn(
            "_invalid_scope_values",
            F.sum(
                F.when(
                    F.col("metric_value").isNull() | F.isnan("metric_value"),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).over(scope_window),
        )
        .where(F.col("_invalid_scope_values") == 0)
        .drop("_invalid_scope_values")
    )
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
    return (
        ranked
        .withColumn("updated_at_utc", F.current_timestamp())
        .withColumn("pipeline_version", F.lit("0.5.1"))
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


def assert_no_missing_values(frame: DataFrame, columns: list[str], label: str) -> None:
    invalid_condition = reduce(
        lambda left, right: left | right,
        [F.col(column).isNull() | F.isnan(F.col(column)) for column in columns],
    )
    invalid = frame.where(invalid_condition).limit(1).count()
    if invalid:
        raise RuntimeError(f"{label} contains null/NaN display values after fill")


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
    aoi = load_aoi_config(options.aoi_config, options.aoi_id)
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
        backbone = complete_grid_backbone(
            spark,
            options.aoi_id,
            aoi,
            history_start,
            options.end_date,
        ).persist(StorageLevel.MEMORY_AND_DISK)
        backbone_rows = backbone.count()
        if backbone_rows == 0:
            raise RuntimeError(
                f"No AOI grid cells found for {options.aoi_id} "
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
            .withColumn("pipeline_version", F.lit("0.5.1"))
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
        # The feature table preserves explained no-data values for auditability.
        # Complete, frontend-facing metric scopes are enforced below when the
        # long map table is built.
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
        scope_columns = [
            "event_date",
            "aoi_id",
            "product_id",
            "metric_id",
            "resolution_km",
        ]
        published_scopes = {
            (
                str(row.event_date),
                row.product_id,
                row.metric_id,
                int(row.resolution_km),
            )
            for row in serving.select(*scope_columns).distinct().collect()
        }
        expected_scopes = {
            (
                str(options.start_date + timedelta(days=offset)),
                product_id,
                metric_id,
                resolution_km,
            )
            for offset in range((options.end_date - options.start_date).days + 1)
            for product_id, metric_id, _ in PRODUCT_SPECS
            for resolution_km in (4, 16, 32)
        }
        omitted_scopes = sorted(expected_scopes - published_scopes)
        for event_date, product_id, metric_id, resolution_km in omitted_scopes:
            print(
                "GOLD_METRIC_SKIPPED "
                f"date={event_date} aoi={options.aoi_id} "
                f"product={product_id} metric={metric_id} "
                f"resolution_km={resolution_km} "
                "reason=incomplete_metric_scope"
            )
        if omitted_scopes:
            print(
                "GOLD_DEGRADED "
                f"aoi={options.aoi_id} omitted_scopes={len(omitted_scopes)} "
                f"published_scopes={len(published_scopes)}"
            )
        assert_missing_values_explained(
            serving,
            [("metric_value", "value_source")],
            "gold_map_metric",
        )
        assert_no_missing_values(
            serving,
            ["metric_value", "relative_score"],
            "gold_map_metric",
        )
        write_iceberg(
            serving,
            f"{namespace}.gold_map_metric",
            ["event_date", "aoi_id", "resolution_km"],
        )
        serving.unpersist()
        wide.unpersist()
        backbone.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
