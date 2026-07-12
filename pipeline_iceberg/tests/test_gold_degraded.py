from __future__ import annotations

import unittest
from datetime import date

from pyspark.sql import SparkSession, types as T

from pipeline_iceberg.jobs.gold import add_derived_products, add_relative_score


class GoldDegradedMetricTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("gold-degraded-metric-test")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_incomplete_metric_day_is_omitted_at_every_resolution(self):
        schema = T.StructType(
            [
                T.StructField("event_date", T.DateType(), False),
                T.StructField("aoi_id", T.StringType(), False),
                T.StructField("product_id", T.StringType(), False),
                T.StructField("metric_id", T.StringType(), False),
                T.StructField("resolution_km", T.IntegerType(), False),
                T.StructField("grid_id", T.StringType(), False),
                T.StructField("grid_row", T.ShortType(), False),
                T.StructField("grid_col", T.ShortType(), False),
                T.StructField("metric_value", T.DoubleType(), True),
                T.StructField("value_source", T.StringType(), False),
                T.StructField("data_coverage", T.DoubleType(), False),
            ]
        )
        rows = [
            (date(2024, 1, 1), "taiwan", "CHL", "chlor_a", 4, "c1", 1, 1, 1.0, "observed", 1.0),
            (date(2024, 1, 1), "taiwan", "CHL", "chlor_a", 4, "c2", 1, 2, 2.0, "observed", 1.0),
            (date(2024, 1, 1), "taiwan", "PRODUCTIVITY", "ocean_productivity_score", 4, "c1", 1, 1, 1.0, "observed", 1.0),
            (date(2024, 1, 1), "taiwan", "PRODUCTIVITY", "ocean_productivity_score", 4, "c2", 1, 2, None, "no_data", 0.5),
            (date(2024, 1, 1), "taiwan", "PRODUCTIVITY", "ocean_productivity_score", 16, "c16", 0, 0, 1.5, "contains_filled", 0.75),
            (date(2024, 1, 2), "taiwan", "PRODUCTIVITY", "ocean_productivity_score", 4, "c1", 1, 1, 1.0, "observed", 1.0),
            (date(2024, 1, 2), "taiwan", "PRODUCTIVITY", "ocean_productivity_score", 4, "c2", 1, 2, 2.0, "observed", 1.0),
        ]
        result = add_relative_score(self.spark.createDataFrame(rows, schema))
        published = {
            (row.event_date, row.metric_id, row.resolution_km)
            for row in result.select(
                "event_date", "metric_id", "resolution_km"
            ).distinct().collect()
        }

        self.assertIn((date(2024, 1, 1), "chlor_a", 4), published)
        self.assertNotIn(
            (date(2024, 1, 1), "ocean_productivity_score", 4), published
        )
        self.assertNotIn(
            (date(2024, 1, 1), "ocean_productivity_score", 16), published
        )
        self.assertIn(
            (date(2024, 1, 2), "ocean_productivity_score", 4), published
        )

    def test_pressure_remains_missing_when_productivity_is_missing(self):
        schema = T.StructType(
            [
                T.StructField("event_date", T.DateType(), False),
                T.StructField("aoi_id", T.StringType(), False),
                T.StructField("grid_id", T.StringType(), False),
                T.StructField("grid_row", T.ShortType(), False),
                T.StructField("grid_col", T.ShortType(), False),
                T.StructField("chlor_a", T.DoubleType(), True),
                T.StructField("poc", T.DoubleType(), True),
                T.StructField("nflh", T.DoubleType(), True),
                T.StructField("fishing_hours", T.DoubleType(), False),
                T.StructField("chlor_a_source", T.StringType(), False),
                T.StructField("poc_source", T.StringType(), False),
                T.StructField("nflh_source", T.StringType(), False),
                T.StructField("sea_temperature_celsius_source", T.StringType(), False),
                T.StructField("fishing_hours_source", T.StringType(), False),
            ]
        )
        rows = [
            (
                date(2024, 1, 1), "taiwan", "c1", 1, 1,
                1.0, 2.0, None, 0.0,
                "observed", "observed", "no_data", "observed", "zero_filled",
            )
        ]
        row = add_derived_products(self.spark.createDataFrame(rows, schema)).first()

        self.assertIsNone(row.ocean_productivity_score)
        self.assertEqual("no_data", row.ocean_productivity_score_source)
        self.assertIsNone(row.sustainability_pressure)
        self.assertEqual("no_data", row.sustainability_pressure_source)


if __name__ == "__main__":
    unittest.main()
