from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

import duckdb

from ocean_pipeline.api import create_app


class ApiTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        root = Path(self.tempdir.name)
        connection = duckdb.connect()
        map_target = (root / "gold_map_metric").as_posix().replace("'", "''")
        summary_target = (
            root / "gold_daily_metric_summary"
        ).as_posix().replace("'", "''")
        connection.execute(
            f"""
            COPY (
              SELECT
                DATE '2024-12-12' AS event_date,
                'taiwan' AS aoi_id,
                'COMBINED' AS product_id,
                'potential_fishing_score' AS metric_id,
                4::INTEGER AS resolution_km,
                'r04km_1512_7152' AS grid_id,
                1512::SMALLINT AS grid_row,
                7152::SMALLINT AS grid_col,
                72.0::DOUBLE AS raw_metric_value,
                72.0::DOUBLE AS relative_score,
                'high' AS display_level,
                0.75::DOUBLE AS data_coverage,
                TIMESTAMP '2024-12-13 00:00:00' AS updated_at_utc,
                'test' AS pipeline_version
            ) TO '{map_target}'
            (FORMAT PARQUET, PARTITION_BY (event_date, aoi_id, resolution_km))
            """
        )
        connection.execute(
            f"""
            COPY (
              SELECT
                DATE '2024-12-12' AS event_date,
                'taiwan' AS aoi_id,
                'COMBINED' AS product_id,
                'potential_fishing_score' AS metric_id,
                4::INTEGER AS resolution_km,
                72.0::DOUBLE AS average_score,
                72.0::DOUBLE AS maximum_score,
                1::BIGINT AS cell_count,
                0.75::DOUBLE AS data_coverage
            ) TO '{summary_target}'
            (FORMAT PARQUET, PARTITION_BY (event_date, aoi_id, resolution_km))
            """
        )
        connection.close()
        os.environ["LOCAL_SERVING_CURRENT"] = str(root)
        os.environ["API_MAX_GRID_CELLS"] = "1000"
        self.client = create_app().test_client()

    def tearDown(self) -> None:
        self.tempdir.cleanup()
        os.environ.pop("LOCAL_SERVING_CURRENT", None)
        os.environ.pop("API_MAX_GRID_CELLS", None)

    @staticmethod
    def query() -> str:
        return (
            "date=2024-12-12&aoi=taiwan&product=COMBINED"
            "&metric=potential_fishing_score&resolution=4"
        )

    def test_health_and_catalog(self) -> None:
        self.assertEqual(self.client.get("/healthz").status_code, 200)
        catalog = self.client.get("/api/v1/catalog")
        self.assertEqual(catalog.status_code, 200)
        self.assertIn("taiwan", {item["id"] for item in catalog.json["aois"]})

    def test_daily_grid_contract(self) -> None:
        response = self.client.get(f"/api/v1/gold/daily-grid?{self.query()}")
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json["grid"][0]["value"], 72.0)
        self.assertEqual(response.json["grid"][0]["resolution_km"], 4)

    def test_summary_and_trend_contract(self) -> None:
        summary = self.client.get(f"/api/v1/gold/summary?{self.query()}")
        self.assertEqual(summary.status_code, 200, summary.text)
        self.assertEqual(summary.json["cells"], 1)
        trend = self.client.get(
            "/api/v1/gold/trend?"
            "aoi=taiwan&product=COMBINED"
            "&metric=potential_fishing_score&resolution=4"
        )
        self.assertEqual(trend.status_code, 200, trend.text)
        self.assertEqual(trend.json["points"][0]["date"], "2024-12-12")

    def test_rejects_invalid_metric_pair(self) -> None:
        response = self.client.get(
            "/api/v1/gold/daily-grid?"
            "date=2024-12-12&aoi=taiwan&product=GFW"
            "&metric=potential_fishing_score&resolution=4"
        )
        self.assertEqual(response.status_code, 400)


if __name__ == "__main__":
    unittest.main()
