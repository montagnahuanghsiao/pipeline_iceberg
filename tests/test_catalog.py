import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ocean_pipeline.catalog import load_aois, load_metrics


class CatalogTest(unittest.TestCase):
    def test_aoi_bounds_are_valid(self):
        for aoi in load_aois().values():
            self.assertLess(aoi.min_lat, aoi.max_lat)
            self.assertLess(aoi.min_lon, aoi.max_lon)

    def test_metric_key_is_unique(self):
        keys = [(item["product_id"], item["metric_id"]) for item in load_metrics()]
        self.assertEqual(len(keys), len(set(keys)))


if __name__ == "__main__":
    unittest.main()
