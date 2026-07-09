"""Iceberg maintenance for Gold tables."""
import argparse
from pyspark.sql import SparkSession


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", default="lake")
    p.add_argument("--namespace", default="ocean")
    p.add_argument("--retain-hours", type=int, default=168)
    args = p.parse_args()
    spark = SparkSession.builder.appName("ocean-iceberg-maintenance").getOrCreate()
    try:
        for table in (
            "gold_daily_grid_features",
            "gold_map_metric",
            "gold_dashboard_daily_metrics",
            "gold_dashboard_status_distribution",
        ):
            name = f"{args.catalog}.{args.namespace}.{table}"
            spark.sql(f"CALL {args.catalog}.system.rewrite_data_files(table => '{args.namespace}.{table}', options => map('target-file-size-bytes','268435456'))")
            spark.sql(f"CALL {args.catalog}.system.rewrite_manifests(table => '{args.namespace}.{table}')")
            spark.sql(f"CALL {args.catalog}.system.expire_snapshots(table => '{args.namespace}.{table}', older_than => TIMESTAMPADD(HOUR, -{args.retain_hours}, CURRENT_TIMESTAMP()))")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
