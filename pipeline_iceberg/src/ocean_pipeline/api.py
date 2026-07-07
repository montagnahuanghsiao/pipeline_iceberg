"""Flask serving API backed by a local, versioned Parquet snapshot."""
from __future__ import annotations

import os
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
from flask import Flask, jsonify, request
from flask_cors import CORS

from ocean_pipeline.catalog import load_aois, load_metrics

ALLOWED_RESOLUTIONS = {4, 16, 32}


def _snapshot_root() -> Path:
    return Path(
        os.environ.get(
            "LOCAL_SERVING_CURRENT",
            "/opt/zfs/project/data/serving/current",
        )
    )


def _parquet_glob(dataset: str) -> str:
    return (_snapshot_root() / dataset / "**" / "*.parquet").as_posix()


def _metric_pairs() -> set[tuple[str, str]]:
    return {(item["product_id"], item["metric_id"]) for item in load_metrics()}


def _required(name: str) -> str:
    value = request.args.get(name, "").strip()
    if not value:
        raise ValueError(f"missing query parameter: {name}")
    return value


def _filters(require_date: bool = True) -> dict[str, Any]:
    event_date = _required("date") if require_date else request.args.get("date")
    if event_date:
        date.fromisoformat(event_date)
    aoi = _required("aoi")
    product = _required("product")
    metric = _required("metric")
    resolution = int(_required("resolution"))
    if aoi not in load_aois():
        raise ValueError(f"unsupported aoi: {aoi}")
    if (product, metric) not in _metric_pairs():
        raise ValueError(f"unsupported product/metric pair: {product}/{metric}")
    if resolution not in ALLOWED_RESOLUTIONS:
        raise ValueError("resolution must be one of 4, 16, 32")
    return {
        "date": event_date,
        "aoi": aoi,
        "product": product,
        "metric": metric,
        "resolution": resolution,
    }


def _query(sql: str, parameters: list[Any]) -> tuple[list[str], list[tuple[Any, ...]]]:
    with duckdb.connect(database=":memory:") as connection:
        result = connection.execute(sql, parameters)
        columns = [item[0] for item in result.description]
        return columns, result.fetchall()


def _dict_rows(sql: str, parameters: list[Any]) -> list[dict[str, Any]]:
    columns, rows = _query(sql, parameters)
    return [dict(zip(columns, row, strict=True)) for row in rows]


def create_app() -> Flask:
    app = Flask(__name__)
    CORS(
        app,
        resources={
            r"/api/*": {
                "origins": os.environ.get("API_CORS_ORIGINS", "*").split(",")
            }
        },
    )

    @app.errorhandler(ValueError)
    def handle_bad_request(error: ValueError):
        return jsonify({"error": str(error)}), 400

    @app.get("/healthz")
    def health():
        root = _snapshot_root()
        ready = all(
            (root / name).is_dir()
            for name in ("gold_map_metric", "gold_daily_metric_summary")
        )
        return jsonify({"status": "ok" if ready else "not_ready"}), 200 if ready else 503

    @app.get("/api/v1/catalog")
    def catalog():
        return jsonify(
            {
                "aois": [
                    {
                        "id": item.id,
                        "label": item.label,
                        "min_lat": item.min_lat,
                        "max_lat": item.max_lat,
                        "min_lon": item.min_lon,
                        "max_lon": item.max_lon,
                    }
                    for item in load_aois().values()
                ],
                "metrics": load_metrics(),
                "resolutions": sorted(ALLOWED_RESOLUTIONS),
            }
        )

    @app.get("/api/v1/availability")
    def availability():
        aoi = request.args.get("aoi")
        product = request.args.get("product")
        metric = request.args.get("metric")
        resolution = request.args.get("resolution")
        if aoi and aoi not in load_aois():
            raise ValueError(f"unsupported aoi: {aoi}")
        if product and metric and (product, metric) not in _metric_pairs():
            raise ValueError(f"unsupported product/metric pair: {product}/{metric}")
        resolution_value = int(resolution) if resolution else None
        if resolution_value is not None and resolution_value not in ALLOWED_RESOLUTIONS:
            raise ValueError("resolution must be one of 4, 16, 32")

        rows = _dict_rows(
            """
            SELECT
                CAST(event_date AS VARCHAR) AS date,
                aoi_id,
                product_id,
                metric_id,
                CAST(resolution_km AS INTEGER) AS resolution_km,
                CAST(cell_count AS BIGINT) AS cells
            FROM read_parquet(?, hive_partitioning = true)
            WHERE (? IS NULL OR aoi_id = ?)
              AND (? IS NULL OR product_id = ?)
              AND (? IS NULL OR metric_id = ?)
              AND (? IS NULL OR resolution_km = ?)
            ORDER BY event_date, aoi_id, product_id, metric_id, resolution_km
            """,
            [
                _parquet_glob("gold_daily_metric_summary"),
                aoi,
                aoi,
                product,
                product,
                metric,
                metric,
                resolution_value,
                resolution_value,
            ],
        )
        dates = sorted({row["date"] for row in rows})
        return jsonify({"dates": dates, "partitions": rows})

    @app.get("/api/v1/gold/daily-grid")
    def daily_grid():
        filters = _filters()
        maximum = int(os.environ.get("API_MAX_GRID_CELLS", "100000"))
        if maximum < 1 or maximum > 500_000:
            raise ValueError("API_MAX_GRID_CELLS must be between 1 and 500000")
        rows = _dict_rows(
            f"""
            SELECT
                grid_id,
                CAST(grid_row AS INTEGER) AS grid_row,
                CAST(grid_col AS INTEGER) AS grid_col,
                CAST(relative_score AS DOUBLE) AS relative_score,
                display_level,
                value_source,
                CAST(data_coverage AS DOUBLE) AS data_coverage,
                CAST(relative_score AS DOUBLE) AS value,
                CAST(resolution_km AS INTEGER) AS resolution_km
            FROM read_parquet(?, hive_partitioning = true)
            WHERE event_date = CAST(? AS DATE)
              AND aoi_id = ?
              AND product_id = ?
              AND metric_id = ?
              AND resolution_km = ?
            ORDER BY grid_row, grid_col
            LIMIT {maximum + 1}
            """,
            [
                _parquet_glob("gold_map_metric"),
                filters["date"],
                filters["aoi"],
                filters["product"],
                filters["metric"],
                filters["resolution"],
            ],
        )
        if len(rows) > maximum:
            raise ValueError(
                f"query exceeds {maximum} cells; choose a coarser resolution"
            )
        if not rows:
            return jsonify({"error": "no matching grid partition"}), 404
        return jsonify({**filters, "source": "gold_serving_snapshot", "grid": rows})

    @app.get("/api/v1/gold/summary")
    def summary():
        filters = _filters()
        rows = _dict_rows(
            """
            SELECT
                CAST(average_score AS DOUBLE) AS average,
                CAST(maximum_score AS DOUBLE) AS maximum,
                CAST(data_coverage AS DOUBLE) AS nasa_coverage,
                CAST(cell_count AS BIGINT) AS cells
            FROM read_parquet(?, hive_partitioning = true)
            WHERE event_date = CAST(? AS DATE)
              AND aoi_id = ?
              AND product_id = ?
              AND metric_id = ?
              AND resolution_km = ?
            LIMIT 1
            """,
            [
                _parquet_glob("gold_daily_metric_summary"),
                filters["date"],
                filters["aoi"],
                filters["product"],
                filters["metric"],
                filters["resolution"],
            ],
        )
        if not rows:
            return jsonify({"error": "no matching summary partition"}), 404
        return jsonify({**filters, **rows[0], "components": []})

    @app.get("/api/v1/gold/trend")
    def trend():
        filters = _filters(require_date=False)
        rows = _dict_rows(
            """
            SELECT
                CAST(event_date AS VARCHAR) AS date,
                CAST(average_score AS DOUBLE) AS value
            FROM read_parquet(?, hive_partitioning = true)
            WHERE aoi_id = ?
              AND product_id = ?
              AND metric_id = ?
              AND resolution_km = ?
            ORDER BY event_date
            """,
            [
                _parquet_glob("gold_daily_metric_summary"),
                filters["aoi"],
                filters["product"],
                filters["metric"],
                filters["resolution"],
            ],
        )
        return jsonify({**filters, "points": rows})

    return app


app = create_app()
