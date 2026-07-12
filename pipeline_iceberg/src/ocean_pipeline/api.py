"""Flask serving API backed by a local, versioned Parquet snapshot."""
from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import duckdb
from flask import Flask, jsonify, request
from flask_cors import CORS

from ocean_pipeline.catalog import load_aois, load_metrics

ALLOWED_RESOLUTIONS = {4, 16, 32}
TREND_COLUMNS = {
    "chlor_a": "chlor_a_score_avg",
    "sea_temperature": "sea_temperature_score_avg",
    "ocean_productivity_score": "ocean_productivity_score_avg",
    "sustainability_pressure": "sustainability_pressure_score_avg",
    "fishing_hours": "fishing_hours_score_avg",
}


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


def _trend_window() -> tuple[str, date | None, date | None]:
    raw = request.args.get("trend_window_days", "30").strip().lower()
    if raw == "all":
        return raw, None, None
    try:
        days = int(raw)
    except ValueError as exc:
        raise ValueError("trend_window_days must be 15, 30, 90 or all") from exc
    if days not in {15, 30, 90}:
        raise ValueError("trend_window_days must be 15, 30, 90 or all")
    selected = date.fromisoformat(_required("date"))
    return raw, selected - timedelta(days=days), selected + timedelta(days=days)


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
            for name in (
                "gold_map_metric",
                "gold_dashboard_daily_metrics",
                "gold_dashboard_status_distribution",
            )
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
                CAST(COUNT(*) AS BIGINT) AS cells
            FROM read_parquet(?, hive_partitioning = true)
            WHERE (? IS NULL OR aoi_id = ?)
              AND (? IS NULL OR product_id = ?)
              AND (? IS NULL OR metric_id = ?)
              AND (? IS NULL OR resolution_km = ?)
            GROUP BY event_date, aoi_id, product_id, metric_id, resolution_km
            ORDER BY event_date, aoi_id, product_id, metric_id, resolution_km
            """,
            [
                _parquet_glob("gold_map_metric"),
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
        score_column = TREND_COLUMNS[filters["metric"]]
        rows = _dict_rows(
            f"""
            SELECT
                CAST({score_column} AS DOUBLE) AS average,
                CAST(data_coverage AS DOUBLE) AS data_coverage,
                CAST(cell_count AS BIGINT) AS cells,
                CAST(chlor_a_avg AS DOUBLE) AS chlor_a_avg,
                CAST(sea_temperature_avg AS DOUBLE) AS sea_temperature_avg,
                CAST(ocean_productivity_avg AS DOUBLE) AS ocean_productivity_avg,
                CAST(sustainability_pressure_avg AS DOUBLE) AS sustainability_pressure_avg,
                CAST(sustainability_pressure_p90 AS DOUBLE) AS sustainability_pressure_p90,
                CAST(fishing_hours_total AS DOUBLE) AS fishing_hours_total,
                CAST(active_cell_ratio AS DOUBLE) AS active_cell_ratio,
                CAST(high_activity_cell_ratio AS DOUBLE) AS high_activity_cell_ratio,
                CAST(high_productivity_cell_ratio AS DOUBLE) AS high_productivity_cell_ratio,
                CAST(high_pressure_cell_ratio AS DOUBLE) AS high_pressure_cell_ratio,
                CAST(share_of_all_fishing_hours AS DOUBLE) AS share_of_all_fishing_hours,
                CAST(fishing_hours_7d_avg AS DOUBLE) AS fishing_hours_7d_avg,
                CAST(sustainability_pressure_7d_avg AS DOUBLE) AS sustainability_pressure_7d_avg
            FROM read_parquet(?, hive_partitioning = true)
            WHERE event_date = CAST(? AS DATE)
              AND aoi_id = ?
              AND resolution_km = ?
            LIMIT 1
            """,
            [
                _parquet_glob("gold_dashboard_daily_metrics"),
                filters["date"],
                filters["aoi"],
                filters["resolution"],
            ],
        )
        if not rows:
            return jsonify({"error": "no matching dashboard partition"}), 404
        row = rows[0]
        components = [
            {
                "label": "高生產力格",
                "value": (row["high_productivity_cell_ratio"] or 0) * 100,
                "text": f"{((row['high_productivity_cell_ratio'] or 0) * 100):.1f}%",
            },
            {
                "label": "高捕魚活動",
                "value": (row["high_activity_cell_ratio"] or 0) * 100,
                "text": f"{((row['high_activity_cell_ratio'] or 0) * 100):.1f}%",
            },
            {
                "label": "高永續壓力",
                "value": (row["high_pressure_cell_ratio"] or 0) * 100,
                "text": f"{((row['high_pressure_cell_ratio'] or 0) * 100):.1f}%",
            },
        ]
        # Do not turn an unavailable derived indicator into a misleading 0%.
        component_availability = [
            row["high_productivity_cell_ratio"] is not None,
            row["high_activity_cell_ratio"] is not None,
            row["high_pressure_cell_ratio"] is not None,
        ]
        components = [
            component
            for component, available in zip(
                components, component_availability, strict=True
            )
            if available
        ]
        return jsonify({**filters, **row, "nasa_coverage": row["data_coverage"], "components": components})

    @app.get("/api/v1/gold/trend")
    def trend():
        filters = _filters(require_date=True)
        trend_window_days, trend_start, trend_end = _trend_window()
        score_column = TREND_COLUMNS[filters["metric"]]
        date_clause = ""
        parameters: list[Any] = [
            _parquet_glob("gold_dashboard_daily_metrics"),
            filters["aoi"],
            filters["resolution"],
        ]
        if trend_start and trend_end:
            date_clause = "AND event_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)"
            parameters.extend([trend_start.isoformat(), trend_end.isoformat()])
        rows = _dict_rows(
            f"""
            SELECT
                CAST(event_date AS VARCHAR) AS date,
                CAST({score_column} AS DOUBLE) AS value
            FROM read_parquet(?, hive_partitioning = true)
            WHERE aoi_id = ?
              AND resolution_km = ?
              AND {score_column} IS NOT NULL
              {date_clause}
            ORDER BY event_date
            """,
            parameters,
        )
        return jsonify(
            {
                **filters,
                "trend_window_days": trend_window_days,
                "trend_start": trend_start.isoformat() if trend_start else None,
                "trend_end": trend_end.isoformat() if trend_end else None,
                "points": rows,
            }
        )

    @app.get("/api/v1/gold/status-distribution")
    def status_distribution():
        filters = _filters()
        rows = _dict_rows(
            """
            SELECT
                status_class,
                CAST(cell_count AS BIGINT) AS cell_count,
                CAST(cell_ratio AS DOUBLE) AS cell_ratio,
                CAST(fishing_hours_total AS DOUBLE) AS fishing_hours_total,
                CAST(productivity_score_avg AS DOUBLE) AS productivity_score_avg,
                CAST(fishing_score_avg AS DOUBLE) AS fishing_score_avg
            FROM read_parquet(?, hive_partitioning = true)
            WHERE event_date = CAST(? AS DATE)
              AND aoi_id = ?
              AND resolution_km = ?
            ORDER BY status_class
            """,
            [
                _parquet_glob("gold_dashboard_status_distribution"),
                filters["date"],
                filters["aoi"],
                filters["resolution"],
            ],
        )
        if not rows:
            return jsonify({**filters, "available": False, "classes": []})
        return jsonify({**filters, "available": True, "classes": rows})

    return app


app = create_app()
