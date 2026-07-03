from __future__ import annotations

import os
import re
from contextlib import closing
from datetime import date
from typing import Any

import trino
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from .catalog import load_aois, load_metrics, metric_ids, product_ids

app = FastAPI(title="OceanGrid API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        value.strip()
        for value in os.getenv(
            "CORS_ORIGINS", "http://localhost:8766,http://127.0.0.1:8766"
        ).split(",")
    ],
    allow_methods=["GET"],
    allow_headers=["*"],
)


def configured_table(env_name: str, default: str) -> str:
    value = os.getenv(env_name, default)
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise RuntimeError(f"Invalid table identifier in {env_name}")
    return value


MAP_TABLE = configured_table("GOLD_MAP_TABLE", "gold_map_metric")
SUMMARY_TABLE = configured_table("GOLD_SUMMARY_TABLE", "gold_daily_metric_summary")


def connection():
    return trino.dbapi.connect(
        host=os.getenv("TRINO_HOST", "localhost"),
        port=int(os.getenv("TRINO_PORT", "8080")),
        user=os.getenv("TRINO_USER", "ocean_api"),
        catalog=os.getenv("TRINO_CATALOG", "iceberg"),
        schema=os.getenv("TRINO_SCHEMA", "ocean"),
        http_scheme=os.getenv("TRINO_HTTP_SCHEME", "http"),
    )


def validate_dimensions(aoi: str, product: str, metric: str) -> None:
    if aoi not in load_aois():
        raise HTTPException(400, "Unknown AOI")
    if product not in product_ids():
        raise HTTPException(400, "Unknown product")
    allowed = {m["metric_id"] for m in load_metrics() if m["product_id"] == product}
    if metric not in metric_ids() or metric not in allowed:
        raise HTTPException(400, "Metric is not available for this product")


def query(sql: str, params: list[Any]) -> tuple[list[str], list[tuple]]:
    with closing(connection()) as conn, closing(conn.cursor()) as cursor:
        cursor.execute(sql, params)
        columns = [item[0] for item in cursor.description]
        return columns, cursor.fetchall()


@app.get("/api/v1/catalog")
def catalog():
    return {
        "aois": [aoi.__dict__ for aoi in load_aois().values()],
        "metrics": load_metrics(),
    }


@app.get("/api/v1/gold/daily-grid")
def daily_grid(event_date: date = Query(alias="date"), aoi: str = "taiwan", product: str = "COMBINED", metric: str = "potential_fishing_score", resolution: int = 4, max_cells: int = Query(100_000, ge=1, le=200_000)):
    validate_dimensions(aoi, product, metric)
    columns, rows = query(
        f"""
        SELECT grid_id, grid_row, grid_col, metric_value, data_coverage
        FROM {MAP_TABLE}
        WHERE event_date = ? AND aoi_id = ? AND product_id = ? AND metric_id = ?
          AND resolution_km = ?
        LIMIT ?
        """,
        [event_date, aoi, product, metric, resolution, max_cells],
    )
    grid = [
        {"date": event_date, "metric": metric, **dict(zip(columns, row)), "value": row[3]}
        for row in rows
    ]
    return {"date": event_date, "aoi": aoi, "product": product, "metric": metric, "resolution": resolution, "source": "iceberg", "grid": grid}


@app.get("/api/v1/gold/summary")
def summary(event_date: date = Query(alias="date"), aoi: str = "taiwan", product: str = "COMBINED", metric: str = "potential_fishing_score", resolution: int = 4):
    validate_dimensions(aoi, product, metric)
    _, rows = query(
        f"""
        SELECT average_value, maximum_value, cell_count, data_coverage
        FROM {SUMMARY_TABLE}
        WHERE event_date = ? AND aoi_id = ? AND product_id = ? AND metric_id = ?
          AND resolution_km = ?
        """,
        [event_date, aoi, product, metric, resolution],
    )
    avg_value, max_value, cells, coverage = rows[0]
    return {"date": event_date, "aoi": aoi, "product": product, "metric": metric, "average": avg_value, "maximum": max_value, "cells": cells, "nasa_coverage": coverage, "partition": f"event_date={event_date}/aoi_id={aoi}", "components": []}


@app.get("/api/v1/gold/trend")
def trend(
    aoi: str = "taiwan",
    product: str = "COMBINED",
    metric: str = "potential_fishing_score",
    resolution: int = 4,
    start_date: date | None = None,
    end_date: date | None = None,
):
    validate_dimensions(aoi, product, metric)
    start_date = start_date or date(1900, 1, 1)
    end_date = end_date or date(2999, 12, 31)
    _, rows = query(
        f"""
        SELECT event_date, average_value
        FROM {SUMMARY_TABLE}
        WHERE event_date BETWEEN ? AND ?
          AND aoi_id = ? AND product_id = ? AND metric_id = ?
          AND resolution_km = ?
        ORDER BY event_date
        """,
        [start_date, end_date, aoi, product, metric, resolution],
    )
    return {
        "aoi": aoi,
        "product": product,
        "metric": metric,
        "points": [{"date": row[0], "value": row[1]} for row in rows],
    }
