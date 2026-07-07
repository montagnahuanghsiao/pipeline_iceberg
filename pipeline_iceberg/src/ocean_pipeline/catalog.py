from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Aoi:
    id: str
    label: str
    min_lat: float
    max_lat: float
    min_lon: float
    max_lon: float


@lru_cache
def load_aois() -> dict[str, Aoi]:
    raw = json.loads((ROOT / "configs" / "aoi_presets.json").read_text(encoding="utf-8"))
    return {key: Aoi(id=key, **value) for key, value in raw.items()}


@lru_cache
def load_metrics() -> list[dict]:
    return json.loads((ROOT / "configs" / "metric_catalog.json").read_text(encoding="utf-8"))


def metric_ids() -> set[str]:
    return {item["metric_id"] for item in load_metrics()}


def product_ids() -> set[str]:
    return {item["product_id"] for item in load_metrics()}
