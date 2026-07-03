"""NASA Ocean Color NetCDF download and Bronze Parquet conversion.

This module deliberately performs only lossless structural work:

1. download a daily NetCDF file (science product, then NRT fallback);
2. apply the NetCDF CF mask/scale decoding;
3. remove only missing/non-finite observations;
4. write the remaining pixels to Parquet in bounded-memory chunks;
5. preserve source schema and variable metadata in the Parquet footer.

Scientific range filtering, quality-level filtering, AOI clipping and duplicate
handling belong in ``nasa_clean.py`` and are intentionally not done here.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import xarray as xr

PIPELINE_VERSION = "2.0.0"
NC_MIN_SIZE = 100 * 1024
DEFAULT_CHUNK_LAT = 200
DEFAULT_LOG_DIR = Path("/opt/zfs/project/logs")
DOWNLOAD_WAIT_SECONDS = (10, 30, 60, 120, 300, 300)
DATE_RE = re.compile(r"AQUA_MODIS\.(\d{8})\.")


@dataclass(frozen=True)
class ProductSpec:
    keyword: str
    variable: str
    quality_candidates: tuple[str, ...]
    canonical_unit: str
    day_night: str


PRODUCTS: dict[str, ProductSpec] = {
    "CHL": ProductSpec("CHL.chlor_a.4km", "chlor_a", ("l3m_qual",), "mg m^-3", "day"),
    "NFLH": ProductSpec(
        "FLH.nflh.4km",
        "nflh",
        ("l3m_qual",),
        "mW cm^-2 um^-1 sr^-1",
        "day",
    ),
    "POC": ProductSpec("POC.poc.4km", "poc", ("l3m_qual",), "mg m^-3", "day"),
    "SST": ProductSpec(
        "SST.sst.4km", "sst", ("qual_sst", "l3m_qual"), "degree_C", "day"
    ),
    "NSST": ProductSpec(
        "NSST.sst.4km",
        "sst",
        ("qual_sst", "l3m_qual"),
        "degree_C",
        "night",
    ),
    "SST4": ProductSpec(
        "SST4.sst4.4km",
        "sst4",
        ("qual_sst4", "l3m_qual"),
        "degree_C",
        "night_preferred",
    ),
}


def default_data_root() -> Path:
    configured = os.getenv("NASA_DATA_ROOT")
    if configured:
        return Path(configured)
    deployment_root = Path("/opt/zfs/project/data")
    if Path("/opt/zfs/project").exists():
        return deployment_root
    return Path(__file__).resolve().parent / "data"


def configure_logging(verbose: bool = False) -> None:
    log_dir = DEFAULT_LOG_DIR
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"nasa_dl_v2_{datetime.now():%Y-%m-%d}.log"
    handlers: list[logging.Handler] = [logging.FileHandler(log_path, encoding="utf-8")]
    if verbose:
        handlers.append(logging.StreamHandler())
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=handlers,
        force=True,
    )


def json_safe(value: Any) -> Any:
    """Convert NetCDF/numpy attributes to JSON-safe values."""
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, np.ndarray):
        return [json_safe(item) for item in value.tolist()]
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, (datetime, date, Path)):
        return str(value)
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def encoded_json(value: Any) -> bytes:
    return json.dumps(json_safe(value), ensure_ascii=False, sort_keys=True).encode(
        "utf-8"
    )


def sha256_file(path: Path, block_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(block_size), b""):
            digest.update(block)
    return digest.hexdigest()


def parse_observation_date(nc_path: Path, supplied: str | None = None) -> date:
    if supplied:
        return datetime.strptime(supplied, "%Y-%m-%d").date()
    match = DATE_RE.search(nc_path.name)
    if not match:
        raise ValueError("無法從檔名解析日期，請提供 --date YYYY-MM-DD")
    return datetime.strptime(match.group(1), "%Y%m%d").date()


def resolve_coordinate_name(ds: xr.Dataset, candidates: tuple[str, ...]) -> str:
    for name in candidates:
        if name in ds.coords or name in ds.variables:
            return name
    raise ValueError(f"NetCDF 缺少必要座標欄位，候選名稱: {candidates}")


def normalize_grid_variable(
    variable: xr.DataArray, lat_name: str, lon_name: str
) -> xr.DataArray:
    """Remove only singleton non-spatial dimensions and retain lat/lon."""
    for dim in tuple(variable.dims):
        if dim in (lat_name, lon_name):
            continue
        if variable.sizes[dim] != 1:
            raise ValueError(
                f"變數 {variable.name} 含非單值額外維度 {dim}={variable.sizes[dim]}"
            )
        variable = variable.isel({dim: 0}, drop=True)
    if set(variable.dims) != {lat_name, lon_name}:
        raise ValueError(
            f"變數 {variable.name} 維度 {variable.dims} 不符合 lat/lon 網格"
        )
    return variable.transpose(lat_name, lon_name)


def find_quality_variable(
    ds: xr.Dataset,
    spec: ProductSpec,
    lat_name: str,
    lon_name: str,
) -> str | None:
    for name in spec.quality_candidates:
        if name not in ds.variables:
            continue
        try:
            normalize_grid_variable(ds[name], lat_name, lon_name)
        except ValueError:
            continue
        else:
            return name
    return None


def missing_sentinels(variable: xr.DataArray) -> list[float]:
    sentinels: list[float] = []
    for source in (variable.attrs, variable.encoding):
        for key in ("_FillValue", "missing_value"):
            raw = source.get(key)
            if raw is None:
                continue
            for item in np.atleast_1d(raw):
                try:
                    sentinels.append(float(item))
                except (TypeError, ValueError):
                    continue
    return sentinels


def parquet_metadata(
    ds: xr.Dataset,
    variable: xr.DataArray,
    lat: xr.DataArray,
    lon: xr.DataArray,
    product: str,
    spec: ProductSpec,
    quality_name: str | None,
    source_path: Path,
    observation_date: date,
    source_sha256: str,
) -> dict[bytes, bytes]:
    unit = variable.attrs.get("units") or spec.canonical_unit
    return {
        b"pipeline_name": b"nasa_dl_v2",
        b"pipeline_version": PIPELINE_VERSION.encode(),
        b"layer": b"bronze",
        b"source_agency": b"NASA OB.DAAC",
        b"source_file": source_path.name.encode("utf-8"),
        b"source_sha256": source_sha256.encode(),
        b"observation_date": observation_date.isoformat().encode(),
        b"nasa_product": product.encode(),
        b"nasa_variable": spec.variable.encode(),
        b"quality_source_variable": (quality_name or "").encode(),
        b"units": str(unit).encode("utf-8"),
        b"day_night": spec.day_night.encode(),
        b"spatial_resolution": b"4km",
        b"coordinate_reference_system": b"EPSG:4326",
        b"missing_value_policy": b"CF-decoded null, NaN, infinity and leaked fill sentinels removed only",
        b"dataset_attrs_json": encoded_json(ds.attrs),
        b"variable_attrs_json": encoded_json(variable.attrs),
        b"variable_encoding_json": encoded_json(variable.encoding),
        b"lat_attrs_json": encoded_json(lat.attrs),
        b"lon_attrs_json": encoded_json(lon.attrs),
    }


def convert_netcdf_to_parquet(
    nc_path: Path,
    parquet_path: Path,
    product: str,
    observation_date: date,
    chunk_lat: int = DEFAULT_CHUNK_LAT,
    overwrite: bool = False,
) -> dict[str, Any]:
    """CF-decode one NetCDF file and remove only missing observations."""
    product = product.upper()
    spec = PRODUCTS[product]
    if parquet_path.exists() and not overwrite:
        raise FileExistsError(f"輸出已存在: {parquet_path}；需要重建時使用 --overwrite")
    if chunk_lat <= 0:
        raise ValueError("chunk_lat 必須大於 0")

    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = parquet_path.with_name(f".{parquet_path.name}.{os.getpid()}.tmp")
    if temp_path.exists():
        temp_path.unlink()

    writer: pq.ParquetWriter | None = None
    source_sha256 = sha256_file(nc_path)
    source_pixels = 0
    written_rows = 0
    removed_missing = 0

    try:
        # decode_cf=True is essential: it applies _FillValue, scale_factor and
        # add_offset lazily to each isel() chunk. It does not load the full grid.
        with xr.open_dataset(
            nc_path, decode_cf=True, mask_and_scale=True, cache=False
        ) as ds:
            if spec.variable not in ds.variables:
                raise ValueError(
                    f"{nc_path.name} 缺少變數 {spec.variable}；實際變數: {list(ds.variables)}"
                )

            lat_name = resolve_coordinate_name(ds, ("lat", "latitude"))
            lon_name = resolve_coordinate_name(ds, ("lon", "longitude"))
            variable = normalize_grid_variable(ds[spec.variable], lat_name, lon_name)
            lat = ds[lat_name]
            lon = ds[lon_name]

            if lat.ndim != 1 or lon.ndim != 1:
                raise ValueError("目前只支援一維 lat/lon 的 NASA Level-3 mapped grid")
            quality_name = find_quality_variable(ds, spec, lat_name, lon_name)
            quality = None
            if quality_name:
                quality = normalize_grid_variable(ds[quality_name], lat_name, lon_name)

            metadata = parquet_metadata(
                ds,
                variable,
                lat,
                lon,
                product,
                spec,
                quality_name,
                nc_path,
                observation_date,
                source_sha256,
            )
            fields = [
                pa.field("date", pa.date32(), nullable=False),
                pa.field("lat", pa.float32(), nullable=False),
                pa.field("lon", pa.float32(), nullable=False),
                pa.field(spec.variable, pa.float32(), nullable=False),
            ]
            if quality is not None:
                fields.append(pa.field("quality_level", pa.int16(), nullable=True))
            schema = pa.schema(fields, metadata=metadata)
            writer = pq.ParquetWriter(temp_path, schema, compression="snappy")

            lat_values = np.asarray(lat.values, dtype=np.float32)
            lon_values = np.asarray(lon.values, dtype=np.float32)
            fill_values = missing_sentinels(variable)

            for start in range(0, lat_values.size, chunk_lat):
                stop = min(start + chunk_lat, lat_values.size)
                chunk = variable.isel({lat_name: slice(start, stop)}).values
                masked = np.ma.asarray(chunk)
                values = np.asarray(masked.filled(np.nan), dtype=np.float64)
                source_pixels += values.size

                valid = np.isfinite(values) & ~np.ma.getmaskarray(masked)
                # Defensive check: if a backend leaks a fill sentinel despite
                # CF decoding, do not persist it as a real observation.
                for sentinel in fill_values:
                    if np.isfinite(sentinel):
                        valid &= values != sentinel

                valid_count = int(valid.sum())
                removed_missing += values.size - valid_count
                if valid_count == 0:
                    continue

                lat_index, lon_index = np.nonzero(valid)
                output: dict[str, pa.Array] = {
                    "date": pa.array(
                        np.full(valid_count, np.datetime64(observation_date, "D")),
                        type=pa.date32(),
                    ),
                    "lat": pa.array(
                        lat_values[start:stop][lat_index], type=pa.float32()
                    ),
                    "lon": pa.array(lon_values[lon_index], type=pa.float32()),
                    spec.variable: pa.array(
                        values[valid].astype(np.float32), type=pa.float32()
                    ),
                }

                if quality is not None:
                    quality_values = np.asarray(
                        np.ma.asarray(
                            quality.isel({lat_name: slice(start, stop)}).values
                        ).filled(np.nan),
                        dtype=np.float64,
                    )[valid]
                    quality_null = ~np.isfinite(quality_values)
                    quality_int = np.where(quality_null, 0, quality_values).astype(
                        np.int16
                    )
                    output["quality_level"] = pa.array(
                        quality_int, mask=quality_null, type=pa.int16()
                    )

                writer.write_table(pa.table(output, schema=schema))
                written_rows += valid_count

        if writer is not None:
            writer.close()
            writer = None
        if written_rows == 0:
            raise ValueError("NetCDF 沒有任何可寫入的有效觀測")
        os.replace(temp_path, parquet_path)
    except Exception:
        if writer is not None:
            writer.close()
        if temp_path.exists():
            temp_path.unlink()
        raise

    stats = {
        "product": product,
        "source_file": str(nc_path),
        "output_file": str(parquet_path),
        "source_pixels": source_pixels,
        "written_rows": written_rows,
        "removed_missing": removed_missing,
        "missing_ratio": removed_missing / source_pixels if source_pixels else None,
        "source_sha256": source_sha256,
    }
    return stats


def download_file(filename: str, target: Path, cookie: str) -> tuple[bool, int]:
    url = f"https://oceandata.sci.gsfc.nasa.gov/getfile/{filename}"
    partial = target.with_suffix(target.suffix + ".part")
    target.parent.mkdir(parents=True, exist_ok=True)

    for attempt in range(len(DOWNLOAD_WAIT_SECONDS) + 1):
        try:
            with requests.get(
                url,
                headers={"Cookie": cookie},
                timeout=(20, 120),
                stream=True,
            ) as response:
                if response.status_code == 429:
                    if attempt >= len(DOWNLOAD_WAIT_SECONDS):
                        return False, 429
                    wait = DOWNLOAD_WAIT_SECONDS[attempt]
                    time.sleep(wait)
                    continue
                if response.status_code != 200:
                    return False, response.status_code
                content_type = response.headers.get("Content-Type", "").lower()
                if "html" in content_type:
                    return False, 401

                with partial.open("wb") as stream:
                    for block in response.iter_content(1024 * 1024):
                        if block:
                            stream.write(block)
            if partial.stat().st_size < NC_MIN_SIZE:
                partial.unlink(missing_ok=True)
                return False, 422
            os.replace(partial, target)
            return True, 200
        except requests.RequestException:
            partial.unlink(missing_ok=True)
            if attempt >= len(DOWNLOAD_WAIT_SECONDS):
                return False, 500
            time.sleep(min(5 * (attempt + 1), 30))
    return False, 500


def build_names(day: date, product: str) -> tuple[str, str, str]:
    spec = PRODUCTS[product]
    base = f"AQUA_MODIS.{day:%Y%m%d}.L3m.DAY.{spec.keyword}"
    return f"{base}.nc", f"{base}.NRT.nc", f"{base}.parquet"


def run_date_range(args: argparse.Namespace) -> int:
    if not args.start or not args.end:
        raise ValueError("下載模式必須同時提供 --start 與 --end")
    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end = datetime.strptime(args.end, "%Y-%m-%d").date()
    if end < start:
        raise ValueError("--end 不可早於 --start")
    cookie = os.getenv("NASA_COOKIE")
    if not cookie:
        raise RuntimeError("找不到 NASA_COOKIE 環境變數")

    total = 0
    succeeded = 0
    failed_files: list[str] = []
    current = start
    while current <= end:
        total += 1

        raw_folder = args.data_root / "raw" / args.type / f"{current:%Y}"
        bronze_folder = args.data_root / "bronze" / args.type / f"{current:%Y}"

        science_name, nrt_name, parquet_name = build_names(current, args.type)
        logging.info("處理中: %s", science_name)

        parquet_path = bronze_folder / parquet_name
        if parquet_path.exists() and not args.overwrite:
            succeeded += 1
            logging.info("成功: %s", science_name)
            current += timedelta(days=1)
            continue

        nc_path = raw_folder / science_name
        success, status = download_file(science_name, nc_path, cookie)
        if not success and status == 404:
            nc_path = raw_folder / nrt_name
            success, status = download_file(nrt_name, nc_path, cookie)
        if not success:
            failed_files.append(science_name)
            logging.error("失敗: %s", science_name)
            current += timedelta(days=1)
            continue

        try:
            convert_netcdf_to_parquet(
                nc_path,
                parquet_path,
                args.type,
                current,
                args.chunk_lat,
                args.overwrite,
            )
            if args.delete_nc_after_success:
                nc_path.unlink(missing_ok=True)
            succeeded += 1
            logging.info("成功: %s", nc_path.name)
        except Exception:
            failed_files.append(nc_path.name)
            logging.error("失敗: %s", nc_path.name)
        current += timedelta(days=1)

    log_summary(total, succeeded, failed_files)
    return 1 if failed_files else 0


def log_summary(total: int, succeeded: int, failed_files: list[str]) -> None:
    logging.info("全部處理完成")
    logging.info("總檔案數: %d", total)
    logging.info("成功數: %d", succeeded)
    logging.info("失敗數: %d", len(failed_files))
    logging.info("失敗檔案: %s", ", ".join(failed_files) if failed_files else "無")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--type", required=True, choices=sorted(PRODUCTS))
    parser.add_argument("--start", help="下載起日 YYYY-MM-DD")
    parser.add_argument("--end", help="下載迄日 YYYY-MM-DD")
    parser.add_argument("--input-nc", type=Path, help="只轉換指定的本機 NetCDF，不下載")
    parser.add_argument("--output", type=Path, help="本機轉換模式的輸出 Parquet")
    parser.add_argument("--date", help="本機轉換模式的觀測日期；省略時由檔名解析")
    parser.add_argument("--data-root", type=Path, default=default_data_root())
    parser.add_argument("--chunk-lat", type=int, default=DEFAULT_CHUNK_LAT)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument(
        "--delete-nc-after-success",
        action="store_true",
        help="轉檔成功後刪除 .nc；預設保留原始檔以利追溯",
    )
    parser.add_argument("--verbose", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    args.type = args.type.upper()
    configure_logging(args.verbose)
    try:
        if args.input_nc:
            logging.info("處理中: %s", args.input_nc.name)
            observation_date = parse_observation_date(args.input_nc, args.date)
            output = args.output or args.input_nc.with_suffix(".parquet")
            stats = convert_netcdf_to_parquet(
                args.input_nc,
                output,
                args.type,
                observation_date,
                args.chunk_lat,
                args.overwrite,
            )
            logging.info("成功: %s", args.input_nc.name)
            log_summary(1, 1, [])
            print(json.dumps(stats, ensure_ascii=False, indent=2))
            return 0
        return run_date_range(args)
    except Exception as exc:
        failed_name = args.input_nc.name if args.input_nc else "pipeline"
        logging.error("失敗: %s", failed_name)
        log_summary(1 if args.input_nc else 0, 0, [failed_name])
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
