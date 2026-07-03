"""Run the source-specific collectors behind one Bronze entrypoint.

Collectors live in ``pipeline_iceberg/collectors`` and are shared by both
pipeline variants.
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import sys
from pathlib import Path
from types import ModuleType

NASA_PRODUCTS = ("CHL", "NFLH", "POC", "SST", "NSST", "SST4")


def load_module(path: Path, name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load collector: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", type=Path, default=Path(os.getenv("PROJECT_ROOT", "/opt/zfs/project")))
    parser.add_argument("--data-root", type=Path, required=True, help="Parent of local bronze/, for example /opt/zfs/project/data")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--source", choices=("nasa", "gfw", "all"), default="all")
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def main() -> int:
    options = parse_args()
    scripts = options.project_root / "pipeline_iceberg" / "collectors"
    failures: list[str] = []
    if options.source in {"nasa", "all"}:
        nasa = load_module(scripts / "nasa.py", "ocean_nasa_collector")
        for product in NASA_PRODUCTS:
            argv = ["--type", product, "--start", options.start, "--end", options.end, "--data-root", str(options.data_root)]
            if options.overwrite:
                argv.append("--overwrite")
            if int(nasa.main(argv)) != 0:
                failures.append(f"NASA:{product}")
    if options.source in {"gfw", "all"}:
        gfw = load_module(scripts / "gfw.py", "ocean_gfw_collector")
        gfw.BASE_DATA_DIR = options.data_root
        gfw.RAW_DIR = options.data_root / "raw"
        gfw.TMP_DIR = options.data_root / "_gfw_tmp"
        gfw.EXTRACT_DIR = gfw.TMP_DIR / "csv"
        gfw.MANIFEST = gfw.TMP_DIR / "gfw_done.json"

        def parquet_dir(year: int) -> Path:
            target = options.data_root / "bronze" / "GFW" / str(year)
            target.mkdir(parents=True, exist_ok=True)
            return target

        gfw.get_parquet_dir = parquet_dir
        gfw.download_data(options.start, options.end, None)
    if failures:
        raise RuntimeError(f"Bronze collectors failed: {', '.join(failures)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
