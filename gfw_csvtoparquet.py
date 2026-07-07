"""
csv_to_parquet.py

批次將 GFW bronze 目錄下（多個年份子目錄）的 CSV 檔案轉換為 Parquet。

目錄結構假設：
    E:\\data\\bronze\\GFW\\
        2022\\
            xxx.csv
            yyy.csv
        2023\\
            zzz.csv
        ...

輸出結構（預設鏡射到 silver 層，維持年份子目錄）：
    E:\\data\\silver\\GFW\\
        2022\\
            xxx.parquet
            yyy.parquet
        2023\\
            zzz.parquet

用法：
    python csv_to_parquet.py
    python csv_to_parquet.py --input E:\\data\\bronze\\GFW --output E:\\data\\silver\\GFW
    python csv_to_parquet.py --in-place        # 直接在原目錄輸出同名 .parquet
    python csv_to_parquet.py --overwrite       # 覆蓋已存在的 parquet
    python csv_to_parquet.py --workers 4       # 平行處理

特性（延續你其他 GFW pipeline 的慣例）：
    - Idempotent：預設略過已存在且較新的 parquet（除非 --overwrite）
    - Chunked 讀取，避免大檔案吃爆記憶體
    - 逐日期 log 檔，append mode，含 Start/End session 標記
    - 失敗時清除半成品檔案（safe_remove），避免留下損毀的 parquet
"""

import argparse
import logging
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

CHUNK_SIZE = 200_000  # 每次讀取的列數，依機器記憶體可調整

# GFW fleet-daily-csvs 常見欄位的固定型別。
# 明確指定型別可避免 pandas 對每個 chunk 各自猜測 dtype，
# 導致像 fishing_hours 在不同 chunk 之間 int64/float64 不一致的問題。
# 只有「CSV 裡實際存在」的欄位才會套用，不存在的欄位會自動略過。
KNOWN_DTYPE_OVERRIDES = {
    "cell_ll_lat": "float64",
    "cell_ll_lon": "float64",
    "hours": "float64",
    "fishing_hours": "float64",   # 沒有捕魚活動時可能是 NaN，務必固定為 float
    "mmsi_present": "float64",    # 同樣可能有 NaN，讀入後視需要再轉型
}

# ---------------------------------------------------------------------------
# 手動設定區：直接改這裡的路徑即可，不需要每次下命令列參數
# ---------------------------------------------------------------------------
raw_data_path = r"D:\data\bronze\GFW\2024"       # 輸入根目錄（內含多個年份子目錄）
output_data_path = r"D:\data\silver\GFW\2024"    # 輸出根目錄（鏡射年份子目錄結構）

# ---------------------------------------------------------------------------
# Logging：沿用 date-based 檔名 + append mode + Start/End 標記的慣例
# ---------------------------------------------------------------------------
def setup_logger(log_dir: Path) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"csv_to_parquet_{datetime.now():%Y%m%d}.log"

    logger = logging.getLogger("csv_to_parquet")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")

    fh = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    return logger


def safe_remove(path: Path):
    """轉換失敗時，清除半成品檔案，避免留下損毀的 parquet。"""
    try:
        if path.exists():
            path.unlink()
    except OSError:
        pass


def needs_conversion(csv_path: Path, parquet_path: Path, overwrite: bool) -> bool:
    if overwrite:
        return True
    if not parquet_path.exists():
        return True
    # 若來源 CSV 比既有 parquet 新，視為需要重新轉換
    return csv_path.stat().st_mtime > parquet_path.stat().st_mtime


def convert_one(csv_path: Path, parquet_path: Path, chunk_size: int = CHUNK_SIZE) -> tuple[str, str]:
    """
    將單一 CSV 以 chunk 方式轉為 Parquet（streaming ParquetWriter）。
    回傳 (狀態, 訊息)，狀態為 'ok' / 'skip' / 'error'。
    """
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    writer = None
    try:
        # 先讀 header，決定哪些已知欄位需要固定 dtype
        header_cols = pd.read_csv(csv_path, nrows=0).columns
        dtype_map = {
            col: dtype for col, dtype in KNOWN_DTYPE_OVERRIDES.items() if col in header_cols
        }

        reader = pd.read_csv(csv_path, chunksize=chunk_size, low_memory=False, dtype=dtype_map)
        for chunk in reader:
            table = pa.Table.from_pandas(chunk, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(parquet_path, table.schema, compression="snappy")
            else:
                # pandas 對每個 chunk 各自推斷 dtype，若某欄位在不同 chunk
                # 出現 int64/float64 混雜（例如有無 NaN 造成推斷不同），
                # 這裡統一 cast 成第一個 chunk（writer）建立時的 schema，
                # 避免 "Table schema does not match" 的錯誤。
                if not table.schema.equals(writer.schema):
                    table = table.cast(writer.schema)
            writer.write_table(table)

        if writer is None:
            # CSV 是空檔（只有 header 或完全空白）
            pd.DataFrame().to_parquet(parquet_path)
            return "ok", f"{csv_path.name} 為空檔，已建立空 parquet"

        return "ok", f"{csv_path.name} -> {parquet_path.name}"

    except Exception as e:  # noqa: BLE001
        return "error", f"{csv_path.name} 轉換失敗：{e}"

    finally:
        if writer is not None:
            writer.close()


def _worker(args):
    csv_path, parquet_path = args
    status, msg = convert_one(csv_path, parquet_path)
    if status == "error":
        safe_remove(parquet_path)
    return status, msg


def find_csv_files(input_root: Path):
    """遞迴尋找所有年份子目錄下的 CSV 檔案。"""
    return sorted(input_root.rglob("*.csv"))


def build_output_path(csv_path: Path, input_root: Path, output_root: Path) -> Path:
    rel = csv_path.relative_to(input_root).with_suffix(".parquet")
    return output_root / rel


def main():
    parser = argparse.ArgumentParser(description="批次轉換 GFW CSV 為 Parquet")
    parser.add_argument("--input", type=Path, default=Path(raw_data_path),
                         help="輸入根目錄（內含多個年份子目錄），預設讀取檔案開頭的 raw_data_path 變數")
    parser.add_argument("--output", type=Path, default=Path(output_data_path),
                         help="輸出根目錄（鏡射年份子目錄結構），預設讀取檔案開頭的 output_data_path 變數")
    parser.add_argument("--in-place", action="store_true",
                         help="直接在來源目錄輸出同名 .parquet（忽略 --output）")
    parser.add_argument("--overwrite", action="store_true",
                         help="覆蓋已存在的 parquet 檔案")
    parser.add_argument("--workers", type=int, default=1,
                         help="平行處理的行程數（預設 1，單執行緒）")
    parser.add_argument("--log-dir", type=Path, default=Path("logs"),
                         help="log 檔輸出目錄")
    args = parser.parse_args()

    logger = setup_logger(args.log_dir)
    logger.info("=== Start session ===")

    input_root = args.input
    output_root = input_root if args.in_place else args.output

    if not input_root.exists():
        logger.error(f"輸入目錄不存在：{input_root}")
        logger.info("=== End session ===")
        sys.exit(1)

    csv_files = find_csv_files(input_root)
    if not csv_files:
        logger.warning(f"在 {input_root} 底下找不到任何 CSV 檔案")
        logger.info("=== End session ===")
        return

    logger.info(f"找到 {len(csv_files)} 個 CSV 檔案，輸入：{input_root}，輸出：{output_root}")

    tasks = []
    skipped = 0
    for csv_path in csv_files:
        parquet_path = build_output_path(csv_path, input_root, output_root)
        if needs_conversion(csv_path, parquet_path, args.overwrite):
            tasks.append((csv_path, parquet_path))
        else:
            skipped += 1

    logger.info(f"待轉換：{len(tasks)} 個，略過（已存在且較新）：{skipped} 個")

    ok_count = 0
    error_count = 0

    if args.workers > 1 and tasks:
        with ProcessPoolExecutor(max_workers=args.workers) as executor:
            futures = {executor.submit(_worker, t): t for t in tasks}
            for future in as_completed(futures):
                status, msg = future.result()
                if status == "ok":
                    ok_count += 1
                    logger.info(msg)
                else:
                    error_count += 1
                    logger.error(msg)
    else:
        for t in tasks:
            status, msg = _worker(t)
            if status == "ok":
                ok_count += 1
                logger.info(msg)
            else:
                error_count += 1
                logger.error(msg)

    logger.info(f"完成：成功 {ok_count}，失敗 {error_count}，略過 {skipped}")
    logger.info("=== End session ===")

    if error_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()