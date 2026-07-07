"""
gfw_zenodo_to_parquet.py
GFW Apparent Fishing Effort v3 — 單機自動化管線
Zenodo record 14982712

對齊 nasa_dl.py 規範：
  - zip 保留:   /opt/zfs/project/data/raw/GFW/{year}/
  - Parquet:    /opt/zfs/project/data/bronze/GFW/{year}/
  - Log:        /opt/zfs/project/logs/gfw_dl_{today}.log
  - CLI:        --start YYYY-MM-DD --end YYYY-MM-DD [--months MM [MM ...]]
"""

import hashlib
import json
import logging
import re
import shutil
import time
import zipfile
import argparse
from datetime import datetime
from pathlib import Path

import requests
from tqdm import tqdm

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

# ───────────────────────── 固定路徑 ─────────────────────────
BASE_DATA_DIR = Path("/opt/zfs/project/data")
LOG_DIR = Path("/opt/zfs/project/logs")
DATA_TYPE = "GFW"

RAW_DIR = Path("/opt/zfs/project/data/raw")  # zip 基底
TMP_DIR = BASE_DATA_DIR / "_gfw_tmp"
EXTRACT_DIR = TMP_DIR / "csv"  # csv 暫存（轉完清除）
MANIFEST = TMP_DIR / "gfw_done.json"

# ───────────────────────── Zenodo 設定 ─────────────────────────
ZENODO_RECORD_ID = "14982712"
ZENODO_API = f"https://zenodo.org/api/records/{ZENODO_RECORD_ID}"
SERIES = "fleet-daily-csvs-100"
YEAR_RE = re.compile(rf"{re.escape(SERIES)}-v3-(\d{{4}})\.zip$")

# ───────────────────────── 轉檔參數 ─────────────────────────
PARQUET_COMPRESSION = "snappy"
CSV_BLOCK_SIZE = 128 * 1024 * 1024
ROW_GROUP_SIZE = 512 * 1024
CHUNK_SIZE = 1 * 1024 * 1024
TIMEOUT = 60

COLUMN_TYPES = {
    "date": pa.string(),
    "cell_ll_lat": pa.float64(),
    "cell_ll_lon": pa.float64(),
    "flag": pa.string(),
    "geartype": pa.string(),
    "hours": pa.float64(),
    "fishing_hours": pa.float64(),
    "mmsi_present": pa.int64(),
}

CLEANUP = "csv"


# ══════════════════════════════════════════════════════════════
# Log 初始化
# ══════════════════════════════════════════════════════════════
def setup_logging():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    today_str = datetime.now().strftime("%Y-%m-%d")
    log_path = LOG_DIR / f"gfw_dl_{today_str}.log"
    logging.basicConfig(
        filename=str(log_path),
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        filemode="a",
    )
    return log_path


# ══════════════════════════════════════════════════════════════
# 路徑工具
# ══════════════════════════════════════════════════════════════
def get_raw_dir(year: int) -> Path:
    """zip 落點: /opt/zfs/project/data/raw/GFW/{year}/"""
    p = RAW_DIR / DATA_TYPE / str(year)
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_parquet_dir(year: int) -> Path:
    """Parquet 落點: /opt/zfs/project/data/bronze/GFW/{year}/"""
    p = Path("/opt/zfs/project/data/bronze") / DATA_TYPE / str(year)
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_parquet_path(year: int, csv_stem: str) -> Path:
    return get_parquet_dir(year) / f"{csv_stem}.parquet"


# ══════════════════════════════════════════════════════════════
# Manifest
# ══════════════════════════════════════════════════════════════
def load_done() -> dict:
    if MANIFEST.exists():
        return json.loads(MANIFEST.read_text())
    return {}


def mark_done(done: dict, key: str, md5: str):
    done[key] = md5
    MANIFEST.write_text(json.dumps(done, indent=2, ensure_ascii=False))


# ══════════════════════════════════════════════════════════════
# 1. Zenodo 檔案清單
# ══════════════════════════════════════════════════════════════
def get_file_list(years: list[int] | None) -> list[dict]:
    logging.info(f"連線 Zenodo API: {ZENODO_API}")
    resp = requests.get(ZENODO_API, timeout=TIMEOUT)
    resp.raise_for_status()
    record = resp.json()

    files = []
    for f in record["files"]:
        key = f["key"]
        m = YEAR_RE.search(key)
        if not m:
            continue
        year = int(m.group(1))
        if years is not None and year not in years:
            continue
        files.append(
            {
                "key": key,
                "year": year,
                "size": f["size"],
                "url": f["links"]["self"],
                "md5": f["checksum"].replace("md5:", ""),
            }
        )
    files.sort(key=lambda x: x["year"])
    return files


# ══════════════════════════════════════════════════════════════
# 2. MD5 校驗
# ══════════════════════════════════════════════════════════════
def md5_of(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as fp:
        for chunk in iter(lambda: fp.read(CHUNK_SIZE), b""):
            h.update(chunk)
    return h.hexdigest()


def is_valid(path: Path, expected_md5: str) -> bool:
    if not path.exists():
        return False
    ok = md5_of(path) == expected_md5
    logging.info(f"MD5 {'通過' if ok else '不符'}: {path.name}")
    return ok


# ══════════════════════════════════════════════════════════════
# 3. 下載（斷點續傳）
# ══════════════════════════════════════════════════════════════
def download(f: dict) -> Path | None:
    out = get_raw_dir(f["year"]) / f["key"]  # ← 依年份存放

    if is_valid(out, f["md5"]):
        logging.info(f"⏭️ 已存在且完整，跳過下載: {f['key']}")
        return out

    resume = out.stat().st_size if out.exists() else 0
    headers = {"Range": f"bytes={resume}-"} if resume else {}
    mode = "ab" if resume else "wb"

    wait_sched = [10, 30, 60, 120, 300, 300]

    for retry in range(len(wait_sched) + 1):
        try:
            with requests.get(
                f["url"], headers=headers, stream=True, timeout=TIMEOUT
            ) as r:
                if r.status_code == 429:
                    if retry >= len(wait_sched):
                        logging.error(f"🚨 超過重試上限仍遭限速，放棄: {f['key']}")
                        return None
                    wait_sec = wait_sched[retry]
                    logging.warning(
                        f"🚨 HTTP 429 限速 (第 {retry+1}/{len(wait_sched)} 次)，"
                        f"等待 {wait_sec} 秒"
                    )
                    time.sleep(wait_sec)
                    continue

                r.raise_for_status()

                with open(out, mode) as fp, tqdm(
                    total=f["size"],
                    initial=resume,
                    unit="B",
                    unit_scale=True,
                    desc=f"⬇ {f['key']}",
                ) as bar:
                    for chunk in r.iter_content(CHUNK_SIZE):
                        fp.write(chunk)
                        bar.update(len(chunk))
            break

        except Exception as e:
            logging.error(f"⚠️ 網路錯誤 {f['key']}: {e}")
            time.sleep(5)

    if not is_valid(out, f["md5"]):
        logging.error(f"❌ MD5 不符，放棄: {f['key']}")
        out.unlink(missing_ok=True)
        return None

    logging.info(f"✅ 下載完成: {f['key']}")
    return out


# ══════════════════════════════════════════════════════════════
# 4. 解壓（月份篩選）
# ══════════════════════════════════════════════════════════════
def extract(zip_path: Path, months: list[str] | None) -> Path:
    target = EXTRACT_DIR / zip_path.stem
    target.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path) as z:
        members = [m for m in z.namelist() if m.endswith(".csv")]
        if months:
            members = [m for m in members if any(mo in m for mo in months)]
        if not members:
            logging.warning(f"⚠️ 解壓後沒有符合的 CSV: {zip_path.name}")
            return target
        needed = [m for m in members if not (target / m).exists()]
        if not needed:
            logging.info(f"⏭️ 已解壓，跳過: {zip_path.name}")
            return target
        for m in tqdm(needed, desc=f"📦 解壓 {zip_path.name}"):
            z.extract(m, target)
    logging.info(f"✅ 解壓 {len(needed)} 個 CSV → {target}")
    return target


# ══════════════════════════════════════════════════════════════
# 5. CSV → Parquet
# ══════════════════════════════════════════════════════════════
def safe_remove(path: Path):
    try:
        if path.exists():
            path.unlink()
    except OSError as e:
        logging.warning(f"⚠️ 無法刪除: {path} ({e})")


def csv_to_parquet(csv_path: Path, parquet_path: Path) -> bool:
    parquet_written = False
    writer = None

    try:
        read_opts = pacsv.ReadOptions(block_size=CSV_BLOCK_SIZE)
        convert_opts = pacsv.ConvertOptions(column_types=COLUMN_TYPES)
        reader = pacsv.open_csv(
            csv_path, read_options=read_opts, convert_options=convert_opts
        )

        try:
            for batch in reader:
                if writer is None:
                    writer = pq.ParquetWriter(
                        str(parquet_path),
                        batch.schema,
                        compression=PARQUET_COMPRESSION,
                    )
                writer.write_batch(batch)
        finally:
            if writer:
                writer.close()
                writer = None
            reader.close()

        if parquet_path.exists() and parquet_path.stat().st_size > 1024:
            parquet_written = True
            logging.info(f"📦 轉檔成功: {parquet_path.name}")
        else:
            logging.error(f"⚠️ Parquet 為空或不存在: {parquet_path.name}")
            safe_remove(parquet_path)

    except Exception as e:
        logging.error(f"⚠️ 轉檔失敗 {csv_path.name}: {e}")
        safe_remove(parquet_path)

    return parquet_written


# ══════════════════════════════════════════════════════════════
# 6. 批次轉檔
# ══════════════════════════════════════════════════════════════
def convert_dir(csv_dir: Path, year: int, months: list[str] | None) -> bool:
    csv_files = sorted(csv_dir.rglob("*.csv"))
    if months:
        csv_files = [p for p in csv_files if any(mo in p.name for mo in months)]
    if not csv_files:
        logging.warning("⚠️ 沒有符合的 CSV 可轉")
        return False

    all_ok = True
    for csv_path in tqdm(csv_files, desc=f"🔄 轉 Parquet [GFW {year}]"):
        parquet_path = get_parquet_path(year, csv_path.stem)

        if parquet_path.exists() and parquet_path.stat().st_size > 1024:
            logging.info(f"⏭️ 已存在 Parquet，跳過: {parquet_path.name}")
            continue

        ok = csv_to_parquet(csv_path, parquet_path)
        if not ok:
            all_ok = False

    pq_files = list(get_parquet_dir(year).glob("*.parquet"))
    if pq_files:
        csv_sz = sum(p.stat().st_size for p in csv_files)
        pq_sz = sum(p.stat().st_size for p in pq_files)
        ratio = csv_sz / pq_sz if pq_sz else 0
        logging.info(
            f"📊 CSV {csv_sz/1e9:.2f} GB → Parquet {pq_sz/1e9:.2f} GB "
            f"(壓縮比 1/{ratio:.1f})"
        )

    return all_ok


# ══════════════════════════════════════════════════════════════
# 7. 清理中間產物
# ══════════════════════════════════════════════════════════════
def cleanup(csv_dir: Path, zip_path: Path, is_partial: bool):
    if CLEANUP == "none":
        return
    if CLEANUP in ("csv", "all") and csv_dir.exists():
        freed = sum(p.stat().st_size for p in csv_dir.rglob("*")) / 1e9
        shutil.rmtree(csv_dir)
        logging.info(f"🧹 已刪 CSV: {csv_dir.name} (釋放 ~{freed:.1f} GB)")
    if CLEANUP == "all" and not is_partial and zip_path.exists():
        freed = zip_path.stat().st_size / 1e9
        zip_path.unlink()
        logging.info(f"🧹 已刪 zip: {zip_path.name} (釋放 ~{freed:.1f} GB)")


# ══════════════════════════════════════════════════════════════
# 8. 主流程
# ══════════════════════════════════════════════════════════════
def download_data(start_str: str, end_str: str, months: list[str] | None):
    log_path = setup_logging()
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    logging.info(
        f"{now} Start [GFW] {start_str} ~ {end_str}"
        + (f" months={months}" if months else "")
    )

    script_start = time.time()
    total_downloaded = 0
    total_processed = 0
    total_skipped = 0
    missing_years = []

    start_year = int(start_str[:4])
    end_year = int(end_str[:4])
    years = list(range(start_year, end_year + 1))
    is_partial = months is not None

    for d in (RAW_DIR, EXTRACT_DIR):  # ← RAW_DIR 基底建立
        d.mkdir(parents=True, exist_ok=True)

    done = load_done()
    files = get_file_list(years)

    if not files:
        logging.warning("⚠️ Zenodo 上找不到符合年份的檔案。")
        return

    todo = files if is_partial else [f for f in files if done.get(f["key"]) != f["md5"]]
    skip = [] if is_partial else [f for f in files if done.get(f["key"]) == f["md5"]]

    if skip:
        logging.info(f"⏭️ 已完成跳過: {[f['year'] for f in skip]}")
        total_skipped += len(skip)

    logging.info(
        f"🆕 本次處理: {[f['year'] for f in todo]} "
        f"(共約 {sum(f['size'] for f in todo)/1e9:.1f} GB zip)"
    )

    for i, f in enumerate(todo, 1):
        logging.info(f"[{i}/{len(todo)}] {f['key']} (year={f['year']})")

        zip_path = download(f)
        if zip_path is None:
            logging.error(f"❌ 下載失敗，略過: {f['key']}")
            missing_years.append(str(f["year"]))
            continue
        total_downloaded += 1

        csv_dir = extract(zip_path, months)
        ok = convert_dir(csv_dir, f["year"], months)

        if ok:
            total_processed += 1
            cleanup(csv_dir, zip_path, is_partial)
            if not is_partial:
                mark_done(done, f["key"], f["md5"])
                logging.info(f"✔ {f['year']} 完成並記錄 manifest。")
            else:
                logging.info(f"✔ {f['year']} 月份子集處理完成（未寫入 manifest）。")
        else:
            missing_years.append(str(f["year"]))
            logging.error(
                f"✖ {f['year']} Parquet 驗證未過，保留中間檔，不記錄 manifest。"
            )

    elapsed = time.time() - script_start
    logging.info("=" * 50)
    logging.info(f"📊 任務完成統計 [GFW] ({start_str} 至 {end_str})")
    logging.info(f"⏱️ 總處理時間: {elapsed:.2f} 秒 (約 {elapsed/60:.2f} 分鐘)")
    logging.info(f"⬇️ 成功下載 zip 數: {total_downloaded}")
    logging.info(f"📦 成功處理年份數: {total_processed}")
    logging.info(f"⏭️ 跳過年份數: {total_skipped}")
    logging.info(f"❌ 失敗年份數: {len(missing_years)}")
    if missing_years:
        logging.info(f"📅 失敗年份清單: {', '.join(missing_years)}")
    logging.info("=" * 50)

    grand = sum(
        p.stat().st_size
        for year in years
        for p in get_parquet_dir(year).glob("*.parquet")
    )
    logging.info(f"Parquet 總大小: {grand/1e9:.2f} GB")
    logging.info(f"位置: {Path('/opt/zfs/project/data/bronze') / DATA_TYPE}")

    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    logging.info(f"{now} End [GFW] {start_str} ~ {end_str}")


# ══════════════════════════════════════════════════════════════
# 9. Entry point
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="GFW Zenodo → Parquet，對齊 nasa_dl.py 路徑規範"
    )
    parser.add_argument("--start", required=True, help="格式: YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="格式: YYYY-MM-DD")
    parser.add_argument(
        "--months",
        nargs="+",
        default=None,
        metavar="YYYY-MM",
        help="只處理特定月份，例如 --months 2024-01 2024-12（不指定 = 整年）",
    )
    args = parser.parse_args()
    download_data(args.start, args.end, args.months)
