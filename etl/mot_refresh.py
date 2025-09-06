#!/usr/bin/env python3
"""
Load DVSA MOT data (2024 monthly zips) into Supabase:

- mot_tests           ← Test Results CSVs (test_result_YYYYMM.csv)
- mot_test_items      ← Test Items CSVs   (test_item_YYYYMM.csv)
- mot_failure_codes   ← seeded from item_detail.csv (lookup)

Idempotent upserts:
- mot_tests.on_conflict = test_id
- mot_test_items.on_conflict = test_id,rfr_id,rfr_type_code,mot_test_rfr_location_type_id

Env:
  SUPABASE_URL
  SUPABASE_SERVICE_ROLE_KEY
  YEARS="2024" (comma-separated ok)
  DATA_DIR (optional; default apps/web/etl/data)
  BATCH_SIZE (optional; default 10000)
"""

from __future__ import annotations
import csv
import os
import sys
from pathlib import Path
from typing import Dict, Iterator, List
from datetime import datetime

from supabase import create_client

# ---------- CSV field size bump (avoid "_csv.Error: field larger than field limit") ----------
_max = sys.maxsize
while True:
    try:
        csv.field_size_limit(_max)
        break
    except OverflowError:
        _max = int(_max / 10)

# ---------- Config ----------
DEFAULT_DATA_DIR = Path("apps/web/etl/data")
DATA_DIR = Path(os.getenv("DATA_DIR") or DEFAULT_DATA_DIR)
BATCH_SIZE = int(os.getenv("BATCH_SIZE") or "10000")

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

print(f"[INFO] DATA_DIR = {DATA_DIR.resolve()}")

# ---------- Helpers ----------

def _open_csv(path: Path) -> Iterator[Dict[str, str]]:
    """
    Open a DVSA CSV with headers.
    - Auto-detect delimiter (pipe vs comma) from the first line
    - Tolerant decoding: utf-8-sig → cp1252 → latin-1
    """
    encodings = ["utf-8-sig", "cp1252", "latin-1"]
    last_err = None

    # detect delimiter
    chosen_delim = None
    for enc in encodings:
        try:
            with path.open("r", encoding=enc, newline="") as f:
                header_line = f.readline()
            chosen_delim = "|" if header_line.count("|") >= header_line.count(",") else ","
            break
        except UnicodeDecodeError as e:
            last_err = e
            continue
    if chosen_delim is None:
        raise last_err or UnicodeDecodeError("utf-8", b"", 0, 1, "Unknown decode error")

    # reopen and stream rows
    for enc in encodings:
        try:
            with path.open("r", encoding=enc, newline="") as f:
                reader = csv.DictReader(f, delimiter=chosen_delim)
                for row in reader:
                    # normalize keys/values
                    yield {(k or "").strip(): (v.strip() if isinstance(v, str) else v)
                           for k, v in row.items()}
            return
        except UnicodeDecodeError as e:
            last_err = e
            continue
    raise last_err or UnicodeDecodeError("utf-8", b"", 0, 1, "Unknown decode error")


def _parse_date(s: str | None) -> str | None:
    if not s:
        return None
    s = s.strip()
    # accept YYYY-MM-DD or DD/MM/YYYY or YYYY/MM/DD
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            pass
    # already ISO? (defensive)
    if len(s) == 10 and s[4] in "-/" and s[7] in "-/":
        try:
            return datetime.fromisoformat(s.replace("/", "-")).date().isoformat()
        except Exception:
            return None
    return None


def _to_int(s: str | None) -> int | None:
    if s is None or s == "":
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def _to_bool(s: str | None) -> bool | None:
    if s is None:
        return None
    t = s.strip().lower()
    if t in ("true", "t", "1", "y", "yes"):
        return True
    if t in ("false", "f", "0", "n", "no"):
        return False
    return None


def _dedupe(rows: List[Dict], keys: List[str]) -> List[Dict]:
    """Return rows with unique key tuples per batch; keep the first occurrence."""
    seen = set()
    out: List[Dict] = []
    for r in rows:
        key = tuple(r.get(k) for k in keys)
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out

# ---------- Header maps (mirror DVSA 2024) ----------

RESULT_ALIASES = {
    # canonical key on right
    "test_id": "test_id",
    "vehicle_id": "vehicle_id",
    "test_date": "test_date",
    "test_class_id": "test_class_id",
    "test_type": "test_type",
    "test_result": "test_result",
    "test_mileage": "test_mileage",
    "postcode_area": "postcode_area",
    "make": "make",
    "model": "model",
    "colour": "colour",
    "fuel_type": "fuel_type",
    "cylinder_capacity": "cylinder_capacity",
    "first_use_date": "first_use_date",
    "completed_date": "completed_date",
    # legacy fallbacks
    "testid": "test_id",
    "vehicleid": "vehicle_id",
}

ITEM_ALIASES = {
    "test_id": "test_id",
    "rfr_id": "rfr_id",
    "rfr_type_code": "rfr_type_code",
    "mot_test_rfr_location_type_id": "mot_test_rfr_location_type_id",
    "dangerous_mark": "dangerous_mark",
    "completed_date": "completed_date",
    # legacy
    "testid": "test_id",
}


def _alias(row: Dict[str, str], mapping: Dict[str, str]) -> Dict[str, str]:
    out = {}
    for k, v in row.items():
        kk = mapping.get((k or "").lower(), (k or "").lower())
        out[kk] = v
    return out


# ---------- Seed failure code lookup ----------

def seed_mot_failure_codes() -> None:
    # from lookup CSV you already provided (apps/web/etl/failure_codes.csv OR item_detail.csv)
    candidates = [
        DATA_DIR.parent.parent / "etl" / "failure_codes.csv",    # legacy path
        DATA_DIR / "item_detail.csv",                            # DVSA lookup
    ]
    src = next((p for p in candidates if p.exists()), None)
    if not src:
        print("[INFO] No failure code lookup CSV found; skipping seed.")
        return

    print("[INFO] Seeding mot_failure_codes from", src.name, "…")
    total = 0
    seen: set[str] = set()
    batch: List[Dict] = []

    def flush():
        nonlocal batch, total
        if batch:
            sb.table("mot_failure_codes").upsert(batch, on_conflict="code").execute()
            total += len(batch)
            batch.clear()

    for r in _open_csv(src):
        code = (r.get("code") or r.get("rfr_id") or "").strip()
        if not code or code in seen:
            continue
        seen.add(code)
        desc = (r.get("description") or r.get("rfr_id_description") or "").strip()
        batch.append({"code": code, "description": desc})
        if len(batch) >= BATCH_SIZE:
            flush()
    flush()

    print(f"[INFO] Seeded/updated {total} mot_failure_codes.")

# ---------- Loaders ----------

def _iter_results_file(path: Path) -> Iterator[Dict]:
    for raw in _open_csv(path):
        r = _alias(raw, RESULT_ALIASES)
        test_id = _to_int(r.get("test_id"))
        if not test_id:
            continue
        yield {
            "test_id": test_id,
            "vehicle_id": _to_int(r.get("vehicle_id")),
            "test_date": _parse_date(r.get("test_date")),
            "test_class_id": r.get("test_class_id") or None,
            "test_type": r.get("test_type") or None,
            "test_result": (r.get("test_result") or "").upper() or None,
            "test_mileage": _to_int(r.get("test_mileage")),
            "postcode_area": r.get("postcode_area") or None,
            "make": r.get("make") or None,
            "model": r.get("model") or None,
            "colour": r.get("colour") or None,
            "fuel_type": r.get("fuel_type") or None,
            "cylinder_capacity": _to_int(r.get("cylinder_capacity")),
            "first_use_date": _parse_date(r.get("first_use_date")),
            "completed_date": _parse_date(r.get("completed_date")),
        }


def _iter_items_file(path: Path) -> Iterator[Dict]:
    for raw in _open_csv(path):
        r = _alias(raw, ITEM_ALIASES)
        test_id = _to_int(r.get("test_id"))
        rfr_id = (r.get("rfr_id") or "").strip()
        rfr_type = (r.get("rfr_type_code") or "").strip()
        loc = (r.get("mot_test_rfr_location_type_id") or "").strip()
        if not (test_id and rfr_id and rfr_type and loc):
            continue
        yield {
            "test_id": test_id,
            "rfr_id": rfr_id,
            "rfr_type_code": rfr_type,
            "mot_test_rfr_location_type_id": loc,
            "dangerous_mark": _to_bool(r.get("dangerous_mark")),
            "completed_date": _parse_date(r.get("completed_date")),
        }


def _upsert_results(rows: List[Dict]) -> None:
    if not rows:
        return
    # ensure one row per test_id in this statement
    rows = _dedupe(rows, ["test_id"])
    if rows:
        print(f"[INFO] Upserting {len(rows)} results …")
        sb.table("mot_tests").upsert(rows, on_conflict="test_id").execute()


def _upsert_items(rows: List[Dict]) -> None:
    if not rows:
        return
    # ensure one row per composite key in this statement
    rows = _dedupe(rows, ["test_id", "rfr_id", "rfr_type_code", "mot_test_rfr_location_type_id"])
    if rows:
        print(f"[INFO] Upserting {len(rows)} items …")
        sb.table("mot_test_items").upsert(
            rows,
            on_conflict="test_id,rfr_id,rfr_type_code,mot_test_rfr_location_type_id",
        ).execute()


def _files_for_year(year: int, prefix: str) -> List[Path]:
    # e.g. prefix="test_result_" or "test_item_"
    return sorted(DATA_DIR.glob(f"{prefix}{year}??.csv"))


def load_year(year: int) -> int:
    total = 0

    results = _files_for_year(year, "test_result_")
    items   = _files_for_year(year, "test_item_")

    if results:
        print(f"[INFO] Reading monthly TESTRESULT files for {year} ({len(results)}) …")
    for f in results:
        print(f"[INFO]  -> {f.name}")
        batch: List[Dict] = []
        for row in _iter_results_file(f):
            batch.append(row)
            if len(batch) >= BATCH_SIZE:
                _upsert_results(batch)
                total += len(batch)
                batch.clear()
        if batch:
            _upsert_results(batch)
            total += len(batch)

    if items:
        print(f"[INFO] Reading monthly TESTITEM files for {year} ({len(items)}) …")
    for f in items:
        print(f"[INFO]  -> {f.name}")
        batch = []
        for row in _iter_items_file(f):
            batch.append(row)
            if len(batch) >= BATCH_SIZE:
                _upsert_items(batch)
                batch.clear()
        if batch:
            _upsert_items(batch)

    return total


def _detect_years() -> List[int]:
    env = os.getenv("YEARS")
    if env:
        return [int(y.strip()) for y in env.split(",") if y.strip()]
    # autodetect from filenames
    ys = set()
    for p in DATA_DIR.glob("test_result_*.csv"):
        ys.add(int(p.stem.split("_")[2][:4]))
    for p in DATA_DIR.glob("test_item_*.csv"):
        ys.add(int(p.stem.split("_")[2][:4]))
    return sorted(ys)


def main():
    seed_mot_failure_codes()

    years = _detect_years()
    if not years:
        print("[WARN] No YEARS detected/found. Put CSVs into", DATA_DIR)
        return
    print(f"[INFO] Loading years: {years}")

    grand_total = 0
    for y in years:
        grand_total += load_year(y)

    print(f"[DONE] Total upserts into mot_tests: {grand_total}")


if __name__ == "__main__":
    main()
