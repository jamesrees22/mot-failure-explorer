#!/usr/bin/env python3
"""
Load DVSA MOT data (2024 monthly zips) into Supabase:

- mot_tests           ← Test Results CSVs (test_result_YYYYMM.csv)
- mot_test_items      ← Test Items CSVs   (test_item_YYYYMM.csv)
- mot_failure_codes   ← seeded from item_detail.csv (lookup)

Env:
  SUPABASE_URL
  SUPABASE_SERVICE_ROLE_KEY
  YEARS="2024"                # comma-separated ok; autodetects if unset
  DATA_DIR (default apps/web/etl/data)
  BATCH_SIZE (default 1000)   # lower = fewer timeouts
  LOAD = "both" | "results" | "items"  (default "both")
  UPSERT_MODE = "insert" | "upsert"     (default "insert")

  # NEW: limit which files are processed without moving files around
  RESULTS_GLOB="test_result_202401.csv"
  ITEMS_GLOB="test_item_202401.csv"
"""

from __future__ import annotations
import csv, os, sys
from pathlib import Path
from typing import Dict, Iterator, List
from datetime import datetime
from supabase import create_client

# --------- pretty/instant printing ----------
def p(msg: str) -> None:
    print(msg, flush=True)

# ---------- CSV field size bump ----------
_max = sys.maxsize
while True:
    try:
        csv.field_size_limit(_max)
        break
    except OverflowError:
        _max = int(_max / 10)

# ---------- Config ----------
DEFAULT_DATA_DIR = Path("apps/web/etl/data")
DATA_DIR     = Path(os.getenv("DATA_DIR") or DEFAULT_DATA_DIR)
BATCH_SIZE   = int(os.getenv("BATCH_SIZE") or "1000")
LOAD_MODE    = (os.getenv("LOAD") or "both").strip().lower()
UPSERT_MODE  = (os.getenv("UPSERT_MODE") or "insert").strip().lower()

# NEW: optional per-file patterns
RESULTS_GLOB = os.getenv("RESULTS_GLOB")  # e.g. "test_result_202401.csv"
ITEMS_GLOB   = os.getenv("ITEMS_GLOB")    # e.g. "test_item_202401.csv"

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

p(f"[INFO] DATA_DIR   = {DATA_DIR.resolve()}")
p(f"[INFO] LOAD_MODE  = {LOAD_MODE}")
p(f"[INFO] UPSERT_MODE= {UPSERT_MODE}")
p(f"[INFO] BATCH_SIZE = {BATCH_SIZE}")
if RESULTS_GLOB:
    p(f"[INFO] RESULTS_GLOB = {RESULTS_GLOB}")
if ITEMS_GLOB:
    p(f"[INFO] ITEMS_GLOB   = {ITEMS_GLOB}")

# ---------- Global de-dupe sets ----------
SEEN_TEST_IDS: set[int] = set()
SEEN_ITEM_KEYS: set[tuple] = set()

# ---------- Helpers ----------
def _open_csv(path: Path) -> Iterator[Dict[str, str]]:
    encodings = ["utf-8-sig", "cp1252", "latin-1"]
    last_err = None
    delim = None
    for enc in encodings:
        try:
            with path.open("r", encoding=enc, newline="") as f:
                header = f.readline()
            delim = "|" if header.count("|") >= header.count(",") else ","
            break
        except UnicodeDecodeError as e:
            last_err = e
    if delim is None:
        raise last_err or UnicodeDecodeError("utf-8", b"", 0, 1, "Unknown decode error")

    for enc in encodings:
        try:
            with path.open("r", encoding=enc, newline="") as f:
                rdr = csv.DictReader(f, delimiter=delim)
                for row in rdr:
                    yield {(k or "").strip(): (v.strip() if isinstance(v, str) else v)
                           for k, v in row.items()}
            return
        except UnicodeDecodeError as e:
            last_err = e
    raise last_err or UnicodeDecodeError("utf-8", b"", 0, 1, "Unknown decode error")

def _parse_date(s: str | None) -> str | None:
    if not s: return None
    s = s.strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(s.replace("/", "-")).date().isoformat()
    except Exception:
        return None

def _to_int(s: str | None) -> int | None:
    if s is None or s == "": return None
    try: return int(float(s))
    except Exception: return None

def _to_bool(s: str | None) -> bool | None:
    if s is None: return None
    t = s.strip().lower()
    if t in ("true","t","1","y","yes"): return True
    if t in ("false","f","0","n","no"): return False
    return None

def _dedupe(rows: List[Dict], keys: List[str]) -> List[Dict]:
    seen, out = set(), []
    for r in rows:
        key = tuple(r.get(k) for k in keys)
        if key in seen: continue
        seen.add(key); out.append(r)
    return out

# ---------- Header maps ----------
RESULT_ALIASES = {
    "test_id":"test_id","vehicle_id":"vehicle_id","test_date":"test_date",
    "test_class_id":"test_class_id","test_type":"test_type","test_result":"test_result",
    "test_mileage":"test_mileage","postcode_area":"postcode_area","make":"make",
    "model":"model","colour":"colour","fuel_type":"fuel_type","cylinder_capacity":"cylinder_capacity",
    "first_use_date":"first_use_date","completed_date":"completed_date",
    "testid":"test_id","vehicleid":"vehicle_id",
}
ITEM_ALIASES = {
    "test_id":"test_id","rfr_id":"rfr_id","rfr_type_code":"rfr_type_code",
    "mot_test_rfr_location_type_id":"mot_test_rfr_location_type_id",
    "dangerous_mark":"dangerous_mark","completed_date":"completed_date",
    "testid":"test_id",
}

def _clean_key(k: str) -> str:
    if not k: return ""
    k = (k.replace("\ufeff","").replace("\xa0"," ")).strip().lower()
    k = k.replace(" ", "_")
    while "__" in k: k = k.replace("__","_")
    return k

def _alias(row: Dict[str,str], mapping: Dict[str,str]) -> Dict[str,str]:
    out = {}
    for k,v in row.items():
        ck = _clean_key(k)
        out[mapping.get(ck, ck)] = v
    return out

# ---------- Seed failure code lookup ----------
def seed_mot_failure_codes() -> None:
    candidates = [
        DATA_DIR.parent.parent / "etl" / "failure_codes.csv",
        DATA_DIR / "item_detail.csv",
    ]
    src = next((p for p in candidates if p.exists()), None)
    if not src:
        p("[INFO] No failure code lookup CSV found; skipping seed.")
        return

    p(f"[INFO] Seeding mot_failure_codes from {src.name} …")
    seen, batch, total = set(), [], 0

    def flush():
        nonlocal batch, total
        if not batch: return
        sb.table("mot_failure_codes").upsert(batch, on_conflict="code").execute()
        total += len(batch); batch.clear()

    for r in _open_csv(src):
        code = (r.get("code") or r.get("rfr_id") or "").strip()
        if not code or code in seen: continue
        seen.add(code)
        desc = (r.get("description") or r.get("rfr_id_description") or "").strip()
        batch.append({"code": code, "description": desc})
        if len(batch) >= BATCH_SIZE: flush()
    flush()
    p(f"[INFO] Seeded/updated {total} mot_failure_codes.")

# ---------- Row iterators ----------
def _iter_results_file(path: Path) -> Iterator[Dict]:
    for raw in _open_csv(path):
        r = _alias(raw, RESULT_ALIASES)
        tid = _to_int(r.get("test_id"))
        if not tid: continue
        yield {
            "test_id": tid,
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
        tid = _to_int(r.get("test_id"))
        rfr = (r.get("rfr_id") or "").strip()
        typ = (r.get("rfr_type_code") or "").strip()
        loc = (r.get("mot_test_rfr_location_type_id") or "").strip()
        if not (tid and rfr and typ and loc): continue
        yield {
            "test_id": tid,
            "rfr_id": rfr,
            "rfr_type_code": typ,
            "mot_test_rfr_location_type_id": loc,
            "dangerous_mark": _to_bool(r.get("dangerous_mark")),
            "completed_date": _parse_date(r.get("completed_date")),
        }

# ---------- DB senders with global de-dupe ----------
def _send_results(rows: List[Dict]) -> None:
    if not rows: return
    rows = _dedupe(rows, ["test_id"])
    filtered, skipped = [], 0
    for r in rows:
        tid = r.get("test_id")
        if tid in SEEN_TEST_IDS:
            skipped += 1
            continue
        SEEN_TEST_IDS.add(tid)
        filtered.append(r)
    if not filtered:
        if skipped: p(f"[INFO] Skipped {skipped} duplicate test_id in-run")
        return
    p(f"[INFO] Inserting {len(filtered)} results" + (f" (skipped {skipped})" if skipped else ""))
    if UPSERT_MODE == "upsert":
        sb.table("mot_tests").upsert(filtered, on_conflict="test_id").execute()
    else:
        sb.table("mot_tests").insert(filtered).execute()

def _send_items(rows: List[Dict]) -> None:
    if not rows: return
    rows = _dedupe(rows, ["test_id","rfr_id","rfr_type_code","mot_test_rfr_location_type_id"])
    filtered, skipped = [], 0
    for r in rows:
        key = (r.get("test_id"), r.get("rfr_id"), r.get("rfr_type_code"), r.get("mot_test_rfr_location_type_id"))
        if key in SEEN_ITEM_KEYS:
            skipped += 1
            continue
        SEEN_ITEM_KEYS.add(key)
        filtered.append(r)
    if not filtered:
        if skipped: p(f"[INFO] Skipped {skipped} duplicate item keys in-run")
        return
    p(f"[INFO] Inserting {len(filtered)} items" + (f" (skipped {skipped})" if skipped else ""))
    if UPSERT_MODE == "upsert":
        sb.table("mot_test_items").upsert(
            filtered,
            on_conflict="test_id,rfr_id,rfr_type_code,mot_test_rfr_location_type_id",
        ).execute()
    else:
        sb.table("mot_test_items").insert(filtered).execute()

# ---------- File helpers ----------
def _files_results(year: int) -> List[Path]:
    """Use RESULTS_GLOB if set, else default year pattern."""
    if RESULTS_GLOB:
        return sorted(DATA_DIR.glob(RESULTS_GLOB))
    return sorted(DATA_DIR.glob(f"test_result_{year}??.csv"))

def _files_items(year: int) -> List[Path]:
    """Use ITEMS_GLOB if set, else default year pattern."""
    if ITEMS_GLOB:
        return sorted(DATA_DIR.glob(ITEMS_GLOB))
    return sorted(DATA_DIR.glob(f"test_item_{year}??.csv"))

def _detect_years() -> List[int]:
    env = os.getenv("YEARS")
    if env:
        return [int(y.strip()) for y in env.split(",") if y.strip()]
    ys = set()
    for p in DATA_DIR.glob("test_result_*.csv"): ys.add(int(p.stem.split("_")[2][:4]))
    for p in DATA_DIR.glob("test_item_*.csv"):   ys.add(int(p.stem.split("_")[2][:4]))
    return sorted(ys)

# ---------- Orchestration ----------
def load_year(year: int, *, do_results: bool, do_items: bool) -> tuple[int,int]:
    total_r, total_i = 0, 0
    if do_results:
        res = _files_results(year)
        if res: p(f"[INFO] Reading TESTRESULT files for {year} ({len(res)}) …")
        for f in res:
            p(f"[INFO]  -> {f.name}")
            batch, flushed = [], 0
            for row in _iter_results_file(f):
                batch.append(row)
                if len(batch) >= BATCH_SIZE:
                    _send_results(batch); flushed += len(batch); total_r += len(batch); batch.clear()
            if batch:
                _send_results(batch); flushed += len(batch); total_r += len(batch)
            p(f"[INFO] Finished {f.name} (rows sent: {flushed})")
    if do_items:
        its = _files_items(year)
        if its: p(f"[INFO] Reading TESTITEM files for {year} ({len(its)}) …")
        for f in its:
            p(f"[INFO]  -> {f.name}")
            batch, flushed = [], 0
            for row in _iter_items_file(f):
                batch.append(row)
                if len(batch) >= BATCH_SIZE:
                    _send_items(batch); flushed += len(batch); total_i += len(batch); batch.clear()
            if batch:
                _send_items(batch); flushed += len(batch); total_i += len(batch)
            p(f"[INFO] Finished {f.name} (rows sent: {flushed})")
    return total_r, total_i

def main():
    SEEN_TEST_IDS.clear()
    SEEN_ITEM_KEYS.clear()
    seed_mot_failure_codes()
    years = _detect_years()
    if not years:
        p(f"[WARN] No YEARS detected/found. Put CSVs into {DATA_DIR}")
        return
    do_results = LOAD_MODE in ("both","results")
    do_items   = LOAD_MODE in ("both","items")
    p(f"[INFO] Loading years: {years}")
    g_r, g_i = 0, 0
    for y in years:
        r,i = load_year(y, do_results=do_results, do_items=do_items)
        g_r += r; g_i += i
    p(f"[DONE] Inserted results: {g_r}, items: {g_i}")

if __name__ == "__main__":
    try:
        main()
    finally:
        # Ensure the process terminates promptly (avoid idle keep-alives in Codespaces)
        os._exit(0)
