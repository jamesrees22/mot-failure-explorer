#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MOT ETL: supports BOTH yearly CSVs and monthly-split CSVs.

Expected inputs in DATA_DIR (defaults to apps/web/etl/data):
- Yearly:
    TESTRESULT_YYYY.csv
    TESTITEM_YYYY.csv
- Monthly (new DVSA split):
    test_result_YYYYMM.csv (12 files)
    test_item_YYYYMM.csv   (12 files)
- Lookups:
    item_detail.csv  (for RfR code/description)
    (item_group.csv, mdr_*.csv optional)

Outputs (Supabase / Postgres):
- mot_failure_codes(code, description)  -- seeded from item_detail.csv
- mot_tests(...)                        -- 1 row per test, with failure_reasons TEXT[]
"""

import csv
import os
import re
import sys
from pathlib import Path
from collections import defaultdict
from typing import Dict, Iterable, Iterator, List, Tuple

# ------------ Config ------------
DATA_DIR = Path(os.getenv("LOCAL_MOT_DIR", "apps/web/etl/data")).resolve()
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))

SUPABASE_URL = os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")  # service role key (server-side)

if not SUPABASE_URL or not SUPABASE_KEY:
    print("[FATAL] SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY not set.")
    sys.exit(1)

# Supabase client
try:
    from supabase import create_client, Client  # type: ignore
except Exception as e:
    print("[FATAL] Missing supabase-py. Run: pip install supabase")
    raise

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ------------ Helpers ------------

def _glob_files(patterns: List[str]) -> List[Path]:
    out: List[Path] = []
    for pat in patterns:
        out.extend(DATA_DIR.glob(pat))
    # sort for stable YYYYMM ordering
    return sorted(out, key=lambda p: p.name.lower())

def _find_yearly_files(year: int) -> Tuple[Path, Path]:
    """
    Return (TESTRESULT_YYYY.csv, TESTITEM_YYYY.csv) if both exist, else (None, None).
    Case-insensitive.
    """
    candidates_result = _glob_files([
        f"TESTRESULT_{year}.csv",
        f"testresult_{year}.csv",
    ])
    candidates_item = _glob_files([
        f"TESTITEM_{year}.csv",
        f"testitem_{year}.csv",
    ])
    if candidates_result and candidates_item:
        return candidates_result[0], candidates_item[0]
    return None, None  # type: ignore

def _find_monthly_files(year: int) -> Tuple[List[Path], List[Path]]:
    """
    Return sorted lists of monthly files for result and item.
    Eg test_result_YYYYMM.csv, test_item_YYYYMM.csv
    """
    res_files = _glob_files([f"test_result_{year}??.csv", f"TEST_RESULT_{year}??.csv"])
    itm_files = _glob_files([f"test_item_{year}??.csv", f"TEST_ITEM_{year}??.csv"])
    return res_files, itm_files

def _has_header(path: Path) -> bool:
    # DVSA files have headers. We'll always treat first row as header.
    return True

def _open_csv(path: Path) -> Iterator[Dict[str, str]]:
    """
    Open a DVSA pipe-delimited CSV with headers.
    Handles UTF-8 BOM.
    """
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter="|")
        for row in reader:
            yield {k.strip(): (v.strip() if isinstance(v, str) else v) for k, v in row.items()}

def _normalise_result_headers(row: Dict[str, str]) -> Dict[str, str]:
    """
    DVSA headers differ slightly between years. Map common aliases.
    """
    m = {k.lower(): v for k, v in row.items()}

    # Common fields we care about
    return {
        "testid": m.get("testid") or m.get("test_id"),
        "testdate": m.get("testdate") or m.get("test_date"),
        "make": m.get("make"),
        "model": m.get("model"),
        "firstusedate": m.get("firstusedate") or m.get("first_use_date") or m.get("first_use_dt"),
        "testresult": m.get("testresult") or m.get("test_result"),
        "testclassid": m.get("testclassid") or m.get("test_class_id"),
        "fueltype": m.get("fueltype") or m.get("fuel_type"),
        "testtype": m.get("testtype") or m.get("test_type"),
        "testmileage": m.get("testmileage") or m.get("odometer"),
    }

def _normalise_item_headers(row: Dict[str, str]) -> Dict[str, str]:
    m = {k.lower(): v for k, v in row.items()}
    return {
        "testid": m.get("testid") or m.get("test_id"),
        "rfrid": m.get("rfrid") or m.get("rfr_id") or m.get("reason_id"),
        "rfrtype": m.get("rfrtype") or m.get("rfr_type"),
    }

def _year_from_date(iso_date: str) -> int:
    if not iso_date:
        return None  # type: ignore
    return int(iso_date[:4])

def _chunk(it: Iterable, size: int) -> Iterator[List]:
    buf = []
    for x in it:
        buf.append(x)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf

# ------------ Load lookups ------------

def seed_mot_failure_codes() -> None:
    """
    Seed/update rfr code -> description from item_detail.csv
    Keeps just two columns in mot_failure_codes: code, description
    """
    detail_paths = _glob_files(["item_detail.csv", "ITEM_DETAIL.csv"])
    if not detail_paths:
        print("[WARN] item_detail.csv not found; skipping mot_failure_codes seed.")
        return

    path = detail_paths[0]
    print(f"[INFO] Seeding mot_failure_codes from {path.name} …")

    seen = {}
    for row in _open_csv(path):
        # item_detail has rfrid, testclassid, rfrdesc
        r = {k.lower(): v for k, v in row.items()}
        code = r.get("rfrid") or r.get("rfr_id")
        desc = r.get("rfrdesc") or r.get("rfr_desc") or r.get("rfr_description")
        if not code or not desc:
            continue
        # Prefer the longest description we've seen for the same code
        if code not in seen or len(desc) > len(seen[code]):
            seen[code] = desc

    if not seen:
        print("[WARN] item_detail.csv had no usable rows; skipping.")
        return

    payload = [{"code": str(k), "description": v} for k, v in seen.items()]
    for batch in _chunk(payload, 2000):
        res = supabase.table("mot_failure_codes").upsert(
            batch,
            on_conflict="code",
            ignore_duplicates=False
        ).execute()
        if getattr(res, "error", None):
            print("[ERROR] Upsert mot_failure_codes:", res.error)
            raise RuntimeError(res.error)
    print(f"[INFO] Seeded/updated {len(payload)} mot_failure_codes.")

# ------------ Build per-test failure reasons from TESTITEM ------------

def collect_failure_reasons_for_year(year: int) -> Dict[str, List[str]]:
    """
    Returns dict: testid -> list[rfrid as text], across either monthly or yearly TESTITEM files.
    We collect ALL defect types and let views decide (F+P) vs advisory/minor. If you prefer,
    filter here to ('F','P').
    """
    yearly_item, _ = _find_yearly_files(year)
    if yearly_item and yearly_item.exists():
        item_files = [yearly_item.parent / f"TESTITEM_{year}.csv"]
    else:
        _, monthly_item_files = _find_monthly_files(year)
        item_files = monthly_item_files

    if not item_files:
        print(f"[WARN] No TESTITEM files found for {year}.")
        return {}

    print(f"[INFO] Scanning TESTITEM rows for {year} ({len(item_files)} file(s)) …")
    by_testid: Dict[str, List[str]] = defaultdict(list)

    for f in item_files:
        print(f"[INFO]  -> {f.name}")
        for raw in _open_csv(f):
            row = _normalise_item_headers(raw)
            testid = row.get("testid")
            rfrid = row.get("rfrid")
            rfrtype = (row.get("rfrtype") or "").upper()

            if not testid or not rfrid:
                continue

            # If you want initial-fail only at load time, uncomment:
            # if rfrtype not in ("F", "P"):
            #     continue

            by_testid[testid].append(str(rfrid))

    return by_testid

# ------------ Load TESTRESULT + stitch items → mot_tests ------------

def load_year(year: int) -> int:
    """
    Load a year using yearly or monthly result files, stitching in failure reasons.
    Returns inserted/updated row count.
    """
    result_yearly, item_yearly = _find_yearly_files(year)
    monthly_res_files, monthly_item_files = _find_monthly_files(year)

    use_yearly = result_yearly and item_yearly
    use_monthly = bool(monthly_res_files and monthly_item_files)

    if not (use_yearly or use_monthly):
        print(f"[WARN] No TESTRESULT/TESTITEM files found for {year}. Skipping.")
        return 0

    # Pre-collect failure reasons map for the year (from either yearly or monthly items)
    testid_to_rfrs = collect_failure_reasons_for_year(year)

    def _iter_result_rows() -> Iterator[Dict[str, str]]:
        if use_yearly:
            f = Path(result_yearly.parent / f"TESTRESULT_{year}.csv")
            print(f"[INFO] Reading {f.name} …")
            yield from (_normalise_result_headers(r) for r in _open_csv(f))
        else:
            print(f"[INFO] Reading monthly TESTRESULT files for {year} ({len(monthly_res_files)}) …")
            for f in monthly_res_files:
                print(f"[INFO]  -> {f.name}")
                for r in _open_csv(f):
                    yield _normalise_result_headers(r)

    def _row_to_record(row: Dict[str, str]) -> Dict:
        testid = row["testid"]
        testdate = row["testdate"]
        make = row.get("make") or ""
        model = row.get("model") or ""
        firstusedate = row.get("firstusedate") or None
        testresult = (row.get("testresult") or "").upper()
        testclassid = row.get("testclassid") or None
        fueltype = row.get("fueltype") or None
        testtype = row.get("testtype") or None
        testmileage = row.get("testmileage") or None

        first_use_year = _year_from_date(firstusedate) if firstusedate else None
        rfrs = testid_to_rfrs.get(testid, [])
        # Build record compatible with mot_tests table you already use
        rec = {
            "test_id": int(testid) if testid and testid.isdigit() else testid,
            "test_date": testdate,
            "make": make,
            "model": model,
            "first_use_year": first_use_year,
            "test_result": testresult,
            "test_class_id": testclassid,
            "fuel_type": fueltype,
            "test_type": testtype,
            "test_mileage": int(testmileage) if (testmileage and testmileage.isdigit()) else None,
            "failure_reasons": rfrs,  # TEXT[] in Postgres
        }
        return rec

    # Stream → batch upserts
    total = 0
    batch: List[Dict] = []
    for row in _iter_result_rows():
        if not row.get("testid"):
            continue
        batch.append(_row_to_record(row))
        if len(batch) >= BATCH_SIZE:
            _upsert_mot_tests(batch)
            total += len(batch)
            batch.clear()
    if batch:
        _upsert_mot_tests(batch)
        total += len(batch)

    print(f"[INFO] Upserted {total} mot_tests rows for {year}.")
    return total

def _upsert_mot_tests(batch: List[Dict]) -> None:
    res = supabase.table("mot_tests").upsert(
        batch,
        on_conflict="test_id",
        ignore_duplicates=False
    ).execute()
    if getattr(res, "error", None):
        print("[ERROR] Upsert mot_tests:", res.error)
        raise RuntimeError(res.error)

# ------------ Entry point ------------

def main():
    print(f"[INFO] DATA_DIR = {DATA_DIR}")
    if not DATA_DIR.exists():
        print(f"[FATAL] DATA_DIR does not exist: {DATA_DIR}")
        sys.exit(1)

    # 1) Seed failure codes (safe to run every time)
    seed_mot_failure_codes()

    # 2) Decide which years to load.
    #    If you want a quick win, set YEARS env like "2024,2023".
    years_env = os.getenv("YEARS")
    if years_env:
        years = [int(y.strip()) for y in years_env.split(",") if y.strip().isdigit()]
    else:
        # Autodiscover years from filenames present in DATA_DIR
        years = _discover_years()

    if not years:
        print("[WARN] No years discovered. Place files in DATA_DIR or set YEARS env.")
        return

    print(f"[INFO] Loading years: {years}")
    grand_total = 0
    for y in years:
        grand_total += load_year(y)

    print(f"[DONE] Total upserts: {grand_total}")

def _discover_years() -> List[int]:
    years = set()
    for p in DATA_DIR.glob("*.csv"):
        m = re.search(r"(20\d{2})", p.name)
        if m:
            years.add(int(m.group(1)))
    return sorted(years)

if __name__ == "__main__":
    main()
