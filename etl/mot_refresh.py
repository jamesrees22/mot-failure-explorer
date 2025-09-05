import os
import pandas as pd
import numpy as np
import datetime as dt
from datetime import datetime, timezone
from glob import glob
from typing import Any, Dict, List, Optional

from supabase import create_client, Client

# ----------------- Supabase -----------------
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ----------------- Config -----------------
DATA_DIR = os.environ.get("LOCAL_MOT_DIR", os.path.join(os.path.dirname(__file__), "data"))
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() in ("1", "true", "yes")
SAMPLE_ROWS = int(os.environ.get("SAMPLE_ROWS", "0"))  # 0 = all
# Keep only relatively recent vehicles (helps keep free tiers happy)
MAX_YEARS_BACK = int(os.environ.get("MOT_MAX_YEARS_BACK", "8"))

# ----------------- Helpers -----------------
def _to_serializable(v: Any) -> Any:
    if isinstance(v, (pd.Timestamp, dt.datetime, dt.date)):
        # Convert any timestamp/date to ISO date
        if isinstance(v, pd.Timestamp):
            return None if pd.isna(v) else v.date().isoformat()
        if isinstance(v, dt.datetime):
            return v.date().isoformat()
        return v.isoformat()
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return float(v)
    try:
        if v is pd.NA or (not isinstance(v, list) and pd.isna(v)):
            return None
    except Exception:
        pass
    return v

def upsert_df(table: str, df: pd.DataFrame, chunk: int = 5000) -> None:
    if df.empty:
        return
    df = df.copy()
    # final NA → None, and JSON-safe conversion
    df = df.where(~df.isna(), None)
    records = df.to_dict(orient="records")
    safe_records = [{k: _to_serializable(v) for k, v in rec.items()} for rec in records]
    for i in range(0, len(safe_records), chunk):
        batch = safe_records[i : i + chunk]
        sb.table(table).upsert(batch).execute()

# ---------- Robust CSV readers (auto-detect delimiter) ----------
def _read_any_csv(path: str) -> pd.DataFrame:
    """
    Read small lookup files with automatic delimiter detection (|, , or tab).
    Falls back to Python engine sniffing.
    """
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        head = f.read(4096)
    if "|" in head and head.count("|") >= head.count(","):
        return pd.read_csv(path, sep="|", low_memory=False)
    if "\t" in head and head.count("\t") > 0:
        return pd.read_csv(path, sep="\t", low_memory=False)
    # Let pandas sniff comma/other
    return pd.read_csv(path, sep=None, engine="python", low_memory=False)

def _read_delim_file(path: str) -> pd.DataFrame:
    """
    Read large DVSA tables (TESTRESULT/TESTITEM) with a quick delimiter sniff.
    """
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        head = f.read(4096)
    if "|" in head and head.count("|") >= head.count(","):
        sep = "|"
    elif "\t" in head and head.count("\t") > 0:
        sep = "\t"
    else:
        sep = ","
    return pd.read_csv(path, sep=sep, low_memory=False)

# ----------------- Seed failure codes (static CSV you already have) -----------------
def seed_failure_codes_from_item_detail(detail_path: str) -> pd.DataFrame:
    """
    item_detail.csv provides RFRID + descriptions and categories.
    """
    if not os.path.exists(detail_path):
        print(f"[warn] item_detail not found: {detail_path}")
        return pd.DataFrame()

    det = _read_any_csv(detail_path)
    # expected columns (names vary slightly by release)
    # RFRID, TESTCLASSID, RFRDESC, RFRINSPMANDESC, RFRADVISORYTEXT, MINORITEM, rfr_deficiency_category ...
    # Normalize columns we will store
    keep = {col.lower(): col for col in det.columns}

    out = pd.DataFrame()
    out["code"] = det[keep.get("rfrid", "RFRID")].astype(str)
    out["description"] = det.get(keep.get("rfrdesc", "RFRDESC"), None)
    out["category"] = det.get(keep.get("rfr_deficiency_category", "rfr_deficiency_category"), None)
    out["test_class_id"] = det.get(keep.get("testclassid", "TESTCLASSID"), None)

    out = out.dropna(subset=["code"]).drop_duplicates(subset=["code"])
    if DRY_RUN:
        print("[DRY_RUN] failure codes head:")
        print(out.head(5))
    else:
        upsert_df("mot_failure_codes", out)
        print(f"Upserted {len(out)} failure codes.")

    return out

# ----------------- DVSA -> our schema mapping -----------------
def load_lookups(dir_path: str) -> Dict[str, pd.DataFrame]:
    lk: Dict[str, pd.DataFrame] = {}
    def _read(name: str) -> Optional[pd.DataFrame]:
        path = os.path.join(dir_path, name)
        if os.path.exists(path):
            return _read_any_csv(path)
        return None

    lk["fuel"]   = _read("mdr_fuel_types.csv")      # code -> label
    lk["outcome"]= _read("mdr_test_outcome.csv")    # code -> label
    lk["type"]   = _read("mdr_test_type.csv")       # code -> label
    lk["detail"] = _read("item_detail.csv")         # RFR descriptions
    lk["group"]  = _read("item_group.csv")          # optional group names
    return lk

def _coerce_date(series: pd.Series, dayfirst=True) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", dayfirst=dayfirst)

def _derive_first_use_year(series: pd.Series) -> pd.Series:
    # try parse dd-mm-yyyy → year
    d = _coerce_date(series, dayfirst=True)
    y = d.dt.year.astype("Int64")
    if y.isna().all():
        # Occasionally year only
        y = pd.to_numeric(series, errors="coerce").astype("Int64")
    return y

def _map_outcome_code_to_pass_fail(code: pd.Series) -> pd.Series:
    """
    DVSA codes:
    P = Pass, F = Fail, PRS = repaired within 1hr (initial fail) etc.
    We treat PRS as FAIL for initial-failure analysis.
    """
    s = code.astype(str).str.upper().str.strip()
    s = s.replace({"PASSED": "P", "FAILED": "F"})
    return s.map({"P": "PASS", "F": "FAIL", "PRS": "FAIL"}).where(lambda x: x.isin(["PASS", "FAIL"]), None)

# ----------------- Main loader for local DVSA CSVs -----------------
def load_local_dvsa(data_dir: str) -> None:
    # 1) Load lookups
    lk = load_lookups(data_dir)

    # Seed failure codes table from item_detail
    if lk.get("detail") is not None:
        seed_failure_codes_from_item_detail(os.path.join(data_dir, "item_detail.csv"))

    # 2) Find annual files (delimiter auto-detected)
    result_paths = sorted(glob(os.path.join(data_dir, "TESTRESULT*.csv")))
    item_paths   = sorted(glob(os.path.join(data_dir, "TESTITEM*.csv")))
    if not result_paths or not item_paths:
        print(f"[info] Put DVSA annual CSVs in {data_dir} (TESTRESULT*.csv and TESTITEM*.csv).")
        return

    # 3) Concatenate per table (limit rows if SAMPLE_ROWS set)
    def _read_many(paths: List[str]) -> pd.DataFrame:
        frames = []
        for p in paths:
            print(f"Reading {os.path.basename(p)} …")
            df = _read_delim_file(p)
            if SAMPLE_ROWS > 0:
                df = df.head(SAMPLE_ROWS)
            frames.append(df)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    tr = _read_many(result_paths)  # Test Result
    ti = _read_many(item_paths)    # Test Item

    print(f"TESTRESULT rows: {len(tr):,}, TESTITEM rows: {len(ti):,}")

    # 4) Keep only Normal tests (NT)
    # Column names vary slightly; harmonize keys we need:
    cols = {c.lower().replace(" ", ""): c for c in tr.columns}

    def c(name_like: str, *alts: str) -> Optional[str]:
        for k, v in cols.items():
            if k == name_like.lower().replace(" ", ""):
                return v
        for alt in alts:
            altk = alt.lower().replace(" ", "")
            if altk in cols:
                return cols[altk]
        return None

    c_test_type   = c("TESTTYPE")
    c_test_result = c("TESTRESULT")
    c_test_date   = c("TESTDATE", "Test Date", "DateofTest")
    c_make        = c("MAKE")
    c_model       = c("MODEL")
    c_mileage     = c("TESTMILEAGE")
    c_postcode    = c("POSTCODEREGION", "Postcode Area")
    c_fuel        = c("FUELTYPE")
    c_first_use   = c("FIRSTUSEDATE", "Vehicle Date of First Use", "FirstUseDate")
    c_test_id     = c("TESTID")
    c_vehicle_id  = c("VEHICLEID")
    c_class_id    = c("TESTCLASSID")

    if c_test_type and "type" in lk and any(col.upper() == "TYPECODE" for col in lk["type"].columns):
        tr = tr[tr[c_test_type].astype(str).str.upper().eq("NT")]

    # 5) Map fields to our mot_tests schema
    tests = pd.DataFrame()
    tests["test_id"] = tr[c_test_id] if c_test_id else pd.NA
    tests["vehicle_id"] = tr[c_vehicle_id] if c_vehicle_id else pd.NA
    tests["test_date"] = _coerce_date(tr[c_test_date], dayfirst=True) if c_test_date else pd.NaT
    tests["make"] = tr[c_make] if c_make else pd.NA
    tests["model"] = tr[c_model] if c_model else pd.NA
    tests["odometer"] = pd.to_numeric(tr[c_mileage], errors="coerce").astype("Int64") if c_mileage else pd.NA
    tests["station_postcode"] = tr[c_postcode] if c_postcode else pd.NA
    tests["fuel_type"] = tr[c_fuel] if c_fuel else pd.NA
    tests["first_use_year"] = _derive_first_use_year(tr[c_first_use]) if c_first_use else pd.NA
    # Result → PASS/FAIL (PRS treated as initial fail)
    tests["result"] = _map_outcome_code_to_pass_fail(tr[c_test_result]) if c_test_result else pd.NA
    tests["test_class_id"] = tr[c_class_id] if c_class_id else pd.NA

    # Apply recent-year filter if configured
    if MAX_YEARS_BACK > 0 and "first_use_year" in tests:
        cutoff = dt.date.today().year - MAX_YEARS_BACK
        tests = tests[(tests["first_use_year"].astype("Float64") >= cutoff) | tests["first_use_year"].isna()]

    # 6) Aggregate failures per TESTID from TESTITEM
    ti_cols = {c.lower(): c for c in ti.columns}
    t_testid = ti_cols.get("testid")
    t_rfrid  = ti_cols.get("rfrid")
    t_rfrtype= ti_cols.get("rfrtype")

    failures_list = pd.Series([], dtype=object)

    if t_testid and t_rfrid and t_rfrtype:
        # Keep only failing types F (Fail) and P (PRS)
        mask = ti[t_rfrtype].astype(str).str.upper().isin(["F", "P"])
        ti_fail = ti.loc[mask, [t_testid, t_rfrid]].copy()
        ti_fail[t_rfrid] = ti_fail[t_rfrid].astype(str)
        agg = ti_fail.groupby(t_testid)[t_rfrid].apply(list).reset_index()
        agg.columns = ["test_id", "failure_reasons"]
        failures_list = agg.set_index("test_id")["failure_reasons"]

    # 7) Attach failure arrays onto tests where possible
    if "test_id" in tests.columns and not failures_list.empty:
        tests["failure_reasons"] = tests["test_id"].map(failures_list)
        tests["failure_reasons"] = tests["failure_reasons"].apply(lambda x: x if isinstance(x, list) else [])
    else:
        tests["failure_reasons"] = [[] for _ in range(len(tests))]

    # 8) Final clean
    required = ["test_date", "make", "model", "result"]
    tests = tests.dropna(subset=required, how="any").drop_duplicates(
        subset=["test_id", "test_date", "make", "model", "odometer", "result"]
    )

    # 9) Upsert to Supabase
    if DRY_RUN:
        print("[DRY_RUN] tests.head():")
        print(tests.head(10))
        print(f"[DRY_RUN] Would upsert {len(tests)} tests")
    else:
        upsert_df("mot_tests", tests)
        print(f"Upserted {len(tests)} tests.")

# ----------------- Simple seed (kept) -----------------
def seed_sample_rows() -> None:
    path = os.path.join(os.path.dirname(__file__), "sample_mot_rows.csv")
    if not os.path.exists(path):
        return
    df = pd.read_csv(path, parse_dates=["test_date"])
    if "failure_reasons" in df.columns:
        df["failure_reasons"] = df["failure_reasons"].fillna("").apply(
            lambda s: [x for x in str(s).split("|") if x]
        )
    if "first_use_year" in df.columns:
        df["first_use_year"] = pd.to_numeric(df["first_use_year"], errors="coerce").astype("Int64")
    if "odometer" in df.columns:
        df["odometer"] = pd.to_numeric(df["odometer"], errors="coerce").astype("Int64")
    subset_cols = [c for c in ["test_date", "make", "model", "odometer", "result"] if c in df.columns]
    if subset_cols:
        df = df.drop_duplicates(subset=subset_cols)
    if DRY_RUN:
        print("[DRY_RUN] sample seed:")
        print(df.head(5))
    else:
        upsert_df("mot_tests", df)

# ----------------- Entrypoint -----------------
if __name__ == "__main__":
    print(f"[{datetime.now(timezone.utc).isoformat()}] MOT refresh starting…")
    # 1) Load failure code dictionary from item_detail → mot_failure_codes
    seed_failure_codes_from_item_detail(os.path.join(DATA_DIR, "item_detail.csv"))
    # 2) Optional seed demo rows
    seed_sample_rows()
    # 3) Load DVSA annual files from DATA_DIR (delimiter auto-detected)
    load_local_dvsa(DATA_DIR)
    print("Done.")
