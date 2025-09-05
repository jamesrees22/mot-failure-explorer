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
MAX_YEARS_BACK = int(os.environ.get("MOT_MAX_YEARS_BACK", "8"))

# ----------------- Helpers -----------------
def _norm(s: str) -> str:
    """lowercase + keep only alphanumerics"""
    return "".join(ch for ch in str(s).lower() if ch.isalnum())

def _contains_all(hay: str, *needles: str) -> bool:
    return all(n in hay for n in needles)

def _to_serializable(v: Any) -> Any:
    if isinstance(v, (pd.Timestamp, dt.datetime, dt.date)):
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
    df = df.where(~df.isna(), None)
    records = df.to_dict(orient="records")
    safe_records = [{k: _to_serializable(v) for k, v in rec.items()} for rec in records]
    for i in range(0, len(safe_records), chunk):
        batch = safe_records[i : i + chunk]
        sb.table(table).upsert(batch).execute()

# ---------- CSV readers (auto-detect delimiter) ----------
def _read_any_csv(path: str) -> pd.DataFrame:
    """Read small lookup files with auto delimiter (|, , or tab)."""
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        head = f.read(4096)
    if "|" in head and head.count("|") >= head.count(","):
        return pd.read_csv(path, sep="|", low_memory=False)
    if "\t" in head and head.count("\t") > 0:
        return pd.read_csv(path, sep="\t", low_memory=False)
    return pd.read_csv(path, sep=None, engine="python", low_memory=False)

def _read_delim_file(path: str) -> pd.DataFrame:
    """Read large DVSA tables with quick delimiter sniff."""
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        head = f.read(4096)
    if "|" in head and head.count("|") >= head.count(","):
        sep = "|"
    elif "\t" in head and head.count("\t") > 0:
        sep = "\t"
    else:
        sep = ","
    return pd.read_csv(path, sep=sep, low_memory=False)

def _resolve_col(df: pd.DataFrame, *candidates: str, keywords: Optional[List[str]]=None) -> Optional[str]:
    """
    Return original column name:
    1) exact by normalised candidate,
    2) else keyword search (all keywords contained in normalised name).
    """
    cmap = {_norm(c): c for c in df.columns}
    for cand in candidates:
        col = cmap.get(_norm(cand))
        if col:
            return col
    if keywords:
        for col in df.columns:
            h = _norm(col)
            if _contains_all(h, *[ _norm(k) for k in keywords ]):
                return col
    return None

# ----------------- Seed failure codes -----------------
def seed_failure_codes_from_item_detail(detail_path: str) -> pd.DataFrame:
    """
    item_detail.csv provides the defect dictionary.
    We upsert only 'code' and 'description' to match your current table schema.
    """
    if not os.path.exists(detail_path):
        print(f"[warn] item_detail not found: {detail_path}")
        return pd.DataFrame()

    det = _read_any_csv(detail_path)
    if det.empty:
        print(f"[warn] item_detail empty: {detail_path}")
        return pd.DataFrame()

    c_rfrid = _resolve_col(det, "RFRID", "rfr_id", "defectid", "rfrcode", "defect_id", keywords=["rfr","id"])
    c_desc  = _resolve_col(det, "RFRDESC", "rfr_description", "description", "rfrtext",
                           keywords=["desc","description","text"])

    if not c_rfrid:
        print("[warn] item_detail has no RFRID/rfr_id column. Available:", list(det.columns))
        return pd.DataFrame()

    out = pd.DataFrame()
    out["code"] = det[c_rfrid].astype(str)
    out["description"] = det[c_desc] if c_desc else None
    out = out.dropna(subset=["code"]).drop_duplicates(subset=["code"])

    # 🔒 Only send columns that exist in your table schema
    # Your table currently has 'code' and 'description'
    out = out[["code", "description"]]

    if DRY_RUN:
        print("[DRY_RUN] failure codes (trimmed to code/description) head:")
        print(out.head(5))
    else:
        upsert_df("mot_failure_codes", out)
        print(f"Upserted {len(out)} failure codes.")
    return out

# ----------------- Lookups -----------------
def load_lookups(dir_path: str) -> Dict[str, pd.DataFrame]:
    lk: Dict[str, pd.DataFrame] = {}
    def _read(name: str) -> Optional[pd.DataFrame]:
        path = os.path.join(dir_path, name)
        if os.path.exists(path):
            return _read_any_csv(path)
        return None

    lk["fuel"]    = _read("mdr_fuel_types.csv")       # (optional)
    lk["outcome"] = _read("mdr_test_outcome.csv")     # id->label
    lk["type"]    = _read("mdr_test_type.csv")        # type codes
    lk["detail"]  = _read("item_detail.csv")
    lk["group"]   = _read("item_group.csv")
    return lk

def _coerce_date(series: pd.Series, dayfirst=True) -> pd.Series:
    s = series.astype(str)
    if s.str.match(r"\d{4}-\d{2}-\d{2}").mean() > 0.8:
        return pd.to_datetime(s, format="%Y-%m-%d", errors="coerce")
    return pd.to_datetime(s, errors="coerce", dayfirst=dayfirst)

def _derive_first_use_year(series: pd.Series) -> pd.Series:
    d = _coerce_date(series, dayfirst=True)
    y = d.dt.year.astype("Int64")
    if y.isna().all():
        y = pd.to_numeric(series, errors="coerce").astype("Int64")
    return y

def _map_outcome_code_to_pass_fail(code: pd.Series) -> pd.Series:
    s = code.astype(str).str.upper().str.strip()
    s = s.replace({"PASSED": "P", "FAILED": "F"})
    return s.map({"P": "PASS", "F": "FAIL", "PRS": "FAIL"}).where(lambda x: x.isin(["PASS", "FAIL"]), None)

# ----------------- Main loader -----------------
def load_local_dvsa(data_dir: str) -> None:
    lk = load_lookups(data_dir)
    if lk.get("detail") is not None:
        seed_failure_codes_from_item_detail(os.path.join(data_dir, "item_detail.csv"))

    result_paths = sorted(glob(os.path.join(data_dir, "TESTRESULT*.csv")))
    item_paths   = sorted(glob(os.path.join(data_dir, "TESTITEM*.csv")))
    if not result_paths or not item_paths:
        print(f"[info] Put DVSA annual CSVs in {data_dir} (TESTRESULT*.csv and TESTITEM*.csv).")
        return

    def _read_many(paths: List[str]) -> pd.DataFrame:
        frames = []
        for p in paths:
            print(f"Reading {os.path.basename(p)} …")
            df = _read_delim_file(p)
            if SAMPLE_ROWS > 0:
                df = df.head(SAMPLE_ROWS)
            frames.append(df)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    tr = _read_many(result_paths)  # TESTRESULT
    ti = _read_many(item_paths)    # TESTITEM

    print(f"TESTRESULT rows: {len(tr):,}, TESTITEM rows: {len(ti):,}")

    # --- Resolve TESTRESULT columns (many variants observed) ---
    def C(*names, kw=None):  # kw = keyword list
        return _resolve_col(tr, *names, keywords=kw)

    c_test_type   = C("TESTTYPE", "test_type", kw=["type"])
    c_test_result = C("TESTRESULT", "test_result", "outcome", "result", "test_result_id", kw=["result"])
    c_test_date   = C("TESTDATE", "DateofTest", "Test Date", "completed_date", "completion_date", kw=["date"])
    c_make        = C("MAKE", "vehicle_make", kw=["make"])
    c_model       = C("MODEL", "vehicle_model", kw=["model"])
    c_mileage     = C("TESTMILEAGE", "odometer", "odometer_reading", "odometer_reading_value", kw=["odometer"])
    c_postcode    = C("POSTCODEREGION", "Postcode Area", "station_postcode", "site_postcode_area", kw=["postcode"])
    c_fuel        = C("FUELTYPE", "fuel_type", "fuel", kw=["fuel"])
    c_first_use   = C("FIRSTUSEDATE", "Vehicle Date of First Use", "FirstUseDate", "first_used_date", kw=["first","use","date"])
    c_test_id     = C("TESTID", "test_id", kw=["test","id"])
    c_vehicle_id  = C("VEHICLEID", "vehicle_id", "vin", kw=["vehicle","id"])
    c_class_id    = C("TESTCLASSID", "test_class_id", "classid", kw=["class","id"])

    if DRY_RUN:
        print("[MAP] test_type    ->", c_test_type)
        print("[MAP] test_result  ->", c_test_result)
        print("[MAP] test_date    ->", c_test_date)
        print("[MAP] make         ->", c_make)
        print("[MAP] model        ->", c_model)
        print("[MAP] mileage      ->", c_mileage)
        print("[MAP] postcode     ->", c_postcode)
        print("[MAP] fuel_type    ->", c_fuel)
        print("[MAP] first_use    ->", c_first_use)
        print("[MAP] test_id      ->", c_test_id)
        print("[MAP] vehicle_id   ->", c_vehicle_id)
        print("[MAP] class_id     ->", c_class_id)

    # Only filter to NT if the column exists and actually contains 'NT'
    if c_test_type:
        vals = tr[c_test_type].astype(str).str.upper().unique()
        if "NT" in vals:
            tr = tr[tr[c_test_type].astype(str).str.upper().eq("NT")]
        else:
            if DRY_RUN:
                print(f"[INFO] test_type present but no NT in values {vals}; skipping NT filter")

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
    tests["test_class_id"] = tr[c_class_id] if c_class_id else pd.NA

    # Map results to PASS/FAIL:
    if c_test_result:
        ser = tr[c_test_result]
        if pd.api.types.is_numeric_dtype(ser) and lk.get("outcome") is not None:
            lo = lk["outcome"]
            id_col = _resolve_col(lo, "TESTRESULT", "id", "code", "test_result_id", kw=["id"]) or lo.columns[0]
            name_col = _resolve_col(lo, "TESTRESULTDESC", "description", "label", kw=["desc","name"]) or lo.columns[-1]
            o_map = dict(zip(lo[id_col].astype(str), lo[name_col].astype(str)))
            mapped = ser.astype(str).map(o_map)
            tests["result"] = _map_outcome_code_to_pass_fail(mapped)
        else:
            tests["result"] = _map_outcome_code_to_pass_fail(ser)
    else:
        tests["result"] = pd.NA

    # Keep recent vehicles if configured
    if MAX_YEARS_BACK > 0 and "first_use_year" in tests:
        cutoff = dt.date.today().year - MAX_YEARS_BACK
        tests = tests[(tests["first_use_year"].astype("Float64") >= cutoff) | tests["first_use_year"].isna()]

    # ---- TESTITEM join for failures ----
    ti_map = {_norm(cn): cn for cn in ti.columns}
    t_testid  = ti_map.get(_norm("TESTID")) or ti_map.get(_norm("test_id"))
    t_rfrid   = ti_map.get(_norm("RFRID")) or ti_map.get(_norm("rfr_id")) or ti_map.get(_norm("defectid"))
    t_rfrtype = ti_map.get(_norm("RFRTYPE")) or ti_map.get(_norm("rfr_type_code")) or ti_map.get(_norm("defecttype"))

    failures_list = pd.Series([], dtype=object)
    if t_testid and t_rfrid and t_rfrtype:
        mask = ti[t_rfrtype].astype(str).str.upper().isin(["F", "P"])  # Fail + PRS
        ti_fail = ti.loc[mask, [t_testid, t_rfrid]].copy()
        ti_fail[t_rfrid] = ti_fail[t_rfrid].astype(str)
        agg = ti_fail.groupby(t_testid)[t_rfrid].apply(list).reset_index()
        agg.columns = ["test_id", "failure_reasons"]
        failures_list = agg.set_index("test_id")["failure_reasons"]

    if "test_id" in tests.columns and not failures_list.empty:
        tests["failure_reasons"] = tests["test_id"].map(failures_list)
        tests["failure_reasons"] = tests["failure_reasons"].apply(lambda x: x if isinstance(x, list) else [])
    else:
        tests["failure_reasons"] = [[] for _ in range(len(tests))]

    required = ["test_date", "make", "model", "result"]
    tests = tests.dropna(subset=required, how="any").drop_duplicates(
        subset=["test_id", "test_date", "make", "model", "odometer", "result"]
    )

    if DRY_RUN:
        print("[DRY_RUN] tests.shape:", tests.shape)
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
    seed_failure_codes_from_item_detail(os.path.join(DATA_DIR, "item_detail.csv"))
    seed_sample_rows()
    load_local_dvsa(DATA_DIR)
    print("Done.")
