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
    """lowercase and strip all non-alphanumerics for flexible matching"""
    return "".join(ch for ch in s.lower() if ch.isalnum())

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

# ---------- Robust CSV readers (auto-detect delimiter) ----------
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

def _resolve_col(df: pd.DataFrame, *candidates: str) -> Optional[str]:
    """Return the original column name matching any candidate by normalised form."""
    cmap = {_norm(c): c for c in df.columns}
    for cand in candidates:
        col = cmap.get(_norm(cand))
        if col:
            return col
    return None

# ----------------- Seed failure codes -----------------
def seed_failure_codes_from_item_detail(detail_path: str) -> pd.DataFrame:
    """
    item_detail.csv provides RFR (defect) dictionary.
    Handles header variants like RFRID / rfr_id / defect_id etc.
    """
    if not os.path.exists(detail_path):
        print(f"[warn] item_detail not found: {detail_path}")
        return pd.DataFrame()

    det = _read_any_csv(detail_path)
    if det.empty:
        print(f"[warn] item_detail empty: {detail_path}")
        return pd.DataFrame()

    # Resolve columns flexibly
    c_rfrid = _resolve_col(det, "RFRID", "rfr_id", "defectid", "rfrcode", "defect_id")
    c_desc  = _resolve_col(det, "RFRDESC", "rfr_description", "description", "rfrtext", "defect_description")
    c_cat   = _resolve_col(det, "rfr_deficiency_category", "deficiency_category", "category", "defcat")
    c_class = _resolve_col(det, "TESTCLASSID", "test_class_id", "classid")

    if not c_rfrid:
        print("[warn] item_detail has no RFRID/rfr_id column. Available:", list(det.columns))
        return pd.DataFrame()

    out = pd.DataFrame()
    out["code"] = det[c_rfrid].astype(str)
    out["description"] = det[c_desc] if c_desc else None
    out["category"] = det[c_cat] if c_cat else None
    out["test_class_id"] = det[c_class] if c_class else None

    out = out.dropna(subset=["code"]).drop_duplicates(subset=["code"])

    if DRY_RUN:
        print("[DRY_RUN] failure codes head:")
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

    lk["fuel"]   = _read("mdr_fuel_types.csv")
    lk["outcome"]= _read("mdr_test_outcome.csv")
    lk["type"]   = _read("mdr_test_type.csv")
    lk["detail"] = _read("item_detail.csv")
    lk["group"]  = _read("item_group.csv")
    return lk

def _coerce_date(series: pd.Series, dayfirst=True) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", dayfirst=dayfirst)

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

# ----------------- Main loader for local DVSA CSVs -----------------
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

    # Flexible column resolution for TESTRESULT
    def c(*names: str) -> Optional[str]:
        cmap = {_norm(col): col for col in tr.columns}
        for n in names:
            col = cmap.get(_norm(n))
            if col:
                return col
        return None

    c_test_type   = c("TESTTYPE")
    c_test_result = c("TESTRESULT")
    c_test_date   = c("TESTDATE", "Test Date", "DateofTest")
    c_make        = c("MAKE")
    c_model       = c("MODEL")
    c_mileage     = c("TESTMILEAGE", "odometer", "mileage")
    c_postcode    = c("POSTCODEREGION", "Postcode Area", "station_postcode")
    c_fuel        = c("FUELTYPE")
    c_first_use   = c("FIRSTUSEDATE", "Vehicle Date of First Use", "FirstUseDate")
    c_test_id     = c("TESTID", "test_id")
    c_vehicle_id  = c("VEHICLEID", "vehicle_id")
    c_class_id    = c("TESTCLASSID", "test_class_id")

    if c_test_type:
        tr = tr[tr[c_test_type].astype(str).str.upper().eq("NT")]

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
    tests["result"] = _map_outcome_code_to_pass_fail(tr[c_test_result]) if c_test_result else pd.NA
    tests["test_class_id"] = tr[c_class_id] if c_class_id else pd.NA

    if MAX_YEARS_BACK > 0 and "first_use_year" in tests:
        cutoff = dt.date.today().year - MAX_YEARS_BACK
        tests = tests[(tests["first_use_year"].astype("Float64") >= cutoff) | tests["first_use_year"].isna()]

    # Flexible column resolution for TESTITEM
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
    seed_failure_codes_from_item_detail(os.path.join(DATA_DIR, "item_detail.csv"))
    seed_sample_rows()
    load_local_dvsa(DATA_DIR)
    print("Done.")
