import os
import io
import zipfile
import pandas as pd
import numpy as np
import datetime as dt
from datetime import datetime
from typing import Any, Dict, Iterable, Optional, List
import requests
from glob import glob

from supabase import create_client, Client

# -------- Supabase client --------
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]  # service role (CI only)
sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# -------- Config --------
# LOCAL MODE: put one or more CSVs in etl/data/*.csv and set DRY_RUN if you want to inspect only
LOCAL_DIR = os.environ.get("LOCAL_MOT_DIR", os.path.join(os.path.dirname(__file__), "data"))
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() in ("1", "true", "yes")
SAMPLE_ROWS = int(os.environ.get("SAMPLE_ROWS", "0"))  # 0 = all rows

# Remote loader (we'll pause this for now; keep for later)
MOT_DATA_URLS = os.environ.get("MOT_DATA_URLS", "").strip().splitlines()
MAX_YEARS_BACK = int(os.environ.get("MOT_MAX_YEARS_BACK", "8"))

# Map raw columns -> our schema (multiple candidates per field allowed)
COLUMN_MAP_CANDIDATES: Dict[str, List[str]] = {
    "test_date":       ["test_date", "TestDate", "dateOfTest", "testdate", "test_date_dt", "DATE_OF_TEST"],
    "make":            ["make", "Make", "vehicleMake", "MAKE"],
    "model":           ["model", "Model", "vehicleModel", "MODEL"],
    "fuel_type":       ["fuel_type", "FuelType", "fuelType", "fuel", "FUEL_TYPE"],
    "first_use_year":  ["first_use_year", "firstUseYear", "firstUsedDate", "firstUsedYear", "DateFirstUsed", "FIRST_USE_YEAR"],
    "odometer":        ["odometer", "odometer_value", "OdometerReading", "odometerValue", "ODOMETER"],
    "result":          ["result", "TestResult", "testResult", "outcome", "RESULT"],
    "station_postcode":["station_postcode", "TestStationPostcode", "testStationPostcode", "postcodeArea", "TEST_STATION_POSTCODE"],
    # free-text reasons (if codes aren't provided)
    "reason_text":     ["RfrAndComments", "reasonsForFailure", "reasonForFailure", "failureReason", "failures"]
}
# If dataset has separate reason code columns, list them here
REASON_CODE_COLUMNS = ["ReasonCode", "rfrCode", "failureCode"]

# -------- Helpers --------
def _to_serializable(v: Any) -> Any:
    if isinstance(v, (pd.Timestamp, dt.datetime, dt.date)):
        if isinstance(v, pd.Timestamp):
            return v.date().isoformat() if not pd.isna(v) else None
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
    if "test_date" in df.columns:
        df["test_date"] = pd.to_datetime(df["test_date"], errors="coerce").dt.date
    df = df.where(~df.isna(), None)
    records = df.to_dict(orient="records")
    safe_records = [{k: _to_serializable(v) for k, v in rec.items()} for rec in records]
    for i in range(0, len(safe_records), chunk):
        batch = safe_records[i : i + chunk]
        sb.table(table).upsert(batch).execute()

def seed_failure_codes() -> None:
    path = os.path.join(os.path.dirname(__file__), "failure_codes.csv")
    if not os.path.exists(path):
        return
    df = pd.read_csv(path).dropna(subset=["code"])
    df["code"] = df["code"].astype(str).str.strip()
    upsert_df("mot_failure_codes", df)

def seed_sample_rows() -> None:
    path = os.path.join(os.path.dirname(__file__), "sample_mot_rows.csv")
    if not os.path.exists(path):
        return
    df = pd.read_csv(path, parse_dates=["test_date"])
    if "failure_reasons" in df.columns:
        df["failure_reasons"] = (
            df["failure_reasons"].fillna("").apply(lambda s: [x for x in str(s).split("|") if x])
        )
    if "first_use_year" in df.columns:
        df["first_use_year"] = pd.to_numeric(df["first_use_year"], errors="coerce").astype("Int64")
    if "odometer" in df.columns:
        df["odometer"] = pd.to_numeric(df["odometer"], errors="coerce").astype("Int64")
    subset_cols = [c for c in ["test_date", "make", "model", "odometer", "result"] if c in df.columns]
    if subset_cols:
        df = df.drop_duplicates(subset=subset_cols)
    if DRY_RUN:
        print("[DRY_RUN] sample seed rows:")
        print(df.head(5))
    else:
        upsert_df("mot_tests", df)

# -------- Normalisation layer --------
def _first_val(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None

def _extract_first_use_year(series: pd.Series) -> pd.Series:
    s = pd.to_datetime(series, errors="coerce")
    year = s.dt.year.astype("Int64")
    if year.isna().all():
        y2 = pd.to_numeric(series, errors="coerce").astype("Int64")
        return y2
    return year

def _split_failure_reasons(df: pd.DataFrame, text_col: Optional[str]) -> List[List[str]]:
    if any(c in df.columns for c in REASON_CODE_COLUMNS):
        arr = []
        for _, row in df.iterrows():
            codes = []
            for c in REASON_CODE_COLUMNS:
                v = row.get(c, None)
                if pd.notna(v):
                    codes.append(str(v).strip())
            arr.append(codes)
        return arr
    if text_col is None:
        return [[] for _ in range(len(df))]
    tokens = (
        df[text_col]
        .fillna("")
        .astype(str)
        .str.replace(r"\s+", " ", regex=True)
        .str.split(r"[;,|]", regex=True)
        .apply(lambda lst: [t.strip() for t in lst if t and len(t.strip()) <= 16])
    )
    return tokens.tolist()

def _normalise_chunk(df: pd.DataFrame) -> pd.DataFrame:
    cols = {k: _first_val(df, v) for k, v in COLUMN_MAP_CANDIDATES.items()}
    # report mapping
    print("Column mapping used:", cols)

    out = pd.DataFrame()
    # test_date
    if cols["test_date"]:
        out["test_date"] = pd.to_datetime(df[cols["test_date"]], errors="coerce").dt.date
    else:
        out["test_date"] = pd.NaT

    # make/model
    out["make"] = df[cols["make"]] if cols["make"] else pd.NA
    out["model"] = df[cols["model"]] if cols["model"] else pd.NA

    # fuel
    out["fuel_type"] = df[cols["fuel_type"]] if cols["fuel_type"] else pd.NA

    # first use year
    if cols["first_use_year"]:
        out["first_use_year"] = _extract_first_use_year(df[cols["first_use_year"]])
    else:
        out["first_use_year"] = pd.NA

    # odometer
    if cols["odometer"]:
        out["odometer"] = pd.to_numeric(df[cols["odometer"]], errors="coerce").astype("Int64")
    else:
        out["odometer"] = pd.NA

    # result
    if cols["result"]:
        out["result"] = (
            df[cols["result"]]
            .astype(str)
            .str.upper()
            .replace({"PASSED": "PASS", "FAILED": "FAIL"})
        )
        out.loc[~out["result"].isin(["PASS", "FAIL"]), "result"] = pd.NA
    else:
        out["result"] = pd.NA

    # postcode
    out["station_postcode"] = df[cols["station_postcode"]] if cols["station_postcode"] else pd.NA

    # failure reasons
    reason_text_col = cols["reason_text"]
    out["failure_reasons"] = _split_failure_reasons(df, reason_text_col)

    # filter to recent years if configured
    if MAX_YEARS_BACK > 0:
        cutoff = dt.date.today().year - MAX_YEARS_BACK
        mask = (out["first_use_year"].astype("Float64") >= cutoff) | out["first_use_year"].isna()
        out = out[mask]

    # minimal required fields
    out = out.dropna(subset=["test_date", "make", "model", "result"], how="any")
    # dedupe
    out = out.drop_duplicates(subset=["test_date", "make", "model", "odometer", "result"])
    return out

# -------- LOCAL: load CSVs from etl/data --------
def load_local_csvs() -> None:
    if not os.path.isdir(LOCAL_DIR):
        print(f"No local directory found: {LOCAL_DIR} (skipping)")
        return

    files = sorted(glob(os.path.join(LOCAL_DIR, "*.csv")))
    if not files:
        print(f"No CSV files in {LOCAL_DIR}. Drop a file there to load.")
        return

    for path in files:
        print(f"Loading local CSV: {path}")
        df = pd.read_csv(path, low_memory=False)
        if SAMPLE_ROWS > 0:
            df = df.head(SAMPLE_ROWS)

        print(f"Detected {len(df)} rows, {len(df.columns)} columns")
        print("Columns:", list(df.columns)[:40], "..." if len(df.columns) > 40 else "")

        norm = _normalise_chunk(df)

        if DRY_RUN:
            print("[DRY_RUN] Normalised head(5):")
            print(norm.head(5))
            print(f"[DRY_RUN] Would upsert {len(norm)} rows from {os.path.basename(path)}")
        else:
            upsert_df("mot_tests", norm)
            print(f"Upserted {len(norm)} rows from {os.path.basename(path)}")

# -------- (Kept for later) Remote loaders --------
def _load_csv_streaming(url: str, chunksize: int = 100_000) -> None:
    print(f"Downloading CSV: {url}")
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        buf = io.BytesIO()
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            buf.write(chunk)
        buf.seek(0)
    reader = pd.read_csv(buf, chunksize=chunksize, low_memory=False)
    total = 0
    for idx, chunk in enumerate(reader, 1):
        norm = _normalise_chunk(chunk)
        upsert_df("mot_tests", norm)
        total += len(norm)
        print(f"  chunk {idx}: inserted {len(norm)} rows (total {total})")

def _load_zip_csv(url: str, inner_name_hint: Optional[str] = None) -> None:
    print(f"Downloading ZIP: {url}")
    with requests.get(url, stream=True, timeout=180) as r:
        r.raise_for_status()
        zbytes = io.BytesIO(r.content)
    with zipfile.ZipFile(zbytes) as zf:
        names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not names:
            print("No CSV found in ZIP.")
            return
        inner = inner_name_hint if inner_name_hint in names else names[0]
        with zf.open(inner) as f:
            reader = pd.read_csv(f, chunksize=100_000, low_memory=False)
            total = 0
            for idx, chunk in enumerate(reader, 1):
                norm = _normalise_chunk(chunk)
                upsert_df("mot_tests", norm)
                total += len(norm)
                print(f"  chunk {idx}: inserted {len(norm)} rows (total {total})")

def refresh_from_dvsa() -> None:
    if not MOT_DATA_URLS:
        print("MOT_DATA_URLS not set; skipping remote DVSA load.")
        return
    for url in MOT_DATA_URLS:
        url = url.strip()
        if not url:
            continue
        try:
            if url.lower().endswith(".zip"):
                _load_zip_csv(url)
            else:
                _load_csv_streaming(url)
        except Exception as e:
            print(f"⚠️ Failed to load {url}: {e}")

# -------- Entrypoint --------
if __name__ == "__main__":
    print(f"[{datetime.utcnow().isoformat()}] MOT refresh starting…")
    seed_failure_codes()
    seed_sample_rows()
    # Local-first manual ingest for inspection
    load_local_csvs()
    # Keep remote disabled for now (uncomment later)
    # refresh_from_dvsa()
    print("Done.")
