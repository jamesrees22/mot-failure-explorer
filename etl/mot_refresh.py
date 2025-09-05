import os
import pandas as pd
import numpy as np
import datetime as dt
from datetime import datetime
from typing import Any

from supabase import create_client, Client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]  # service role (write only in CI)
sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def _to_serializable(v: Any) -> Any:
    """
    Make values JSON/PostgREST friendly:
    - pandas/NumPy timestamps -> ISO date string
    - pandas/NumPy integers/floats with NA -> native int/float or None
    - pandas NA/NaN/NaT -> None
    - leave lists/dicts/str/bool as is
    """
    # Dates / times
    if isinstance(v, (pd.Timestamp, dt.datetime, dt.date)):
        # Use date-only for this dataset (test_date is a date)
        # If it's a datetime or pandas Timestamp, take the date component.
        if isinstance(v, pd.Timestamp):
            return v.date().isoformat() if not pd.isna(v) else None
        if isinstance(v, dt.datetime):
            return v.date().isoformat()
        return v.isoformat()  # dt.date

    # NumPy scalar numbers
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return float(v)

    # Pandas NA / NaN
    try:
        # pd.isna(list) raises, so guard
        if v is pd.NA or (not isinstance(v, list) and pd.isna(v)):
            return None
    except Exception:
        pass

    return v


def upsert_df(table: str, df: pd.DataFrame, chunk: int = 5000) -> None:
    """
    Prepare a DataFrame for PostgREST upsert:
    - Coerce 'test_date' to python date
    - Replace pandas NA with None
    - Convert tricky scalars to JSON-safe types
    """
    if df.empty:
        return

    df = df.copy()

    # Normalise date column(s)
    if "test_date" in df.columns:
        df["test_date"] = pd.to_datetime(df["test_date"], errors="coerce").dt.date

    # Ensure Int64/Float64 nullable dtypes won't leak NA scalars
    # We'll convert NA -> None below; keep python types where possible
    for col in df.columns:
        if pd.api.types.is_integer_dtype(df[col]):
            # keep as pandas nullable Int64, then convert element-wise in records
            pass
        elif pd.api.types.is_float_dtype(df[col]):
            pass

    # Replace pandas NA/NaT with None (but keep lists intact)
    df = df.where(~df.isna(), None)

    # Build safe records
    records = df.to_dict(orient="records")
    safe_records = [{k: _to_serializable(v) for k, v in rec.items()} for rec in records]

    # Chunked upsert
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

    # "a|b|c" -> ["a","b","c"] for text[] column
    if "failure_reasons" in df.columns:
        df["failure_reasons"] = (
            df["failure_reasons"]
            .fillna("")
            .apply(lambda s: [x for x in str(s).split("|") if x])
        )

    # Coerce numeric columns to nullable pandas types; upsert_df will JSON-sanitise
    if "first_use_year" in df.columns:
        df["first_use_year"] = pd.to_numeric(df["first_use_year"], errors="coerce").astype("Int64")
    if "odometer" in df.columns:
        df["odometer"] = pd.to_numeric(df["odometer"], errors="coerce").astype("Int64")

    # Minimal dedupe guard
    subset_cols = [c for c in ["test_date", "make", "model", "odometer", "result"] if c in df.columns]
    if subset_cols:
        df = df.drop_duplicates(subset=subset_cols)

    upsert_df("mot_tests", df)


if __name__ == "__main__":
    print(f"[{datetime.utcnow().isoformat()}] MOT refresh starting…")
    seed_failure_codes()
    seed_sample_rows()
    print("Done.")
