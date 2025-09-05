import os, pandas as pd
from datetime import datetime
from supabase import create_client, Client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]  # service role (write)
sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def upsert_df(table: str, df: pd.DataFrame, chunk=5000):
    records = df.to_dict(orient="records")
    for i in range(0, len(records), chunk):
        batch = records[i:i+chunk]
        sb.table(table).upsert(batch).execute()

def seed_failure_codes():
    path = os.path.join(os.path.dirname(__file__), "failure_codes.csv")
    if not os.path.exists(path): return
    df = pd.read_csv(path).dropna(subset=["code"])
    df["code"] = df["code"].astype(str).str.strip()
    upsert_df("mot_failure_codes", df)

def seed_sample_rows():
    path = os.path.join(os.path.dirname(__file__), "sample_mot_rows.csv")
    if not os.path.exists(path): return
    df = pd.read_csv(path, parse_dates=["test_date"])
    df["failure_reasons"] = df["failure_reasons"].fillna("").apply(lambda s: [x for x in str(s).split("|") if x])
    df["first_use_year"] = pd.to_numeric(df["first_use_year"], errors="coerce").astype("Int64")
    df["odometer"] = pd.to_numeric(df["odometer"], errors="coerce").astype("Int64")
    df = df.drop_duplicates(subset=["test_date","make","model","odometer","result"])
    upsert_df("mot_tests", df)

if __name__ == "__main__":
    print(f"[{datetime.utcnow().isoformat()}] MOT refresh starting…")
    seed_failure_codes()
    seed_sample_rows()
    print("Done.")
