import os, duckdb
import pandas as pd
from fredapi import Fred
from dotenv import load_dotenv
load_dotenv()

FRED_API_KEY     = os.getenv("FRED_API_KEY")
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")

fred = Fred(api_key=FRED_API_KEY)
conn = duckdb.connect(f"md:cred_spread?motherduck_token={MOTHERDUCK_TOKEN}")

SERIES = {
    "hy_oas": "BAMLH0A0HYM2",
    "ig_oas": "BAMLC0A0CM",
}

# Delete existing rows for these series
for name in SERIES:
    deleted = conn.execute(
        "DELETE FROM raw.fred_series WHERE series_name = ?", [name]
    ).rowcount
    print(f"Deleted {deleted} rows for {name}")

# Repull from 1996 — earliest FRED has for these series
from datetime import datetime
ingested_at = datetime.utcnow().isoformat()

for name, series_id in SERIES.items():
    print(f"Pulling {series_id}...")
    raw = fred.get_series(
        series_id,
        observation_start="1996-01-01",
        frequency="d",
        aggregation_method="avg",
    )
    df = raw.reset_index()
    df.columns = ["date", "value"]
    df["series_name"]  = name
    df["series_id"]    = series_id
    df["ingested_at"]  = ingested_at
    df["date"]         = pd.to_datetime(df["date"]).dt.date
    df = df.dropna(subset=["value"])

    conn.register("backfill", df)
    conn.execute("""
        INSERT INTO raw.fred_series (date, series_name, series_id, value, ingested_at)
        SELECT date, series_name, series_id, value, CAST(ingested_at AS TIMESTAMP)
        FROM backfill
    """)
    print(f"  Inserted {len(df)} rows for {name}")

# Verify
print(conn.execute("""
    SELECT series_name, MIN(date), MAX(date), COUNT(*)
    FROM raw.fred_series
    GROUP BY series_name
    ORDER BY series_name
""").df().to_string())

conn.close()