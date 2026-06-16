"""
ingestion.py
------------
FRED API → MotherDuck ingestion pipeline for credit spread regime detection.

Tables created:
  raw.fred_series       — append-only raw series data with ingestion metadata
  raw.ingestion_log     — audit trail of every run

Usage:
  python ingestion.py

Requires:
  FRED_API_KEY and MOTHERDUCK_TOKEN in environment variables or .env file
"""

import os
import logging
from datetime import datetime, date
from typing import Optional

import pandas as pd
import duckdb
from fredapi import Fred
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

load_dotenv()

FRED_API_KEY = os.getenv("FRED_API_KEY")
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")
MOTHERDUCK_DB = os.getenv("MOTHERDUCK_DB", "cred_spread")


SERIES = {
    "hy_oas":      "BAMLH0A0HYM2",   # ICE BofA US High Yield OAS
    "ig_oas":      "BAMLC0A0CM",     # ICE BofA US Investment Grade OAS
    "ted_spread":  "TEDRATE",        # TED Spread (discontinued 2023-06-30)
    "yield_curve": "T10Y2Y",         # 10Y-2Y Treasury Spread
    "vix":         "VIXCLS",         # CBOE VIX
}

# TEDRATE was discontinued — cap it to avoid FRED returning empty
SERIES_END_OVERRIDES = {
    "ted_spread": "2023-06-30",
}

INGESTION_START = "2005-01-01"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# FRED extraction
# ---------------------------------------------------------------------------

def extract_series(fred: Fred, name: str, series_id: str) -> pd.DataFrame:
    """
    Pull a single FRED series and return a normalised DataFrame.
    Keeps NaN rows (weekends/holidays) so downstream dbt can handle fill policy explicitly.
    """
    end_override = SERIES_END_OVERRIDES.get(name)

    log.info(f"Pulling FRED series: {series_id} ({name})")

    raw = fred.get_series(
        series_id,
        observation_start=INGESTION_START,
        observation_end=end_override,   # None pulls through today
        frequency="d",
        aggregation_method="avg",
    )

    df = raw.reset_index()
    df.columns = ["date", "value"]
    df["series_name"] = name
    df["series_id"] = series_id
    df["ingested_at"] = datetime.utcnow().isoformat()
    df["date"] = pd.to_datetime(df["date"]).dt.date

    log.info(f"  → {len(df)} rows pulled ({df['date'].min()} to {df['date'].max()})")

    return df[["date", "series_name", "series_id", "value", "ingested_at"]]


def extract_all_series(fred: Fred) -> pd.DataFrame:
    """Pull all configured FRED series and concatenate."""
    frames = []
    for name, series_id in SERIES.items():
        try:
            df = extract_series(fred, name, series_id)
            frames.append(df)
        except Exception as e:
            log.error(f"Failed to pull {series_id} ({name}): {e}")
            raise

    combined = pd.concat(frames, ignore_index=True)
    log.info(f"Total rows extracted: {len(combined)}")
    return combined


# ---------------------------------------------------------------------------
# MotherDuck setup
# ---------------------------------------------------------------------------

def get_connection() -> duckdb.DuckDBPyConnection:
    """Connect to MotherDuck."""
    if not MOTHERDUCK_TOKEN:
        raise ValueError("MOTHERDUCK_TOKEN not set in environment")

    conn_str = f"md:{MOTHERDUCK_DB}?motherduck_token={MOTHERDUCK_TOKEN}"
    log.info(f"Connecting to MotherDuck database: {MOTHERDUCK_DB}")
    conn = duckdb.connect(conn_str)
    return conn


def bootstrap_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Create raw schema and tables if they don't exist.
    raw.fred_series is append-only — no upserts, no deletes.
    """
    log.info("Bootstrapping schema...")

    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw.fred_series (
            date         DATE        NOT NULL,
            series_name  VARCHAR     NOT NULL,
            series_id    VARCHAR     NOT NULL,
            value        DOUBLE,
            ingested_at  TIMESTAMP   NOT NULL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw.ingestion_log (
            run_id          VARCHAR     NOT NULL,
            run_at          TIMESTAMP   NOT NULL,
            series_pulled   INTEGER     NOT NULL,
            rows_inserted   INTEGER     NOT NULL,
            min_date        DATE,
            max_date        DATE,
            status          VARCHAR     NOT NULL,
            error_message   VARCHAR
        )
    """)

    log.info("Schema bootstrap complete")


# ---------------------------------------------------------------------------
# Incremental load logic
# ---------------------------------------------------------------------------

def get_latest_loaded_date(conn: duckdb.DuckDBPyConnection, series_name: str) -> Optional[date]:
    """
    Return the most recent date already loaded for a given series.
    Used to avoid re-inserting historical data on subsequent runs.
    """
    result = conn.execute("""
        SELECT MAX(date)
        FROM raw.fred_series
        WHERE series_name = ?
    """, [series_name]).fetchone()

    latest = result[0] if result else None
    return latest


def filter_new_rows(
    df: pd.DataFrame,
    conn: duckdb.DuckDBPyConnection
) -> pd.DataFrame:
    """
    For each series, drop rows already present in MotherDuck.
    Keeps the raw table append-only without duplicating history.
    """
    filtered_frames = []

    for series_name, group in df.groupby("series_name"):
        latest = get_latest_loaded_date(conn, series_name)

        if latest is not None:
            new_rows = group[group["date"] > latest]
            log.info(
                f"  {series_name}: latest loaded={latest}, "
                f"new rows to insert={len(new_rows)}"
            )
        else:
            new_rows = group
            log.info(f"  {series_name}: no existing data, inserting all {len(new_rows)} rows")

        filtered_frames.append(new_rows)

    return pd.concat(filtered_frames, ignore_index=True)


# ---------------------------------------------------------------------------
# Load to MotherDuck
# ---------------------------------------------------------------------------

def load_to_motherduck(
    df: pd.DataFrame,
    conn: duckdb.DuckDBPyConnection,
    run_id: str
) -> int:
    """
    Insert new rows into raw.fred_series.
    Returns number of rows inserted.
    """
    if df.empty:
        log.info("No new rows to insert")
        return 0

    # Register DataFrame as a DuckDB relation so we can INSERT SELECT from it
    conn.register("new_data", df)

    conn.execute("""
        INSERT INTO raw.fred_series
            (date, series_name, series_id, value, ingested_at)
        SELECT
            date,
            series_name,
            series_id,
            value,
            CAST(ingested_at AS TIMESTAMP)
        FROM new_data
    """)

    rows_inserted = len(df)
    log.info(f"Inserted {rows_inserted} rows into raw.fred_series")
    return rows_inserted


def write_ingestion_log(
    conn: duckdb.DuckDBPyConnection,
    run_id: str,
    rows_inserted: int,
    df: pd.DataFrame,
    status: str,
    error_message: Optional[str] = None
) -> None:
    """Write a run record to raw.ingestion_log."""
    min_date = df["date"].min() if not df.empty else None
    max_date = df["date"].max() if not df.empty else None

    conn.execute("""
        INSERT INTO raw.ingestion_log
            (run_id, run_at, series_pulled, rows_inserted, min_date, max_date, status, error_message)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        run_id,
        datetime.utcnow(),
        len(SERIES),
        rows_inserted,
        min_date,
        max_date,
        status,
        error_message,
    ])

    log.info(f"Ingestion log written: run_id={run_id}, status={status}")


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_extract(df: pd.DataFrame) -> None:
    """
    Basic sanity checks on extracted data before loading.
    Raises on failure so the run aborts cleanly.
    """
    for series_name, group in df.groupby("series_name"):
        null_pct = group["value"].isna().mean()

        # Weekends/holidays will produce some nulls — 40% is a generous ceiling
        if null_pct > 0.40:
            raise ValueError(
                f"{series_name} has {null_pct:.1%} null values — "
                f"possible API or series issue"
            )

        # HY OAS should never be negative
        if series_name == "hy_oas":
            if (group["value"].dropna() < 0).any():
                raise ValueError(f"{series_name} contains negative values — data integrity issue")

    log.info("Validation passed")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def run_ingestion() -> None:
    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log.info(f"=== Ingestion run started: {run_id} ===")

    if not FRED_API_KEY:
        raise ValueError("FRED_API_KEY not set in environment")

    fred = Fred(api_key=FRED_API_KEY)
    conn = get_connection()

    try:
        bootstrap_schema(conn)
        raw_df = extract_all_series(fred)
        validate_extract(raw_df)
        new_df = filter_new_rows(raw_df, conn)
        rows_inserted = load_to_motherduck(new_df, conn, run_id)
        write_ingestion_log(conn, run_id, rows_inserted, new_df, status="success")
        log.info(f"=== Ingestion run complete: {run_id} ===")

    except Exception as e:
        log.error(f"Ingestion failed: {e}")
        write_ingestion_log(conn, run_id, 0, pd.DataFrame(), status="failed", error_message=str(e))
        raise

    finally:
        conn.close()


if __name__ == "__main__":
    run_ingestion()