"""
deploy_flight.py
----------------
Create or update the MotherDuck Flights that run this pipeline.

  python scripts/deploy_flight.py            # create/update both Flights
  python scripts/deploy_flight.py --run      # ...then trigger daily on demand
  python scripts/deploy_flight.py --logs     # show the last run's logs

Two Flights share one source file (flight_entrypoint.py) and differ only by
their MODE config:

  cred_spread_daily     ingestion -> dbt build -> scoring
                        13:00 UTC Mon-Fri (09:00 EDT / 08:00 EST)
  cred_spread_retrain   ...plus refit the HMM and rebuild the prediction table
                        on demand only

Requires MOTHERDUCK_TOKEN in the environment or .env, and the 'fred' flights
secret to already exist:

  CREATE SECRET FRED IN MOTHERDUCK (
      TYPE FLIGHTS, PARAMS MAP {'API_KEY': '<your fred key>'}
  );
"""

import argparse
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
import duckdb

REPO_ROOT = Path(__file__).resolve().parent.parent
ENTRYPOINT = REPO_ROOT / "flight_entrypoint.py"
REQUIREMENTS = REPO_ROOT / "requirements.txt"

SECRET_NAME = "fred"          # MotherDuck lowercases secret names
DAILY = "cred_spread_daily"
RETRAIN = "cred_spread_retrain"

# 13:00 UTC = 09:00 EDT, but 08:00 EST. Flight cron has no timezone field, so
# the run drifts an hour against wall-clock when DST ends. That is harmless
# here: FRED publishes the OAS series on a one-business-day lag, so the run
# only needs to land comfortably after midnight ET.
DAILY_CRON = "0 13 * * 1-5"

# The Flight runtime installs these itself; prefect is deliberately excluded
# since orchestration is now MotherDuck's job.
EXCLUDE_PREFIXES = ("prefect",)


def flight_requirements() -> str:
    """requirements.txt minus comments, blanks, and orchestration-only deps."""
    lines = []
    for raw in REQUIREMENTS.read_text().splitlines():
        line = raw.split("#")[0].strip()
        if not line:
            continue
        if line.lower().startswith(EXCLUDE_PREFIXES):
            continue
        lines.append(line)
    return "\n".join(lines)


def upsert(con, name: str, source: str, requirements: str,
           mode: str, cron: str | None) -> str:
    """Create the Flight, or update it in place if it already exists."""
    existing = con.execute(
        "SELECT flight_id FROM MD_LIST_FLIGHTS() WHERE flight_name = ?", [name]
    ).fetchall()

    config = f"MAP {{'MODE': '{mode}'}}"
    cron_sql = "NULL" if cron is None else f"'{cron}'"

    if existing:
        fid = existing[0][0]
        con.execute(
            f"""SELECT * FROM MD_UPDATE_FLIGHT(
                    flight_id := ?,
                    source_code := ?,
                    requirements_txt := ?,
                    config := {config},
                    schedule_cron := {cron_sql},
                    flight_secret_names := ['{SECRET_NAME}'],
                    max_runtime_sec := 1800)""",
            [fid, source, requirements],
        )
        print(f"  updated {name} ({fid})")
    else:
        row = con.execute(
            f"""SELECT flight_id FROM MD_CREATE_FLIGHT(
                    name := ?,
                    source_code := ?,
                    requirements_txt := ?,
                    config := {config},
                    schedule_cron := {cron_sql},
                    flight_secret_names := ['{SECRET_NAME}'],
                    max_runtime_sec := 1800)""",
            [name, source, requirements],
        ).fetchone()
        fid = row[0]
        print(f"  created {name} ({fid})")

    return fid


def wait_for_run(con, fid, run_number, timeout_sec=1800) -> str:
    """Poll until the run reaches a terminal state; return that status."""
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        time.sleep(10)
        status = str(con.execute(
            "SELECT status FROM MD_LIST_FLIGHT_RUNS(flight_id := ?) "
            "WHERE run_number = ?", [fid, run_number]
        ).fetchone()[0])
        if any(k in status for k in ("SUCCEEDED", "FAILED", "CANCELLED")):
            return status
        print(f"    {status}")
    return "TIMEOUT"


def show_logs(con, fid, run_number) -> None:
    logs = con.execute(
        "SELECT logs FROM MD_GET_FLIGHT_LOGS(flight_id := ?, run_number := ?)",
        [fid, run_number],
    ).fetchone()
    print(logs[0] if logs else "(no logs)")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", action="store_true",
                    help="trigger the daily flight after deploying")
    ap.add_argument("--logs", action="store_true",
                    help="print logs from the daily flight's most recent run")
    args = ap.parse_args()

    load_dotenv(REPO_ROOT / ".env")
    token = os.getenv("MOTHERDUCK_TOKEN")
    if not token:
        sys.exit("MOTHERDUCK_TOKEN not set")

    con = duckdb.connect(f"md:?motherduck_token={token}")

    secrets = [r[0] for r in con.execute(
        "SELECT name FROM duckdb_secrets() WHERE type = 'flights'").fetchall()]
    if SECRET_NAME not in secrets:
        sys.exit(f"flights secret '{SECRET_NAME}' not found. Create it with:\n"
                 f"  CREATE SECRET FRED IN MOTHERDUCK "
                 f"(TYPE FLIGHTS, PARAMS MAP {{'API_KEY': '<key>'}});")

    source = ENTRYPOINT.read_text()
    reqs = flight_requirements()

    if args.logs:
        fid = con.execute(
            "SELECT flight_id FROM MD_LIST_FLIGHTS() WHERE flight_name = ?",
            [DAILY]).fetchone()[0]
        last = con.execute(
            "SELECT max(run_number) FROM MD_LIST_FLIGHT_RUNS(flight_id := ?)",
            [fid]).fetchone()[0]
        show_logs(con, fid, last)
        return 0

    print("deploying flights:")
    daily_id = upsert(con, DAILY, source, reqs, "daily", DAILY_CRON)
    upsert(con, RETRAIN, source, reqs, "retrain", None)

    if args.run:
        print(f"\ntriggering {DAILY}...")
        run_number = int(con.execute(
            "SELECT run_number FROM MD_RUN_FLIGHT(flight_id := ?)",
            [daily_id]).fetchone()[0])
        status = wait_for_run(con, daily_id, run_number)
        print(f"\nstatus: {status}\n")
        show_logs(con, daily_id, run_number)
        if "SUCCEEDED" not in status:
            return 1

    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
