"""
flight_entrypoint.py
--------------------
The program MotherDuck Flights executes on a schedule.

This file's contents are uploaded verbatim as a Flight's `source_code` by
scripts/deploy_flight.py — it is not imported by anything else in the repo.

Why it fetches the repo at runtime
----------------------------------
A Flight's source is a single inline string; there is no documented way to
package multiple files or a directory alongside it. The pipeline needs
ingestion.py, regime_scorer.py, hmm_trainer.py, model_store.py *and* the whole
dbt project, so the entrypoint downloads the repository tarball from GitHub on
each run and shells out to the scripts already there.

That couples production to the repo: by default a run uses whatever is on
`main`. Set the REPO_REF config value to a tag or commit SHA to pin it.

Config (supplied as environment variables by MD_CREATE_FLIGHT / MD_RUN_FLIGHT):
  MODE        'daily' (default) or 'retrain'
  REPO_REF    git ref to fetch; defaults to 'main'

Secrets:
  MOTHERDUCK_TOKEN  injected automatically by the runtime
  FRED_API_KEY      derived from the 'fred' flights secret — see normalize_env()
"""

import io
import os
import subprocess
import sys
import tarfile
import time
import urllib.request

REPO = "jfeinberg32/cred_spread"
MODE = os.getenv("MODE", "daily").strip().lower()
REPO_REF = os.getenv("REPO_REF", "main").strip()

WORKDIR = "/tmp/cred_spread"


def log(msg: str) -> None:
    """Timestamped print — stdout is what shows up in MD_GET_FLIGHT_LOGS."""
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def normalize_env() -> None:
    """
    Expose the FRED key under the name ingestion.py expects.

    A `TYPE FLIGHTS` secret is injected as `<secret_name>_<PARAM>`, and
    MotherDuck lowercases secret names — so the secret `FRED` with param
    `API_KEY` arrives as `fred_API_KEY`, never `FRED_API_KEY`. The runtime
    also exposes a bare `API_KEY`, but that is used as a fallback only since
    it would collide if a second secret ever defined the same param name.
    """
    if os.getenv("FRED_API_KEY"):
        return

    for candidate in ("fred_API_KEY", "API_KEY"):
        value = os.getenv(candidate)
        if value:
            os.environ["FRED_API_KEY"] = value
            log(f"mapped {candidate} -> FRED_API_KEY")
            return


def fetch_repo() -> str:
    """Download and unpack the repo tarball; return the extracted path."""
    url = f"https://github.com/{REPO}/archive/{REPO_REF}.tar.gz"
    log(f"fetching {url}")

    with urllib.request.urlopen(url, timeout=120) as resp:
        blob = resp.read()
    log(f"  {len(blob):,} bytes")

    os.makedirs(WORKDIR, exist_ok=True)
    with tarfile.open(fileobj=io.BytesIO(blob), mode="r:gz") as tar:
        # Tarball root is "<repo>-<ref>/"; strip it so paths land in WORKDIR.
        root = tar.getnames()[0].split("/")[0]
        tar.extractall(WORKDIR)

    path = os.path.join(WORKDIR, root)
    log(f"  extracted to {path}")
    return path


def run(cmd: list, cwd: str) -> None:
    """Run a subprocess, forward its output, and raise on failure."""
    log(f"$ {' '.join(cmd)}")
    proc = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)

    for line in (proc.stdout or "").splitlines():
        print(f"    {line}", flush=True)

    if proc.returncode != 0:
        for line in (proc.stderr or "").splitlines():
            print(f"    ! {line}", flush=True)
        raise RuntimeError(f"{cmd[1] if len(cmd) > 1 else cmd[0]} exited {proc.returncode}")


def main() -> None:
    log(f"mode={MODE} ref={REPO_REF}")

    normalize_env()

    if not os.getenv("MOTHERDUCK_TOKEN"):
        raise RuntimeError("MOTHERDUCK_TOKEN not injected into the runtime")
    if not os.getenv("FRED_API_KEY"):
        raise RuntimeError(
            "FRED_API_KEY unavailable — is the 'fred' secret attached via "
            "flight_secret_names?"
        )

    repo = fetch_repo()
    py = sys.executable

    # 1. Pull new FRED observations into raw.fred_series.
    log("=== ingestion ===")
    run([py, "ingestion.py"], cwd=repo)

    # 2. Rebuild the dbt models. `build`, not `run`, so the data tests gate
    #    the model step — a bad OAS value should stop the pipeline here
    #    rather than propagate into the HMM.
    log("=== dbt build ===")
    if not os.path.isdir(os.path.join(repo, "dbt", "dbt_packages")):
        run([py, "-m", "dbt.cli.main", "deps",
             "--project-dir", "dbt", "--profiles-dir", "dbt"], cwd=repo)
    run([py, "-m", "dbt.cli.main", "build",
         "--project-dir", "dbt", "--profiles-dir", "dbt"], cwd=repo)

    # 3. Score. Retraining rewrites the whole prediction table, because a new
    #    fit can permute the hidden state indices and leave the table holding
    #    output from two different models.
    if MODE == "retrain":
        log("=== hmm retrain ===")
        run([py, "hmm_trainer.py"], cwd=repo)
        log("=== scoring (full refresh) ===")
        run([py, "regime_scorer.py", "--full-refresh"], cwd=repo)
    else:
        log("=== scoring ===")
        run([py, "regime_scorer.py"], cwd=repo)

    log("FLIGHT_OK")


if __name__ == "__main__":
    main()
