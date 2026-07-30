"""
flows.py
--------
Prefect orchestration for the credit spread regime pipeline.

Two deployments (see prefect.yaml):

  daily-regime-refresh  ->  credit_spread_pipeline_flow
      ingestion.py -> dbt build -> regime_scorer.py
      09:00 ET, Mon-Fri. Scores with the model already on disk and
      never retrains, so regime labels stay comparable run to run.

  hmm-retrain           ->  hmm_retrain_flow
      hmm_trainer.py -> full rebuild of ml.regime_predictions
      Triggered manually (or on a slow schedule). Retraining can
      reassign hidden-state indices and change the BIC-selected
      n_components, so the prediction table is rebuilt from scratch
      instead of being left as a mix of two models' output.

Local runs:
  python flows.py            # daily pipeline
  python flows.py retrain    # retrain + rescore
"""

import os
import subprocess
import sys
from pathlib import Path

from prefect import flow, get_run_logger, task

REPO_ROOT = Path(__file__).resolve().parent
DBT_DIR = REPO_ROOT / "dbt"

# The step scripts resolve their own paths from __file__, so all this needs
# to guarantee is that `import ingestion` / `import regime_scorer` find the
# repo regardless of the worker's working directory.
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run(cmd: list, logger) -> None:
    """Run a subprocess from the repo root, forwarding output to the run log."""
    logger.info("$ " + " ".join(cmd))
    result = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True)

    if result.stdout:
        logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr or "(no stderr)")
        raise RuntimeError(f"{cmd[0]} exited {result.returncode}")


def _notify(message: str, logger) -> None:
    """
    Send a Slack alert if a SlackWebhook block named 'credit-spread-alerts'
    exists. A missing block is not a pipeline failure — the message is in the
    run log either way.
    """
    try:
        from prefect.blocks.notifications import SlackWebhook

        SlackWebhook.load("credit-spread-alerts").notify(message)
        logger.info("Slack alert sent")
    except Exception as e:
        logger.info(
            f"Slack alert skipped ({type(e).__name__}). Create a SlackWebhook "
            f"block named 'credit-spread-alerts' in Prefect Cloud to enable."
        )


def _motherduck_connect():
    """Open a MotherDuck connection using the same env vars as the scripts."""
    import duckdb
    from dotenv import load_dotenv

    load_dotenv(REPO_ROOT / ".env")

    token = os.getenv("MOTHERDUCK_TOKEN")
    if not token:
        raise ValueError("MOTHERDUCK_TOKEN not set")

    db = os.getenv("MOTHERDUCK_DB", "cred_spread")
    return duckdb.connect(f"md:{db}?motherduck_token={token}")


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(retries=3, retry_delay_seconds=300, name="ingest-fred-data")
def ingest_data() -> None:
    """
    Pull the configured FRED series into raw.fred_series.

    Long retry delay on purpose: this guards against a FRED API blip or rate
    limit, and retrying 60 seconds later usually hits the same condition.
    run_ingestion() is incremental and writes its own audit row, so retrying
    after a partial failure is safe.
    """
    from ingestion import run_ingestion

    run_ingestion()


@task(retries=1, name="dbt-deps")
def dbt_deps() -> None:
    """Install dbt packages on first run only — dbt_utils rarely changes."""
    logger = get_run_logger()

    if (DBT_DIR / "dbt_packages").exists():
        logger.info("dbt_packages present — skipping dbt deps")
        return

    _run(
        ["dbt", "deps", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)],
        logger,
    )


@task(retries=2, retry_delay_seconds=60, name="dbt-build")
def dbt_build() -> None:
    """
    Refresh staging/intermediate/mart models and run the data tests.

    `build` rather than `run` so assert_no_negative_oas and the schema tests
    gate the model step — a bad OAS value should stop the pipeline before it
    reaches the HMM, not after.
    """
    logger = get_run_logger()

    _run(
        ["dbt", "build", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)],
        logger,
    )


@task(retries=1, retry_delay_seconds=30, name="score-hmm-regimes")
def score_hmm_regimes(full_refresh: bool = False) -> None:
    """
    Score mart_regime__features with the saved HMM and write
    ml.regime_predictions.

    full_refresh=True deletes and rebuilds the table — used after a retrain,
    when the existing rows were scored by a different model.
    """
    from regime_scorer import run_scoring

    run_scoring(full_refresh=full_refresh)


@task(retries=1, name="train-hmm")
def train_hmm() -> None:
    """Refit the HMM and overwrite models/hmm_model.pkl + hmm_metadata.json."""
    from hmm_trainer import run_training

    run_training()


@task(name="check-regime-change")
def check_regime_change() -> None:
    """
    Compare the two most recent scored days and alert on a regime shift.

    Reads back from ml.regime_predictions rather than taking a value from the
    scoring task, so this stays correct on runs where FRED published nothing
    and the scorer inserted zero rows.
    """
    logger = get_run_logger()

    conn = _motherduck_connect()
    try:
        rows = conn.execute("""
            SELECT
                date,
                regime_label,
                GREATEST(
                    COALESCE(prob_compression, 0),
                    COALESCE(prob_elevated, 0),
                    COALESCE(prob_stress, 0)
                ) AS confidence
            FROM ml.regime_predictions
            ORDER BY date DESC
            LIMIT 2
        """).fetchall()
    finally:
        conn.close()

    if len(rows) < 2:
        logger.info("Fewer than two scored days — skipping regime-change check")
        return

    curr_date, curr_label, curr_conf = rows[0]
    prev_label = rows[1][1]

    if curr_label == prev_label:
        logger.info(f"No regime change — still '{curr_label}' as of {curr_date}")
        return

    msg = (
        f"Credit regime shift: {prev_label} -> {curr_label} "
        f"as of {curr_date} (confidence {curr_conf:.2f})"
    )
    logger.warning(msg)
    _notify(msg, logger)


# ---------------------------------------------------------------------------
# Flows
# ---------------------------------------------------------------------------

@flow(name="credit-spread-regime-pipeline", log_prints=True)
def credit_spread_pipeline_flow() -> None:
    """Daily refresh: new FRED observations through to scored regimes."""
    ingest_data()
    dbt_deps()
    dbt_build()
    score_hmm_regimes(full_refresh=False)
    check_regime_change()


@flow(name="hmm-retrain", log_prints=True)
def hmm_retrain_flow(rescore_history: bool = True) -> None:
    """
    Refit the HMM on everything currently in mart_regime__features.

    rescore_history defaults to True because a new fit can permute the hidden
    state indices, so leaving existing predictions in place would mix two
    models in one table. ml.regime_predictions is fully derivable from the
    mart plus the model, so rebuilding it loses nothing.
    """
    train_hmm()
    score_hmm_regimes(full_refresh=rescore_history)
    check_regime_change()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "retrain":
        hmm_retrain_flow()
    else:
        credit_spread_pipeline_flow()
