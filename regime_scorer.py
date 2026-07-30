"""
regime_scorer.py
----------------
Loads the trained HMM from models/hmm_model.pkl, scores
mart_regime__features, and writes regime predictions back
to MotherDuck as ml.regime_predictions.

Run after hmm_trainer.py. Safe to rerun — uses incremental
load logic, only inserting rows newer than the latest scored date.

Output table:
  ml.regime_predictions
    date                  DATE
    regime_label          VARCHAR   -- compression / elevated / stress
    regime_state          INTEGER   -- raw HMM state index
    prob_compression      DOUBLE    -- posterior probability per regime
    prob_elevated         DOUBLE
    prob_stress           DOUBLE
    hy_oas                DOUBLE    -- carried through for dashboard queries
    stress_composite      DOUBLE
    scored_at             TIMESTAMP

Usage:
  python regime_scorer.py
"""

import os
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import duckdb
import joblib
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

load_dotenv()

MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")
MOTHERDUCK_DB    = os.getenv("MOTHERDUCK_DB", "cred_spread")

# Resolved from this file, not the cwd — the Prefect worker's working
# directory is not guaranteed to be the repo root.
MODEL_DIR        = Path(__file__).resolve().parent / "models"
MODEL_PATH       = MODEL_DIR / "hmm_model.pkl"
METADATA_PATH    = MODEL_DIR / "hmm_metadata.json"

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
# Load model artifacts
# ---------------------------------------------------------------------------

def load_model_artifacts() -> tuple:
    """
    Load trained HMM, scaler, and metadata from disk.
    Raises clearly if files are missing — don't let a missing
    model silently score with stale or default parameters.
    """
    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            f"Model not found at {MODEL_PATH}. "
            "Run hmm_trainer.py first."
        )
    if not METADATA_PATH.exists():
        raise FileNotFoundError(
            f"Metadata not found at {METADATA_PATH}. "
            "Run hmm_trainer.py first."
        )

    artifacts = joblib.load(MODEL_PATH)
    model     = artifacts["model"]
    scaler    = artifacts["scaler"]

    with open(METADATA_PATH) as f:
        metadata = json.load(f)

    log.info(f"Model loaded: n_components={metadata['n_components']}, "
             f"trained_at={metadata['trained_at']}")
    log.info(f"Regime labels: {metadata['regime_labels']}")

    return model, scaler, metadata


# ---------------------------------------------------------------------------
# Load features
# ---------------------------------------------------------------------------

def load_features(conn: duckdb.DuckDBPyConnection, feature_cols: list) -> pd.DataFrame:
    """Load full feature set from mart_regime__features."""
    cols = ", ".join(feature_cols)
    query = f"""
        SELECT
            date,
            {cols},
            hy_oas,
            stress_composite
        FROM main_marts.mart_regime__features
        ORDER BY date
    """
    df = conn.execute(query).df()
    log.info(f"Loaded {len(df)} rows from mart_regime__features")
    log.info(f"Date range: {df['date'].min()} to {df['date'].max()}")
    return df


# ---------------------------------------------------------------------------
# Incremental filtering
# ---------------------------------------------------------------------------

def get_latest_scored_date(conn: duckdb.DuckDBPyConnection) -> Optional[object]:
    """Return most recent date already in ml.regime_predictions."""
    try:
        result = conn.execute(
            "SELECT MAX(date) FROM ml.regime_predictions"
        ).fetchone()
        return result[0] if result else None
    except Exception:
        # Table doesn't exist yet on first run
        return None


def filter_unscored(df: pd.DataFrame, latest: Optional[object]) -> pd.DataFrame:
    """
    Return only rows newer than the latest scored date.

    Applied to scoring *output*, not input — see run_scoring() for why the
    HMM must see the full sequence even when only a few rows get inserted.
    """
    if latest is None:
        log.info("No existing predictions — inserting full history")
        return df

    new_rows = df[pd.to_datetime(df["date"]) > pd.Timestamp(latest)]
    log.info(f"Latest scored date: {latest} — {len(new_rows)} new rows to insert")
    return new_rows


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def score_regimes(
    df: pd.DataFrame,
    model,
    scaler,
    metadata: dict,
) -> pd.DataFrame:
    """
    Score each row with:
      - Most likely regime state (Viterbi decoding)
      - Posterior probabilities per state (forward-backward)
      - Human-readable regime label

    Note: Viterbi decoding uses the full sequence — regime assignments
    account for transition probabilities, not just emission probabilities.
    This means a single anomalous day won't flip the regime if the
    transition matrix says the current regime is sticky.
    """
    feature_cols   = metadata["feature_cols"]
    regime_labels  = {int(k): v for k, v in metadata["regime_labels"].items()}
    n_components   = metadata["n_components"]

    X = df[feature_cols].values
    X_scaled = scaler.transform(X)

    # Viterbi: most likely state sequence given full observation history
    _, state_sequence = model.decode(X_scaled, algorithm="viterbi")

    # Posterior probabilities: P(state | all observations)
    posteriors = model.predict_proba(X_scaled)   # shape: (n_obs, n_components)

    # Build probability columns keyed to regime label
    # Find which state index maps to each label
    label_to_state = {v: k for k, v in regime_labels.items()}

    results = df[["date", "hy_oas", "stress_composite"]].copy()
    results["regime_state"] = state_sequence
    results["regime_label"] = results["regime_state"].map(regime_labels)
    results["scored_at"]    = datetime.utcnow()

    # Probability columns — handle missing labels gracefully
    for label in ["compression", "elevated", "stress"]:
        col = f"prob_{label}"
        if label in label_to_state:
            state_idx = label_to_state[label]
            results[col] = posteriors[:, state_idx]
        else:
            # Label not present in this model (e.g. 2-component model)
            results[col] = np.nan

    log.info(f"Scored {len(results)} rows")
    log.info(f"Regime distribution:\n{results['regime_label'].value_counts().to_string()}")

    return results


# ---------------------------------------------------------------------------
# Bootstrap schema and load
# ---------------------------------------------------------------------------

def bootstrap_ml_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Create ml schema and regime_predictions table if not exists."""
    conn.execute("CREATE SCHEMA IF NOT EXISTS ml")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ml.regime_predictions (
            date              DATE        NOT NULL,
            regime_label      VARCHAR     NOT NULL,
            regime_state      INTEGER     NOT NULL,
            prob_compression  DOUBLE,
            prob_elevated     DOUBLE,
            prob_stress       DOUBLE,
            hy_oas            DOUBLE,
            stress_composite  DOUBLE,
            scored_at         TIMESTAMP   NOT NULL
        )
    """)
    log.info("ml schema bootstrapped")


def load_predictions(
    conn: duckdb.DuckDBPyConnection,
    results: pd.DataFrame,
) -> int:
    """Insert scored rows into ml.regime_predictions."""
    if results.empty:
        log.info("No new rows to insert")
        return 0

    conn.register("scored", results)
    conn.execute("""
        INSERT INTO ml.regime_predictions (
            date,
            regime_label,
            regime_state,
            prob_compression,
            prob_elevated,
            prob_stress,
            hy_oas,
            stress_composite,
            scored_at
        )
        SELECT
            date,
            regime_label,
            regime_state,
            prob_compression,
            prob_elevated,
            prob_stress,
            hy_oas,
            stress_composite,
            scored_at
        FROM scored
    """)

    log.info(f"Inserted {len(results)} rows into ml.regime_predictions")
    return len(results)


# ---------------------------------------------------------------------------
# Sanity check
# ---------------------------------------------------------------------------

def log_current_regime(conn: duckdb.DuckDBPyConnection) -> None:
    """Log the most recent regime assignment as a sanity check."""
    row = conn.execute("""
        SELECT
            date,
            regime_label,
            ROUND(prob_compression, 3) as prob_compression,
            ROUND(prob_elevated, 3)    as prob_elevated,
            ROUND(prob_stress, 3)      as prob_stress,
            ROUND(hy_oas, 3)           as hy_oas
        FROM ml.regime_predictions
        ORDER BY date DESC
        LIMIT 1
    """).df()

    log.info("=== Current Regime ===")
    for col in row.columns:
        log.info(f"  {col}: {row[col].values[0]}")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def run_scoring(full_refresh: bool = False) -> None:
    """
    Score the feature mart and append new rows to ml.regime_predictions.

    The HMM is always decoded over the *full* feature history, then the result
    is filtered down to unscored dates. Filtering first would hand Viterbi a
    short isolated sequence — on a daily schedule that's a handful of rows —
    which restarts decoding from the model's initial state distribution and
    throws away exactly the transition stickiness the HMM is there to provide.
    Decoding ~2k rows is milliseconds, so there is no reason to do otherwise.

    full_refresh=True clears the table and reinserts everything. Use it after
    retraining, when a new fit may have permuted the hidden state indices and
    the existing rows are no longer comparable to new ones.
    """
    log.info("=== Regime scoring started ===")

    if not MOTHERDUCK_TOKEN:
        raise ValueError("MOTHERDUCK_TOKEN not set")

    model, scaler, metadata = load_model_artifacts()

    conn = duckdb.connect(
        f"md:{MOTHERDUCK_DB}?motherduck_token={MOTHERDUCK_TOKEN}"
    )

    try:
        bootstrap_ml_schema(conn)

        if full_refresh:
            log.warning("full_refresh=True — clearing ml.regime_predictions")
            conn.execute("DELETE FROM ml.regime_predictions")
            latest = None
        else:
            latest = get_latest_scored_date(conn)

        features = load_features(conn, metadata["feature_cols"])

        if features.empty:
            log.info("Feature mart is empty — nothing to score")
            return

        # Decode the whole sequence, then keep only the unscored tail.
        results = score_regimes(features, model, scaler, metadata)
        new_results = filter_unscored(results, latest)

        if new_results.empty:
            log.info("All rows already scored — nothing to do")
            return

        load_predictions(conn, new_results)
        log_current_regime(conn)

    finally:
        conn.close()

    log.info("=== Regime scoring complete ===")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Score credit spread regimes")
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="clear ml.regime_predictions and rescore all history",
    )
    args = parser.parse_args()

    run_scoring(full_refresh=args.full_refresh)