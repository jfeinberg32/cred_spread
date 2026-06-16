"""
hmm_trainer.py
--------------
Reads mart_regime__features from MotherDuck, fits a Gaussian HMM,
selects optimal number of regimes via BIC, and serializes the trained
model and metadata to disk.

Output files:
  models/hmm_model.pkl        — trained GaussianHMM
  models/hmm_metadata.json    — feature cols, n_components, BIC scores,
                                 regime labels, training date range

Usage:
  python hmm_trainer.py
"""

import os
import json
import logging
import joblib
import warnings
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import duckdb
from hmmlearn.hmm import GaussianHMM
from sklearn.preprocessing import StandardScaler
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

load_dotenv()

MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")
MOTHERDUCK_DB    = os.getenv("MOTHERDUCK_DB", "cred_spread")

# Columns passed to the HMM — order must match mart_regime__features
FEATURE_COLS = [
    "hy_oas_zscore_252d",
    "ig_oas_zscore_252d",
    "vix_zscore_252d",
    "hy_oas_mom_21d",
    "hy_oas_vol_21d",
    "stress_composite",
]

# Range of regime counts to evaluate via BIC
N_COMPONENTS_RANGE = range(2, 4)

# HMM training parameters
N_ITER    = 1000
TOL       = 1e-4
N_INIT    = 25       # manual random restarts — picks best log-likelihood
COVAR     = "full"   # full covariance matrix per regime

MODEL_DIR = Path("models")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Suppress hmmlearn convergence warnings during BIC search
warnings.filterwarnings("ignore", category=UserWarning, module="hmmlearn")


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_features() -> pd.DataFrame:
    """Load mart_regime__features from MotherDuck."""
    if not MOTHERDUCK_TOKEN:
        raise ValueError("MOTHERDUCK_TOKEN not set")

    conn_str = f"md:{MOTHERDUCK_DB}?motherduck_token={MOTHERDUCK_TOKEN}"
    log.info("Connecting to MotherDuck...")
    conn = duckdb.connect(conn_str)

    query = f"""
        SELECT
            date,
            {', '.join(FEATURE_COLS)},
            hy_oas,
            stress_composite
        FROM main_marts.mart_regime__features
        ORDER BY date
    """

    df = conn.execute(query).df()
    conn.close()

    log.info(f"Loaded {len(df)} rows from mart_regime__features")
    log.info(f"Date range: {df['date'].min()} to {df['date'].max()}")
    return df


# ---------------------------------------------------------------------------
# Preprocessing
# ---------------------------------------------------------------------------

def preprocess(df: pd.DataFrame) -> tuple[np.ndarray, StandardScaler, pd.Series]:
    """
    Scale features to zero mean / unit variance.
    HMMs are sensitive to feature scale — without this, hy_oas_mom_21d
    (raw spread points) would dominate z-score features (already scaled).
    Returns scaled array, fitted scaler, and date index.
    """
    X = df[FEATURE_COLS].values
    dates = df["date"]

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    log.info(f"Feature matrix shape: {X_scaled.shape}")
    return X_scaled, scaler, dates


# ---------------------------------------------------------------------------
# BIC-based model selection
# ---------------------------------------------------------------------------

def compute_bic(model: GaussianHMM, X: np.ndarray) -> float:
    """
    BIC = -2 * log_likelihood + n_params * log(n_samples)
    Lower BIC = better model accounting for complexity penalty.
    """
    log_likelihood = model.score(X)
    n_samples, n_features = X.shape
    n_components = model.n_components

    # Parameter count for full covariance GaussianHMM:
    # transition matrix: n_components * (n_components - 1)
    # means: n_components * n_features
    # covariances: n_components * n_features * (n_features + 1) / 2
    n_params = (
        n_components * (n_components - 1) +
        n_components * n_features +
        n_components * n_features * (n_features + 1) // 2
    )

    bic = -2 * log_likelihood * n_samples + n_params * np.log(n_samples)
    return bic


def fit_best_hmm(X: np.ndarray, n_components: int, n_init: int) -> GaussianHMM:
    """
    Fit HMM with multiple random restarts, return best by log-likelihood.
    Manual replacement for n_init parameter unavailable in older hmmlearn.
    """
    best_model = None
    best_score = -np.inf

    for i in range(n_init):
        model = GaussianHMM(
            n_components=n_components,
            covariance_type=COVAR,
            n_iter=N_ITER,
            tol=TOL,
            random_state=i,
        )
        try:
            model.fit(X)
            score = model.score(X)
            if score > best_score:
                best_score = score
                best_model = model
        except Exception:
            continue

    if best_model is None:
        raise RuntimeError(f"All {n_init} restarts failed for n_components={n_components}")

    return best_model


def select_n_components(X: np.ndarray) -> tuple[int, dict]:
    """
    Fit HMMs for each candidate n_components and select via BIC.
    Returns optimal n and full BIC scores dict.
    """
    bic_scores = {}

    for n in N_COMPONENTS_RANGE:
        log.info(f"Fitting HMM with n_components={n} ({N_INIT} restarts)...")
        model = fit_best_hmm(X, n_components=n, n_init=N_INIT)
        bic = compute_bic(model, X)
        bic_scores[n] = round(bic, 2)
        log.info(f"  n={n} -> BIC={bic:.2f}")

    optimal_n = min(bic_scores, key=bic_scores.get)
    log.info(f"Optimal n_components={optimal_n} (lowest BIC={bic_scores[optimal_n]})")
    return optimal_n, bic_scores


# ---------------------------------------------------------------------------
# Final model training
# ---------------------------------------------------------------------------

def train_final_model(X: np.ndarray, n_components: int) -> GaussianHMM:
    """Train final HMM with more restarts for better convergence."""
    log.info(f"Training final model with n_components={n_components}...")
    model = fit_best_hmm(X, n_components=n_components, n_init=50)
    log.info(f"Model converged: {model.monitor_.converged}")
    log.info(f"Final log-likelihood: {model.score(X):.4f}")
    return model


# ---------------------------------------------------------------------------
# Regime labeling
# ---------------------------------------------------------------------------

def label_regimes(model: GaussianHMM, scaler: StandardScaler) -> dict:
    """
    Assign human-readable labels to each hidden state based on the
    mean HY OAS z-score for that state.

    States are ranked by their mean hy_oas_zscore_252d:
      lowest  → compression (tight spreads, risk-on)
      middle  → elevated    (wider spreads, uncertain)
      highest → stress      (blow-out spreads, risk-off)

    This is the only defensible labeling approach — don't manually
    assign labels without grounding them in the model's learned means.
    """
    # Recover original-scale means for interpretability
    means_scaled = model.means_                        # shape: (n, n_features)
    means_original = scaler.inverse_transform(means_scaled)
    means_df = pd.DataFrame(means_original, columns=FEATURE_COLS)

    # Rank states by HY OAS z-score mean
    hy_zscore_col = "hy_oas_zscore_252d"
    ranked = means_df[hy_zscore_col].rank().astype(int)

    n = model.n_components
    label_map = {}

    if n == 2:
        rank_to_label = {1: "compression", 2: "stress"}
    elif n == 3:
        rank_to_label = {1: "compression", 2: "elevated", 3: "stress"}
    elif n == 4:
        rank_to_label = {1: "compression", 2: "elevated", 3: "wide", 4: "stress"}
    else:
        rank_to_label = {r: f"regime_{r}" for r in range(1, n + 1)}

    for state_idx, rank in ranked.items():
        label_map[int(state_idx)] = rank_to_label[rank]

    log.info("Regime labels assigned:")
    for state, label in label_map.items():
        hy_z = means_df.loc[state, hy_zscore_col]
        log.info(f"  State {state} → {label} (mean HY OAS z-score: {hy_z:.3f})")

    return label_map


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def save_model(
    model: GaussianHMM,
    scaler: StandardScaler,
    regime_labels: dict,
    bic_scores: dict,
    dates: pd.Series,
    n_components: int,
) -> None:
    """Save model, scaler, and metadata to models/ directory."""
    MODEL_DIR.mkdir(exist_ok=True)

    # Serialize model and scaler together — scorer needs both
    joblib.dump({"model": model, "scaler": scaler}, MODEL_DIR / "hmm_model.pkl")
    log.info(f"Model saved to {MODEL_DIR / 'hmm_model.pkl'}")

    # Metadata for the scorer and dashboard
    metadata = {
        "trained_at":    datetime.utcnow().isoformat(),
        "n_components":  n_components,
        "feature_cols":  FEATURE_COLS,
        "regime_labels": regime_labels,
        "bic_scores":    {str(k): v for k, v in bic_scores.items()},
        "train_start":   str(dates.min()),
        "train_end":     str(dates.max()),
        "n_obs":         len(dates),
        "covariance_type": COVAR,
        "converged":     bool(model.monitor_.converged),
    }

    with open(MODEL_DIR / "hmm_metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)

    log.info(f"Metadata saved to {MODEL_DIR / 'hmm_metadata.json'}")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def run_training() -> None:
    log.info("=== HMM Training started ===")

    df        = load_features()
    X, scaler, dates = preprocess(df)

    optimal_n, bic_scores = select_n_components(X)
    model     = train_final_model(X, optimal_n)

    regime_labels = label_regimes(model, scaler)

    save_model(model, scaler, regime_labels, bic_scores, dates, optimal_n)

    log.info("=== HMM Training complete ===")


if __name__ == "__main__":
    run_training()