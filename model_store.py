"""
model_store.py
--------------
Versioned storage for trained model artifacts in MotherDuck.

Why this exists: MotherDuck Flights get a fresh, isolated filesystem per run,
so anything hmm_trainer.py writes to models/ is gone the moment the run ends.
Keeping the artifact in the warehouse means the scorer can read back exactly
what the trainer produced.

It also removes a portability trap. A joblib pickle of a scikit-learn
estimator is only reliably loadable by the same scikit-learn version that
wrote it — a model trained on a laptop and loaded in a Linux runtime is a
version mismatch waiting to happen. Because both sides of that exchange are
now Flight runs pinned by the same requirements.txt, they always agree.

Table:
  ml.model_artifacts
    name         VARCHAR    -- logical artifact name, e.g. 'hmm'
    version      BIGINT     -- monotonically increasing per name
    created_at   TIMESTAMP
    payload      BLOB       -- joblib-serialized bytes
    metadata     VARCHAR    -- JSON, same content as hmm_metadata.json
"""

import json
import logging
from datetime import datetime
from typing import Optional, Tuple

import duckdb

log = logging.getLogger(__name__)

TABLE = "ml.model_artifacts"


def bootstrap(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the artifact table if it doesn't exist."""
    conn.execute("CREATE SCHEMA IF NOT EXISTS ml")
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            name        VARCHAR   NOT NULL,
            version     BIGINT    NOT NULL,
            created_at  TIMESTAMP NOT NULL,
            payload     BLOB      NOT NULL,
            metadata    VARCHAR   NOT NULL
        )
    """)


def save(
    conn: duckdb.DuckDBPyConnection,
    name: str,
    payload: bytes,
    metadata: dict,
) -> int:
    """
    Store a new version of an artifact. Returns the version written.

    Versions are append-only — old ones are never overwritten, so a bad
    retrain can be diagnosed against the model it replaced.
    """
    bootstrap(conn)

    current = conn.execute(
        f"SELECT max(version) FROM {TABLE} WHERE name = ?", [name]
    ).fetchone()[0]
    version = (current or 0) + 1

    conn.execute(
        f"INSERT INTO {TABLE} (name, version, created_at, payload, metadata) "
        f"VALUES (?, ?, ?, ?, ?)",
        [name, version, datetime.utcnow(), payload, json.dumps(metadata)],
    )

    log.info(f"Saved artifact '{name}' version {version} ({len(payload):,} bytes)")
    return version


def load_latest(
    conn: duckdb.DuckDBPyConnection,
    name: str,
) -> Optional[Tuple[bytes, dict, int]]:
    """
    Return (payload, metadata, version) for the newest version of an artifact,
    or None if the artifact has never been saved.
    """
    try:
        row = conn.execute(
            f"SELECT payload, metadata, version FROM {TABLE} "
            f"WHERE name = ? ORDER BY version DESC LIMIT 1",
            [name],
        ).fetchone()
    except duckdb.CatalogException:
        # Table doesn't exist yet — first run, before any training.
        return None

    if row is None:
        return None

    payload, metadata_json, version = row
    return bytes(payload), json.loads(metadata_json), version
