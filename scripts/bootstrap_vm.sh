#!/usr/bin/env bash
#
# bootstrap_vm.sh — one-shot setup of the cred_spread pipeline on a fresh VM.
#
#   sudo mkdir -p /opt/cred_spread && sudo chown "$USER" /opt/cred_spread
#   curl -sL https://github.com/jfeinberg32/cred_spread/archive/refs/heads/main.tar.gz \
#     | tar xz --strip-components=1 -C /opt/cred_spread
#   cd /opt/cred_spread && bash scripts/bootstrap_vm.sh
#
# The tarball fetch is deliberate — it needs only curl, whereas `git clone`
# needs git, which on a minimal Oracle Linux image means invoking dnf. On a
# 498 MB instance dnf loads enough repo metadata to render the box unusable
# for 15+ minutes, so this script avoids it entirely and uses uv instead:
# uv ships a static binary, fetches its own standalone CPython, and resolved
# and installed this project's full dependency tree in 50 seconds where pip
# would have taken 5-15 minutes and risked an OOM.
#
# Idempotent: safe to re-run after fixing a failure. Prompts for the three
# secrets it needs (FRED, MotherDuck, Prefect) rather than taking them as
# arguments, so they stay out of your shell history.

set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV="$REPO_DIR/.venv"
SERVICE_USER="${SUDO_USER:-$USER}"
WORK_POOL="vm-process-pool"

cd "$REPO_DIR"

say()  { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33m    %s\033[0m\n' "$*"; }
die()  { printf '\n\033[1;31mERROR: %s\033[0m\n' "$*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# 0. Platform + memory
# ---------------------------------------------------------------------------

say "Platform: $(. /etc/os-release && echo "$PRETTY_NAME") ($(uname -m))"

MEM_MB=$(free -m | awk '/^Mem:/{print $2}')
SWAP_MB=$(free -m | awk '/^Swap:/{print $2}')
echo "    RAM ${MEM_MB}MB, swap ${SWAP_MB}MB"

# Measured on VM.Standard.E2.1.Micro: a full import of prefect + pandas +
# numpy + duckdb + sklearn + hmmlearn peaks at ~184 MB RSS, so 498 MB of RAM
# is genuinely workable. What is not workable is having no swap headroom —
# the worker and a flow subprocess coexist, and dbt adds a third process.
# Require ~2 GB combined so peaks swap rather than getting OOM-killed.
if [ $((MEM_MB + SWAP_MB)) -lt 2000 ]; then
    die "Only $((MEM_MB + SWAP_MB))MB RAM+swap. Add swap first:
    sudo dd if=/dev/zero of=/swapfile-4g bs=1M count=4096
    sudo chmod 600 /swapfile-4g && sudo mkswap /swapfile-4g && sudo swapon /swapfile-4g
    echo '/swapfile-4g none swap sw 0 0' | sudo tee -a /etc/fstab"
fi

# ---------------------------------------------------------------------------
# 1. uv
# ---------------------------------------------------------------------------

say "Ensuring uv is available"

export PATH="$HOME/.local/bin:$PATH"

if command -v uv >/dev/null 2>&1; then
    echo "    already installed ($(uv --version))"
else
    command -v curl >/dev/null 2>&1 || die "curl is required to install uv"
    curl -LsSf https://astral.sh/uv/install.sh | sh >/dev/null 2>&1
    hash -r
    command -v uv >/dev/null 2>&1 || die "uv install failed"
    echo "    installed $(uv --version)"
fi

# ---------------------------------------------------------------------------
# 2. Python + dependencies
# ---------------------------------------------------------------------------
# hmmlearn publishes wheels for cp310-cp313 only; outside that range pip falls
# back to a source build needing a compiler and a lot of RAM. Pinning 3.12
# keeps every dependency on a prebuilt wheel.

say "Installing Python 3.12 and the virtualenv"

uv python install 3.12
uv venv --python 3.12 "$VENV"

say "Resolving dependencies"
uv pip install --python "$VENV/bin/python" -r requirements.txt
echo "    done"

# ---------------------------------------------------------------------------
# 3. Secrets
# ---------------------------------------------------------------------------

say "Configuring credentials"

if [ -f "$REPO_DIR/.env" ]; then
    echo "    .env already present — leaving it alone"
else
    echo "    Creating .env (input is hidden, nothing is echoed)"
    read -rsp "    FRED_API_KEY: "     FRED_KEY; echo
    read -rsp "    MOTHERDUCK_TOKEN: " MD_TOKEN; echo
    [ -n "$FRED_KEY" ] || die "FRED_API_KEY cannot be empty"
    [ -n "$MD_TOKEN" ]  || die "MOTHERDUCK_TOKEN cannot be empty"

    umask 077
    cat > "$REPO_DIR/.env" <<EOF
FRED_API_KEY=$FRED_KEY
MOTHERDUCK_TOKEN=$MD_TOKEN
MOTHERDUCK_DB=cred_spread
EOF
    chmod 600 "$REPO_DIR/.env"
    unset FRED_KEY MD_TOKEN
    echo "    .env written (mode 600)"
fi

# ---------------------------------------------------------------------------
# 4. Smoke test before involving Prefect
# ---------------------------------------------------------------------------
# Fail here, on a plain Python traceback, rather than inside a flow run where
# the error arrives wrapped in orchestration noise.

say "Smoke test: MotherDuck connectivity"
"$VENV/bin/python" - <<'PYEOF'
import os, sys
from dotenv import load_dotenv
import duckdb

load_dotenv()
tok = os.getenv("MOTHERDUCK_TOKEN")
db  = os.getenv("MOTHERDUCK_DB", "cred_spread")
if not tok:
    sys.exit("MOTHERDUCK_TOKEN missing from .env")

con = duckdb.connect(f"md:{db}?motherduck_token={tok}")
con.execute("SELECT 1").fetchone()
print("    authenticated against MotherDuck")

# Report existing state if present — a brand new database is fine, the
# ingestion step below will create these.
for tbl in ("raw.fred_series", "main_marts.mart_regime__features"):
    try:
        n = con.execute(f"SELECT count(*) FROM {tbl}").fetchone()[0]
        print(f"    {tbl}: {n:,} rows")
    except Exception:
        print(f"    {tbl}: not created yet")
con.close()
PYEOF

# ---------------------------------------------------------------------------
# 5. Prefect Cloud
# ---------------------------------------------------------------------------

say "Connecting to Prefect Cloud"

if "$VENV/bin/prefect" config view 2>/dev/null | grep -q "PREFECT_API_URL.*cloud"; then
    echo "    already authenticated"
else
    echo "    Create an API key: app.prefect.cloud -> your avatar -> API keys"
    read -rsp "    PREFECT_API_KEY: " PF_KEY; echo
    [ -n "$PF_KEY" ] || die "PREFECT_API_KEY cannot be empty"
    "$VENV/bin/prefect" cloud login -k "$PF_KEY"
    unset PF_KEY
fi

say "Creating work pool '$WORK_POOL'"
if "$VENV/bin/prefect" work-pool inspect "$WORK_POOL" >/dev/null 2>&1; then
    echo "    already exists"
else
    "$VENV/bin/prefect" work-pool create "$WORK_POOL" --type process
fi

# ---------------------------------------------------------------------------
# 6. Prime the warehouse, then refit the model
# ---------------------------------------------------------------------------
# Order matters. hmm_model.pkl was pickled on macOS and joblib/scikit-learn
# pickles are not guaranteed to load across versions, so it has to be refit
# here — but training reads mart_regime__features, which means ingestion and
# dbt must run first. Running the daily flow instead would try to *score* with
# the unportable pickle before we ever get to replace it.

say "Step 1/2: ingestion + dbt build"
"$VENV/bin/python" ingestion.py
"$VENV/bin/dbt" deps  --project-dir dbt --profiles-dir dbt
"$VENV/bin/dbt" build --project-dir dbt --profiles-dir dbt

say "Step 2/2: refitting the HMM with this machine's library versions"
warn "This rebuilds ml.regime_predictions from scratch. Ctrl-C within 10s to abort."
sleep 10
"$VENV/bin/python" flows.py retrain

# ---------------------------------------------------------------------------
# 7. Deployments
# ---------------------------------------------------------------------------

say "Registering deployments"

# prefect.yaml's pull step hardcodes /opt/cred_spread; correct it if the repo
# was cloned somewhere else so the worker doesn't chase a missing directory.
if [ "$REPO_DIR" != "/opt/cred_spread" ]; then
    warn "Repo is at $REPO_DIR, not /opt/cred_spread — patching prefect.yaml"
    sed -i "s#directory: /opt/cred_spread#directory: $REPO_DIR#" prefect.yaml
fi

"$VENV/bin/prefect" deploy --all

# ---------------------------------------------------------------------------
# 8. Worker service
# ---------------------------------------------------------------------------

say "Installing the prefect-worker systemd unit"

sudo tee /etc/systemd/system/prefect-worker.service >/dev/null <<EOF
[Unit]
Description=Prefect worker for cred_spread
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$SERVICE_USER
WorkingDirectory=$REPO_DIR
EnvironmentFile=$REPO_DIR/.env
ExecStart=$VENV/bin/prefect worker start --pool $WORK_POOL
Restart=always
RestartSec=15
# No MemoryMax: on a small instance the kernel OOM killer plus swap is the
# backstop, and a hard cap here just guarantees the worker dies during a
# flow run instead of swapping through it.
OOMPolicy=continue

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now prefect-worker
sleep 5

if systemctl is-active --quiet prefect-worker; then
    say "Worker is running"
else
    die "Worker failed to start — check: journalctl -u prefect-worker -n 50"
fi

# ---------------------------------------------------------------------------

cat <<EOF

$(say "Bootstrap complete")

  Worker:   systemctl status prefect-worker
  Logs:     journalctl -u prefect-worker -f
  Schedule: 09:00 ET, Mon-Fri

  Trigger a run now without waiting for the schedule:
    $VENV/bin/prefect deployment run 'credit-spread-regime-pipeline/daily-regime-refresh'

EOF
