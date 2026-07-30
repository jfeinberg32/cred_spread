# Deployment — Oracle Cloud VM + Prefect Cloud

Prefect Cloud holds the schedule and run history; an always-on worker on the
Oracle VM does the actual execution. Nothing inbound is ever required — the
worker polls Prefect Cloud outbound over HTTPS, so the VM needs no ingress
rules and no public service.

```
Prefect Cloud (free)            Oracle VM (Always Free)         MotherDuck (free)
  schedule + run log   <──poll──   prefect worker  ──────────────>  cred_spread
                                     └─ flows.py
                                          ingestion.py  ──> raw.fred_series
                                          dbt build     ──> main_marts.*
                                          regime_scorer ──> ml.regime_predictions
```

## Free-tier constraints that actually bind

| | Limit | Effect here |
|---|---|---|
| Prefect Cloud Hobby | 1 workspace, 2 users, **5 deployments** | We use 2. Fine. |
| Prefect Cloud | Control plane only since Mar 2026 — bring your own compute | Exactly why the VM exists. |
| Oracle Always Free A1 | **2 OCPU / 12 GB** (halved from 4/24 in 2026) | Ample. Retraining is the only heavy step: 25–50 HMM restarts over ~500 rows. |
| Oracle Always Free | **Idle instances get reclaimed** | See below — this one will bite you. |
| MotherDuck free | 10 GB storage | This dataset is megabytes. |

### The idle-reclaim problem

Oracle reclaims Always Free compute that looks idle over a 7-day window. A VM
whose only job is a once-a-weekday flow run is a plausible candidate. Two
mitigations, in order of preference:

1. **Upgrade the account to Pay As You Go.** Always Free resources stay free
   and become exempt from idle reclamation. You add a card and are not
   charged as long as you stay inside the Always Free shapes. This is the
   real fix.
2. Stay on Always Free and accept the risk. The Prefect worker polls
   continuously, which keeps a trickle of network and CPU activity, but that
   is not a guarantee.

Either way: **treat the VM as disposable.** Everything that matters lives in
MotherDuck or git. The one exception is `models/hmm_model.pkl`, which is
committed to the repo — but see the note on pickle portability below.

## Provisioning

Shape: **VM.Standard.A1.Flex**, 2 OCPU / 12 GB, Ubuntu 22.04 or 24.04 (ARM).
A1 capacity in a given region is often exhausted; retry, or fall back to
`VM.Standard.E2.1.Micro` (AMD, 1 GB RAM). The micro shape will run the daily
flow but is tight for retraining — run `hmm-retrain` locally if you land there.

```bash
sudo apt update && sudo apt install -y python3.12 python3.12-venv git

sudo mkdir -p /opt/cred_spread && sudo chown $USER:$USER /opt/cred_spread
git clone https://github.com/<you>/cred_spread.git /opt/cred_spread
cd /opt/cred_spread

python3.12 -m venv .venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -r requirements.txt
```

## Secrets

Both `ingestion.py` and `regime_scorer.py` already call `load_dotenv()`, so a
`.env` file is the path of least resistance:

```bash
cat > /opt/cred_spread/.env <<'EOF'
FRED_API_KEY=...
MOTHERDUCK_TOKEN=...
MOTHERDUCK_DB=cred_spread
EOF
chmod 600 /opt/cred_spread/.env
```

`.env` is already gitignored. If you would rather not keep plaintext creds on
the VM, create `Secret` blocks in Prefect Cloud and set the env vars in the
work pool's job template instead — `load_dotenv()` is a no-op when the vars
are already in the environment, so no code changes are needed.

## Regenerate the model on the VM

`models/hmm_model.pkl` was trained on macOS. joblib pickles of scikit-learn
estimators are not guaranteed to load cleanly across scikit-learn versions —
you get an `InconsistentVersionWarning` at best and a hard failure at worst.
Rebuild it once with the VM's installed versions before trusting the schedule:

```bash
cd /opt/cred_spread
.venv/bin/python flows.py retrain
```

This refits the HMM and rebuilds `ml.regime_predictions` from scratch, so the
whole table comes from one model. Confirm the run succeeded, then commit the
regenerated `models/` artifacts if you want them tracked.

## Connect to Prefect Cloud

```bash
cd /opt/cred_spread
.venv/bin/prefect cloud login          # opens a browser-auth flow; paste an API key if headless
.venv/bin/prefect work-pool create vm-process-pool --type process
.venv/bin/prefect deploy --all
```

`prefect deploy --all` reads `prefect.yaml` and registers both deployments.
The `pull` step points at `/opt/cred_spread`; change it there if you cloned
somewhere else.

## Run the worker as a service

The worker must be running for scheduled runs to execute — if it is down, runs
pile up as `Late` rather than failing.

```ini
# /etc/systemd/system/prefect-worker.service
[Unit]
Description=Prefect worker for cred_spread
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/opt/cred_spread
EnvironmentFile=/opt/cred_spread/.env
ExecStart=/opt/cred_spread/.venv/bin/prefect worker start --pool vm-process-pool
Restart=always
RestartSec=15
# Guard against a runaway retrain on a 12 GB box
MemoryMax=8G

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now prefect-worker
sudo systemctl status prefect-worker
journalctl -u prefect-worker -f
```

`prefect cloud login` writes its API key to `~/.prefect/profiles.toml` for the
user that ran it — that must be the same `User=` in the unit file, or the
worker starts unauthenticated.

## Verify

```bash
# Force a run without waiting for 09:00
.venv/bin/prefect deployment run 'credit-spread-regime-pipeline/daily-regime-refresh'
```

Then check the run in the Prefect Cloud UI, and:

```sql
SELECT * FROM raw.ingestion_log ORDER BY run_at DESC LIMIT 5;
SELECT date, regime_label, prob_stress FROM ml.regime_predictions ORDER BY date DESC LIMIT 5;
```

## Operational notes

- **Weekend/holiday runs are no-ops.** FRED publishes nothing, `filter_new_rows`
  inserts 0 rows, and the scorer logs "All rows already scored". The flow still
  succeeds. That is the intended behavior, not a failure to alert on.
- **Failure notification** is worth wiring up in Prefect Cloud: Automations →
  on `Flow run` entering `Failed` → notify. Otherwise a silently broken
  pipeline just means stale predictions no one notices.
- **`dbt deps` runs only when `dbt/dbt_packages/` is missing.** Delete that
  directory to force a package refresh after bumping `packages.yml`.
- **Retraining is not on the daily path**, by design — see the comments in
  `prefect.yaml`. The quarterly schedule is registered but `active: false`;
  flip it once you're comfortable with the label-stability tradeoff.
