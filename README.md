# Kalshi Hourly/Daily Market Ingestor (Princeton HPC, Apptainer)

This repo runs a **Kalshi hourly + daily** market ingestor on Princeton Research Computing clusters (no Docker). It uses:

- **Apptainer** (Singularity) containers
- **PostgreSQL** (run as an Apptainer instance inside the SLURM job)
- **Kalshi official Python SDK** for REST (events, markets, candlesticks)
- **Kalshi WebSockets** for real-time ticker + market lifecycle updates

## What this ingestor stores

- **Events** (question-level metadata) filtered to `strike_period in {hour, day}`
- **Markets** (outcomes) for those events (via `with_nested_markets=true`)
- **Real-time ticks** via WebSocket `ticker` channel (append-only history)
- **Lifecycle updates** via WebSocket `market_lifecycle_v2` channel (settled/determined/etc.)
- **Historic backfill** for markets (configurable by status):
  - discovers events via `min_close_ts`
  - backfills **candlesticks** at allowed minute intervals `1 / 60 / 1440`

### Convenience views

- `event_outcomes_latest`: latest tick per outcome per event
- `event_leader_latest`: highest implied YES mid per event (useful for multi-outcome events)

---

## Folder structure

```text
kalshi_hpc_ingestor/
├── README.md
├── LICENSE
├── .env.example
├── apptainer/
│   └── kalshi_ingestor.def
├── sql/
│   └── schema.sql
├── src/
│   ├── core/
│   ├── db/
│   ├── ingest/
│   │   └── ws/
│   ├── jobs/
│   ├── kalshi/
│   ├── predictions/
│   ├── queue/
│   ├── rag/
│   ├── services/
│   ├── templates/
│   └── web_portal/
└── scripts/
    └── run_ingestor.sbatch
```

## Prerequisites

- Princeton cluster account + SLURM access
- `apptainer` module available on the cluster
- Kalshi API credentials:
  - `KALSHI_API_KEY_ID`
  - `KALSHI_PRIVATE_KEY_PEM_PATH` (RSA private key)

> WebSockets require authenticated headers + RSA-PSS signatures.

---

## Setup

### 1) Copy and edit environment file

```bash
cp .env.example .env
nano .env
```

Fields you must set:

- `KALSHI_API_KEY_ID`
- `KALSHI_PRIVATE_KEY_PEM_PATH`
- `PGDATA_HOST` (persistent path on `$SCRATCH` or `$WORK`)

To enable WebSocket ingestion (ticks + lifecycle), set:

- `KALSHI_WS_ENABLE=1`

Optional WS overrides:

- `KALSHI_WS_URL` (default `wss://api.elections.kalshi.com/trade-api/ws/v2`)
- `KALSHI_WS_CHANNELS` (default `ticker,market_lifecycle_v2`)

Suggested defaults:

- `STRIKE_PERIODS=hour,day`
- `STRIKE_HOUR_MAX_HOURS=2`
- `STRIKE_DAY_MAX_HOURS=36`
- `DISCOVERY_EVENT_STATUSES=open`
- `BACKFILL_EVENT_STATUSES=open,closed,settled`
- `DISCOVERY_SECONDS=1800`
- `WS_SUB_REFRESH_SECONDS=60`
- `BACKFILL_SECONDS=900`
- `CLOSED_CLEANUP_SECONDS=3600`
- `CLOSED_CLEANUP_EVENT_STATUSES=closed,settled`
- `CLOSED_CLEANUP_GRACE_MINUTES=30`
- `KALSHI_WS_ENABLE=1`

### Build the Apptainer image

From the project root:

```bash
module load apptainer
```

Option A: fakeroot build (if enabled)

```bash
apptainer build --fakeroot kalshi_ingestor.sif apptainer/kalshi_ingestor.def
```

Option B: build locally and upload

```bash
# on your laptop/desktop (with Apptainer installed)
apptainer build kalshi_ingestor.sif apptainer/kalshi_ingestor.def
scp kalshi_ingestor.sif <cluster>:/path/to/kalshi_hpc_ingestor/
```

### Run on SLURM

Submit:

```bash
sbatch scripts/run_ingestor.sbatch
```

This job:

- pulls (or reuses) a `postgres:16` image as `postgres_16.sif`
- starts Postgres as an Apptainer instance (`pgdb`)
- runs `kalshi_ingestor.sif` with the repo bind-mounted to `/app`
- runs `python -m src.services.rest_service` + `python -m src.services.ws_service`

Logs:

- `kalshi_hd.<jobid>.out`
- `kalshi_hd.<jobid>.err`

Stop:

- cancel the job (`scancel <jobid>`), which triggers cleanup and stops the Postgres instance

### Running WS and REST separately

If you want WebSocket ingestion isolated from REST backfill/discovery, launch
two processes:

```bash
python -m src.services.rest_service
python -m src.services.ws_service
```

The bundled scripts (`scripts/run_ingestor.sh` and `scripts/run_ingestor.sbatch`)
now start the REST and WS processes separately.

---

## Production env isolation

For production, avoid a shared `.env`. Use per-service env files based on:

- `configs/env/rest.env.example`
- `configs/env/ws.env.example`
- `configs/env/worker.env.example`
- `configs/env/rag.env.example`
- `configs/env/portal.env.example`

Systemd unit templates live in `configs/systemd/` and reference these env files.
Use `configs/env/admin.env.example` for admin-only tasks like role/user setup.

Secrets isolation:
- Store Kalshi API keys only in REST/WS/worker envs.
- Store RAG/LLM credentials only in the RAG env.
- Keep portal env limited to DB + portal settings.

---

## Schema migrations

Run schema initialization with an admin-capable database user:

```bash
python -m src.services.migrator
```

After migrations are applied, keep `DB_INIT_SCHEMA=0` for all services.

---

## Health checks

Per-service health checks:

```bash
python -m src.services.healthcheck --service rest
python -m src.services.healthcheck --service ws
python -m src.services.healthcheck --service worker
python -m src.services.healthcheck --service rag
```

Set `SERVICE_HEALTH_PORT` in a service env to expose `/healthz`.

---

## Work queue (RabbitMQ + Postgres)

For multi-worker scaling, you can enable a DB-backed work queue with RabbitMQ
signaling. The REST process enqueues backfill work into the `work_queue` table,
and any number of workers can consume and run the heavy candlestick pulls.

Enable in `.env`:

```bash
WORK_QUEUE_ENABLE=1
WORK_QUEUE_JOB_TYPES=backfill_market
WORK_QUEUE_NAME_BACKFILL_MARKET=kalshi.backfill
RABBITMQ_URL=amqp://guest:guest@localhost:5672/
WORK_QUEUE_NAME=kalshi.backfill
WORK_QUEUE_PREFETCH=4
WORK_QUEUE_POLL_SECONDS=10
WORK_QUEUE_RETRY_SECONDS=60
WORK_QUEUE_LOCK_TIMEOUT_SECONDS=900
WORK_QUEUE_MAX_ATTEMPTS=5
WORK_QUEUE_CLEANUP_HOURS=24
WORK_QUEUE_PUBLISH=1
```

Run the scheduler (discovery + enqueue):

```bash
python -m src.services.rest_service
```

Run one or more workers (each pulls from RabbitMQ and the DB):

```bash
python -m src.services.worker_service
```

Notes:
- Workers currently process `backfill_market` jobs only; WS ingestion is unchanged.
- When the queue is enabled, the REST process enqueues backfill work instead of
  fetching candles locally.
- If RabbitMQ is unavailable or disabled (`WORK_QUEUE_PUBLISH=0`), jobs remain in
  Postgres and workers fall back to polling.

Queue separation:
- Use `WORK_QUEUE_NAME_<JOB_TYPE>` (e.g., `WORK_QUEUE_NAME_BACKFILL_MARKET`) to
  route jobs into per-type queues.
- Set `WORK_QUEUE_JOB_TYPES=backfill_market` on workers so they only claim
  backfill jobs from the DB.

---

## Agent predictions (RAG)

Optional prediction loop that runs on a schedule, stores per-market forecasts in
`market_predictions`, and can pull context from `rag_documents`.

Enable in `.env`:

```bash
PREDICTION_ENABLE=1
PREDICTION_INTERVAL_HOUR_MINUTES=10
PREDICTION_INTERVAL_DAY_MINUTES=30
PREDICTION_POLL_SECONDS=60
```

Run the prediction loop separately (it does not run inside the REST/WS ingestor):

```bash
python -m src.services.rag_service
```

Or use the standalone script (starts Postgres + the RAG loop only):

```bash
scripts/run_rag.sh
```

Optional configuration:

```bash
PREDICTION_HANDLER=your_module:predict_event
PREDICTION_AGENT_NAME=rag-agent
PREDICTION_MODEL_NAME=your-model
PREDICTION_CONTEXT_DOCS=20
PREDICTION_PROMPT_PATH=/path/to/prompt.txt
```

Handler signature:

```python
def predict_event(event: dict, markets: list[dict], context: dict, prompt: str) -> list[dict]:
    # Return dicts with: market_ticker, yes_prob, optional confidence, rationale, raw
    ...
```

The `context` dict includes `documents` from the `rag_documents` table (where you
can store embeddings or text chunks), plus event + market metadata from the DB.

---

## What the processes do

Service contracts and ownership rules:

- System diagram: `docs/system_diagram.md`
- `docs/contracts.md`
- `docs/operations.md`
- `docs/service_registry.md`
- `sql/roles.sql`

Per-service responsibility and dependency docs:

- `docs/services/rest.md`
- `docs/services/ws.md`
- `docs/services/worker.md`
- `docs/services/rag.md`
- `docs/services/web_portal.md`

### Discovery loop (every `DISCOVERY_SECONDS`)

- calls `get_events` for statuses in `DISCOVERY_EVENT_STATUSES` with `with_nested_markets=true`
- filters events to strike_period in `STRIKE_PERIODS`
- upserts events + markets
- maintains `active_markets` (open/paused markets only)

### WebSocket loop (continuous)

- connects to `wss://api.elections.kalshi.com/trade-api/ws/v2`
- subscribes to channels:
  - `ticker` (stores in `market_ticks`)
  - `market_lifecycle_v2` (stores in `lifecycle_events`)
- periodically diffs `active_markets` and updates the WS subscription using `update_subscription`:
  - `add_markets` / `delete_markets`
- removes markets from `active_markets` when lifecycle says settled/determined/deactivated

### Backfill loop (every `BACKFILL_SECONDS`)

- scans events with statuses in `BACKFILL_EVENT_STATUSES` since last cursor using `min_close_ts`
- filters to hourly/daily
- upserts events + markets
- pulls candlesticks for each market and stores in `market_candles` (paged in `CANDLE_LOOKBACK_HOURS` chunks)
- `period_interval` is minutes and supports only `1`, `60`, `1440`

This makes your dataset robust to downtime: even if WS missed an entire market, you still get the full OHLC trajectory via candlesticks.

### Closed cleanup loop (every `CLOSED_CLEANUP_SECONDS`)

- finds closed markets missing settlement values or candlestick coverage
- fetches only those missing market payloads and candle ranges
- prunes stale `active_markets` rows past the grace window

---

## Querying the database

## Web portal

The portal renders two tables (Active Markets and Closed Markets) straight from Postgres.

Run locally:

```bash
python -m src.services.portal_service
```

Optional environment variables:

- `WEB_PORTAL_HOST` (default `0.0.0.0`)
- `WEB_PORTAL_PORT` (default `8000`)
- `WEB_PORTAL_LIMIT` (default `75`, max `2000`)
- `WEB_PORTAL_LAZY_LOAD` (default `1`; fetch portal tables after initial page load)
- `WEB_PORTAL_PASSWORD` (required to log in)
- `WEB_PORTAL_SECRET_KEY` (required; session signing key)
- `WEB_DB_POOL_ENABLE` (default `1`; set to `0` to disable pooled DB connections)
- `WEB_DB_POOL_MIN` (default `2`; reserved DB connections per web worker)
- `WEB_DB_POOL_MAX` (default `8`; cap DB connections per web worker)
- `WEB_DB_POOL_TIMEOUT` (default `3`; seconds to wait for a pooled connection)
- `WEB_DB_POOL_PREWARM` (default `1`; open reserved connections at startup)
- `WEB_PORTAL_SNAPSHOT_ALLOW_CLOSED` (default `0`; set to `1` to allow snapshots for closed markets/events)
- `WEB_PORTAL_TRUST_PROXY` (default unset; set to `1`/`2` to trust that many proxy hops)
- `WEB_PORTAL_COOKIE_SECURE` (default unset; set to `1` for HTTPS cookies)
- `WEB_PORTAL_COOKIE_SAMESITE` (default unset; set to `Lax`, `Strict`, or `None`)
- `WEB_PORTAL_COOKIE_HTTPONLY` (default unset; set to `1` to force HttpOnly)
- `WEB_PORTAL_COOKIE_DOMAIN` (default unset; set to a parent domain if needed)
- `WEB_PORTAL_COOKIE_PATH` (default unset; set to `/` or a subpath)
- `WEB_PORTAL_COOKIE_NAME` (default unset; override the session cookie name)

The portal only needs `DATABASE_URL` set; it does not require Kalshi API credentials.

Run with gunicorn (recommended for shared access):

```bash
python -m pip install gunicorn
gunicorn --factory -w 2 -b 0.0.0.0:8123 src.web_portal.app:create_app
```

Gunicorn will automatically load `gunicorn.conf.py` from this directory, which
sets a longer timeout and uses thread workers to avoid sync worker timeouts.
Override defaults with environment variables if needed, for example:

```bash
WEB_PORTAL_TIMEOUT=180 WEB_PORTAL_THREADS=8 gunicorn --factory -w 2 -b 0.0.0.0:8123 src.web_portal.app:create_app
```

### Azure App Service deployment

If you want GitHub Actions to configure Azure App Service settings during
deploys, create a service principal and store the JSON in `AZURE_CREDENTIALS`:

```bash
az ad sp create-for-rbac \
  --name "kalshi-data-ingestor-deploy" \
  --role "Contributor" \
  --scopes "/subscriptions/179a1c0b-5228-4f8e-904d-c6bb1072f1eb/resourceGroups/liv" \
  --sdk-auth
```

Save the command output as the GitHub secret `AZURE_CREDENTIALS`, and add
`WEB_PORTAL_SECRET_KEY` as a secret for deployments.

---

Outcomes for hourly/daily events (latest tick per outcome)

```sql
SELECT *
FROM event_outcomes_latest
WHERE strike_period IN ('hour', 'day')
ORDER BY strike_date DESC, event_ticker, implied_yes_mid DESC;
```

Leader per event (highest implied YES mid)

```sql
SELECT *
FROM event_leader_latest
WHERE strike_period IN ('hour', 'day')
ORDER BY strike_date DESC;
```

Historic real-time ticks for a market

```sql
SELECT ts, price_dollars, yes_bid_dollars, yes_ask_dollars, volume, open_interest
FROM market_ticks
WHERE ticker = 'YOUR-MARKET-TICKER'
ORDER BY ts;
```

Candlestick backfill for an ended market

```sql
SELECT end_period_ts, open, high, low, close, volume
FROM market_candles
WHERE market_ticker = 'YOUR-MARKET-TICKER'
  AND period_interval_minutes = 60
ORDER BY end_period_ts;
```

---

## Notes and tuning

### Strike period strings

The filter uses the event field `strike_period`. If you notice other spellings (e.g., hourly/daily), add them to `STRIKE_PERIODS`.

### Tick volume

`market_ticks` is append-only and can grow quickly. Options:

- keep everything (best for research)
- downsample in a materialized view (e.g., minute buckets)
- move old ticks to cheaper storage (partitioning)

### Rate limits and batch sizes

`WS_BATCH_SIZE` controls how many markets you add/remove per update call.

`MAX_ACTIVE_TICKERS` caps subscription size.

`KALSHI_CANDLE_MIN_INTERVAL_SECONDS` adds pacing between candlestick requests to avoid 429s.

### Security

Keep your private key file readable only by you (`chmod 600 key.pem`).

Bind-mount only the directory containing the key into the container.

---

## Troubleshooting

### Postgres won't start

- confirm `PGDATA_HOST` is on a writable filesystem
- ensure permissions are strict enough:

```bash
chmod 700 "$PGDATA_HOST"
```

### WebSocket auth errors

- confirm `KALSHI_API_KEY_ID` and private key path
- confirm private key is RSA and matches the API key
- check system time skew (WS signatures use current timestamp)

### No data / empty active_markets

- discovery filters to strike_period=hour/day only
- confirm Kalshi currently has such events
- temporarily widen `STRIKE_PERIODS` to test

### Missing ticks, candles, or settlement values

- `market_ticks` is only populated when WebSocket ingestion is enabled (`KALSHI_WS_ENABLE=1`) and authenticated; check WS auth errors and verify `active_markets` is non-empty.
- Backfill runs only for statuses in `BACKFILL_EVENT_STATUSES`; include `open` if you want candles for active markets.
- If a market has not settled, `settlement_value` and `settlement_value_dollars` remain null.

---

## License

Your choice (MIT/Apache/etc.). This repo is intended as a minimal ingest pipeline; please review Kalshi's API terms for allowed usage and redistribution.
