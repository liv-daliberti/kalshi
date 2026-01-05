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
- **Historic backfill** for markets that ended while you were offline:
  - discovers newly closed/settled events via `min_close_ts`
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
│   ├── settings.py
│   ├── db.py
│   ├── kalshi_sdk.py
│   ├── discovery.py
│   ├── ws_ingest.py
│   ├── backfill.py
│   └── main.py
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

Suggested defaults:

- `STRIKE_PERIODS=hour,day`
- `DISCOVERY_SECONDS=1800`
- `WS_SUB_REFRESH_SECONDS=60`
- `BACKFILL_SECONDS=900`

### Build the Apptainer image

From the project root:

```bash
module load apptainer
```

Option A: fakeroot build (if enabled)

```bash
apptainer build --fakeroot kalshi_ingestor.sif apptainer/kalshi_ingestor.def
```

Option B: remote build

```bash
apptainer build --remote kalshi_ingestor.sif apptainer/kalshi_ingestor.def
```

Option C: build locally and upload

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
- runs `python -m src.main` (discovery + backfill + websockets concurrently)

Logs:

- `kalshi_hd.<jobid>.out`
- `kalshi_hd.<jobid>.err`

Stop:

- cancel the job (`scancel <jobid>`), which triggers cleanup and stops the Postgres instance

---

## What the processes do

### Discovery loop (every `DISCOVERY_SECONDS`)

- calls `get_events(status="open", with_nested_markets=true)`
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

- scans closed + settled events since last cursor using `min_close_ts`
- filters to hourly/daily
- upserts events + markets
- pulls candlesticks for each market and stores in `market_candles`
- `period_interval` is minutes and supports only `1`, `60`, `1440`

This makes your dataset robust to downtime: even if WS missed an entire market, you still get the full OHLC trajectory via candlesticks.

---

## Querying the database

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

---

## License

Your choice (MIT/Apache/etc.). This repo is intended as a minimal ingest pipeline; please review Kalshi's API terms for allowed usage and redistribution.
