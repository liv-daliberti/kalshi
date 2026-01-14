# Service Contracts

This document defines service boundaries, shared data contracts, and ownership
rules for the Kalshi ingestor toolkit. Each service should treat shared tables
as read-only unless it is the designated owner.

## Services

- REST discovery/backfill (`python -m src.services.rest_service`)
- WebSocket ingestion (`python -m src.services.ws_service`)
- Queue worker (`python -m src.services.worker_service`)
- RAG predictions (`python -m src.services.rag_service`)
- Web portal (`python -m src.services.portal_service`)

Legacy: `python -m src.services.main` with `KALSHI_RUN_MODE` is still supported.

## Data ownership (tables)

| Table | Owner | Notes |
| --- | --- | --- |
| `events` | REST | REST upserts event metadata from Kalshi. |
| `markets` | REST | REST upserts market metadata from Kalshi. |
| `active_markets` | REST | REST maintains open/paused market set. |
| `market_candles` | REST + Worker | REST backfills directly; worker backfills via queue jobs. |
| `market_ticks` | WS | WS writes live ticks. |
| `lifecycle_events` | WS | WS writes lifecycle events. |
| `work_queue` | Worker | REST enqueues jobs; worker transitions status. |
| `prediction_runs` | RAG | RAG writes run metadata. |
| `market_predictions` | RAG | RAG writes predictions. |
| `rag_documents` | RAG | RAG upserts embeddings/chunks. |
| `ingest_state` | Shared | Key-level ownership below. |
| `schema_version` | Migrator | Written by schema migrator; read-only elsewhere. |

## ingest_state keys (ownership)

| Key | Owner | Writer(s) | Reader(s) |
| --- | --- | --- | --- |
| `last_discovery_ts` | REST | REST | Portal, others |
| `last_min_close_ts` | REST | REST, Worker | Portal, others |
| `last_worker_ts` | Worker | Worker | Portal, others |
| `last_tick_ts` | WS | WS | Portal, others |
| `last_ws_tick_ts` | WS | WS | Portal, others |
| `last_prediction_ts` | RAG | RAG | Portal, others |

Rule: only the owning service updates a key. If multiple writers are unavoidable
(e.g., worker backfill updates `last_min_close_ts`), they must follow the same
semantics and timestamp format.

## Queue payload contract

`job_type=backfill_market`

Payload fields:
- `series_ticker` (string, required)
- `strike_period` (string, required, e.g., `hour`/`day`)
- `market` (object, required; must include `ticker`)
- `force_full` (boolean, optional)

Behavior:
- REST enqueues jobs.
- Worker claims and processes jobs, then updates job status.
- RabbitMQ routing can be split per job type via
  `WORK_QUEUE_NAME_<JOB_TYPE>` (e.g., `WORK_QUEUE_NAME_BACKFILL_MARKET`).

## Environment configuration (per service)

Rule: keep secrets scoped to the service that needs them. Do not share RAG
credentials with REST/WS/worker, and avoid putting Kalshi keys into portal envs.

### REST
- Required: `DATABASE_URL`, `KALSHI_API_KEY_ID`, `KALSHI_PRIVATE_KEY_PEM_PATH`
- Optional: `KALSHI_HOST`, `DISCOVERY_SECONDS`, `BACKFILL_SECONDS`,
  `STRIKE_PERIODS`, `DISCOVERY_EVENT_STATUSES`, `BACKFILL_EVENT_STATUSES`,
  `CANDLE_*`, `WORK_QUEUE_*`

### WS
- Required: `DATABASE_URL`, `KALSHI_API_KEY_ID`, `KALSHI_PRIVATE_KEY_PEM_PATH`
- Optional: `KALSHI_HOST`, `WS_SUB_REFRESH_SECONDS`, `WS_SHARD_*`

### Worker
- Required: `DATABASE_URL`, `KALSHI_API_KEY_ID`, `KALSHI_PRIVATE_KEY_PEM_PATH`
- Optional: `WORK_QUEUE_*`, `RABBITMQ_URL`, `CANDLE_*`

### RAG
- Required: `DATABASE_URL`, `PREDICTION_ENABLE=1`
- Optional: `PREDICTION_*`, `RAG_*`, Azure/OpenAI credentials if using external
  handlers

### Web portal
- Required: `DATABASE_URL`
- Optional: `WEB_PORTAL_*`, `WEB_PORTAL_PASSWORD`, Kalshi credentials for
  health checks
- Optional: `WEB_PORTAL_BACKFILL_MODE=disabled|queue` (default disabled). Queue
  mode enqueues `work_queue` jobs and requires DB permissions to insert
  (portal role grants `INSERT` on `work_queue` only).

## Guardrails

Code-level enforcement is available via:

- `SERVICE_GUARDRAILS=1` to enable ownership checks.
- `SERVICE_ROLE` to override the inferred role (defaults to `KALSHI_RUN_MODE`
  when set to `rest`, `ws`, `worker`, or `rag`).

Guardrails enforce:
- `ingest_state` key ownership.
- `work_queue` operation ownership.
- Service entrypoints (REST/WS/worker/RAG) executing under the expected role.

## Database roles (optional enforcement)

The SQL grants in `sql/roles.sql` define per-service roles. Apply them with an
admin user, then set `DATABASE_URL` for each service to a user that is a member
of the corresponding role.

When using restricted roles, run migrations separately and set
`DB_INIT_SCHEMA=0` for services after the schema is applied.

## Schema migrations

The migrator job (`python -m src.services.migrator`) owns schema initialization and must
run with an admin-capable `DATABASE_URL`. All other services should keep
`DB_INIT_SCHEMA=0`.

## Cross-service coupling to remove

- REST should not start prediction loops in-process; RAG runs independently.
- WS should only depend on `active_markets` (read-only) and its own WS state.
- Worker should avoid updating REST-owned cursors unless explicitly required.
- Portal should be read-only for shared tables.

## Backward compatibility

- Existing schema is unchanged; the contract enforces write ownership only.
- The queue payload contract must remain stable for existing workers.
