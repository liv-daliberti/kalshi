# Queue worker service

## Responsibilities
- Consume work items from `work_queue` (DB) and optional RabbitMQ notifications.
- Execute per-market backfill jobs.
- Mark jobs running/done/failed and handle retries.

## Data dependencies
- Tables: `work_queue`, `market_candles`, `ingest_state`.
- Ingest state keys: `last_min_close_ts` (via backfill updates), `last_worker_ts`.

## Infra dependencies
- Postgres (`DATABASE_URL`).
- Kalshi REST API (`KALSHI_HOST`, `KALSHI_API_KEY_ID`, `KALSHI_PRIVATE_KEY_PEM_PATH`).
- Optional RabbitMQ (`RABBITMQ_URL`) when `WORK_QUEUE_ENABLE=1`.

## Runtime
- Entrypoint: `python -m src.services.worker_service`
- Legacy: `KALSHI_RUN_MODE=worker python -m src.services.main`
- Optional: `SERVICE_HEALTH_PORT` exposes `/healthz`

## Queue routing
- Set `WORK_QUEUE_JOB_TYPES` to the job types this worker should claim.
- Set `WORK_QUEUE_NAME_BACKFILL_MARKET` (or `WORK_QUEUE_NAME`) to match the
  RabbitMQ queue it listens on.

## Upstream SLA + backpressure
- Depends on REST to enqueue backfill jobs (`WORK_QUEUE_ENABLE=1` + backfill loop).
- Expect new jobs at least every `BACKFILL_SECONDS` when markets need updates.
- Backpressure controls: keep `WORK_QUEUE_PREFETCH=1`, monitor
  `metric=worker.queue` for `oldest_pending_s`, and add more workers before
  increasing per-worker concurrency.
