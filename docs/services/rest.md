# REST discovery + backfill service

## Responsibilities
- Poll Kalshi REST API for events/markets and upsert them.
- Maintain `active_markets` for open/paused markets.
- Backfill candlesticks on a schedule.
- Update ingest cursors in `ingest_state`.
- Optionally enqueue backfill jobs to the work queue.

## Data dependencies
- Tables: `events`, `markets`, `active_markets`, `market_candles`, `ingest_state`.
- Ingest state keys: `last_discovery_ts`, `last_min_close_ts`.

## Infra dependencies
- Postgres (`DATABASE_URL`).
- Kalshi REST API (`KALSHI_HOST`, `KALSHI_API_KEY_ID`, `KALSHI_PRIVATE_KEY_PEM_PATH`).
- Optional RabbitMQ when `WORK_QUEUE_ENABLE=1`.

## Runtime
- Entrypoint: `python -m src.services.rest_service`
- Legacy: `KALSHI_RUN_MODE=rest python -m src.services.main`
- Optional: `SERVICE_HEALTH_PORT` exposes `/healthz`

## Queue routing
- Use `WORK_QUEUE_NAME_BACKFILL_MARKET` to route backfill jobs.

## Downstream SLA expectations
- WS relies on `active_markets` being refreshed; keep `DISCOVERY_SECONDS`
  within the alert threshold for `last_discovery_ts`.
- Worker relies on REST to enqueue backfill jobs; keep `BACKFILL_SECONDS`
  aligned with your desired queue freshness.
