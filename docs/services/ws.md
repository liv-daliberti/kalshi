# WebSocket ingestion service

## Responsibilities
- Maintain a WebSocket connection to Kalshi.
- Subscribe to tick + lifecycle channels.
- Write ticks and lifecycle events to the database.
- Update WS health timestamps in `ingest_state`.
- Refresh subscriptions from `active_markets`.

## Data dependencies
- Tables: `market_ticks`, `lifecycle_events`, `active_markets`, `ingest_state`.
- Ingest state keys: `last_tick_ts`, `last_ws_tick_ts`.

## Infra dependencies
- Postgres (`DATABASE_URL`).
- Kalshi WS endpoint (`KALSHI_HOST`, `KALSHI_API_KEY_ID`, `KALSHI_PRIVATE_KEY_PEM_PATH`).

## Upstream SLA + backpressure
- Depends on REST to refresh `active_markets` within `DISCOVERY_SECONDS` cadence.
- If `active_markets` is stale, WS continues on the last snapshot; alert on
  `last_discovery_ts` staleness and restart REST before WS.
- WS backpressure uses reconnect circuit breaker (`WS_FAILURE_THRESHOLD`,
  `WS_FAILURE_COOLDOWN_SECONDS`) to avoid tight reconnect loops.

## Runtime
- Entrypoint: `python -m src.services.ws_service`
- Legacy: `KALSHI_RUN_MODE=ws python -m src.services.main`
- Optional: `SERVICE_HEALTH_PORT` exposes `/healthz`
