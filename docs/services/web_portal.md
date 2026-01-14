# Web portal service

## Responsibilities
- Serve UI for events/markets and health metrics.
- Display queue stats and ingest timestamps.
- Optional snapshot polling for live market views.
- Optional backfill enqueue via `WEB_PORTAL_BACKFILL_MODE=queue` (disabled by default;
  requires `INSERT` on `work_queue`).

## Data dependencies
- Tables: `events`, `markets`, `market_ticks`, `market_candles`,
  `active_markets`, `work_queue`, `ingest_state`.

## Infra dependencies
- Postgres (`DATABASE_URL`).
- Optional Kalshi API credentials for client health checks.

## Runtime
- Entrypoint: `python -m src.services.portal_service`
- Optional: `gunicorn -w 2 -b 0.0.0.0:8123 src.web_portal:app`

## Health thresholds
- `WEB_PORTAL_DISCOVERY_STALE_SECONDS`
- `WEB_PORTAL_BACKFILL_STALE_SECONDS`
- `WEB_PORTAL_WS_STALE_SECONDS`
- `WEB_PORTAL_RAG_STALE_SECONDS`
