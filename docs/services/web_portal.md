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
- Optional: `gunicorn --factory -w 2 -b 0.0.0.0:8123 src.web_portal.app:create_app`

## Auth and cookies
- `WEB_PORTAL_SECRET_KEY` is required for session signing.
- `WEB_PORTAL_TRUST_PROXY` sets the number of trusted proxy hops for `X-Forwarded-*`.
- Cookie flags can be overridden with `WEB_PORTAL_COOKIE_SECURE`,
  `WEB_PORTAL_COOKIE_SAMESITE`, `WEB_PORTAL_COOKIE_HTTPONLY`,
  `WEB_PORTAL_COOKIE_DOMAIN`, `WEB_PORTAL_COOKIE_PATH`, and
  `WEB_PORTAL_COOKIE_NAME`.

## Static assets
- `WEB_PORTAL_STATIC_CACHE_MAX_AGE_SEC` controls `Cache-Control` for `/static` assets.
- Enable gzip/brotli at your proxy; for example (nginx, brotli module required):
  ```
  gzip on;
  gzip_types text/css application/javascript application/json image/svg+xml;
  brotli on;
  brotli_types text/css application/javascript application/json image/svg+xml;
  ```

## Performance
- Per-request timing logs include total route time and DB time.
- `WEB_PORTAL_DB_SLOW_QUERY_MS` logs portal queries that exceed the threshold (0 disables).
- Pagination query params: `active_page`, `scheduled_page`, `closed_page` (0-based).

## Health thresholds
- `WEB_PORTAL_DISCOVERY_STALE_SECONDS`
- `WEB_PORTAL_BACKFILL_STALE_SECONDS`
- `WEB_PORTAL_WS_STALE_SECONDS`
- `WEB_PORTAL_RAG_STALE_SECONDS`
