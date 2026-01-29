# Operations Runbook

This doc describes executable entrypoints, deployment options, health signals,
and a migration plan for splitting the toolkit into independent services.

## Service entrypoints

| Service | Entrypoint | Notes |
| --- | --- | --- |
| REST discovery/backfill | `python -m src.services.rest_service` | REST-only scheduler. |
| WebSocket ingestion | `python -m src.services.ws_service` | WS-only ingestion. |
| Queue worker | `python -m src.services.worker_service` | Processes `work_queue` jobs. |
| RAG predictions | `python -m src.services.rag_service` | Requires `PREDICTION_ENABLE=1`. |
| Web portal | `python -m src.services.portal_service` | Use gunicorn in production. |
| Migrator (schema) | `python -m src.services.migrator` | One-shot schema init. |

Bundled scripts:
- `scripts/run_ingestor.sh`: starts REST + WS (+ workers).
- `scripts/run_rag.sh`: starts RAG loop with Postgres bootstrap.

Service-specific env examples (copy and fill in secrets):
- `configs/env/rest.env.example`
- `configs/env/ws.env.example`
- `configs/env/worker.env.example`
- `configs/env/rag.env.example`
- `configs/env/portal.env.example`
- `configs/env/admin.env.example` (admin-only, for roles/users)

Systemd unit templates:
- `configs/systemd/kalshi-rest.service`
- `configs/systemd/kalshi-ws.service`
- `configs/systemd/kalshi-worker.service`
- `configs/systemd/kalshi-rag.service`
- `configs/systemd/kalshi-portal.service`
- `configs/systemd/kalshi-migrator.service`

Systemd resource limits:
- Each unit sets `CPUQuota`, `MemoryMax`, `IOWeight`, `TasksMax`, `LimitNOFILE`.
- Tune values per environment before production rollout.

Health checks:
- CLI: `python -m src.services.healthcheck --service rest` (exit code 0/1).
- Optional endpoint: set `SERVICE_HEALTH_PORT=8xxx` to expose `/healthz` in each service.

Queue routing:
- Split RabbitMQ queues by job type using `WORK_QUEUE_NAME_<JOB_TYPE>`.
- Set `WORK_QUEUE_JOB_TYPES` on workers to claim only the intended job types.
- Keep `WORK_QUEUE_NAME` aligned with the job-specific queue name for that worker.
- Worker queue metrics: set `WORKER_QUEUE_METRICS_SECONDS` (default `60`) to log lag stats.

## Deployment examples

### systemd (one unit per service)

Example: REST service (`/etc/systemd/system/kalshi-rest.service`)

```
[Unit]
Description=Kalshi REST discovery/backfill
After=network.target

[Service]
Type=simple
WorkingDirectory=/opt/kalshi_ingestor
EnvironmentFile=/opt/kalshi_ingestor/.env
Environment=SERVICE_GUARDRAILS=1
Environment=DB_INIT_SCHEMA=0
ExecStart=/opt/kalshi_ingestor/.venv/bin/python -m src.services.rest_service
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

Repeat with:
- `python -m src.services.ws_service` for WS
- `python -m src.services.worker_service` for worker
- `python -m src.services.rag_service` for RAG
- `python -m src.services.portal_service` (or gunicorn) for portal

Example: migrator job (`/etc/systemd/system/kalshi-migrator.service`)

```
[Unit]
Description=Kalshi schema migrator
After=network.target

[Service]
Type=oneshot
WorkingDirectory=/opt/kalshi_ingestor
EnvironmentFile=/opt/kalshi_ingestor/.env
ExecStart=/opt/kalshi_ingestor/.venv/bin/python -m src.services.migrator
```

### Docker (per-service container)

```
docker run --rm \
  --env-file .env \
  -e SERVICE_GUARDRAILS=1 \
  kalshi_ingestor:latest \
  python -m src.services.rest_service
```

### Apptainer

```
apptainer exec \
  --bind "$PWD":/app \
  --pwd /app \
  --env SERVICE_GUARDRAILS=1 \
  --env DATABASE_URL="$DATABASE_URL" \
  kalshi_ingestor.sif \
  python -m src.services.rag_service

You can also use `apptainer run` with a command:

```
KALSHI_SERVICE=rest apptainer run kalshi_ingestor.sif
apptainer run kalshi_ingestor.sif python -m src.services.ws_service
```
```

## Health and observability

Primary health signals:
- `ingest_state` keys: `last_discovery_ts`, `last_min_close_ts`, `last_worker_ts`,
  `last_tick_ts`, `last_ws_tick_ts`, `last_prediction_ts`.
- Web portal health page: `/health` (renders high-level status cards).
- Queue stats: `work_queue` counts (pending/running/failed).

Pinned staleness thresholds (seconds):
- Discovery: `WEB_PORTAL_DISCOVERY_STALE_SECONDS` (default `max(2*DISCOVERY_SECONDS, 600)`).
- Backfill: `WEB_PORTAL_BACKFILL_STALE_SECONDS` (default `max(2*BACKFILL_SECONDS, 600)`).
- WebSocket: `WEB_PORTAL_WS_STALE_SECONDS` (default `120`).
- RAG: `WEB_PORTAL_RAG_STALE_SECONDS` (default `300`).
- Worker: `HEALTH_WORKER_STALE_SECONDS` (default `max(2*WORK_QUEUE_POLL_SECONDS, 60)`).

Recommended logging/metrics:
- Enable `LOG_LEVEL=INFO` (or `DEBUG` for troubleshooting).
- Use `LOG_HTTP_LEVEL=DEBUG` to surface urllib3/requests logs (default is quieter when `LOG_LEVEL=DEBUG`).
- Set `LOG_DIR=/var/log/kalshi_ingestor` (or `LOG_FILE=/path/to/service.log`) for per-service files.
- Use `LOG_STDOUT=1` to keep journald/stdout in addition to file logging.
- Alert if `last_ws_tick_ts` or `last_prediction_ts` exceeds stale thresholds.
- Track per-service error rates and queue lag (pending/running duration).
- Log lines include a service tag derived from `SERVICE_ROLE`/`KALSHI_RUN_MODE`.
- Metrics log lines include: `metric=rest.discovery`, `metric=rest.backfill`,
  `metric=rest.closed_cleanup`, `metric=rag.predictions`, `metric=ws.reconnect`,
  `metric=worker.queue`.

Circuit breakers/backpressure:
- REST loops: `REST_FAILURE_THRESHOLD` + `REST_CIRCUIT_BREAKER_SECONDS`.
- RAG loop: `RAG_FAILURE_THRESHOLD` + `RAG_CIRCUIT_BREAKER_SECONDS`.
- WebSocket reconnects: `WS_FAILURE_THRESHOLD` + `WS_FAILURE_COOLDOWN_SECONDS`.

Concurrency pins:
- Worker: `WORK_QUEUE_PREFETCH=1` keeps one in-flight job per worker.
- WS: `WS_SHARD_COUNT=1` + `WS_SHARD_ID=0` pins the shard count.

## Migration steps (clean rollout)

1) Run the migrator with an admin user: `python -m src.services.migrator`.
2) Apply `sql/roles.sql` to create per-service roles and grants (or run
   `ENV_FILE=configs/env/admin.env python scripts/apply_roles.py`).
3) Create DB users for each service and assign roles (done by `apply_roles.py`).
4) Update each service `DATABASE_URL` to use its role-specific user.
5) Deploy services independently (REST, WS, worker, RAG, portal).
6) Enable guardrails: `SERVICE_GUARDRAILS=1`.
7) Validate health signals and data writes (see below).

## Schema patches

- Apply the portal rollup advisory lock hotfix on an existing DB (psycopg):
  ```
  python - <<'PY'
  import os
  import psycopg

  sql_path = "sql/patch_portal_rollup_lock.sql"
  db_url = os.environ["DATABASE_URL"]
  with open(sql_path, "r", encoding="utf-8") as f:
    sql = f.read()
  with psycopg.connect(db_url) as conn:
    with conn.cursor() as cur:
      cur.execute(sql)
    conn.commit()
  print("Applied portal rollup lock patch.")
  PY
  ```

## Testing and validation

Smoke checks:
- REST: new rows in `events/markets`, `last_discovery_ts` updated.
- WS: ticks flowing into `market_ticks`, `last_ws_tick_ts` updated.
- Worker: `work_queue` transitions from pending to done.
- RAG: new `prediction_runs` and `market_predictions`; `last_prediction_ts` updated.
- Portal: `/health` renders and shows non-stale services.

## Backward-compat shims

- `python -m src.services.main` with `KALSHI_RUN_MODE` still runs the combined modes.
- Schema initialization defaults to on; set `DB_INIT_SCHEMA=0` per service once
  migrations are done.
- Guardrails are optional; set `SERVICE_GUARDRAILS=0` to disable.
- With `SERVICE_GUARDRAILS=1`, `KALSHI_RUN_MODE=all` is rejected to prevent mixed-mode writes.
- Env isolation: set `SERVICE_ENV_GUARDRAILS=warn|enforce` to detect cross-service
  credentials (e.g., `RAG_*/AZURE_*` on REST/WS/worker/portal or Kalshi API keys on RAG).
- Use `SERVICE_ENV_ALLOW_PREFIXES` / `SERVICE_ENV_ALLOW_KEYS` to explicitly ignore
  shared vars in dev environments.
- Schema validation is enabled by default; override with `SCHEMA_VALIDATE=0` or
  adjust `SCHEMA_COMPAT_MIN`/`SCHEMA_COMPAT_MAX`.
