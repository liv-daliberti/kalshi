# Service Registry

Keep this registry updated so deployments remain isolated and ownership is clear.

| Service | Owner | Entrypoint | Ports | Health | Env file | DB role | Queue | Dependencies |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| REST discovery/backfill | TBD | `python -m src.services.rest_service` | none | `SERVICE_HEALTH_PORT` (`/healthz`) | `configs/env/rest.env.example` | `kalshi_rest` | `work_queue` publish | Postgres, Kalshi REST, RabbitMQ (optional) |
| WebSocket ingestion | TBD | `python -m src.services.ws_service` | none | `SERVICE_HEALTH_PORT` (`/healthz`) | `configs/env/ws.env.example` | `kalshi_ws` | none | Postgres, Kalshi WS |
| Queue worker | TBD | `python -m src.services.worker_service` | none | `SERVICE_HEALTH_PORT` (`/healthz`) | `configs/env/worker.env.example` | `kalshi_worker` | `work_queue` claim | Postgres, RabbitMQ (optional) |
| RAG predictions | TBD | `python -m src.services.rag_service` | none | `SERVICE_HEALTH_PORT` (`/healthz`) | `configs/env/rag.env.example` | `kalshi_rag` | none | Postgres, LLM + embeddings |
| Web portal | TBD | `python -m src.services.portal_service` | `WEB_PORTAL_PORT` | `GET /health` | `configs/env/portal.env.example` | `kalshi_portal` | `work_queue` enqueue (backfill) | Postgres, Kalshi REST |
| Migrator | TBD | `python -m src.services.migrator` | none | none | `configs/env/admin.env.example` | admin | none | Postgres |

Notes:
- Owners should map to a team or on-call rotation for alert routing.
- If `WORK_QUEUE_PUBLISH=0`, REST does DB-only queue writes and RabbitMQ is optional.
- Health ports are optional; set `SERVICE_HEALTH_PORT` to expose `/healthz` for services without HTTP APIs.
