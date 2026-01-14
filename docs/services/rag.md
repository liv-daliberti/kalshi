# RAG predictions service

## Responsibilities
- Schedule prediction passes and write forecasts.
- Load event/market context from Postgres.
- Maintain `rag_documents` embeddings/chunks.
- Store `prediction_runs` and `market_predictions`.
- Emit health heartbeat in `ingest_state`.

## Data dependencies
- Tables: `events`, `markets`, `market_ticks`, `market_candles`.
- RAG tables: `rag_documents`, `prediction_runs`, `market_predictions`.
- Ingest state keys: `last_prediction_ts`.

## Infra dependencies
- Postgres (`DATABASE_URL`).
- Optional embedding/LLM providers (e.g., Azure/OpenAI) when using external handlers.

## Runtime
- Entrypoint: `python -m src.services.rag_service` (requires `PREDICTION_ENABLE=1`)
- Legacy: `KALSHI_RUN_MODE=rag python -m src.services.main`
- Optional: `SERVICE_HEALTH_PORT` exposes `/healthz`
