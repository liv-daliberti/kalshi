#!/usr/bin/env bash
set -euo pipefail

# Oryx sets APP_PATH to the extracted app under /tmp/...
APP_ROOT="${APP_PATH:-/home/site/wwwroot}"
cd "$APP_ROOT"

LOG_DIR="/home/LogFiles/kalshi"
mkdir -p "$LOG_DIR"

if [ -z "${DATABASE_URL:-}" ]; then
  echo "DATABASE_URL is not set; aborting startup." >&2
  exit 1
fi

REST_DB_URL="${DATABASE_URL_REST:-$DATABASE_URL}"
WS_DB_URL="${DATABASE_URL_WS:-$DATABASE_URL}"
WORKER_DB_URL="${DATABASE_URL_WORKER:-$DATABASE_URL}"
RAG_DB_URL="${DATABASE_URL_RAG:-$DATABASE_URL}"
PORTAL_DB_URL="${DATABASE_URL_PORTAL:-$DATABASE_URL}"
DB_ENV_STRIP=(
  DATABASE_URL_REST=
  DATABASE_URL_WS=
  DATABASE_URL_WORKER=
  DATABASE_URL_RAG=
  DATABASE_URL_PORTAL=
)

# Ensure DB schema exists before starting any services
python -m src.services.migrator > "$LOG_DIR/migrator.log" 2>&1 || {
  echo "Migrator failed; see $LOG_DIR/migrator.log" >&2
  exit 1
}

# Internal services
nohup env "${DB_ENV_STRIP[@]}" PORT=8001 SERVICE_HEALTH_PORT=8001 \
  DATABASE_URL="$REST_DB_URL" \
  python -m src.services.rest_service \
  > "$LOG_DIR/rest.log" 2>&1 &
nohup env "${DB_ENV_STRIP[@]}" PORT=8002 SERVICE_HEALTH_PORT=8002 \
  DATABASE_URL="$WS_DB_URL" \
  python -m src.services.ws_service \
  > "$LOG_DIR/ws.log" 2>&1 &

# Workers (unique health ports)
WORKER_COUNT="${WORK_QUEUE_WORKERS:-4}"
for i in $(seq 1 "$WORKER_COUNT"); do
  hp=$((8100 + i))
  nohup env "${DB_ENV_STRIP[@]}" PORT="$hp" SERVICE_HEALTH_PORT="$hp" \
    DATABASE_URL="$WORKER_DB_URL" \
    python -m src.services.worker_service \
    > "$LOG_DIR/worker_${i}.log" 2>&1 &
done

# RAG (optional)
nohup env "${DB_ENV_STRIP[@]}" PORT=8004 SERVICE_HEALTH_PORT=8004 \
  DATABASE_URL="$RAG_DB_URL" \
  python -m src.services.rag_service \
  > "$LOG_DIR/rag.log" 2>&1 &

# Portal
exec env "${DB_ENV_STRIP[@]}" DATABASE_URL="$PORTAL_DB_URL" \
  gunicorn --bind 0.0.0.0:"${PORT:-8000}" --workers 2 --threads 4 --timeout 180 \
  "src.web_portal.app:create_app()" \
  > "$LOG_DIR/portal.log" 2>&1
