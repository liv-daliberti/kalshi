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

# Ensure DB schema exists before starting any services
python -m src.services.migrator > "$LOG_DIR/migrator.log" 2>&1 || {
  echo "Migrator failed; see $LOG_DIR/migrator.log" >&2
  exit 1
}

# Internal services
nohup env PORT=8001 SERVICE_HEALTH_PORT=8001 python -m src.services.rest_service \
  > "$LOG_DIR/rest.log" 2>&1 &
nohup env PORT=8002 SERVICE_HEALTH_PORT=8002 python -m src.services.ws_service \
  > "$LOG_DIR/ws.log" 2>&1 &

# Workers (unique health ports)
WORKER_COUNT="${WORK_QUEUE_WORKERS:-4}"
for i in $(seq 1 "$WORKER_COUNT"); do
  hp=$((8100 + i))
  nohup env PORT="$hp" SERVICE_HEALTH_PORT="$hp" python -m src.services.worker_service \
    > "$LOG_DIR/worker_${i}.log" 2>&1 &
done

# RAG (optional)
nohup env PORT=8004 SERVICE_HEALTH_PORT=8004 python -m src.services.rag_service \
  > "$LOG_DIR/rag.log" 2>&1 &

# Portal
exec gunicorn --bind 0.0.0.0:"${PORT:-8000}" --workers 2 --threads 4 --timeout 180 \
  "src.web_portal.app:create_app()" \
  > "$LOG_DIR/portal.log" 2>&1
