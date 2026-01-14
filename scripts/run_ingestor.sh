#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_DIR="${LOG_DIR:-$ROOT/log}"
mkdir -p "$LOG_DIR"

# Prefer local virtualenv for host-side utilities when available.
VENV_PATH="${VENV_PATH:-$ROOT/.venv}"
if [ -f "$VENV_PATH/bin/activate" ]; then
  # shellcheck source=/dev/null
  source "$VENV_PATH/bin/activate"
fi

if ! command -v apptainer >/dev/null 2>&1; then
  echo "apptainer not found on PATH." >&2
  exit 1
fi

# Try to override systemd cgroups when allowed (avoid dbus dependency).
if [ -z "${APPTAINER_CONF_FILE:-}" ] && [ -r /etc/apptainer/apptainer.conf ]; then
  APPTAINER_CONF_FILE="$LOG_DIR/apptainer.conf"
  cp /etc/apptainer/apptainer.conf "$APPTAINER_CONF_FILE"
  sed -i 's/^[[:space:]]*systemd cgroups = .*/systemd cgroups = no/' "$APPTAINER_CONF_FILE"
  export APPTAINER_CONF_FILE
fi

# Load env vars (exports everything)
ENV_FILE="${ENV_FILE:-$ROOT/.env}"
if [ ! -f "$ENV_FILE" ]; then
  echo "Missing env file: $ENV_FILE. Create it from .env.example or set ENV_FILE." >&2
  exit 1
fi
set -a
# shellcheck source=/dev/null
source "$ENV_FILE"
set +a

# Ensure LOG_DIR exists after ENV_FILE overrides.
if [ -z "${LOG_DIR:-}" ]; then
  LOG_DIR="$ROOT/log"
fi
mkdir -p "$LOG_DIR"

# Silence Apptainer XDG_RUNTIME_DIR warnings and avoid cgroup/dbus issues.
export XDG_RUNTIME_DIR="${TMPDIR:-/tmp}"
export APPTAINER_NO_CGROUPS=1
export SINGULARITY_NO_CGROUPS=1
export APPTAINER_CGROUPS=0
export SINGULARITY_CGROUPS=0
export APPTAINER_SYSTEMD_CGROUPS=0
export SINGULARITY_SYSTEMD_CGROUPS=0

# Prefer a host CA bundle when available (avoids SSL MITM issues on clusters).
CA_BUNDLE_HOST="${KALSHI_CA_BUNDLE:-}"
if [ -z "$CA_BUNDLE_HOST" ]; then
  for p in /etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem /etc/ssl/certs/ca-certificates.crt; do
    if [ -r "$p" ]; then
      CA_BUNDLE_HOST="$p"
      break
    fi
  done
fi
CA_BUNDLE_ARGS=()
if [ -n "$CA_BUNDLE_HOST" ]; then
  CA_BUNDLE_CONTAINER="/opt/ca-bundle/ca.pem"
  CA_BUNDLE_ARGS+=(--bind "$CA_BUNDLE_HOST:$CA_BUNDLE_CONTAINER:ro")
  CA_BUNDLE_ARGS+=(--env REQUESTS_CA_BUNDLE="$CA_BUNDLE_CONTAINER")
  CA_BUNDLE_ARGS+=(--env SSL_CERT_FILE="$CA_BUNDLE_CONTAINER")
fi

# Default PGDATA_HOST to a local folder if not set in the env file.
if [ -z "${PGDATA_HOST:-}" ]; then
  export PGDATA_HOST="$ROOT/kalshi_pgdata"
else
  pg_parent="$(dirname "$PGDATA_HOST")"
  if [ ! -e "$PGDATA_HOST" ] && [ ! -w "$pg_parent" ]; then
    echo "PGDATA_HOST parent not writable ($pg_parent); falling back to $ROOT/kalshi_pgdata" >&2
    export PGDATA_HOST="$ROOT/kalshi_pgdata"
  elif [ -e "$PGDATA_HOST" ] && [ ! -w "$PGDATA_HOST" ]; then
    echo "PGDATA_HOST not writable ($PGDATA_HOST); falling back to $ROOT/kalshi_pgdata" >&2
    export PGDATA_HOST="$ROOT/kalshi_pgdata"
  fi
fi

# Persistent PGDATA on shared filesystem
mkdir -p "$PGDATA_HOST"
chmod 700 "$PGDATA_HOST"

# Pull Postgres sif once (or keep in $ROOT)
if [ ! -f "$ROOT/postgres_16.sif" ]; then
  apptainer pull "$ROOT/postgres_16.sif" docker://postgres:16
fi

if [ ! -f "$ROOT/kalshi_ingestor.sif" ]; then
  echo "Missing image: $ROOT/kalshi_ingestor.sif. Build it with apptainer build." >&2
  exit 1
fi

# Keep the container Python deps up to date using a venv bound on the host.
APPTAINER_PIP_INSTALL="${APPTAINER_PIP_INSTALL:-1}"
APPTAINER_VENV_PATH="${APPTAINER_VENV_PATH:-/app/.venv_apptainer}"
APPTAINER_VENV_BIN="${APPTAINER_VENV_PATH}/bin"
APPTAINER_PY_ENV_ARGS=(--env VIRTUAL_ENV="$APPTAINER_VENV_PATH" --env PATH="$APPTAINER_VENV_BIN:/usr/local/bin:/usr/bin:/bin")
if [ "$APPTAINER_PIP_INSTALL" != "0" ]; then
  if [ ! -f "$ROOT/requirements.txt" ]; then
    echo "Missing requirements.txt at $ROOT/requirements.txt" >&2
    exit 1
  fi
  echo "Updating Apptainer Python environment (venv: $APPTAINER_VENV_PATH)." >&2
  apptainer exec \
    "${CA_BUNDLE_ARGS[@]}" \
    --bind "$ROOT":/app \
    --env APPTAINER_VENV_PATH="$APPTAINER_VENV_PATH" \
    --env PIP_DISABLE_PIP_VERSION_CHECK=1 \
    "$ROOT/kalshi_ingestor.sif" \
    /bin/sh -c 'set -e
if [ ! -x "$APPTAINER_VENV_PATH/bin/python" ]; then
  python -m venv "$APPTAINER_VENV_PATH"
fi
"$APPTAINER_VENV_PATH/bin/python" -m pip install --upgrade pip setuptools wheel
"$APPTAINER_VENV_PATH/bin/python" -m pip install --upgrade -r /app/requirements.txt
'
fi

# Start Postgres directly (avoids instance/cgroup/dbus issues).
export POSTGRES_USER="${POSTGRES_USER:-kalshi}"
export POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-kalshi}"
export POSTGRES_DB="${POSTGRES_DB:-kalshi}"
POSTGRES_PORT="${POSTGRES_PORT:-55432}"
POSTGRES_MAX_CONNECTIONS="${POSTGRES_MAX_CONNECTIONS:-}"
PGDATA_CONTAINER="/var/lib/postgresql/data"
PG_ISREADY_FALLBACK=0

if [ ! -f "$PGDATA_HOST/PG_VERSION" ]; then
  pwfile="$(mktemp)"
  chmod 600 "$pwfile"
  printf '%s' "$POSTGRES_PASSWORD" >"$pwfile"
  apptainer exec \
    --bind "$PGDATA_HOST:$PGDATA_CONTAINER" \
    "$ROOT/postgres_16.sif" \
    initdb -D "$PGDATA_CONTAINER" -U "$POSTGRES_USER" --pwfile="$pwfile"
  rm -f "$pwfile"
fi

POSTGRES_LOG="$LOG_DIR/postgres.log"
PG_PID=""
REST_PID=""
WS_PIDS=()
WORKER_PIDS=()
QUEUE_MAINT_PID=""
PG_ALREADY_RUNNING=0
PID_FILE="$PGDATA_HOST/postmaster.pid"
terminate_postgres_pid () {
  local pid="$1"
  if [ -z "$pid" ] || ! kill -0 "$pid" 2>/dev/null; then
    return
  fi
  echo "Stopping existing postgres (pid $pid)." >&2
  kill "$pid" 2>/dev/null || true
  for _ in {1..10}; do
    if ! kill -0 "$pid" 2>/dev/null; then
      return
    fi
    sleep 0.5
  done
  echo "Postgres pid $pid did not exit; killing." >&2
  kill -9 "$pid" 2>/dev/null || true
}
terminate_postgres_tree () {
  local pid="$1"
  local child_pids=""
  if [ -z "$pid" ]; then
    return
  fi
  if command -v pgrep >/dev/null 2>&1; then
    child_pids="$(pgrep -P "$pid" -x postgres 2>/dev/null || true)"
  fi
  terminate_postgres_pid "$pid"
  for child in $child_pids; do
    terminate_postgres_pid "$child"
  done
}
if [ -f "$PID_FILE" ]; then
  pg_pid="$(head -n 1 "$PID_FILE" | tr -d '[:space:]')"
  pg_comm="$(ps -p "$pg_pid" -o comm= 2>/dev/null || true)"
  pg_comm="${pg_comm//[[:space:]]/}"
  pg_state="$(ps -p "$pg_pid" -o stat= 2>/dev/null || true)"
  pg_state="${pg_state//[[:space:]]/}"
  if [ -n "$pg_comm" ] && echo "$pg_comm" | grep -qi '^postgres' && ! echo "$pg_state" | grep -q '[TZ]'; then
    echo "Postgres already running (pid $pg_pid); using existing server." >&2
    PG_ALREADY_RUNNING=1
  else
    if [ -n "$pg_comm" ] && echo "$pg_comm" | grep -qi '^postgres' && echo "$pg_state" | grep -q '[TZ]'; then
      echo "Postgres pid $pg_pid is stopped; terminating." >&2
      terminate_postgres_pid "$pg_pid"
    fi
    echo "Removing stale postmaster.pid (pid ${pg_pid:-unknown})." >&2
    rm -f "$PID_FILE"
  fi
fi
if [ "$PG_ALREADY_RUNNING" -eq 1 ] && [ -n "$POSTGRES_MAX_CONNECTIONS" ]; then
  echo "POSTGRES_MAX_CONNECTIONS is set but Postgres is already running; restart to apply." >&2
fi

start_postgres () {
  local pg_extra_args=()
  if [ -n "$POSTGRES_MAX_CONNECTIONS" ]; then
    pg_extra_args=(-c "max_connections=$POSTGRES_MAX_CONNECTIONS")
  fi
  apptainer exec \
    --bind "$PGDATA_HOST:$PGDATA_CONTAINER" \
    "$ROOT/postgres_16.sif" \
    postgres -D "$PGDATA_CONTAINER" -h 127.0.0.1 -p "$POSTGRES_PORT" -k /tmp \
    "${pg_extra_args[@]}" \
    >>"$POSTGRES_LOG" 2>&1 &
  PG_PID=$!
}

pg_is_ready () {
  if apptainer exec "$ROOT/postgres_16.sif" \
    pg_isready -U "$POSTGRES_USER" -d postgres -h 127.0.0.1 -p "$POSTGRES_PORT" \
    >/dev/null 2>&1; then
    return 0
  fi
  if [ "$PG_ISREADY_FALLBACK" -eq 0 ]; then
    echo "pg_isready via apptainer failed; falling back to TCP probe." >&2
    PG_ISREADY_FALLBACK=1
  fi
  if command -v python3 >/dev/null 2>&1; then
    python3 - <<PY >/dev/null 2>&1
import socket, sys
sock = socket.socket()
sock.settimeout(1)
try:
    sock.connect(("127.0.0.1", int("$POSTGRES_PORT")))
except Exception:
    sys.exit(1)
else:
    sys.exit(0)
PY
    return $?
  fi
  if command -v python >/dev/null 2>&1; then
    python - <<PY >/dev/null 2>&1
import socket, sys
sock = socket.socket()
sock.settimeout(1)
try:
    sock.connect(("127.0.0.1", int("$POSTGRES_PORT")))
except Exception:
    sys.exit(1)
else:
    sys.exit(0)
PY
    return $?
  fi
  return 1
}

has_pg_listener () {
  if command -v lsof >/dev/null 2>&1; then
    lsof -n -iTCP:"$POSTGRES_PORT" -sTCP:LISTEN >/dev/null 2>&1
    return $?
  fi
  if command -v ss >/dev/null 2>&1; then
    ss -ltn "sport = :$POSTGRES_PORT" 2>/dev/null | tail -n +2 | grep -q .
    return $?
  fi
  if command -v netstat >/dev/null 2>&1; then
    netstat -ltn 2>/dev/null | awk -v port=":$POSTGRES_PORT" '$4 ~ port"$" {found=1} END{exit found?0:1}'
    return $?
  fi
  return 1
}

clear_stale_socket () {
  local socket="/tmp/.s.PGSQL.${POSTGRES_PORT}"
  local lock="/tmp/.s.PGSQL.${POSTGRES_PORT}.lock"
  if [ ! -e "$socket" ] && [ ! -e "$lock" ]; then
    return
  fi
  if has_pg_listener; then
    return
  fi
  if command -v pgrep >/dev/null 2>&1 && pgrep -x postgres >/dev/null 2>&1; then
    return
  fi
  echo "Removing stale postgres socket files in /tmp." >&2
  rm -f "$socket" "$lock"
}

stop_stale_postgres () {
  if ! command -v pgrep >/dev/null 2>&1; then
    return
  fi
  local pids=""
  pids="$(pgrep -f "/usr/lib/postgresql/16/bin/postgres -D $PGDATA_CONTAINER" 2>/dev/null || true)"
  if [ -n "$pids" ]; then
    echo "Stopping stale postgres processes: $pids" >&2
    for pid in $pids; do
      terminate_postgres_tree "$pid"
    done
  fi
  local user_pids=""
  user_pids="$(pgrep -u "$USER" -x postgres 2>/dev/null || true)"
  if [ -n "$user_pids" ]; then
    echo "Stopping postgres processes for user $USER: $user_pids" >&2
    for pid in $user_pids; do
      terminate_postgres_pid "$pid"
    done
  fi
}

cleanup_shared_memory () {
  if ! command -v ipcs >/dev/null 2>&1; then
    return
  fi
  if ! command -v ipcrm >/dev/null 2>&1; then
    return
  fi
  while read -r shmid owner nattch; do
    if [ -z "$shmid" ] || [ "$owner" != "$USER" ]; then
      continue
    fi
    if [ "$nattch" != "0" ]; then
      continue
    fi
    echo "Removing stale shared memory segment $shmid (owner $owner)." >&2
    ipcrm -m "$shmid" 2>/dev/null || true
  done < <(ipcs -m | awk 'NR>3 {print $2, $3, $6}')
}

if [ "$PG_ALREADY_RUNNING" -ne 1 ]; then
  if has_pg_listener; then
    if pg_is_ready >/dev/null 2>&1; then
      echo "Postgres already responding on 127.0.0.1:$POSTGRES_PORT; using existing server." >&2
      PG_ALREADY_RUNNING=1
    else
      echo "Port $POSTGRES_PORT is in use but Postgres is not responding; attempting cleanup." >&2
      stop_stale_postgres
      cleanup_shared_memory
      clear_stale_socket
      if has_pg_listener && ! pg_is_ready >/dev/null 2>&1; then
        echo "Port $POSTGRES_PORT is still in use. Set POSTGRES_PORT or stop the conflicting process." >&2
        exit 1
      fi
    fi
  else
    clear_stale_socket
  fi
fi
if [ "$PG_ALREADY_RUNNING" -ne 1 ]; then
  start_postgres
fi

cleanup () {
  if [ -n "$QUEUE_MAINT_PID" ] && kill -0 "$QUEUE_MAINT_PID" 2>/dev/null; then
    kill "$QUEUE_MAINT_PID" || true
    wait "$QUEUE_MAINT_PID" || true
  fi
  if [ -n "$REST_PID" ] && kill -0 "$REST_PID" 2>/dev/null; then
    kill "$REST_PID" || true
    wait "$REST_PID" || true
  fi
  for pid in "${WS_PIDS[@]}"; do
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" || true
      wait "$pid" || true
    fi
  done
  for pid in "${WORKER_PIDS[@]}"; do
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" || true
      wait "$pid" || true
    fi
  done
  if [ -n "$PG_PID" ] && kill -0 "$PG_PID" 2>/dev/null; then
    terminate_postgres_tree "$PG_PID"
    wait "$PG_PID" || true
  fi
}
trap cleanup EXIT
trap 'cleanup; exit 130' TSTP

# Wait until DB ready
ready=0
for i in {1..60}; do
  if pg_is_ready; then
    ready=1
    break
  fi
  sleep 1
done
if [ "$ready" -ne 1 ] && [ "$PG_ALREADY_RUNNING" -eq 1 ]; then
  echo "Postgres was marked running but is not responding; restarting." >&2
  PG_ALREADY_RUNNING=0
  rm -f "$PID_FILE"
  clear_stale_socket
  start_postgres
  for i in {1..60}; do
    if pg_is_ready; then
      ready=1
      break
    fi
    sleep 1
  done
fi
if [ "$ready" -ne 1 ]; then
  echo "Postgres did not become ready. Check $POSTGRES_LOG for details." >&2
  exit 1
fi

# Ensure target database exists.
if ! PGPASSWORD="$POSTGRES_PASSWORD" PGDATABASE=postgres apptainer exec "$ROOT/postgres_16.sif" \
  psql -h 127.0.0.1 -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d postgres -tAc \
  "SELECT 1 FROM pg_database WHERE datname='${POSTGRES_DB}'" | grep -q 1; then
  PGPASSWORD="$POSTGRES_PASSWORD" PGDATABASE=postgres apptainer exec "$ROOT/postgres_16.sif" \
    createdb -h 127.0.0.1 -p "$POSTGRES_PORT" -U "$POSTGRES_USER" "$POSTGRES_DB" || true
fi

# Ensure DATABASE_URL points to localhost on this node
export DATABASE_URL="postgresql://${POSTGRES_USER}@127.0.0.1:${POSTGRES_PORT}/${POSTGRES_DB}"
# Strip other DATABASE_URL_* vars to avoid guardrail noise inside services.
unset DATABASE_URL_REST DATABASE_URL_WS DATABASE_URL_WORKER DATABASE_URL_RAG DATABASE_URL_PORTAL || true
REST_DB_URL="${DATABASE_URL_REST:-$DATABASE_URL}"
WS_DB_URL="${DATABASE_URL_WS:-$DATABASE_URL}"
WORKER_DB_URL="${DATABASE_URL_WORKER:-$DATABASE_URL}"
DB_INIT_SCHEMA="${DB_INIT_SCHEMA:-0}"

INGESTOR_MODE="${INGESTOR_MODE:-all}"
INGESTOR_MODE="$(echo "$INGESTOR_MODE" | tr '[:upper:]' '[:lower:]')"
RUN_REST=1
RUN_WS=1
RUN_WORKER=1
case "$INGESTOR_MODE" in
  all|"")
    ;;
  ws-worker|worker-ws|ws_worker|worker_ws)
    RUN_REST=0
    ;;
  ws)
    RUN_REST=0
    RUN_WORKER=0
    ;;
  worker)
    RUN_REST=0
    RUN_WS=0
    ;;
  rest)
    RUN_WS=0
    RUN_WORKER=0
    ;;
  *)
    echo "Unknown INGESTOR_MODE=$INGESTOR_MODE (use all, ws-worker, ws, worker, rest)." >&2
    exit 1
    ;;
esac

if [ "$RUN_WORKER" -eq 1 ] && [ "${WORK_QUEUE_ENABLE:-0}" = "1" ] && [ "${WORK_QUEUE_MAINTENANCE_ENABLE:-1}" = "1" ]; then
  queue_maint_interval="${WORK_QUEUE_MAINTENANCE_SECONDS:-900}"
  apptainer exec \
    "${CA_BUNDLE_ARGS[@]}" \
    "${APPTAINER_PY_ENV_ARGS[@]}" \
    --bind "$ROOT":/app \
    --pwd /app \
    --env DATABASE_URL="$WORKER_DB_URL" \
    --env SERVICE_GUARDRAILS=1 \
    --env SERVICE_ROLE=worker \
    --env WORK_QUEUE_MAINTENANCE_SECONDS="$queue_maint_interval" \
    "$ROOT/kalshi_ingestor.sif" \
    python /app/scripts/queue_maintenance.py --loop --interval-seconds "$queue_maint_interval" &
  QUEUE_MAINT_PID=$!
fi

PIDS=()

if [ "$RUN_REST" -eq 1 ]; then
  apptainer exec \
    "${CA_BUNDLE_ARGS[@]}" \
    "${APPTAINER_PY_ENV_ARGS[@]}" \
    --bind "$ROOT":/app \
    --pwd /app \
    --bind "$(dirname "$KALSHI_PRIVATE_KEY_PEM_PATH")":/secrets \
    --env KALSHI_PRIVATE_KEY_PEM_PATH="/secrets/$(basename "$KALSHI_PRIVATE_KEY_PEM_PATH")" \
    --env KALSHI_HOST="$KALSHI_HOST" \
    --env DATABASE_URL="$REST_DB_URL" \
    --env BACKUP_DATABASE_URL="${BACKUP_DATABASE_URL:-}" \
    --env DB_INIT_SCHEMA="$DB_INIT_SCHEMA" \
    --env SERVICE_GUARDRAILS=1 \
    "$ROOT/kalshi_ingestor.sif" \
    python -m src.services.rest_service &
  REST_PID=$!
  PIDS+=("$REST_PID")
fi

WS_PIDS=()
if [ "$RUN_WS" -eq 1 ]; then
  WS_SHARD_COUNT="${WS_SHARD_COUNT:-1}"
  if ! [[ "$WS_SHARD_COUNT" =~ ^[0-9]+$ ]] || [ "$WS_SHARD_COUNT" -lt 1 ]; then
    echo "Invalid WS_SHARD_COUNT=$WS_SHARD_COUNT (must be >= 1)" >&2
    exit 1
  fi

  for ((shard=0; shard<WS_SHARD_COUNT; shard++)); do
    apptainer exec \
      "${CA_BUNDLE_ARGS[@]}" \
      "${APPTAINER_PY_ENV_ARGS[@]}" \
      --bind "$ROOT":/app \
      --pwd /app \
      --bind "$(dirname "$KALSHI_PRIVATE_KEY_PEM_PATH")":/secrets \
      --env KALSHI_PRIVATE_KEY_PEM_PATH="/secrets/$(basename "$KALSHI_PRIVATE_KEY_PEM_PATH")" \
      --env KALSHI_HOST="$KALSHI_HOST" \
      --env DATABASE_URL="$WS_DB_URL" \
      --env DB_INIT_SCHEMA="$DB_INIT_SCHEMA" \
      --env SERVICE_GUARDRAILS=1 \
      --env WS_SHARD_KEY="${WS_SHARD_KEY:-event}" \
      --env WS_SHARD_COUNT="$WS_SHARD_COUNT" \
      --env WS_SHARD_ID="$shard" \
      --env WS_HEARTBEAT_SECONDS="${WS_HEARTBEAT_SECONDS:-}" \
      --env WS_TICK_STALE_SECONDS="${WS_TICK_STALE_SECONDS:-}" \
      "$ROOT/kalshi_ingestor.sif" \
      python -m src.services.ws_service &
    WS_PIDS+=("$!")
    PIDS+=("${WS_PIDS[-1]}")
  done
fi

WORKER_PIDS=()
if [ "$RUN_WORKER" -eq 1 ] && [ "${WORK_QUEUE_ENABLE:-0}" = "1" ]; then
  worker_count="${WORK_QUEUE_WORKERS:-1}"
  if [ "$worker_count" -gt 0 ]; then
    for i in $(seq 1 "$worker_count"); do
      apptainer exec \
        "${CA_BUNDLE_ARGS[@]}" \
        "${APPTAINER_PY_ENV_ARGS[@]}" \
        --bind "$ROOT":/app \
        --pwd /app \
        --bind "$(dirname "$KALSHI_PRIVATE_KEY_PEM_PATH")":/secrets \
        --env KALSHI_PRIVATE_KEY_PEM_PATH="/secrets/$(basename "$KALSHI_PRIVATE_KEY_PEM_PATH")" \
        --env KALSHI_HOST="$KALSHI_HOST" \
        --env DATABASE_URL="$WORKER_DB_URL" \
        --env DB_INIT_SCHEMA="$DB_INIT_SCHEMA" \
        --env SERVICE_GUARDRAILS=1 \
        "$ROOT/kalshi_ingestor.sif" \
        python -m src.services.worker_service &
      WORKER_PIDS+=("$!")
      PIDS+=("${WORKER_PIDS[-1]}")
    done
  fi
elif [ "$RUN_WORKER" -eq 1 ]; then
  echo "WORK_QUEUE_ENABLE=0; skipping worker processes." >&2
fi

if [ "${#PIDS[@]}" -eq 0 ]; then
  echo "No services started for INGESTOR_MODE=$INGESTOR_MODE." >&2
  exit 1
fi

wait "${PIDS[@]}"
