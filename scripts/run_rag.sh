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

_env_truthy () {
  local raw="${1:-}"
  raw="${raw,,}"
  case "$raw" in
    1|true|t|yes|y|on) return 0 ;;
    *) return 1 ;;
  esac
}

if ! _env_truthy "${PREDICTION_ENABLE:-}" && ! _env_truthy "${PREDICTIONS_ENABLE:-}"; then
  echo "PREDICTION_ENABLE is not set; enable it in $ENV_FILE to run RAG." >&2
  exit 1
fi

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

# Start Postgres directly (avoids instance/cgroup/dbus issues) when DATABASE_URL is not set.
export POSTGRES_USER="${POSTGRES_USER:-kalshi}"
export POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-kalshi}"
export POSTGRES_DB="${POSTGRES_DB:-kalshi}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_MAX_CONNECTIONS="${POSTGRES_MAX_CONNECTIONS:-}"
PGDATA_CONTAINER="/var/lib/postgresql/data"
PG_ISREADY_FALLBACK=0

POSTGRES_LOG="$LOG_DIR/postgres.log"
PG_PID=""
RAG_PID=""
PG_ALREADY_RUNNING=0
PG_MANAGED=0
PID_FILE=""
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
}

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

setup_local_postgres () {
  if has_pg_listener && pg_is_ready >/dev/null 2>&1; then
    echo "Postgres already responding on 127.0.0.1:$POSTGRES_PORT; using existing server." >&2
    PG_ALREADY_RUNNING=1
    return
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
  PG_MANAGED=1

  PID_FILE="$PGDATA_HOST/postmaster.pid"
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

  if [ "$PG_ALREADY_RUNNING" -ne 1 ]; then
    if has_pg_listener; then
      if pg_is_ready >/dev/null 2>&1; then
        echo "Postgres already responding on 127.0.0.1:$POSTGRES_PORT; using existing server." >&2
        PG_ALREADY_RUNNING=1
        PG_MANAGED=0
        return
      fi
      echo "Port $POSTGRES_PORT is in use but Postgres is not responding; attempting cleanup." >&2
      stop_stale_postgres
      cleanup_shared_memory
      clear_stale_socket
      if has_pg_listener && ! pg_is_ready >/dev/null 2>&1; then
        echo "Port $POSTGRES_PORT is still in use. Set POSTGRES_PORT or stop the conflicting process." >&2
        exit 1
      fi
    else
      clear_stale_socket
    fi
  fi
  if [ "$PG_ALREADY_RUNNING" -ne 1 ]; then
    start_postgres
  fi
}

cleanup () {
  if [ -n "$RAG_PID" ] && kill -0 "$RAG_PID" 2>/dev/null; then
    kill "$RAG_PID" || true
    wait "$RAG_PID" || true
  fi
  if [ -n "$PG_PID" ] && kill -0 "$PG_PID" 2>/dev/null; then
    terminate_postgres_tree "$PG_PID"
    wait "$PG_PID" || true
  fi
}
trap cleanup EXIT
trap 'cleanup; exit 130' TSTP

if [ -z "${DATABASE_URL:-}" ]; then
  setup_local_postgres
  # Wait until DB ready
  ready=0
  for i in {1..60}; do
    if pg_is_ready; then
      ready=1
      break
    fi
    sleep 1
  done
  if [ "$ready" -ne 1 ] && [ "$PG_ALREADY_RUNNING" -eq 1 ] && [ "$PG_MANAGED" -eq 1 ]; then
    echo "Postgres was marked running but is not responding; restarting." >&2
    PG_ALREADY_RUNNING=0
    if [ -n "$PID_FILE" ]; then
      rm -f "$PID_FILE"
    fi
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
    psql -h 127.0.0.1 -U "$POSTGRES_USER" -d postgres -tAc \
    "SELECT 1 FROM pg_database WHERE datname='${POSTGRES_DB}'" | grep -q 1; then
    PGPASSWORD="$POSTGRES_PASSWORD" PGDATABASE=postgres apptainer exec "$ROOT/postgres_16.sif" \
      createdb -h 127.0.0.1 -U "$POSTGRES_USER" "$POSTGRES_DB" || true
  fi

  # Ensure DATABASE_URL points to localhost on this node
  export DATABASE_URL="postgresql://${POSTGRES_USER}@127.0.0.1:${POSTGRES_PORT}/${POSTGRES_DB}"
else
  echo "Using existing DATABASE_URL for RAG." >&2
fi
RAG_DB_URL="${DATABASE_URL_RAG:-$DATABASE_URL}"
DB_INIT_SCHEMA="${DB_INIT_SCHEMA:-0}"

RAG_ENV_ARGS=()
if [ -n "${KALSHI_PRIVATE_KEY_PEM_PATH:-}" ]; then
  RAG_ENV_ARGS+=(--bind "$(dirname "$KALSHI_PRIVATE_KEY_PEM_PATH")":/secrets)
  RAG_ENV_ARGS+=(--env KALSHI_PRIVATE_KEY_PEM_PATH="/secrets/$(basename "$KALSHI_PRIVATE_KEY_PEM_PATH")")
fi
if [ -n "${KALSHI_HOST:-}" ]; then
  RAG_ENV_ARGS+=(--env KALSHI_HOST="$KALSHI_HOST")
fi

apptainer exec \
  "${CA_BUNDLE_ARGS[@]}" \
  "${APPTAINER_PY_ENV_ARGS[@]}" \
  --bind "$ROOT":/app \
  --pwd /app \
  "${RAG_ENV_ARGS[@]}" \
  --env DATABASE_URL="$RAG_DB_URL" \
  --env DB_INIT_SCHEMA="$DB_INIT_SCHEMA" \
  --env SERVICE_GUARDRAILS=1 \
  "$ROOT/kalshi_ingestor.sif" \
  python -m src.services.rag_service &
RAG_PID=$!

wait "$RAG_PID"
