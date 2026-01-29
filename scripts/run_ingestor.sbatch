#!/bin/bash
#SBATCH --job-name=kalshi-hd
#SBATCH --time=24:00:00
#SBATCH --cpus-per-task=2
#SBATCH --mem=6G
#SBATCH --output=log/kalshi_hd.%j.out
#SBATCH --error=log/kalshi_hd.%j.err

set -euo pipefail

# Project root (assumes you sbatch from kalshi_hpc_ingestor/)
ROOT="$PWD"
# Ensure the log directory exists for Slurm output files.
mkdir -p "$ROOT/log"

if ! command -v apptainer >/dev/null 2>&1; then
  if [ -f /etc/profile.d/modules.sh ]; then
    # Ensure the module function is available in non-interactive shells.
    # shellcheck source=/dev/null
    source /etc/profile.d/modules.sh
  fi
  if command -v module >/dev/null 2>&1; then
    module load apptainer \
      || module load apptainer/1.2 \
      || module load apptainer/1.1 \
      || module load singularity \
      || {
        echo "Apptainer module not found. Run 'module avail apptainer singularity' and update scripts/run_ingestor.sbatch." >&2
        exit 1
      }
  else
    echo "Apptainer not found on PATH and the module command is unavailable." >&2
    exit 1
  fi
fi

# Try to override systemd cgroups when allowed (avoid dbus dependency).
if [ -z "${APPTAINER_CONF_FILE:-}" ] && [ -r /etc/apptainer/apptainer.conf ]; then
  APPTAINER_CONF_FILE="$ROOT/log/apptainer.conf"
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
source "$ENV_FILE"
set +a

# Silence Apptainer XDG_RUNTIME_DIR warnings.
export XDG_RUNTIME_DIR="${TMPDIR:-/tmp}"
# Avoid cgroup/dbus issues on rootless HPC nodes.
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

# Default SCRATCH to the job submission directory if not provided.
if [ -z "${SCRATCH:-}" ]; then
  export SCRATCH="$ROOT"
fi
# Default PGDATA_HOST if not set in the env file.
if [ -z "${PGDATA_HOST:-}" ]; then
  export PGDATA_HOST="$SCRATCH/kalshi_pgdata"
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

# Start Postgres instance
export POSTGRES_USER="${POSTGRES_USER:-kalshi}"
export POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-kalshi}"
export POSTGRES_DB="${POSTGRES_DB:-kalshi}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"

apptainer instance start \
  --writable-tmpfs \
  --bind "$PGDATA_HOST":/var/lib/postgresql/data \
  --env POSTGRES_USER="$POSTGRES_USER" \
  --env POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
  --env POSTGRES_DB="$POSTGRES_DB" \
  "$ROOT/postgres_16.sif" pgdb

cleanup () {
  if [ -n "${REST_PID:-}" ] && kill -0 "$REST_PID" 2>/dev/null; then
    kill "$REST_PID" || true
    wait "$REST_PID" || true
  fi
  if [ -n "${WS_PID:-}" ] && kill -0 "$WS_PID" 2>/dev/null; then
    kill "$WS_PID" || true
    wait "$WS_PID" || true
  fi
  for pid in "${WORKER_PIDS[@]:-}"; do
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" || true
      wait "$pid" || true
    fi
  done
  apptainer instance stop pgdb || true
}
trap cleanup EXIT

# Wait until DB ready
for i in {1..60}; do
  if apptainer exec instance://pgdb pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" -h 127.0.0.1 -p "$POSTGRES_PORT"; then
    break
  fi
  sleep 1
done

# Ensure DATABASE_URL points to localhost on this node
export DATABASE_URL="postgresql://${POSTGRES_USER}@127.0.0.1:${POSTGRES_PORT}/${POSTGRES_DB}"
REST_DB_URL="${DATABASE_URL_REST:-$DATABASE_URL}"
WS_DB_URL="${DATABASE_URL_WS:-$DATABASE_URL}"
WORKER_DB_URL="${DATABASE_URL_WORKER:-$DATABASE_URL}"
unset DATABASE_URL_REST DATABASE_URL_WS DATABASE_URL_WORKER DATABASE_URL_RAG DATABASE_URL_PORTAL || true
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

if [ "$RUN_WS" -eq 1 ]; then
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
    "$ROOT/kalshi_ingestor.sif" \
    python -m src.services.ws_service &
  WS_PID=$!
  PIDS+=("$WS_PID")
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
