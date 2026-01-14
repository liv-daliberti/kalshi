#!/usr/bin/env bash
set -euo pipefail

ACTION="${1:-start}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PGDATA="${PGDATA:-/tmp/kalshi_pgdata_${USER}}"
PGPORT="${PGPORT:-55432}"
LOGFILE="${LOGFILE:-/tmp/kalshi_postgres.log}"
PGSOCKETDIR="${PGSOCKETDIR:-/tmp}"
POSTGRES_MAX_CONNECTIONS="${POSTGRES_MAX_CONNECTIONS:-600}"
POSTGRES_SIF="${POSTGRES_SIF:-}"
POSTGRES_INSTANCE="${POSTGRES_INSTANCE:-kalshi-pg-demo}"
POSTGRES_DB="${POSTGRES_DB:-kalshi}"
DATABASE_URL_VAR="${DATABASE_URL_VAR:-DATABASE_URL}"
INIT_SCHEMA="${INIT_SCHEMA:-0}"
SCHEMA_PATH="${SCHEMA_PATH:-$ROOT_DIR/sql/schema.sql}"

RUNNER=()
USE_APPTAINER=0
run_pg() {
  if [[ "${#RUNNER[@]}" -gt 0 ]]; then
    "${RUNNER[@]}" "$@"
  else
    "$@"
  fi
}

resolve_cmd() {
  local cmd="$1"
  if command -v "$cmd" >/dev/null 2>&1; then
    command -v "$cmd"
    return 0
  fi
  if command -v pg_config >/dev/null 2>&1; then
    local bindir
    bindir="$(pg_config --bindir 2>/dev/null || true)"
    if [[ -n "$bindir" && -x "$bindir/$cmd" ]]; then
      echo "$bindir/$cmd"
      return 0
    fi
  fi
  for path in /usr/lib/postgresql/*/bin/"$cmd" \
              /usr/pgsql-*/bin/"$cmd" \
              /usr/local/pgsql/bin/"$cmd" \
              /usr/local/opt/postgresql*/bin/"$cmd" \
              /opt/homebrew/opt/postgresql*/bin/"$cmd"; do
    if [[ -x "$path" ]]; then
      echo "$path"
      return 0
    fi
  done
  return 1
}

INITDB="$(resolve_cmd initdb || true)"
PG_CTL="$(resolve_cmd pg_ctl || true)"
CREATEDB="$(resolve_cmd createdb || true)"

if [[ -z "$INITDB" || -z "$PG_CTL" || -z "$CREATEDB" ]]; then
  if [[ -z "$POSTGRES_SIF" && -f "./postgres_16.sif" ]]; then
    POSTGRES_SIF="./postgres_16.sif"
  fi
  if [[ -n "$POSTGRES_SIF" ]]; then
    if ! command -v apptainer >/dev/null 2>&1; then
      echo "Missing apptainer; required to use $POSTGRES_SIF." >&2
      exit 1
    fi
    USE_APPTAINER=1
  else
    echo "Missing Postgres client utilities (initdb/pg_ctl/createdb)." >&2
    echo "Install Postgres locally or set POSTGRES_SIF=./postgres_16.sif." >&2
    exit 1
  fi
fi

if [[ "$ACTION" == "stop" || "$ACTION" == "reset" ]]; then
  if [[ "$USE_APPTAINER" -eq 1 ]]; then
    apptainer instance stop "$POSTGRES_INSTANCE" >/dev/null 2>&1 || true
  else
    run_pg pg_ctl -D "$PGDATA" stop >/dev/null 2>&1 || true
  fi
  if [[ "$ACTION" == "reset" ]]; then
    rm -rf "$PGDATA"
    echo "Postgres data directory reset at $PGDATA"
  else
    echo "Postgres stopped."
  fi
  exit 0
fi

if [[ "$USE_APPTAINER" -eq 1 ]]; then
  LOGDIR="$(dirname "$LOGFILE")"
  mkdir -p "$PGDATA" "$PGSOCKETDIR" "$LOGDIR"
  if ! apptainer instance list | awk 'NR>1 {print $1}' | grep -qx "$POSTGRES_INSTANCE"; then
    apptainer instance start \
      --bind "$PGDATA:$PGDATA" \
      --bind "$PGSOCKETDIR:$PGSOCKETDIR" \
      --bind "$LOGDIR:$LOGDIR" \
      "$POSTGRES_SIF" "$POSTGRES_INSTANCE" >/dev/null
  fi
  RUNNER=(apptainer exec "instance://$POSTGRES_INSTANCE")
fi

if [[ ! -s "$PGDATA/PG_VERSION" ]]; then
  run_pg initdb -D "$PGDATA" -A trust >/dev/null
fi

# Ensure localhost TCP is trusted for demo use.
if ! grep -q "127.0.0.1/32" "$PGDATA/pg_hba.conf"; then
  {
    echo ""
    echo "# Demo trust auth for local connections"
    echo "host all all 127.0.0.1/32 trust"
    echo "host all all ::1/128 trust"
  } >> "$PGDATA/pg_hba.conf"
fi

if ! run_pg pg_ctl -D "$PGDATA" status >/dev/null 2>&1; then
  max_conn_arg=""
  if [[ -n "$POSTGRES_MAX_CONNECTIONS" ]]; then
    max_conn_arg=" -c max_connections=$POSTGRES_MAX_CONNECTIONS"
  fi
  run_pg pg_ctl -D "$PGDATA" \
    -o "-F -p $PGPORT -h 127.0.0.1 -k $PGSOCKETDIR$max_conn_arg" \
    -l "$LOGFILE" start >/dev/null
fi

run_pg createdb -h 127.0.0.1 -p "$PGPORT" "$POSTGRES_DB" >/dev/null 2>&1 || true

if [[ "$INIT_SCHEMA" == "1" ]]; then
  if [[ -f "$SCHEMA_PATH" ]]; then
    run_pg psql -h 127.0.0.1 -p "$PGPORT" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 < "$SCHEMA_PATH"
  else
    echo "Schema file not found at $SCHEMA_PATH" >&2
    exit 1
  fi
fi

echo "Postgres running on 127.0.0.1:$PGPORT"
echo "export ${DATABASE_URL_VAR}=postgresql://$USER@127.0.0.1:$PGPORT/$POSTGRES_DB"
