#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

export POSTGRES_INSTANCE="${POSTGRES_INSTANCE:-kalshi-pg-backup}"
export PGPORT="${PGPORT:-55433}"
export PGDATA="${PGDATA:-/tmp/kalshi_pgdata_backup_${USER}}"
export POSTGRES_DB="${POSTGRES_DB:-kalshi_backup}"
export DATABASE_URL_VAR="${DATABASE_URL_VAR:-BACKUP_DATABASE_URL}"
export INIT_SCHEMA="${INIT_SCHEMA:-1}"
export SCHEMA_PATH="${SCHEMA_PATH:-$ROOT_DIR/sql/schema.sql}"

exec "$ROOT_DIR/scripts/demo_postgres.sh" "$@"
