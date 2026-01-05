#!/bin/bash
#SBATCH --job-name=kalshi-hd
#SBATCH --time=24:00:00
#SBATCH --cpus-per-task=2
#SBATCH --mem=6G
#SBATCH --output=kalshi_hd.%j.out
#SBATCH --error=kalshi_hd.%j.err

set -euo pipefail

module load apptainer

# Project root (assumes you sbatch from kalshi_hpc_ingestor/)
ROOT="$PWD"

# Load env vars (exports everything)
set -a
source "$ROOT/.env"
set +a

# Persistent PGDATA on shared filesystem
mkdir -p "$PGDATA_HOST"
chmod 700 "$PGDATA_HOST"

# Pull Postgres sif once (or keep in $ROOT)
if [ ! -f "$ROOT/postgres_16.sif" ]; then
  apptainer pull "$ROOT/postgres_16.sif" docker://postgres:16
fi

# Start Postgres instance
export POSTGRES_USER=kalshi
export POSTGRES_PASSWORD=kalshi
export POSTGRES_DB=kalshi

apptainer instance start \
  --writable-tmpfs \
  --bind "$PGDATA_HOST":/var/lib/postgresql/data \
  --env POSTGRES_USER="$POSTGRES_USER" \
  --env POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
  --env POSTGRES_DB="$POSTGRES_DB" \
  "$ROOT/postgres_16.sif" pgdb

cleanup () {
  apptainer instance stop pgdb || true
}
trap cleanup EXIT

# Wait until DB ready
for i in {1..60}; do
  if apptainer exec instance://pgdb pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" -h 127.0.0.1 -p 5432; then
    break
  fi
  sleep 1
done

# Ensure DATABASE_URL points to localhost on this node
export DATABASE_URL="postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@127.0.0.1:5432/${POSTGRES_DB}"

# Run ingestor container (bind code + bind private key)
apptainer run \
  --bind "$ROOT":/app \
  --bind "$(dirname "$KALSHI_PRIVATE_KEY_PEM_PATH")":/secrets \
  --env KALSHI_PRIVATE_KEY_PEM_PATH="/secrets/$(basename "$KALSHI_PRIVATE_KEY_PEM_PATH")" \
  --env DATABASE_URL="$DATABASE_URL" \
  "$ROOT/kalshi_ingestor.sif"
