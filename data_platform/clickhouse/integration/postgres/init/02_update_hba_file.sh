#!/bin/bash
set -e

echo "host    oltp    clickhouse_user    172.16.0.0/12    password" >> "$PGDATA/pg_hba.conf"

echo "pg_hba.conf updated for clickhouse_user" >&2


echo "Reloading PostgreSQL configuration..."

psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  -c "SELECT pg_reload_conf();"

echo "PostgreSQL configuration reloaded."