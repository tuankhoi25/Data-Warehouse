#!/usr/bin/env bash
set -e

# Tạo role (không chứa password trong SQL để tránh lộ logs)
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER "${POSTGRES_CLICKHOUSE_USER}" WITH PASSWORD '${POSTGRES_CLICKHOUSE_PASSWORD}' SUPERUSER;
EOSQL



# Thêm rule vào pg_hba.conf (dùng scram-sha-256 cho an toàn)
echo "host    oltp    ${POSTGRES_CLICKHOUSE_USER}    172.16.0.0/12    scram-sha-256" >> "$PGDATA/pg_hba.conf"

# Reload config để áp dụng pg_hba.conf mới
psql -v ON_ERROR_STOP=1 \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    -c "SELECT pg_reload_conf();"

echo "PostgreSQL configuration reloaded."