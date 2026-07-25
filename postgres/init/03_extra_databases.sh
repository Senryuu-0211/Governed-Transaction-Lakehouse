#!/bin/bash
# =============================================================================
# Tạo 3 database phụ + 2 user least-privilege — chạy trên VOLUME TRỐNG (init).
# Trước đây tôi tạo tay 3 DB này; đưa vào init để `down -v` + up TÁI LẬP ĐƯỢC
# (nếu không, reset_all sẽ phá catalog backend / marts / superset).
#   - iceberg_catalog : JDBC backend cho Iceberg REST (tự tạo bảng khi kết nối)
#   - marts           : serving copy cho Superset (push_marts ghi vào)
#   - superset        : metadata nội bộ Superset
# Users: marts_ro (chỉ SELECT marts) · superset_meta (owner superset).
# =============================================================================
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
	CREATE DATABASE iceberg_catalog OWNER $POSTGRES_USER;

	CREATE USER marts_ro PASSWORD '$MARTS_RO_PASSWORD';
	CREATE DATABASE marts OWNER $POSTGRES_USER;
	GRANT CONNECT ON DATABASE marts TO marts_ro;

	CREATE USER superset_meta PASSWORD '$SUPERSET_META_PASSWORD';
	CREATE DATABASE superset OWNER superset_meta;
EOSQL

# Quyền trong db marts: marts_ro chỉ đọc, kể cả bảng push_marts tạo về SAU.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname marts <<-EOSQL
	GRANT USAGE ON SCHEMA public TO marts_ro;
	ALTER DEFAULT PRIVILEGES FOR ROLE $POSTGRES_USER IN SCHEMA public
	  GRANT SELECT ON TABLES TO marts_ro;
EOSQL

echo ">> 03_extra_databases: iceberg_catalog + marts + superset san sang"
