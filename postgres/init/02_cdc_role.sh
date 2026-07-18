#!/bin/bash
# =============================================================================
# CDC least-privilege setup — chạy sau 01_schema.sql (thứ tự alphabet).
# =============================================================================
# Debezium KHÔNG dùng superuser. Tạo role riêng chỉ có đúng quyền cần:
#   - REPLICATION : để mở replication slot đọc WAL (bắt buộc cho logical decoding)
#   - LOGIN       : kết nối được
#   - SELECT      : đọc snapshot ban đầu 3 bảng (không được ghi/sửa gì)
# Publication `dbz_publication` giới hạn đúng 3 bảng cần CDC — không phát cả DB.
# Least-privilege là chuẩn ngân hàng: nếu credential Debezium lộ, nó cũng chỉ đọc.
#
# Mật khẩu lấy từ env DEBEZIUM_PASSWORD (từ .env) — không hardcode vào file versioned.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE ROLE debezium WITH REPLICATION LOGIN PASSWORD '${DEBEZIUM_PASSWORD}';

    GRANT CONNECT ON DATABASE ${POSTGRES_DB} TO debezium;
    GRANT USAGE  ON SCHEMA public            TO debezium;
    GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
    -- Bảng tạo sau (nếu có) cũng tự được SELECT — tránh sót quyền khi schema tiến hóa.
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;

    -- Publication giới hạn đúng 3 bảng cần capture (Step 1b connector sẽ dùng cái này).
    CREATE PUBLICATION dbz_publication FOR TABLE accounts, merchants, transactions;
EOSQL

echo ">> CDC role 'debezium' + publication 'dbz_publication' created (least-privilege)"
