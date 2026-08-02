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

    -- =======================================================================
    -- BẢNG SIGNAL — nửa PHỤC HỒI của rủi ro CDC #1 (issue 003).
    --
    -- Vì sao BẮT BUỘC có, dù ta đã dùng Kafka signal channel:
    --   Incremental snapshot chạy thuật toán DBLog: nó chia bảng thành chunk và
    --   chèn WATERMARK low/high để bảo đảm dòng snapshot cũ KHÔNG ghi đè dòng
    --   stream mới hơn. Watermark đó phải đi qua chính luồng replication -> phải
    --   là thao tác GHI vào một bảng NẰM TRONG PUBLICATION.
    --   Kafka channel chỉ mang được MỆNH LỆNH, không mang được watermark.
    --   (MySQL có chế độ read.only dùng GTID thay watermark; Postgres KHÔNG có.)
    --
    -- Vì sao việc này KHÔNG phá least-privilege:
    --   Quyền ghi cấp cho ĐÚNG MỘT bảng hạ tầng, không phải bảng nghiệp vụ.
    --   verify.sh vẫn khẳng định debezium bị chặn ghi vào `merchants`, và có thêm
    --   một check mới khẳng định ranh giới đó: ghi được signal, KHÔNG ghi được
    --   dữ liệu nghiệp vụ. Ranh giới giờ được kiểm CHỦ ĐỘNG chứ không chỉ giả định.
    -- =======================================================================
    CREATE TABLE debezium_signal (
        id   VARCHAR(42)  PRIMARY KEY,
        type VARCHAR(32)  NOT NULL,
        data VARCHAR(2048)
    );

    -- DELETE cần vì watermarking strategy mặc định là INSERT_DELETE: chèn marker
    -- rồi xoá ngay, để bảng không phình. Không có DELETE thì snapshot vẫn chạy
    -- nhưng bảng lớn dần mãi.
    GRANT SELECT, INSERT, UPDATE, DELETE ON debezium_signal TO debezium;

    -- Publication giới hạn đúng 3 bảng nghiệp vụ + bảng signal.
    -- Bảng signal PHẢI có mặt ở đây, nếu không watermark không đi qua replication
    -- stream và incremental snapshot không phân định được ranh giới chunk.
    CREATE PUBLICATION dbz_publication
        FOR TABLE accounts, merchants, transactions, debezium_signal;
EOSQL

echo ">> CDC role 'debezium' + publication 'dbz_publication' created (least-privilege)"
