#!/usr/bin/env bash
# Bootstrap Superset: migrate metadata -> tạo admin -> init roles -> chạy server.
# Idempotent: chạy lại mỗi lần `up` đều an toàn (create-admin bỏ qua nếu đã có).
set -e

superset db upgrade

superset fab create-admin \
  --username admin \
  --firstname Mr --lastname Senryuu \
  --email "${SUPERSET_ADMIN_EMAIL:-admin@gtl.local}" \
  --password "${SUPERSET_ADMIN_PASSWORD}" || true

superset init

# Tạo sẵn kết nối tới db marts (read-only) để dashboard trỏ vào ngay.
# marts_ro chỉ có SELECT -> Superset không thể ghi bậy vào serving layer.
superset set-database-uri \
  --database_name "GTL Marts" \
  --uri "postgresql+psycopg2://marts_ro:${MARTS_RO_PASSWORD}@postgres:5432/marts" 2>/dev/null || true

exec /usr/bin/run-server.sh
