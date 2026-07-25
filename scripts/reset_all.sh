#!/usr/bin/env bash
# =============================================================================
# Reset TOÀN BỘ pipeline về trạng thái trắng — XOÁ DỮ LIỆU.
#
# VÌ SAO PHẢI CÓ SCRIPT NÀY (chứ không chỉ gõ `docker compose down -v`):
#
#   Khoá chính của cả 3 bảng là `GENERATED ALWAYS AS IDENTITY`, và faker chạy với
#   RANDOM_SEED cố định. Sau khi xoá volume Postgres, sequence quay về 1 — nên lứa
#   dữ liệu MỚI sẽ tái sử dụng ĐÚNG những txn_id / account_id / merchant_id của lứa
#   CŨ, nhưng nội dung khác.
#
#   Nếu Bronze còn giữ dữ liệu cũ mà lứa mới đổ chồng lên:
#     - Bronze chứa hai thế hệ TRÙNG khoá chính;
#     - Silver `MERGE INTO ... ON txn_id` sẽ gộp hai giao dịch KHÔNG liên quan làm một;
#     - reconciliation "tổng Bronze = tổng Gold" sai mà không có lỗi nào được ném ra.
#
#   `docker compose down -v` chỉ xoá NAMED VOLUME (Postgres, Kafka, MinIO). Nó KHÔNG
#   đụng tới hai thứ nằm trên host: catalog sqlite (bind mount) và checkpoint của
#   Spark. Bỏ sót checkpoint còn tệ hơn: stream sẽ tưởng mình đã đọc tới offset cũ
#   và BỎ QUA dữ liệu mới.
#
#   Nên reset phải nguyên tử trên BỐN kho, hoặc đừng reset.
#
# Dùng:  bash scripts/reset_all.sh          (hỏi xác nhận)
#        bash scripts/reset_all.sh --yes    (không hỏi — dùng trong script khác)
# =============================================================================
set -uo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT" || exit 1

CATALOG_URL="${CATALOG_URL:-http://localhost:8181}"
AUTO_YES=0
[ "${1:-}" = "--yes" ] && AUTO_YES=1

echo "=============================================================="
echo " RESET TOÀN BỘ — CÁC THỨ SAU SẼ BỊ XOÁ VĨNH VIỄN"
echo "=============================================================="
echo "  1. Postgres  : toàn bộ accounts / merchants / transactions"
echo "  2. Kafka     : toàn bộ topic + offset + replication slot state"
echo "  3. MinIO     : toàn bộ bảng Bronze (bucket warehouse)"
echo "  4. Host      : $PROJECT_ROOT/iceberg-catalog/  (catalog sqlite)"
echo "                 $PROJECT_ROOT/_checkpoints/      (offset của Spark)"
echo
echo "  Đây là dữ liệu test do faker sinh. KHÔNG chạy trên hệ thống thật."
echo

if [ "$AUTO_YES" -eq 0 ]; then
  printf "Gõ RESET để xác nhận: "
  read -r answer
  if [ "$answer" != "RESET" ]; then
    echo "Đã huỷ, không xoá gì."
    exit 1
  fi
fi

# --- Bước 1: dừng streaming job ---------------------------------------------
# Phải dừng TRƯỚC khi xoá. Nếu để chạy, nó sẽ ghi tiếp vào Bronze trong lúc ta
# đang dọn, và tạo lại checkpoint ngay sau khi vừa xoá.
echo
echo "[1/5] dừng streaming job..."
pids=$(pgrep -f "bronze_stream.py" 2>/dev/null)
if [ -n "$pids" ]; then
  # SIGINT trước để job dừng query cho gọn, rồi mới cưỡng chế nếu còn sống.
  kill -INT $pids 2>/dev/null
  for _ in $(seq 1 15); do
    pgrep -f "bronze_stream.py" >/dev/null 2>&1 || break
    sleep 1
  done
  pgrep -f "bronze_stream.py" >/dev/null 2>&1 && kill -9 $(pgrep -f "bronze_stream.py") 2>/dev/null
  echo "      đã dừng"
else
  echo "      không có job nào đang chạy"
fi

# --- Bước 2: hạ stack + xoá named volume ------------------------------------
echo "[2/5] hạ stack và xoá volume (Postgres, Kafka, MinIO)..."
docker compose down -v >/dev/null 2>&1
echo "      xong"

# --- Bước 3: dọn state trên host --------------------------------------------
# Hai thư mục này `down -v` KHÔNG chạm tới. Đây là bước hay bị quên nhất.
echo "[3/5] xoá catalog sqlite và checkpoint trên host..."
rm -rf "$PROJECT_ROOT/iceberg-catalog"/* "$PROJECT_ROOT/_checkpoints"/*
mkdir -p "$PROJECT_ROOT/iceberg-catalog" "$PROJECT_ROOT/_checkpoints"
echo "      xong"

# --- Bước 4: dựng lại --------------------------------------------------------
echo "[4/5] dựng lại stack (init script chạy lại trên volume trống)..."
docker compose up -d >/dev/null 2>&1
echo "      đợi Postgres healthy..."
for _ in $(seq 1 60); do
  status=$(docker inspect -f '{{.State.Health.Status}}' gtl-postgres 2>/dev/null)
  [ "$status" = "healthy" ] && break
  sleep 2
done
[ "$status" = "healthy" ] && echo "      Postgres healthy" || echo "      ⚠️  Postgres chưa healthy, kiểm tra bằng: docker compose ps"

echo "      đợi Iceberg REST catalog..."
for _ in $(seq 1 30); do
  curl -sf "$CATALOG_URL/v1/config" >/dev/null 2>&1 && break
  sleep 2
done
# Catalog vừa bị xoá sạch nên namespace cũng mất — tạo lại để job stream có chỗ
# đặt bảng. Idempotent: chạy lại không sao.
curl -s -X POST "$CATALOG_URL/v1/namespaces" -H 'Content-Type: application/json' \
     -d '{"namespace":["bronze"]}' >/dev/null 2>&1
echo "      namespace bronze sẵn sàng"

# Phase 2.5: connector phát Avro qua Apicurio -> registry PHẢI sống trước khi
# đăng ký connector (không thì auto-register schema thất bại).
echo "      đợi Apicurio Schema Registry..."
for _ in $(seq 1 40); do
  curl -sf "http://localhost:8087/apis/registry/v2/system/info" >/dev/null 2>&1 && break
  sleep 2
done

# --- Bước 5: đăng ký lại connector -------------------------------------------
echo "[5/5] đăng ký lại Debezium connector (Avro)..."
bash "$PROJECT_ROOT/scripts/register-connector.sh" >/dev/null 2>&1 \
  && echo "      xong" || echo "      ⚠️  đăng ký thất bại, chạy tay: bash scripts/register-connector.sh"

cat <<EOF

=============================================================
 RESET XONG. Kho đã sạch và dựng lại (4 DB Postgres tái lập qua init script).
 Bước tiếp — khởi động lại Bronze stream (chờ connector snapshot xong để có schema Avro):

   PYTHONPATH=spark ~/working/gtl-spark-venv/bin/python spark/bronze_layer/bronze_stream.py

 Rồi:  bash scripts/dbt.sh build --full-refresh   (rebuild Silver/Gold/marts từ Bronze mới)
       PYTHONPATH=spark ~/working/gtl-spark-venv/bin/python spark/gold_layer/push_marts.py
       bash scripts/verify.sh && bash scripts/verify_1b.sh && bash scripts/verify_1c.sh

 LƯU Ý: Superset metadata cũng mới -> dataset marts cần đăng ký lại (xem worklog).
=============================================================
EOF
