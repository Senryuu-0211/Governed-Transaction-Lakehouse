#!/usr/bin/env bash
# =============================================================================
# Verify Phase 1 Step 1c — Bronze Iceberg trên MinIO. Exit != 0 nếu có check fail.
#
# Thứ tự: hạ tầng (curl/docker, rẻ) -> lag của stream -> nội dung dữ liệu (Spark,
# đắt). Nếu MinIO hay catalog chết thì biết ngay, khỏi chờ Spark khởi động.
#
# NGUYÊN TẮC: một check chỉ được FAIL khi PIPELINE hỏng. Bảng Bronze rỗng vì
# thượng nguồn Kafka không còn message nào (retention đã xoá) KHÔNG phải lỗi
# pipeline — báo fail ở đó chỉ tập cho ta thói quen phớt lờ cảnh báo.
# =============================================================================
set -uo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PYTHON="${VENV_PYTHON:-$HOME/working/gtl-spark-venv/bin/python}"
MINIO_URL="${MINIO_URL:-http://localhost:9001}"
CATALOG_URL="${CATALOG_URL:-http://localhost:8181}"
KAFKA="${KAFKA_CONTAINER:-gtl-kafka}"
KBIN=/opt/kafka/bin
# Dưới ngưỡng này coi như đã bám realtime; trên ngưỡng thì phải chứng minh đang
# đuổi kịp (lag giảm) chứ không phải đứng hình.
LAG_OK=100000
RESAMPLE_SECONDS=8

pass=0; fail=0
ok() { echo "  ✅ $1"; pass=$((pass+1)); }
no() { echo "  ❌ $1"; fail=$((fail+1)); }
info() { echo "  ℹ️  $1"; }

kafka_offset() {  # $1=topic  $2=-1 latest | -2 earliest
  docker exec "$KAFKA" $KBIN/kafka-get-offsets.sh --bootstrap-server localhost:9092 \
    --topic "$1" --time "$2" 2>/dev/null | awk -F: '{s+=$3} END{print s+0}'
}

stream_offset() {  # $1=table -> vị trí stream đã commit theo checkpoint
  local f
  f=$(ls -1t "$PROJECT_ROOT/_checkpoints/$1/offsets/"* 2>/dev/null | head -1)
  [ -n "$f" ] && tail -1 "$f" 2>/dev/null | grep -o '"0":[0-9]*' | cut -d: -f2 || echo 0
}

echo "== Phase 1 Step 1c — verify =="

# --- 1. MinIO sống ----------------------------------------------------------
code=$(curl -s -o /dev/null -w '%{http_code}' "$MINIO_URL/minio/health/live" 2>/dev/null)
[ "$code" = "200" ] && ok "MinIO healthy ($MINIO_URL)" || no "MinIO không phản hồi (HTTP ${code:-none})"

# --- 2. Bucket warehouse tồn tại --------------------------------------------
# Hứng output ra biến RỒI mới grep: `docker` thoát mã != 0 sẽ đầu độc exit code
# của pipeline và làm grep báo sai âm (bài học từ verify.sh).
buckets=$(docker run --rm --network governed-txn-lakehouse_default --entrypoint sh \
            --env-file "$PROJECT_ROOT/.env" minio/mc:RELEASE.2024-09-16T17-43-14Z -c \
            'mc alias set gtl http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1; mc ls gtl/' 2>/dev/null)
printf '%s\n' "$buckets" | grep -q "warehouse" && ok "bucket warehouse tồn tại" || no "không thấy bucket warehouse"

# --- 3. Iceberg REST catalog ------------------------------------------------
cfg=$(curl -s "$CATALOG_URL/v1/config" 2>/dev/null)
printf '%s\n' "$cfg" | grep -q "endpoints" && ok "Iceberg REST catalog phản hồi" || no "REST catalog không phản hồi"

tables=$(curl -s "$CATALOG_URL/v1/namespaces/bronze/tables" 2>/dev/null)
for t in transactions accounts merchants; do
  printf '%s\n' "$tables" | grep -q "\"$t\"" && ok "bảng bronze.$t đã đăng ký" || no "thiếu bảng bronze.$t"
done

# --- 4. Checkpoint có commit ------------------------------------------------
for t in transactions accounts merchants; do
  d="$PROJECT_ROOT/_checkpoints/$t/commits"
  if [ -d "$d" ] && [ -n "$(ls -A "$d" 2>/dev/null)" ]; then
    ok "checkpoint $t đã commit batch (restart sẽ resume đúng chỗ)"
  else
    no "checkpoint $t chưa commit batch nào"
  fi
done

# --- 5. Lag của stream + xác định bảng nào THỰC SỰ có data thượng nguồn ------
echo
echo "-- lag so với Kafka --"
declare -A lag_before
expect_data=""
for t in transactions accounts merchants; do
  # Đọc vị trí stream TRƯỚC, hỏi Kafka SAU. Ngược lại thì stream có thể tiến lên
  # trong lúc đo và lag ra số âm — vô hại nhưng gây hoang mang khi đọc kết quả.
  cur=$(stream_offset "$t")
  latest=$(kafka_offset "gtl.public.$t" -1)
  earliest=$(kafka_offset "gtl.public.$t" -2)
  avail=$((latest - earliest))
  lag=$((latest - cur))
  lag_before[$t]=$lag
  printf "     %-13s stream=%-11s latest=%-11s lag=%-10s (Kafka còn %s message)\n" \
         "$t" "$cur" "$latest" "$lag" "$avail"
  [ "$avail" -gt 0 ] && expect_data="${expect_data}${t},"
done

# Lag lớn có thể là đang backfill hợp lệ. Lấy mẫu lần hai: giảm = đang đuổi kịp,
# đứng yên = stream chết. Chỉ "đứng yên khi còn lag" mới là lỗi.
need_resample=0
for t in transactions accounts merchants; do
  [ "${lag_before[$t]}" -gt "$LAG_OK" ] && need_resample=1
done
if [ "$need_resample" -eq 1 ]; then
  info "có topic lag > $LAG_OK, lấy mẫu lại sau ${RESAMPLE_SECONDS}s để phân biệt backfill với đứng hình"
  sleep "$RESAMPLE_SECONDS"
fi

for t in transactions accounts merchants; do
  before=${lag_before[$t]}
  if [ "$before" -le "$LAG_OK" ]; then
    ok "$t bám realtime (lag=$before)"
  else
    latest=$(kafka_offset "gtl.public.$t" -1)
    after=$((latest - $(stream_offset "$t")))
    if [ "$after" -lt "$before" ]; then
      ok "$t đang đuổi kịp (lag $before -> $after)"
    else
      no "$t lag không giảm ($before -> $after) — stream có thể đã chết"
    fi
  fi
done

# --- 6. Nội dung dữ liệu (Spark) --------------------------------------------
echo
echo "-- kiểm tra nội dung bằng Spark (mất ~1 phút) --"
if [ ! -x "$VENV_PYTHON" ]; then
  no "không tìm thấy python venv tại $VENV_PYTHON"
else
  # Hứng ra biến TRƯỚC rồi mới grep: nối thẳng vào pipe thì exit code của Spark
  # bị exit code của grep che mất.
  spark_out=$(cd "$PROJECT_ROOT" && EXPECT_DATA="${expect_data%,}" PYTHONPATH=spark \
              "$VENV_PYTHON" spark/bronze_layer/verify_bronze.py 2>/dev/null)
  spark_rc=$?
  printf '%s\n' "$spark_out" | grep -E "\[OK\]|\[FAIL\]|\[NOTE\]|op distribution|^== spark"
  [ "$spark_rc" -eq 0 ] && ok "Spark checks đều pass" || no "Spark checks có mục fail (xem ở trên)"
fi

echo
echo "== PASS=${pass} FAIL=${fail} =="
[ "$fail" -eq 0 ]
