#!/usr/bin/env bash
# =============================================================================
# Công tắc BẬT/TẮT toàn pipeline — để không đốt tiền S3 lúc không dùng.
#
#   bash scripts/pipeline.sh stop     # tắt hết   -> chi phí ~$0 (chỉ còn storage)
#   bash scripts/pipeline.sh start    # bật lại   -> near-real-time chạy tiếp
#   bash scripts/pipeline.sh status   # đang thế nào? lag bao nhiêu? S3 tốn gì?
#
# VÌ SAO CẦN SCRIPT (không gõ tay từng lệnh):
#   Có 3 thứ ghi lên S3, tắt THIẾU một cái là vẫn tốn tiền — và cái hay quên nhất
#   (bronze_stream) lại chính là cái ghi LIÊN TỤC:
#     1. bronze_stream  — tiến trình HOST, Airflow KHÔNG quản  <-- hay quên nhất
#     2. 2 DAG Airflow  — gtl_transform @hourly, gtl_maintenance @daily
#     3. faker + stack  — vẫn sinh data vào Postgres/Kafka
#
# TẮT KHÔNG MẤT DATA: Kafka giữ retention 30 ngày, checkpoint Spark giữ offset ->
# bật lại là nuốt tiếp đúng chỗ đã dừng (exactly-once), không hụt giao dịch nào.
# =============================================================================
set -uo pipefail
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT" || exit 1

VENV_PYTHON="${VENV_PYTHON:-$HOME/working/gtl-spark-venv/bin/python}"
AIRFLOW_WORKER="${AIRFLOW_WORKER:-airflow-docker-airflow-worker-1}"
STREAM_LOG="$HOME/gtl-stream.log"
DAGS=(gtl_transform gtl_maintenance)

af() { docker exec "$AIRFLOW_WORKER" airflow "$@" >/dev/null 2>&1; }

# Ghi cờ CHỦ ĐÍCH cho Phase 5: tắt bằng script này là "tôi cố ý tắt" -> alert im
# lặng. Còn pipeline đang bật mà chết thì alert BẮN. Không có cờ này thì đêm nào
# tắt cho đỡ tốn tiền S3 cũng sinh một loạt cảnh báo đúng-nhưng-vô-dụng, và người
# ta sẽ tập thói quen phớt lờ alert.
# Chỉ ghi file trạng thái, KHÔNG ghi thẳng .prom — file đó do metrics_exporter.py
# độc quyền ghi (hai người ghi = có lúc node_exporter đọc phải file dở).
METRICS_DIR="${GTL_METRICS_DIR:-$HOME/working/metrics}"
set_flag() { mkdir -p "$METRICS_DIR" && echo "$1" > "$METRICS_DIR/.pipeline_state"; }

stream_pids() { pgrep -f "bronze_layer/bronze_stream.py" 2>/dev/null; }

# Batch mới nhất trong một thư mục checkpoint của Spark (offsets/ hoặc commits/).
# Duyệt bằng GLOB chứ không `ls | grep`: parse output của ls là sai về nguyên tắc
# (tên file có ký tự lạ là hỏng) và shellcheck SC2010 chặn đúng chỗ đó.
# So sánh bằng -gt (số học) chứ không theo thứ tự chữ: "10" phải lớn hơn "9".
newest_batch() {
  local dir="$1" best="" name
  for path in "$dir"/*; do
    name="${path##*/}"
    [[ "$name" =~ ^[0-9]+$ ]] || continue
    if [ -z "$best" ] || [ "$name" -gt "$best" ]; then best="$name"; fi
  done
  printf '%s' "$best"
}

# ---------------------------------------------------------------------------
stop_pipeline() {
  echo "== TẮT PIPELINE =="
  # Hạ cờ TRƯỚC khi tắt: đặt sau thì có một khoảng stream đã chết mà cờ còn =1 ->
  # đúng khoảnh khắc đó exporter chạy là bắn cảnh báo oan.
  set_flag 0

  # 1) Stream TRƯỚC: nó là thứ ghi S3 liên tục. SIGINT để Spark đóng query gọn
  #    (checkpoint được flush đúng), chỉ cưỡng chế nếu chai lì.
  echo "[1/3] dừng bronze_stream..."
  # mapfile thay vì để shell tự tách chuỗi: stream có thể có nhiều PID, mà tách
  # không dấu ngoặc là loại lỗi im lặng (một khoảng trắng lạ trong output là giết
  # nhầm tiến trình). Mảng nói rõ ý định "đây là DANH SÁCH pid".
  mapfile -t pids < <(stream_pids)
  if [ ${#pids[@]} -gt 0 ]; then
    kill -INT "${pids[@]}" 2>/dev/null
    for _ in $(seq 1 20); do
      [ -z "$(stream_pids)" ] && break
      sleep 1
    done
    mapfile -t stubborn < <(stream_pids)
    [ ${#stubborn[@]} -gt 0 ] && kill -9 "${stubborn[@]}" 2>/dev/null
    echo "      đã dừng (checkpoint giữ nguyên offset)"
  else
    echo "      không chạy"
  fi

  # 2) Pause DAG: không thì Airflow vẫn kích job theo lịch khi stack bật lại.
  echo "[2/3] pause DAG Airflow..."
  for d in "${DAGS[@]}"; do
    af dags pause "$d" && echo "      $d paused" || echo "      ⚠️ không pause được $d"
  done

  # 3) `stop` chứ KHÔNG `down`: giữ container + volume, bật lại nhanh và không
  #    mất Postgres/Kafka. `down -v` mới là xoá data (đó là việc của reset_all.sh).
  echo "[3/3] dừng container (giữ nguyên data)..."
  docker compose stop >/dev/null 2>&1 && echo "      đã dừng stack"

  echo
  echo "✅ ĐÃ TẮT. Không còn gì ghi lên S3 -> chi phí chỉ còn tiền lưu trữ."
  echo "   Bật lại: bash scripts/pipeline.sh start"
}

# ---------------------------------------------------------------------------
start_pipeline() {
  echo "== BẬT PIPELINE =="

  echo "[1/4] khởi động container..."
  docker compose up -d >/dev/null 2>&1
  echo "      đợi Postgres + Kafka sẵn sàng..."
  for _ in $(seq 1 60); do
    st=$(docker inspect -f '{{.State.Health.Status}}' gtl-postgres 2>/dev/null)
    [ "$st" = "healthy" ] && break
    sleep 2
  done
  [ "$st" = "healthy" ] && echo "      Postgres healthy" || echo "      ⚠️ Postgres chưa healthy"

  # Iceberg REST phải sống TRƯỚC khi stream ghi, không thì commit đầu tiên chết.
  echo "[2/4] đợi Iceberg REST catalog..."
  for _ in $(seq 1 30); do
    curl -sf http://localhost:8181/v1/config >/dev/null 2>&1 && break
    sleep 2
  done
  curl -sf http://localhost:8181/v1/config >/dev/null 2>&1 \
    && echo "      catalog sẵn sàng" || echo "      ⚠️ catalog chưa phản hồi"

  echo "[3/4] unpause DAG Airflow..."
  for d in "${DAGS[@]}"; do
    af dags unpause "$d" && echo "      $d chạy theo lịch" || echo "      ⚠️ không unpause được $d"
  done

  # setsid: tách hẳn khỏi session gọi -> stream sống tiếp khi đóng terminal/SSH
  # (bài học: chạy như background job của phiên thì phiên chết là stream chết).
  echo "[4/4] khởi động bronze_stream (detached)..."
  if [ -n "$(stream_pids)" ]; then
    echo "      đã chạy sẵn, bỏ qua"
  else
    setsid bash -c "cd '$PROJECT_ROOT' && PYTHONPATH=spark '$VENV_PYTHON' \
      spark/bronze_layer/bronze_stream.py > '$STREAM_LOG' 2>&1" </dev/null &
    disown 2>/dev/null
    sleep 25   # Spark cold-start ~20s trước khi có batch đầu
    [ -n "$(stream_pids)" ] && echo "      đang chạy (log: $STREAM_LOG)" \
                           || echo "      ⚠️ chưa lên — xem $STREAM_LOG"
  fi

  # Dựng cờ SAU CÙNG: stream vừa lên cần vài chục giây mới có commit đầu, dựng cờ
  # sớm là alert freshness bắn ngay trong lúc khởi động bình thường.
  set_flag 1

  echo
  echo "✅ ĐÃ BẬT. Kiểm: bash scripts/pipeline.sh status"
}

# ---------------------------------------------------------------------------
show_status() {
  echo "== TRẠNG THÁI PIPELINE =="

  # `grep -c .` chứ không `wc -l`: khi không có container nào chạy, output vẫn là
  # MỘT dòng trống -> wc -l trả 1 (báo sai "1/8 đang chạy" lúc đã tắt sạch).
  running=$(docker compose ps --services --filter status=running 2>/dev/null | grep -c .)
  total=$(docker compose config --services 2>/dev/null | grep -c .)
  echo "container   : $running/$total đang chạy"

  if [ -n "$(stream_pids)" ]; then
    # pgrep hay báo dương tính giả -> xác nhận bằng ĐỘ TƯƠI của checkpoint commit,
    # đó mới là bằng chứng stream thực sự ghi được (bài học 26-07).
    f=$(newest_batch _checkpoints/transactions/commits)
    if [ -n "$f" ]; then
      age=$(( $(date +%s) - $(stat -c %Y "_checkpoints/transactions/commits/$f") ))
      [ "$age" -lt 300 ] && echo "bronze_stream: 🟢 đang ghi (commit ${age}s trước)" \
                         || echo "bronze_stream: 🟡 tiến trình sống nhưng commit ${age}s trước — nghi treo"
    else
      echo "bronze_stream: 🟡 vừa khởi động, chưa có commit"
    fi
  else
    echo "bronze_stream: ⚪ tắt"
  fi

  for d in "${DAGS[@]}"; do
    p=$(docker exec "$AIRFLOW_WORKER" airflow dags details "$d" -o plain 2>/dev/null \
        | awk '/^is_paused/{print $2}')
    case "$p" in
      False) echo "DAG $d: 🟢 chạy theo lịch" ;;
      True)  echo "DAG $d: ⚪ paused" ;;
      *)     echo "DAG $d: ? (không đọc được)" ;;
    esac
  done

  # Lag = bằng chứng khách quan pipeline có bám kịp nguồn không.
  end=$(docker exec gtl-kafka /opt/kafka/bin/kafka-get-offsets.sh \
        --bootstrap-server localhost:9092 --topic gtl.public.transactions --time -1 2>/dev/null | cut -d: -f3)
  d="_checkpoints/transactions/offsets"
  cur=$(tail -1 "$d/$(newest_batch "$d")" 2>/dev/null \
        | grep -oE '"0":[0-9]+' | grep -oE '[0-9]+$')
  [ -n "$end" ] && [ -n "$cur" ] && echo "lag Bronze  : $((end - cur)) message"

  echo
  # LIST request có tính phí nhưng cỡ bucket này là không đáng kể.
  "$VENV_PYTHON" "$PROJECT_ROOT/scripts/s3_admin.py" usage 2>/dev/null | head -5
}

# ---------------------------------------------------------------------------
case "${1:-}" in
  stop)   stop_pipeline ;;
  start)  start_pipeline ;;
  status) show_status ;;
  *)      sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//' ; exit 1 ;;
esac
