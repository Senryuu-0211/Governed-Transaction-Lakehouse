#!/usr/bin/env bash
# =============================================================================
# Phục hồi CDC: yêu cầu Debezium snapshot lại MỘT bảng, không dừng streaming.
#
#   bash scripts/cdc_resnapshot.sh public.merchants
#   bash scripts/cdc_resnapshot.sh public.accounts public.merchants
#   bash scripts/cdc_resnapshot.sh --status          # xem signal đã gửi + trạng thái connector
#   bash scripts/cdc_resnapshot.sh --reset           # dọn state snapshot kẹt (giữ vị trí WAL)
#
# ⭐ ĐÂY LÀ NỬA CÒN THIẾU CỦA RỦI RO CDC #1 (issue 003).
#   `max_slot_wal_keep_size=10GB` cố ý HY SINH replication slot để cứu DB nguồn
#   khi WAL tích tụ — "thà mất CDC còn hơn sập core DB". Nhưng đánh đổi đó chỉ
#   đứng vững nếu re-snapshot THẬT SỰ làm được. Trước script này, lựa chọn duy
#   nhất là snapshot lại TOÀN BỘ 3 bảng, tức đúng cái điều thiết kế sinh ra để
#   tránh. Hệ thống có cơ chế PHÁT HIỆN mà không có cơ chế PHỤC HỒI thì mới chỉ
#   xong một nửa.
#
# VÌ SAO INCREMENTAL SNAPSHOT (thuật toán DBLog), không phải snapshot thường:
#   · snapshot thường đọc TOÀN BỘ mọi bảng và CHẶN streaming -> có khoảng mù
#   · incremental chia chunk theo khoá chính, chạy SONG SONG với streaming
#   · kích hoạt cho ĐÚNG bảng cần, không phải restart connector
#   · đứt giữa chừng thì RESUME được
#   · dùng watermark để bản snapshot cũ KHÔNG BAO GIỜ ghi đè bản stream mới hơn
#
# ⚠️ GIỚI HẠN PHẢI BIẾT TRƯỚC KHI DỰA VÀO NÓ:
#   Nó khôi phục TRẠNG THÁI HIỆN TẠI, KHÔNG khôi phục LỊCH SỬ THAY ĐỔI. Dòng quay
#   lại mang `op=r` với giá trị tại thời điểm snapshot; những lần chuyển trạng thái
#   đã mất (PENDING -> COMPLETED -> REVERSED) thì mất luôn. Với bảng dim thì đủ;
#   với bảng fact cần đúng lịch sử thì phải lấy lại từ nguồn khác.
# =============================================================================
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 1

KAFKA_CONTAINER="${KAFKA_CONTAINER:-gtl-kafka}"
SIGNAL_TOPIC="gtl-signals"
# Key của message PHẢI bằng `topic.prefix` của connector. Debezium lọc signal theo
# key này để nhiều connector dùng chung một topic mà không giẫm chân nhau — gửi
# sai key thì message vào topic nhưng KHÔNG connector nào nhận, và không có lỗi nào.
TOPIC_PREFIX="gtl"
CONNECTOR="gtl-postgres-connector"

show_status() {
  echo "== SIGNAL ĐÃ GỬI =="
  docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic "$SIGNAL_TOPIC" \
    --from-beginning --timeout-ms 4000 --property print.key=true 2>/dev/null \
    || echo "  (chưa có signal nào)"
  echo
  echo "== TRẠNG THÁI CONNECTOR =="
  curl -s "http://localhost:8083/connectors/$CONNECTOR/status" 2>/dev/null \
    | python3 -c "
import json,sys
d=json.load(sys.stdin)
print(' connector:', d['connector']['state'])
for t in d.get('tasks',[]):
    print(f\"  task {t['id']}: {t['state']}\")
    if t.get('trace'): print('   ', t['trace'].splitlines()[0][:120])" 2>/dev/null \
    || echo "  (không đọc được — Kafka Connect chưa lên?)"
}

reset_state() {
  # Gỡ state snapshot kẹt khỏi offset của connector.
  #
  # VÌ SAO CẦN (phát hiện 05-08): signal `stop-snapshot` KHÔNG dọn được state.
  # Debezium log "Requested stop of snapshot" nhưng `incremental_snapshot_collections`
  # vẫn còn nguyên trong offset — và khi còn nguyên thì MỌI yêu cầu snapshot sau đó
  # bị CHẶN IM LẶNG: signal được nhận, không lỗi, không log, không chạy. Mất khá lâu
  # mới nhận ra vì mọi dấu hiệu bề mặt đều bình thường.
  #
  # Chỉ gỡ các khoá `incremental_snapshot_*`, GIỮ NGUYÊN lsn/txId/ts_usec -> không
  # mất vị trí WAL, không phải snapshot lại từ đầu.
  echo "== DỌN STATE SNAPSHOT KẸT =="
  echo "[1/3] dừng connector..."
  curl -s -X PUT "http://localhost:8083/connectors/$CONNECTOR/stop" >/dev/null
  for _ in $(seq 1 15); do
    [ "$(curl -s "http://localhost:8083/connectors/$CONNECTOR/status" \
        | python3 -c 'import json,sys;print(json.load(sys.stdin)["connector"]["state"])' 2>/dev/null)" = "STOPPED" ] && break
    sleep 2
  done

  echo "[2/3] gỡ khoá incremental_snapshot_* (giữ vị trí WAL)..."
  curl -s "http://localhost:8083/connectors/$CONNECTOR/offsets" | python3 -c "
import json, sys
d = json.load(sys.stdin); o = d['offsets'][0]
o['offset'] = {k: v for k, v in o['offset'].items() if not k.startswith('incremental_snapshot')}
print(json.dumps({'offsets': [o]}))" \
    | curl -s -X PATCH -H "Content-Type: application/json" --data @- \
      "http://localhost:8083/connectors/$CONNECTOR/offsets" >/dev/null

  echo "[3/3] bật lại connector..."
  curl -s -X PUT "http://localhost:8083/connectors/$CONNECTOR/resume" >/dev/null
  for _ in $(seq 1 20); do
    [ "$(curl -s "http://localhost:8083/connectors/$CONNECTOR/status" \
        | python3 -c 'import json,sys;print(json.load(sys.stdin)["connector"]["state"])' 2>/dev/null)" = "RUNNING" ] && break
    sleep 2
  done

  left=$(curl -s "http://localhost:8083/connectors/$CONNECTOR/offsets" \
        | python3 -c "
import json,sys
o=json.load(sys.stdin)['offsets'][0]['offset']
print(','.join(k for k in o if k.startswith('incremental')) or 'SẠCH')" 2>/dev/null)
  echo
  echo "khoá incremental còn lại: $left"
}

[ "${1:-}" = "--status" ] && { show_status; exit 0; }
[ "${1:-}" = "--reset" ]  && { reset_state; exit 0; }
[ $# -eq 0 ] && { sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'; exit 1; }

# Kiểm connector còn sống TRƯỚC khi gửi: signal vào một topic mà không ai đọc thì
# im lặng tuyệt đối — đúng kiểu hỏng tệ nhất khi đang xử lý sự cố.
state=$(curl -s "http://localhost:8083/connectors/$CONNECTOR/status" 2>/dev/null \
        | python3 -c "import json,sys;print(json.load(sys.stdin)['connector']['state'])" 2>/dev/null)
if [ "$state" != "RUNNING" ]; then
  echo "❌ connector đang '$state' (cần RUNNING). Signal gửi đi sẽ không ai nhận."
  echo "   Kiểm: bash scripts/cdc_resnapshot.sh --status"
  exit 1
fi

# Dựng danh sách bảng thành JSON array
collections=$(printf '"%s",' "$@" | sed 's/,$//')
payload="{\"type\":\"execute-snapshot\",\"data\":{\"data-collections\":[$collections],\"type\":\"INCREMENTAL\"}}"

echo "== YÊU CẦU SNAPSHOT LẠI =="
echo "  bảng   : $*"
echo "  topic  : $SIGNAL_TOPIC (key=$TOPIC_PREFIX)"
echo

printf '%s|%s\n' "$TOPIC_PREFIX" "$payload" | docker exec -i "$KAFKA_CONTAINER" \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic "$SIGNAL_TOPIC" \
  --property "parse.key=true" --property "key.separator=|" 2>/dev/null

if [ $? -eq 0 ]; then
  echo "✅ đã gửi signal."
  echo
  echo "Theo dõi (snapshot chạy nền, song song với streaming):"
  echo "  docker logs -f gtl-connect 2>&1 | grep -i 'incremental snapshot'"
  echo
  echo "Xác nhận data quay lại (dòng snapshot mang op='r'):"
  echo "  bash scripts/pipeline.sh status     # lag Bronze phải nhích lên rồi về 0"
  echo
  echo "⚠️ Bronze sẽ có dòng TRÙNG — đó là ĐÚNG. Silver khử trùng theo _kafka_offset"
  echo "   (giữ bản mới nhất mỗi khoá), nên Gold không đội số. Kiểm bằng đối soát:"
  echo "   bash scripts/dbt.sh build --select audit_reconciliation"
else
  echo "❌ gửi signal thất bại."
  exit 1
fi
