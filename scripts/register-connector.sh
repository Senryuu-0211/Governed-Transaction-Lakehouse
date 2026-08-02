#!/usr/bin/env bash
# =============================================================================
# Đăng ký Debezium Postgres connector qua Kafka Connect REST API (idempotent).
# Mật khẩu debezium lấy từ .env (không nằm trong file connector versioned).
# =============================================================================
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# Tách khai báo và gán: `export X="$(cmd)"` luôn trả mã thoát của EXPORT (0),
# nên .env thiếu khoá này thì script vẫn chạy tiếp với mật khẩu RỖNG và lỗi
# chỉ lộ ra ở Debezium với một thông báo chẳng liên quan (shellcheck SC2155).
DEBEZIUM_PASSWORD="$(grep -E '^DEBEZIUM_PASSWORD=' .env | cut -d= -f2-)"
export DEBEZIUM_PASSWORD

KAFKA_CONTAINER="${KAFKA_CONTAINER:-gtl-kafka}"
SIGNAL_TOPIC="gtl-signals"

echo ">> chờ Kafka Connect REST (localhost:8083)..."
until curl -sf http://localhost:8083/connectors >/dev/null 2>&1; do sleep 3; done

# Topic signal PHẢI tồn tại trước khi connector khởi động — nó subscribe ngay lúc
# start, và một topic thiếu chỉ hiện ra dưới dạng "signal gửi đi không ai nhận",
# tức là lúc SỰ CỐ THẬT mới biết là hỏng. Tạo ở đây để việc dựng lại từ đầu không
# phụ thuộc trí nhớ ai. 1 partition: thứ tự signal phải tuyệt đối.
echo ">> bảo đảm topic signal '$SIGNAL_TOPIC' tồn tại..."
docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic "$SIGNAL_TOPIC" --partitions 1 --replication-factor 1 >/dev/null 2>&1 \
  && echo "   ok" || echo "   ⚠️ không tạo được (Kafka chưa sẵn sàng?)"

# Nạp JSON + tiêm mật khẩu vào field database.password.
# LỌC BỎ các khoá `_comment*`: JSON không có cú pháp comment, nhưng file config
# này cần giải thích VÌ SAO (nhất là phần signal channel). Giải pháp: giữ chú
# thích trong file, gỡ ra trước khi gửi — Kafka Connect chỉ nhận giá trị CHUỖI
# nên một mảng chú thích sẽ làm hỏng request.
payload="$(python3 -c "
import json, os
c = json.load(open('debezium/connector-config.json'))
c['config'] = {k: v for k, v in c['config'].items() if not k.startswith('_comment')}
c['config']['database.password'] = os.environ['DEBEZIUM_PASSWORD']
print(json.dumps(c))")"
name="$(printf '%s' "$payload" | python3 -c "import json,sys; print(json.load(sys.stdin)['name'])")"

if curl -sf "http://localhost:8083/connectors/$name" >/dev/null 2>&1; then
  echo ">> connector '$name' đã tồn tại -> PUT cập nhật config"
  printf '%s' "$payload" | python3 -c "import json,sys; print(json.dumps(json.load(sys.stdin)['config']))" \
    | curl -s -X PUT -H "Content-Type: application/json" --data @- "http://localhost:8083/connectors/$name/config" >/dev/null
else
  echo ">> tạo connector '$name'"
  printf '%s' "$payload" | curl -s -X POST -H "Content-Type: application/json" --data @- http://localhost:8083/connectors >/dev/null
fi

sleep 3
echo ">> trạng thái connector:"
curl -s "http://localhost:8083/connectors/$name/status" | python3 -m json.tool
