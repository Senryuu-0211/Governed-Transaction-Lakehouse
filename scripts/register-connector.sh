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

echo ">> chờ Kafka Connect REST (localhost:8083)..."
until curl -sf http://localhost:8083/connectors >/dev/null 2>&1; do sleep 3; done

# Nạp JSON + tiêm mật khẩu vào field database.password
payload="$(python3 -c "import json,os; c=json.load(open('debezium/connector-config.json')); c['config']['database.password']=os.environ['DEBEZIUM_PASSWORD']; print(json.dumps(c))")"
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
