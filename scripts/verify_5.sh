#!/usr/bin/env bash
# =============================================================================
# Kiểm chứng Phase 5 — Observability.
#
# Kiểm CHUỖI, không kiểm từng mảnh. Một hệ giám sát chỉ có giá trị khi số liệu đi
# TRỌN đường từ nguồn tới cảnh báo; đứt ở đâu cũng là mù, mà mù kiểu "dashboard
# vẫn xanh" thì còn tệ hơn không có dashboard.
#
#   metrics_exporter.py -> .prom -> node_exporter -> Prometheus -> Grafana -> alert
#
# Chạy: bash scripts/verify_5.sh
# =============================================================================
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 1

MON_DIR="$HOME/working/projects/Resource-Monitoring-Dashboard"
METRICS_DIR="${GTL_METRICS_DIR:-$HOME/working/metrics}"
NODE="100.71.245.124:9100"
PROM="100.71.245.124:9090"
GRAF="100.71.245.124:3000"
GF_USER=$(grep '^GF_ADMIN_USER=' "$MON_DIR/.env" | cut -d= -f2)
GF_PASS=$(grep '^GF_ADMIN_PASSWORD=' "$MON_DIR/.env" | cut -d= -f2)

pass=0; fail=0
check() {  # check "mô tả" "lệnh" "chuỗi mong đợi"
  local out; out=$(eval "$2" 2>/dev/null)
  # Gán ra biến rồi mới grep, KHÔNG pipe thẳng: `set -o pipefail` + `grep -q` làm
  # grep thoát sớm -> SIGPIPE -> exit 141 -> báo FAIL oan (bài học verify_3.sh).
  if [[ "$out" == *"$3"* ]]; then
    printf '  ✅ %s\n' "$1"; pass=$((pass+1))
  else
    printf '  ❌ %s\n     mong đợi chứa: %s\n     nhận được   : %s\n' "$1" "$3" "${out:0:120}"
    fail=$((fail+1))
  fi
}

echo "== VERIFY PHASE 5 — OBSERVABILITY =="
echo
echo "[1/5] Nguồn: exporter ghi được file .prom"
check "exporter chạy không lỗi" \
      "~/working/gtl-spark-venv/bin/python scripts/metrics_exporter.py" "metric ->"
check "gtl_exporter_up = 1 (mọi nguồn số liệu đều lấy được)" \
      "grep '^gtl_exporter_up ' $METRICS_DIR/gtl.prom" "gtl_exporter_up 1"
check "có metric rủi ro #1 (WAL slot)" \
      "grep -c '^gtl_replication_slot_lag_bytes{' $METRICS_DIR/gtl.prom" "1"
check "có metric rủi ro #3 (mép retention, dạng TỶ LỆ)" \
      "grep -c '^gtl_kafka_retention_margin_ratio{' $METRICS_DIR/gtl.prom" "3"
check "cron đã cài (2 phút/lần)" "crontab -l" "metrics_exporter.py"

echo
echo "[2/5] Thu thập: node_exporter đọc được thư mục textfile"
check "textfile collector không lỗi đọc" \
      "curl -s http://$NODE/metrics | grep '^node_textfile_scrape_error'" \
      "node_textfile_scrape_error 0"
check "metric GTL xuất hiện trên /metrics của node_exporter" \
      "curl -s http://$NODE/metrics | grep -c '^gtl_'" "2"
# Chứng minh KHÔNG phá thứ đang có: dashboard host vẫn phải đủ metric như cũ.
check "metric host vẫn nguyên (>1000 series cpu/mem/disk/net)" \
      "curl -s http://$NODE/metrics | grep -cE '^node_(cpu|memory|filesystem|network|load)' | awk '\$1>1000{print \"OK\"}'" \
      "OK"

echo
echo "[3/5] Lưu trữ: Prometheus scrape được"
check "Prometheus có gtl_exporter_up" \
      "curl -s --data-urlencode 'query=gtl_exporter_up' http://$PROM/api/v1/query" \
      '"__name__":"gtl_exporter_up"'
check "target node đang UP" \
      "curl -s http://$PROM/api/v1/targets | python3 -c \"import json,sys;print([t['health'] for t in json.load(sys.stdin)['data']['activeTargets'] if t['labels']['job']=='node'][0])\"" \
      "up"

echo
echo "[4/5] Cảnh báo: Grafana nạp đủ luật + kênh gửi"
check "9 luật alert đã provision" \
      "curl -s -u '$GF_USER:$GF_PASS' http://$GRAF/api/v1/provisioning/alert-rules | python3 -c 'import json,sys;print(len(json.load(sys.stdin)))'" \
      "9"
check "contact point gtl-email tồn tại" \
      "curl -s -u '$GF_USER:$GF_PASS' http://$GRAF/api/v1/provisioning/contact-points" "gtl-email"
check "dashboard GTL Pipeline Health đã lên" \
      "curl -s -u '$GF_USER:$GF_PASS' 'http://$GRAF/api/search?query=Pipeline%20Health'" \
      "gtl-pipeline-health"
# Luật phải ĐANG ĐƯỢC ĐÁNH GIÁ, không chỉ tồn tại trên đĩa: một luật provision xong
# mà không chạy thì cũng im lặng y như không có luật.
check "các luật đang được đánh giá thật" \
      "curl -s -u '$GF_USER:$GF_PASS' http://$GRAF/api/prometheus/grafana/api/v1/rules | grep -c 'gtl-'" "1"

echo
echo "[5/5] Cổng maintenance window: tắt pipeline thì alert phải IM"
# Đây là phần dễ tưởng đúng nhất mà thực ra hay sai: nếu cổng không hoạt động thì
# mỗi đêm tắt pipeline cho đỡ tốn tiền S3 là sáng ra một hộp thư đầy cảnh báo, và
# người ta sẽ học cách phớt lờ alert — hỏng toàn bộ mục đích của Phase 5.
STATE="$METRICS_DIR/.pipeline_state"
SAVED=$(cat "$STATE" 2>/dev/null || echo "")
echo "0" > "$STATE"
~/working/gtl-spark-venv/bin/python scripts/metrics_exporter.py >/dev/null 2>&1
check "cờ hạ xuống 0 khi pipeline tắt" \
      "grep '^gtl_pipeline_enabled ' $METRICS_DIR/gtl.prom" "gtl_pipeline_enabled 0"

# Ghi xong file .prom KHÔNG có nghĩa Prometheus đã biết: nó scrape 15s/lần. Hỏi
# ngay lập tức là hỏi số liệu CŨ và test fail oan (đã dính lúc viết file này).
# Chờ tới khi Prometheus thật sự thấy giá trị mới rồi mới kiểm biểu thức cổng.
printf '     (chờ Prometheus scrape...)\n'
for _ in $(seq 1 12); do
  v=$(curl -s --data-urlencode 'query=gtl_pipeline_enabled' "http://$PROM/api/v1/query" \
      | python3 -c 'import json,sys;r=json.load(sys.stdin)["data"]["result"];print(r[0]["value"][1] if r else "")' 2>/dev/null)
  [ "$v" = "0" ] && break
  sleep 3
done

check "biểu thức có cổng trả VỀ RỖNG khi cờ = 0" \
      "curl -s --data-urlencode 'query=min(gtl_bronze_freshness_seconds) and on() (max(gtl_pipeline_enabled) == 1)' http://$PROM/api/v1/query | python3 -c 'import json,sys;print(\"RONG\" if not json.load(sys.stdin)[\"data\"][\"result\"] else \"CO_DU_LIEU\")'" \
      "RONG"
# Trả lại đúng trạng thái trước khi test, không để lại tác dụng phụ.
[ -n "$SAVED" ] && echo "$SAVED" > "$STATE" || rm -f "$STATE"
~/working/gtl-spark-venv/bin/python scripts/metrics_exporter.py >/dev/null 2>&1

echo
echo "================================"
printf 'KẾT QUẢ: %d PASS · %d FAIL\n' "$pass" "$fail"
[ "$fail" -eq 0 ] && echo "✅ Phase 5 thông suốt từ nguồn tới cảnh báo" \
                  || echo "❌ Còn mắt xích đứt — xem chi tiết ở trên"
exit $(( fail > 0 ))
