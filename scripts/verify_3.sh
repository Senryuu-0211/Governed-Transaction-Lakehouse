#!/usr/bin/env bash
# =============================================================================
# Verify Phase 3 — Airflow orchestrate. Exit != 0 nếu có check fail.
# =============================================================================
set -uo pipefail
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKER="airflow-docker-airflow-worker-1"
AF() { docker exec "$WORKER" airflow "$@"; }

pass=0; fail=0
ok() { echo "  ✅ $1"; pass=$((pass+1)); }
no() { echo "  ❌ $1"; fail=$((fail+1)); }

echo "== Phase 3 — verify =="

# 1. Connection SSH tồn tại
AF connections get gtl_host_ssh >/dev/null 2>&1 && ok "connection gtl_host_ssh tồn tại" || no "thiếu connection gtl_host_ssh"

# 2. Không lỗi parse DAG
errs=$(AF dags list-import-errors 2>/dev/null | grep -E 'gtl_(transform|maintenance|common)' || true)
[ -z "$errs" ] && ok "DAG parse sạch (không import error)" || no "DAG có import error: $errs"

# 3. Hai DAG đã nạp
for d in gtl_transform gtl_maintenance; do
  AF dags list 2>/dev/null | grep -q "$d" && ok "DAG $d đã nạp" || no "DAG $d chưa nạp"
done

# 4. Cổng chặn được BẢO ĐẢM bởi cấu trúc: push_marts tồn tại trong gtl_transform
tr=$(AF tasks list gtl_transform 2>/dev/null | grep -c push_marts || echo 0)
[ "$tr" -ge 1 ] && ok "task push_marts tồn tại trong gtl_transform" || no "thiếu task push_marts"

# 5. SSH thông + cdc_health chạy thật trên host (đi qua đúng cầu SSH).
# Hứng output vào biến TRƯỚC rồi mới grep: tránh pipefail + grep -q (grep khớp
# xong đóng pipe -> airflow nhận SIGPIPE exit 141 -> pipefail báo fail giả).
cdc_out="$(AF tasks test gtl_transform cdc_health 2>&1)"
if printf '%s' "$cdc_out" | grep -q "freshest commit age"; then
  ok "cdc_health chạy qua SSH tới host (stream tươi)"
else
  no "cdc_health lỗi — kiểm cầu SSH hoặc stream đã chết"
fi

echo
echo "== PASS=${pass} FAIL=${fail} =="
[ "$fail" -eq 0 ]
