#!/usr/bin/env bash
# =============================================================================
# Verify Phase 1 Step 1a — checkpoint tự động (thay cho gõ psql tay).
# Exit != 0 nếu bất kỳ check nào fail -> dùng được làm CI gate sau này.
# =============================================================================
set -uo pipefail

CID="${PG_CONTAINER:-gtl-postgres}"
DBU="${POSTGRES_USER:-bank}"
DBN="${POSTGRES_DB:-banking}"

q() { docker exec "$CID" psql -U "$DBU" -d "$DBN" -tAc "$1" 2>/dev/null; }

pass=0; fail=0
ok() { echo "  ✅ $1"; pass=$((pass+1)); }
no() { echo "  ❌ $1"; fail=$((fail+1)); }

echo "== Phase 1 Step 1a — verify =="

# 1. Logical replication bật
[ "$(q "SHOW wal_level")" = "logical" ] && ok "wal_level = logical" || no "wal_level != logical"

# 2. Seed counts
acc=$(q "SELECT count(*) FROM accounts");     [ "${acc:-0}" -eq 100 ] && ok "accounts = 100" || no "accounts = ${acc}"
mer=$(q "SELECT count(*) FROM merchants");     [ "${mer:-0}" -eq 50 ]  && ok "merchants = 50"  || no "merchants = ${mer}"
txn=$(q "SELECT count(*) FROM transactions");  [ "${txn:-0}" -gt 0 ]   && ok "transactions = ${txn} (>0)" || no "transactions = 0"

# 3. Vòng đời status (INSERT+UPDATE+REVERSAL)
comp=$(q "SELECT count(*) FROM transactions WHERE status='COMPLETED'"); [ "${comp:-0}" -gt 0 ] && ok "COMPLETED = ${comp}" || no "no COMPLETED"
rev=$(q  "SELECT count(*) FROM transactions WHERE status='REVERSED'");  [ "${rev:-0}"  -gt 0 ] && ok "REVERSED = ${rev}"   || no "no REVERSED"

# 4. Trigger updated_at chạy (có hàng updated_at > created_at)
upd=$(q "SELECT count(*) FROM transactions WHERE updated_at > created_at"); [ "${upd:-0}" -gt 0 ] && ok "trigger updated_at fired (${upd} rows)" || no "updated_at trigger không chạy"

# 5. CDC least-privilege: role debezium có REPLICATION
[ "$(q "SELECT rolreplication FROM pg_roles WHERE rolname='debezium'")" = "t" ] && ok "role debezium (REPLICATION)" || no "role debezium thiếu/không REPLICATION"

# 6. Publication giới hạn đúng bảng
[ "$(q "SELECT count(*) FROM pg_publication WHERE pubname='dbz_publication'")" = "1" ] && ok "publication dbz_publication" || no "publication thiếu"

# 7. debezium KHÔNG ghi được (chỉ SELECT).
# Hứng output ra biến trước — KHÔNG pipe trực tiếp qua grep, vì docker exec thoát mã 1
# (psql lỗi) sẽ làm pipefail trả non-zero dù grep đã match -> false negative.
wr_out=$(docker exec "$CID" psql -U debezium -d "$DBN" -tAc \
          "INSERT INTO merchants(merchant_name,category,city) VALUES('x','x','x')" 2>&1 || true)
if echo "$wr_out" | grep -q "permission denied"; then
  ok "debezium bị chặn ghi (least-privilege đúng)"
else
  no "debezium ghi được — SAI least-privilege"
fi

# 8. Logical slot tạo/xóa được (CDC-ready)
if q "SELECT 1 FROM pg_create_logical_replication_slot('verify_slot','pgoutput')" >/dev/null \
   && q "SELECT pg_drop_replication_slot('verify_slot')" >/dev/null; then
  ok "logical slot create/drop (pgoutput)"
else
  no "không tạo/xóa được logical slot"
fi

echo "== PASS=${pass} FAIL=${fail} =="
[ "$fail" -eq 0 ]
