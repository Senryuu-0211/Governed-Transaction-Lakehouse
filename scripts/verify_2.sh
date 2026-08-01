#!/usr/bin/env bash
# =============================================================================
# Verify Phase 2 — Transform Medallion (dbt Silver+Gold+marts) + serving.
# Exit != 0 nếu có check fail.
#
# Checkpoint theo PLAN_V2: Gold star schema · dbt tests pass · REVERSAL không
# double-count · dbt docs lineage · + serving (marts→Postgres→Superset).
# =============================================================================
set -uo pipefail
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT" || exit 1
set -a; . ./.env; set +a
CAT=http://localhost:8181
SUP=http://localhost:8088

pass=0; fail=0
ok() { echo "  ✅ $1"; pass=$((pass+1)); }
no() { echo "  ❌ $1"; fail=$((fail+1)); }

echo "== Phase 2 — verify =="

# 1. Bảng Silver/Gold/marts đã đăng ký trong Iceberg catalog
for ns in silver gold marts; do
  tbls=$(curl -s "$CAT/v1/namespaces/$ns/tables" 2>/dev/null)
  cnt=$(printf '%s' "$tbls" | grep -o '"name"' | wc -l)
  [ "$cnt" -ge 3 ] && ok "namespace $ns có $cnt bảng" || no "namespace $ns chỉ có $cnt bảng"
done
# Star schema: đủ fact + 4 dim
gold=$(curl -s "$CAT/v1/namespaces/gold/tables" 2>/dev/null)
for t in fact_transactions dim_account dim_merchant dim_date dim_channel; do
  printf '%s' "$gold" | grep -q "\"$t\"" && ok "gold.$t" || no "thiếu gold.$t"
done

# 2. dbt tests — HỢP ĐỒNG DATA gác cổng (gồm assert_reversed_never_counted)
echo "-- dbt test (contracts + business rules, ~1 phút) --"
tout=$(bash scripts/dbt.sh test 2>&1)
line=$(printf '%s' "$tout" | grep -E "Done\. PASS=" | tail -1)
if printf '%s' "$line" | grep -qE "ERROR=0 .* FAIL=0|PASS=[0-9]+ WARN=[0-9]+ ERROR=0"; then
  ok "dbt tests: $(printf '%s' "$line" | sed 's/.*Done\. //')"
else
  no "dbt tests fail: ${line:-không parse được}"
fi
# Business rule riêng: REVERSAL/FAILED/PENDING net=0
printf '%s' "$tout" | grep -q "PASS assert_reversed_never_counted\|assert_reversed_never_counted .* PASS" \
  && ok "REVERSAL không double-count (assert pass)" || no "kiểm REVERSAL không thấy pass"

# 3. Lineage artifact (dbt docs)
[ -s dbt_project/target/manifest.json ] && [ -s dbt_project/target/catalog.json ] \
  && ok "dbt lineage (manifest+catalog) tồn tại" || no "thiếu dbt docs artifact"

# 4. Marts trong Postgres + tươi
rows=$(docker exec -e PGPASSWORD="$MARTS_RO_PASSWORD" gtl-postgres psql -U marts_ro -d marts -tAc "SELECT count(*) FROM mart_daily_volume" 2>/dev/null)
[ "${rows:-0}" -gt 0 ] && ok "marts.mart_daily_volume có $rows dòng (Postgres)" || no "mart_daily_volume rỗng"
fresh=$(docker exec -e PGPASSWORD="$MARTS_RO_PASSWORD" gtl-postgres psql -U marts_ro -d marts -tAc "SELECT extract(epoch from now()-max(_refreshed_at))::int FROM mart_daily_volume" 2>/dev/null)
[ -n "$fresh" ] && ok "mart _refreshed_at ${fresh}s trước (freshness stamp có)" || no "không đọc được _refreshed_at"

# 5. marts_ro đúng least-privilege: chỉ SELECT, không ghi được
w=$(docker exec -e PGPASSWORD="$MARTS_RO_PASSWORD" gtl-postgres psql -U marts_ro -d marts -tAc "CREATE TABLE _ro_probe(x int)" 2>&1)
printf '%s' "$w" | grep -qi "permission denied" && ok "marts_ro bị chặn ghi (least-privilege)" || no "marts_ro ghi được (SAI quyền)"

# 6. Superset sống + query được marts (đường giao hàng business)
code=$(curl -s -o /dev/null -w '%{http_code}' "$SUP/health" 2>/dev/null)
[ "$code" = "200" ] && ok "Superset healthy ($SUP)" || no "Superset không phản hồi (HTTP ${code:-none})"
AT=$(curl -s "$SUP/api/v1/security/login" -H "Content-Type: application/json" \
     -d "{\"username\":\"admin\",\"password\":\"${SUPERSET_ADMIN_PASSWORD}\",\"provider\":\"db\"}" \
     2>/dev/null | python3 -c "import json,sys;print(json.load(sys.stdin).get('access_token',''))" 2>/dev/null)
if [ -n "$AT" ]; then
  ds=$(curl -s "$SUP/api/v1/dataset/" -H "Authorization: Bearer $AT" 2>/dev/null | python3 -c "import json,sys;print(json.load(sys.stdin).get('count',0))" 2>/dev/null)
  [ "${ds:-0}" -ge 3 ] && ok "Superset có $ds dataset trỏ marts" || no "Superset thiếu dataset marts (${ds:-0})"
else
  no "Superset login thất bại"
fi

echo
echo "== PASS=${pass} FAIL=${fail} =="
[ "$fail" -eq 0 ]
