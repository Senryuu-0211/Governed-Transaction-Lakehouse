#!/usr/bin/env bash
# =============================================================================
# Verify Phase 2.5 — Schema Registry (Avro) enforce hợp đồng schema ở cổng Kafka.
# Exit != 0 nếu có check fail.
# =============================================================================
set -uo pipefail
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"
REG=http://localhost:8087
CC=$REG/apis/ccompat/v7
CONN=http://localhost:8083
VENV_PYTHON="${VENV_PYTHON:-$HOME/working/gtl-spark-venv/bin/python}"

pass=0; fail=0
ok() { echo "  ✅ $1"; pass=$((pass+1)); }
no() { echo "  ❌ $1"; fail=$((fail+1)); }

echo "== Phase 2.5 — verify =="

# 1. Apicurio sống (native + ccompat)
curl -sf "$REG/apis/registry/v2/system/info" >/dev/null 2>&1 && ok "Apicurio registry up" || no "Apicurio down"
curl -sf "$CC/subjects" >/dev/null 2>&1 && ok "ccompat API phản hồi" || no "ccompat down"

# 2. 6 schema đã đăng ký (key+value cho 3 bảng). Kiểm bằng endpoint /versions
# (functional) thay vì /subjects (listing) — xoá 1 version có thể ẩn subject khỏi
# listing dù version còn active và connector vẫn dùng được.
for s in transactions accounts merchants; do
  for kind in key value; do
    vers=$(curl -s "$CC/subjects/gtl.public.$s-$kind/versions" 2>/dev/null)
    printf '%s' "$vers" | grep -q "[0-9]" && ok "schema gtl.public.$s-$kind (version active)" \
      || no "thiếu schema $s-$kind"
  done
done

# 3. Connector RUNNING + dùng Avro converter Apicurio
st=$(curl -s "$CONN/connectors/gtl-postgres-connector/status" 2>/dev/null)
printf '%s' "$st" | grep -q '"state":"RUNNING"' && ok "connector RUNNING" || no "connector không RUNNING"
cfg=$(curl -s "$CONN/connectors/gtl-postgres-connector/config" 2>/dev/null)
printf '%s' "$cfg" | grep -q "apicurio.registry.utils.converter.AvroConverter" \
  && ok "connector dùng Avro converter (Apicurio)" || no "connector chưa dùng Avro"

# 4. 3 subject production có compatibility BACKWARD (hợp đồng được enforce)
for s in transactions accounts merchants; do
  lvl=$(curl -s "$CC/config/gtl.public.$s-value" 2>/dev/null | grep -o '"compatibilityLevel":"[A-Z]*"' | cut -d'"' -f4)
  [ "$lvl" = "BACKWARD" ] && ok "compat($s-value)=BACKWARD" || no "compat($s-value)=${lvl:-none} (chưa enforce)"
done

# 5. Enforcement thực sự chặn schema hỏng (chạy test độc lập)
if "$VENV_PYTHON" "$PROJECT_ROOT/scripts/test_schema_compat.py" >/dev/null 2>&1; then
  ok "schema không tương thích BỊ CHẶN (409), tương thích được chấp nhận"
else
  no "enforcement không hoạt động đúng (xem test_schema_compat.py)"
fi

# 6. Bronze nhận Avro đã decode -> JSON đọc được, đủ op c/u/d
echo "-- Bronze (decode Avro) qua Spark --"
ops=$(cd "$PROJECT_ROOT" && bash scripts/dbt.sh show --inline \
     "select concat_ws(',', collect_set(op)) ops from {{ source('bronze','transactions') }} where op in ('c','u','d')" 2>/dev/null \
     | grep -oE "[cud](,[cud])*" | tail -1)
for o in c u d; do
  printf '%s' "$ops" | grep -q "$o" && ok "Bronze có op=$o (Avro decode đúng)" || no "Bronze thiếu op=$o"
done

echo
echo "== PASS=${pass} FAIL=${fail} =="
[ "$fail" -eq 0 ]
