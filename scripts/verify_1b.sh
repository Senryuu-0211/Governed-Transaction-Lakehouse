#!/usr/bin/env bash
# =============================================================================
# Verify Phase 1 Step 1b — Debezium + Kafka. Exit != 0 nếu có check fail.
# =============================================================================
set -uo pipefail
K=/opt/kafka/bin
KC="${KAFKA_CONTAINER:-gtl-kafka}"
CONN="${CONNECTOR:-gtl-postgres-connector}"

pass=0; fail=0
ok() { echo "  ✅ $1"; pass=$((pass+1)); }
no() { echo "  ❌ $1"; fail=$((fail+1)); }

echo "== Phase 1 Step 1b — verify =="

# 1. Kafka Connect REST
curl -sf http://localhost:8083/connectors >/dev/null 2>&1 && ok "Kafka Connect REST up" || no "Connect REST down"

# 2. Connector + task RUNNING
st=$(curl -s "http://localhost:8083/connectors/$CONN/status" 2>/dev/null)
cstate=$(printf '%s' "$st" | python3 -c "import json,sys;print(json.load(sys.stdin)['connector']['state'])" 2>/dev/null || echo "?")
tstate=$(printf '%s' "$st" | python3 -c "import json,sys;d=json.load(sys.stdin);print(d['tasks'][0]['state'] if d.get('tasks') else 'NONE')" 2>/dev/null || echo "?")
[ "$cstate" = "RUNNING" ] && ok "connector RUNNING" || no "connector state=$cstate"
[ "$tstate" = "RUNNING" ] && ok "task RUNNING" || no "task state=$tstate"

# 3. Topic mỗi bảng
topics=$(docker exec "$KC" $K/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null)
for t in accounts merchants transactions; do
  printf '%s\n' "$topics" | grep -q "gtl.public.$t" && ok "topic gtl.public.$t" || no "topic gtl.public.$t thiếu"
done

# 4. transactions topic có message
off=$(docker exec "$KC" $K/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic gtl.public.transactions 2>/dev/null | awk -F: '{s+=$3} END{print s+0}')
[ "${off:-0}" -gt 0 ] && ok "transactions topic có ${off} messages" || no "transactions topic rỗng"

# 5. Debezium envelope hợp lệ (op/after/source)
msg=$(docker exec "$KC" $K/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
        --topic gtl.public.transactions --from-beginning --max-messages 1 --timeout-ms 10000 2>/dev/null | head -1)
if printf '%s' "$msg" | python3 -c "import json,sys;o=json.load(sys.stdin);assert all(k in o for k in ('op','after','source'))" 2>/dev/null; then
  ok "Debezium envelope hợp lệ (op/before/after/source)"
else
  no "envelope không hợp lệ"
fi

# 6. Replication slot active + giới hạn bảo vệ core DB (CDC risk #1)
slot=$(docker exec gtl-postgres psql -U bank -d banking -tAc "SELECT active FROM pg_replication_slots WHERE slot_name='debezium_slot'" 2>/dev/null)
[ "$slot" = "t" ] && ok "slot debezium_slot active" || no "slot không active (state=${slot:-none})"
lim=$(docker exec gtl-postgres psql -U bank -d banking -tAc "SHOW max_slot_wal_keep_size" 2>/dev/null)
[ "$lim" = "10GB" ] && ok "max_slot_wal_keep_size=10GB (bảo vệ core DB)" || no "slot limit=${lim:-?}"

echo "== PASS=${pass} FAIL=${fail} =="
[ "$fail" -eq 0 ]
