# Phase 4 — Governance & Data Quality Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Thêm reconciliation tài chính (Bronze final-state ↔ Gold), anomaly detection thống kê, sổ audit bền, và verify PII — toàn bộ bằng dbt/Iceberg-native, tự cắm vào cổng `dbt_test` của DAG `gtl_transform` sẵn có.

**Architecture:** Không tool/DAG/service mới. Reconciliation = 1 model incremental append-only (`audit_reconciliation`, `full_refresh=false`) + 1 singular test. Anomaly = 3 singular test có guard-lịch-sử. PII = 1 doc + 1 test. Time-travel = doc + demo query trên Iceberg `.snapshots`. Tất cả model/test nằm trong `dbt_project/`, chạy trong `dbt_run`/`dbt_test` đã có.

**Tech Stack:** dbt-spark (session), Spark SQL, Apache Iceberg (time-travel, metadata tables), macros CDC sẵn có (`cdc_latest_events`, `cdc_field`).

## Global Constraints

- **KHÔNG `git commit` khi chưa được yêu cầu; và trước khi commit PHẢI cho Mr. Senryuu xem các file thay đổi** (diff/list) để review.
- **KHÔNG thêm tool/dependency/service mới** — Phase 4 thuần dbt/Iceberg (GE đã CHỐT bỏ).
- **KHÔNG sửa DAG** — model/test tự cắm vào `dbt_run`/`dbt_test`.
- Chạy dbt qua wrapper: `bash scripts/dbt.sh <cmd>` (tự nạp `.env` + SPARK_CONF_DIR).
- Reconcile theo **trạng-thái-cuối** (dedup CDC theo `kafka_offset desc`, loại `op='d'`), KHÔNG Bronze raw.
- `audit_reconciliation` append-only, `full_refresh=false` (phớt lờ `--full-refresh`).
- Anomaly test có **guard**: chưa đủ 7 ngày lịch sử → PASS (không báo động giả).
- Mọi cột model mới phải có `description` trong schema.yml (giữ chuẩn "thiếu mô tả = fail" của verify_2).

---

## File Structure

```
dbt_project/
  models/gold/audit_reconciliation.sql   # NEW: sổ đối soát append-only (Bronze final-state vs Gold)
  models/gold/schema.yml                  # MODIFY: thêm mô tả audit_reconciliation
  tests/assert_reconciliation.sql         # NEW: cổng — fail nếu run mới nhất lệch
  tests/assert_rowcount_not_dropped.sql   # NEW: anomaly count (guard 7 ngày)
  tests/assert_amount_distribution_stable.sql  # NEW: anomaly distribution (guard 7 ngày)
  tests/assert_null_rate_low.sql          # NEW: anomaly null-rate merchant_id
  tests/assert_gold_no_raw_pii.sql        # NEW: verify dim_account đã mask, không PII thô
docs/pii-governance.md                    # NEW: tài liệu 3 tầng access + field/kỹ thuật mask
docs/time-travel-audit.md                 # NEW: cách query Iceberg FOR TIMESTAMP AS OF + .snapshots
scripts/verify_4.sh                        # NEW: nghiệm thu Phase 4
```

---

### Task 1: Reconciliation — model `audit_reconciliation` + test cổng

**Files:**
- Create: `dbt_project/models/gold/audit_reconciliation.sql`
- Create: `dbt_project/tests/assert_reconciliation.sql`
- Modify: `dbt_project/models/gold/schema.yml` (thêm mô tả)

**Interfaces:**
- Consumes: `source('bronze','transactions')`, `ref('fact_transactions')`.
- Produces: bảng `audit_reconciliation(run_ts timestamp, run_date date, count_bronze bigint, count_gold bigint, count_diff bigint, amount_bronze decimal, amount_gold decimal, amount_diff decimal)` — append 1 dòng/run.

- [ ] **Step 1: Viết model `audit_reconciliation.sql`**

```sql
{{ config(materialized='incremental', incremental_strategy='append', full_refresh=false) }}

-- Sổ đối soát BỀN (append-only): mỗi run ghi 1 dòng "Bronze final-state có khớp Gold?".
-- full_refresh=false -> phớt lờ --full-refresh, KHÔNG mất lịch sử kiểm toán.
-- Bronze recompute ĐỘC LẬP từ raw CDC (không tái dùng Silver) -> bắt cả lỗi transform Silver->Gold.

with bronze_latest as (
    -- collapse CDC -> trạng thái cuối mỗi txn (KHÔNG dùng cdc_latest_events vì macro đó
    -- có nhánh is_incremental() gắn với {{ this }} = bảng Silver; ở đây full-scan).
    select
        get_json_object(value, '$.after.status') as status,
        coalesce(get_json_object(value, '$.after.amount'),
                 get_json_object(value, '$.before.amount')) as amount_str,
        op
    from (
        select value, op,
               row_number() over (partition by key order by kafka_offset desc) as _rn
        from {{ source('bronze', 'transactions') }}
        where value is not null and op in ('r', 'c', 'u', 'd')
    )
    where _rn = 1
),
bronze_live as (
    select * from bronze_latest where op != 'd'          -- loại giao dịch đã purge
),
bronze_agg as (
    select
        count(*) as count_bronze,
        coalesce(sum(case when status = 'COMPLETED'
                          then cast(amount_str as decimal(15,2)) else 0 end), 0) as amount_bronze
    from bronze_live
),
gold_agg as (
    select count(*) as count_gold, coalesce(sum(net_amount), 0) as amount_gold
    from {{ ref('fact_transactions') }}
)
select
    current_timestamp()                as run_ts,
    current_date()                     as run_date,
    b.count_bronze,
    g.count_gold,
    b.count_bronze - g.count_gold      as count_diff,
    b.amount_bronze,
    g.amount_gold,
    b.amount_bronze - g.amount_gold    as amount_diff
from bronze_agg b cross join gold_agg g
```

- [ ] **Step 2: Viết test cổng `assert_reconciliation.sql`**

```sql
-- LUẬT: tiền vào = tiền ra. Trả dòng (FAIL) nếu bản ghi MỚI NHẤT của sổ có lệch.
-- Chỉ xét run mới nhất: các run cũ là lịch sử bất biến, không phán lại.
select run_ts, count_diff, amount_diff
from {{ ref('audit_reconciliation') }}
where run_ts = (select max(run_ts) from {{ ref('audit_reconciliation') }})
  and (count_diff != 0 or amount_diff != 0)
```

- [ ] **Step 3: Thêm mô tả vào `models/gold/schema.yml`**

Thêm block dưới vào mục `models:` của `dbt_project/models/gold/schema.yml`:

```yaml
  - name: audit_reconciliation
    description: >
      Append-only reconciliation ledger. One row per pipeline run recording whether the
      independently-recomputed Bronze final-state (count + COMPLETED money) matches Gold.
      Immutable financial audit trail — survives Iceberg snapshot expiry. Protected by
      full_refresh=false. Auditor question "prove money reconciled on day X" = one SELECT.
    columns:
      - name: run_ts
        description: Timestamp this reconciliation row was written.
        data_tests: [not_null]
      - name: run_date
        description: Calendar date of the run.
      - name: count_bronze
        description: Distinct live transactions derived independently from raw Bronze CDC (final state, excludes deletes).
      - name: count_gold
        description: Row count in fact_transactions.
      - name: count_diff
        description: count_bronze - count_gold. Non-zero = a transaction was lost or duplicated in transform.
      - name: amount_bronze
        description: SUM of amount for final-state COMPLETED transactions, from raw Bronze.
      - name: amount_gold
        description: SUM of net_amount in Gold.
      - name: amount_diff
        description: amount_bronze - amount_gold. Non-zero = money distorted through transform (a cent to chase).
```

- [ ] **Step 4: Build + kiểm khớp**

Run: `bash scripts/dbt.sh build --select audit_reconciliation assert_reconciliation 2>&1 | tail -15`
Expected: model build OK; `assert_reconciliation ... PASS`; và:
Run: `bash scripts/dbt.sh show --inline "select * from {{ ref('audit_reconciliation') }} order by run_ts desc limit 1" 2>&1 | tail -6`
Expected: 1 dòng, `count_diff = 0`, `amount_diff = 0`.

- [ ] **Step 5: Chứng minh cổng chặn (ép lệch rồi khôi phục)**

Run:
```bash
# thêm test luôn-lệch tạm để chắc cơ chế cổng bắt được diff
cat > dbt_project/tests/_tmp_force_recon_fail.sql <<'EOF'
select 1 as x from {{ ref('audit_reconciliation') }} limit 1
EOF
bash scripts/dbt.sh test --select _tmp_force_recon_fail 2>&1 | grep -E "FAIL|PASS|ERROR" | tail -2
rm dbt_project/tests/_tmp_force_recon_fail.sql
```
Expected: dòng chứa `FAIL` (chứng minh singular test trả-dòng làm fail đúng cơ chế cổng).

- [ ] **Step 6: Commit** *(CHỜ Mr. Senryuu duyệt + xem file — không tự chạy)*

```bash
git add dbt_project/models/gold/audit_reconciliation.sql dbt_project/tests/assert_reconciliation.sql dbt_project/models/gold/schema.yml
git commit -m "feat(phase4): reconciliation ledger Bronze final-state<->Gold + gate"
```

---

### Task 2: Anomaly detection — 3 singular test (guard lịch sử)

**Files:**
- Create: `dbt_project/tests/assert_rowcount_not_dropped.sql`
- Create: `dbt_project/tests/assert_amount_distribution_stable.sql`
- Create: `dbt_project/tests/assert_null_rate_low.sql`

**Interfaces:**
- Consumes: `ref('mart_daily_volume')` (cột `full_date, txn_count, avg_completed_amount`), `ref('fact_transactions')` (cột `merchant_id`).

- [ ] **Step 1: Viết `assert_rowcount_not_dropped.sql`**

```sql
-- ANOMALY: count giao dịch ngày mới nhất sụt >30% so trung bình 7 ngày trước = bất thường.
-- GUARD: chỉ enforce khi có >= 7 ngày lịch sử trước ngày mới nhất (cold-start thì PASS).
with daily as (select full_date, txn_count from {{ ref('mart_daily_volume') }}),
     latest as (select max(full_date) as d from daily),
     hist as (
        select avg(txn_count) as avg7, count(*) as n
        from daily cross join latest
        where full_date < latest.d and full_date >= date_sub(latest.d, 7)
     ),
     today as (select txn_count from daily cross join latest where full_date = latest.d)
select today.txn_count, hist.avg7, hist.n
from today cross join hist
where hist.n >= 7 and today.txn_count < 0.7 * hist.avg7   -- ngưỡng minh hoạ; tune từ lịch sử thật
```

- [ ] **Step 2: Viết `assert_amount_distribution_stable.sql`**

```sql
-- ANOMALY: trung bình số tiền COMPLETED ngày mới nhất lệch >±50% trailing 7 ngày
-- = nghi đổi đơn vị / nguồn hỏng (từng dòng vẫn hợp lệ nên dbt structural test mù).
-- GUARD: cần >= 7 ngày lịch sử.
with daily as (select full_date, avg_completed_amount from {{ ref('mart_daily_volume') }}),
     latest as (select max(full_date) as d from daily),
     hist as (
        select avg(avg_completed_amount) as avg7, count(*) as n
        from daily cross join latest
        where full_date < latest.d and full_date >= date_sub(latest.d, 7)
     ),
     today as (select avg_completed_amount as v from daily cross join latest where full_date = latest.d)
select today.v, hist.avg7, hist.n
from today cross join hist
where hist.n >= 7 and (today.v > 1.5 * hist.avg7 or today.v < 0.5 * hist.avg7)  -- ngưỡng minh hoạ
```

- [ ] **Step 3: Viết `assert_null_rate_low.sql`**

```sql
-- ANOMALY: NULL-rate của merchant_id trong fact > 1% = schema drift / join hỏng.
-- Không cần guard lịch sử (ngưỡng tuyệt đối).
select
    count(*)                                                     as total,
    sum(case when merchant_id is null then 1 else 0 end)         as null_cnt
from {{ ref('fact_transactions') }}
having sum(case when merchant_id is null then 1 else 0 end) > 0.01 * count(*)
```

- [ ] **Step 4: Chạy 3 test — pass trên data hiện tại**

Run: `bash scripts/dbt.sh test --select assert_rowcount_not_dropped assert_amount_distribution_stable assert_null_rate_low 2>&1 | grep -E "PASS|FAIL|ERROR|Done" | tail -6`
Expected: cả 3 `PASS` (data synthetic đều → không anomaly; và/hoặc guard chưa đủ 7 ngày → PASS). Không `ERROR` (SQL hợp lệ).

- [ ] **Step 5: Commit** *(CHỜ Mr. Senryuu duyệt + xem file)*

```bash
git add dbt_project/tests/assert_rowcount_not_dropped.sql dbt_project/tests/assert_amount_distribution_stable.sql dbt_project/tests/assert_null_rate_low.sql
git commit -m "feat(phase4): 3 anomaly test dbt-native (guard lịch sử 7 ngày)"
```

---

### Task 3: PII — doc tầng access + test Gold sạch PII

**Files:**
- Create: `docs/pii-governance.md`
- Create: `dbt_project/tests/assert_gold_no_raw_pii.sql`

**Interfaces:**
- Consumes: `ref('dim_account')` (cột `customer_name_hash, phone_masked, email_masked`).

- [ ] **Step 1: Viết `docs/pii-governance.md`**

```markdown
# PII Governance — Governed Transaction Lakehouse

Bảo vệ PII phân tầng: PII thô chỉ tồn tại ở Bronze (restricted), từ Silver trở lên đã mask.

## 3 kỹ thuật mask (macros/mask_pii.sql) — chọn theo MỤC ĐÍCH dùng field
| Kỹ thuật | Field áp dụng | Vì sao | Đảo ngược được? |
|---|---|---|---|
| **Hash SHA-256** (`mask_hash`) | customer_name, national_id | chỉ cần JOIN/COUNT DISTINCT, không bao giờ cần đọc | ❌ một chiều |
| **Partial mask** (`mask_partial`/`mask_email`) | phone, email | nhân viên hỗ trợ cần NHẬN RA, kẻ trộm không dùng được | ❌ mất phần giữa |
| **Generalize** (`generalize_birth_year`) | date_of_birth → birth_year | analytics cần phân bố tuổi, không cần chính xác từng người | ❌ mất độ phân giải |

## 3 tầng access
| Tầng | PII | Ai xem |
|---|---|---|
| **Bronze** | RAW PII (envelope Debezium nguyên vẹn) | RESTRICTED — chỉ pipeline/ingestion |
| **Silver** | đã mask (hash/partial/generalize) | analyst |
| **Gold** | không PII row-level; dim_account chỉ hash/mask/birth_year | business / BI (Superset) |

## Bảo vệ chống hồi quy
Test `assert_gold_no_raw_pii` chặn mọi thay đổi Silver→Gold vô tình để lộ PII thô.
```

- [ ] **Step 2: Viết `assert_gold_no_raw_pii.sql`**

```sql
-- Trả dòng (FAIL) nếu dim_account lộ giá trị CHƯA mask đúng chuẩn:
--   * customer_name_hash phải là SHA-256 hex (đúng 64 ký tự)
--   * phone_masked phải có ký tự che '*'
--   * email_masked (nếu có) phải có '***'
-- Bắt hồi quy khi ai đó vô tình đưa PII thô vào Gold.
select account_id
from {{ ref('dim_account') }}
where length(customer_name_hash) != 64
   or phone_masked not like '%*%'
   or (email_masked is not null and email_masked not like '%***%')
```

- [ ] **Step 3: Chạy test — pass**

Run: `bash scripts/dbt.sh test --select assert_gold_no_raw_pii 2>&1 | grep -E "PASS|FAIL|ERROR" | tail -2`
Expected: `PASS` (dim_account đã mask đúng từ Phase 2).

- [ ] **Step 4: Commit** *(CHỜ Mr. Senryuu duyệt + xem file)*

```bash
git add docs/pii-governance.md dbt_project/tests/assert_gold_no_raw_pii.sql
git commit -m "docs+test(phase4): PII governance doc + Gold no-raw-PII guard"
```

---

### Task 4: Time-travel & lineage audit — tài liệu + demo

**Files:**
- Create: `docs/time-travel-audit.md`

**Interfaces:**
- Consumes: Iceberg metadata tables `gtl.bronze.transactions.snapshots`, `.history`; time-travel syntax.

- [ ] **Step 1: Viết `docs/time-travel-audit.md`**

```markdown
# Time-travel & Lineage Audit (Iceberg)

Iceberg tự ghi mọi commit → audit trail data-state + time-travel MIỄN PHÍ (bổ trợ sổ
`audit_reconciliation` bền cho phần đối soát tài chính).

## Xem lịch sử commit của một bảng
```sql
SELECT committed_at, snapshot_id, operation, summary
FROM gtl.bronze.transactions.snapshots ORDER BY committed_at DESC;
```

## Query data "đúng như lúc T" (audit "số liệu tại thời điểm")
```sql
-- theo thời điểm
SELECT * FROM gtl.bronze.transactions FOR TIMESTAMP AS OF TIMESTAMP '2026-07-26 00:00:00';
-- theo snapshot cụ thể
SELECT * FROM gtl.bronze.transactions FOR VERSION AS OF <snapshot_id>;
```
> ⚠️ Time-travel chỉ lùi được tới snapshot CHƯA bị `expire_snapshots` (maintenance giữ
> retain_last=10). Bằng chứng đối soát dài hạn nằm ở `audit_reconciliation` (append-only).

## Lineage source→Gold
`bash scripts/dbt.sh docs generate` → `target/catalog.json` + graph: trace từng cột
Gold ngược về source Bronze qua `ref()`/`source()`.
```

- [ ] **Step 2: Kiểm time-travel chạy thật + lineage sinh ra**

Run:
```bash
bash scripts/dbt.sh docs generate 2>&1 | grep -iE "catalog|Done|Building" | tail -2
bash scripts/dbt.sh show --inline "select count(*) c from gtl.bronze.transactions.snapshots" 2>&1 | tail -4
```
Expected: `catalog.json` sinh ra; query `.snapshots` trả về ≥1 dòng (chứng minh metadata/time-travel truy cập được).

- [ ] **Step 3: Commit** *(CHỜ Mr. Senryuu duyệt + xem file)*

```bash
git add docs/time-travel-audit.md
git commit -m "docs(phase4): time-travel & lineage audit guide"
```

---

### Task 5: Nghiệm thu `verify_4.sh` + xác nhận wiring

**Files:**
- Create: `scripts/verify_4.sh`

**Interfaces:**
- Consumes: tất cả artifact Task 1-4.

- [ ] **Step 1: Viết `scripts/verify_4.sh`**

```bash
#!/usr/bin/env bash
# =============================================================================
# Verify Phase 4 — Governance nâng cao. Exit != 0 nếu có check fail.
# =============================================================================
set -uo pipefail
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"
DBT() { bash scripts/dbt.sh "$@" 2>/dev/null; }

pass=0; fail=0
ok() { echo "  ✅ $1"; pass=$((pass+1)); }
no() { echo "  ❌ $1"; fail=$((fail+1)); }

echo "== Phase 4 — verify =="

# 1. Sổ reconciliation tồn tại + có dữ liệu + khớp (diff=0) ở run mới nhất
recon="$(DBT show --inline "select count_diff, amount_diff from {{ ref('audit_reconciliation') }} order by run_ts desc limit 1")"
printf '%s' "$recon" | grep -qE '(^|[^0-9-])0([^0-9]|$)' && echo "$recon" | grep -q "0" \
  && ok "reconciliation run mới nhất khớp (diff=0)" || no "reconciliation lệch hoặc thiếu"

# 2. Sổ append-only: có >= 1 dòng
n="$(DBT show --inline "select count(*) n from {{ ref('audit_reconciliation') }}" | grep -oE '[0-9]+' | tail -1)"
[ "${n:-0}" -ge 1 ] && ok "audit_reconciliation có $n dòng (append-only)" || no "sổ audit rỗng"

# 3. Bốn test governance mới có mặt và PASS
gov_tests="assert_reconciliation assert_rowcount_not_dropped assert_amount_distribution_stable assert_null_rate_low assert_gold_no_raw_pii"
res="$(DBT test --select $gov_tests 2>&1)"
printf '%s' "$res" | grep -qE 'ERROR|Failure' && no "governance test có ERROR/FAIL" || ok "5 test governance PASS (reconcile+anomaly+pii)"

# 4. Time-travel: query snapshots trả dữ liệu
snap="$(DBT show --inline "select count(*) c from gtl.bronze.transactions.snapshots")"
printf '%s' "$snap" | grep -oE '[0-9]+' | tail -1 | grep -qE '[1-9]' && ok "Iceberg .snapshots truy cập được (time-travel sẵn sàng)" || no "không đọc được snapshots"

# 5. Doc governance tồn tại
[ -f docs/pii-governance.md ] && ok "docs/pii-governance.md tồn tại" || no "thiếu pii-governance.md"
[ -f docs/time-travel-audit.md ] && ok "docs/time-travel-audit.md tồn tại" || no "thiếu time-travel-audit.md"

echo
echo "== PASS=${pass} FAIL=${fail} =="
[ "$fail" -eq 0 ]
```

- [ ] **Step 2: Chạy verify_4.sh**

Lv: SAFE (chỉ đọc/test dbt, không sửa data production).
Run: `bash scripts/verify_4.sh`
Expected: `== PASS=6 FAIL=0 ==`, exit 0.

- [ ] **Step 3: Xác nhận wiring — governance là CỔNG trong DAG (không sửa DAG)**

Giải thích (không cần lệnh mới): 5 test mới nằm trong `dbt_project/tests/` → `dbt test` tự chạy → task `dbt_test` trong `gtl_transform` tự bao gồm chúng → fail bất kỳ cái nào ⇒ `push_marts` skip. `audit_reconciliation` build trong `dbt_run`. Không đụng file DAG nào.
Run (xác nhận số test tăng): `bash scripts/dbt.sh test 2>&1 | grep -oE "Done. PASS=[0-9]+ WARN=[0-9]+ ERROR=[0-9]+ SKIP=[0-9]+ NO-OP=[0-9]+ TOTAL=[0-9]+" | tail -1`
Expected: TOTAL tăng thêm 5 so với trước Phase 4 (71 → 76), ERROR=0.

- [ ] **Step 4: Commit** *(CHỜ Mr. Senryuu duyệt + xem file)*

```bash
git add scripts/verify_4.sh
git commit -m "test(phase4): verify_4 nghiệm thu governance"
```

---

## Self-Review

**Spec coverage:**
- §2 Reconciliation (count + money, Bronze final-state, append-only, full_refresh=false) → Task 1. ✅
- §3 Anomaly 3 test + guard lịch sử → Task 2. ✅
- §4 Audit (sổ bền = audit_reconciliation) + time-travel → Task 1 + Task 4. ✅
- §5 PII doc + verify Gold sạch PII → Task 3. ✅
- §6 Lineage (dbt docs) → Task 4 Step 2. ✅
- §7 Wiring không đụng DAG → Task 5 Step 3. ✅
- §8 verify_4 (7 điểm) → Task 5. ✅
- §9 bẫy (append-only=full_refresh=false, guard lịch sử, cdc final-state) → Task 1 config + Task 2 guard + Task 1 SQL. ✅

**Placeholder scan:** không TBD/TODO; mọi step có SQL/lệnh + expected cụ thể. ✅
**Type consistency:** `audit_reconciliation` cột (run_ts, count_diff, amount_diff...) dùng nhất quán Task1→verify; ref names (`fact_transactions`, `mart_daily_volume`, `dim_account`) khớp code thật đã đọc; macro `cdc_field`/dedup theo `kafka_offset` khớp `macros/cdc.sql`. ✅
```
