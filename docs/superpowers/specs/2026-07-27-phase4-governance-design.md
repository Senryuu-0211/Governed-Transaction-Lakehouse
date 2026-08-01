# Phase 4 — Governance & Data Quality nâng cao (Design Spec)

> Ngày: 2026-07-27 · Trạng thái: **đã duyệt thiết kế, chờ review spec** · Phần của: Governed Transaction Lakehouse

## 1. Mục tiêu & nguyên lý

Nâng governance từ "test cấu trúc" (Phase 2) lên **audit + đối soát tài chính + phát hiện bất thường thống kê + PII hoàn chỉnh** — thứ đòi phán đoán con người, phần "bán" mạnh nhất của một DE ngân hàng.

**Nguyên lý nền:** toàn bộ Phase 4 **cưỡi trên đường ray Phase 3** — KHÔNG tool mới, KHÔNG DAG mới, KHÔNG service mới. Reconciliation + anomaly là dbt model/test → tự chạy trong `dbt_run`/`dbt_test` của DAG `gtl_transform` sẵn có, tự động được cổng `all_success` chặn. Đây là cổ tức của kiến trúc Phase 3 sạch.

**Ngoài phạm vi (Phase khác):** alerting/notification (Phase 5 — giữ đúng thứ tự plan, dù đã thấy đau vụ stream chết âm thầm); DataHub/Trino (on-demand sau Phase 4); Great Expectations (đã CHỐT bỏ — anomaly làm dbt-native, không rước tool JD không đòi).

## 2. Reconciliation — tiền vào = tiền ra

**Sửa so với plan gốc:** plan ví dụ `SUM(amount)` trên `bronze.transactions` thô là SAI — Bronze là CDC raw (nhiều event/txn, có cả trước-ảnh, delete). Phải đối soát theo **trạng thái cuối**, tái dùng macro `cdc_latest_events` (đã có từ Silver).

- **Count reconcile:** số `txn_id` distinct ở Bronze (event cuối cùng, loại `op='d'`/deleted) **==** `COUNT(*)` `fact_transactions`. Lệch → mất hoặc nhân bản giao dịch.
- **Money reconcile:** `SUM(amount)` các txn trạng-thái-cuối `COMPLETED` ở Bronze **==** `SUM(net_amount)` Gold. Lệch → tiền méo qua transform. (net_amount = amount khi COMPLETED, 0 còn lại → định nghĩa khớp nhau.)

**Hiện thực:**
- Model `models/gold/audit_reconciliation.sql` — **incremental, append-only**. Config:
  `materialized='incremental'`, KHÔNG unique_key (không overwrite), **`full_refresh=false`** để model
  PHỚT LỜ cờ `--full-refresh` → sổ audit không bao giờ bị dựng lại/xoá lịch sử kể cả khi chạy
  `dbt build --full-refresh`. Mỗi run INSERT 1 dòng. Cột: `run_ts, run_date, count_bronze, count_gold,
  count_diff, amount_bronze, amount_gold, amount_diff`.
- Test `tests/assert_reconciliation.sql` — trả dòng (FAIL) nếu bản ghi mới nhất có `count_diff != 0 OR amount_diff != 0`. → cổng chặn.

## 3. Anomaly detection (statistical) — dbt-native, 3 singular test

Bắt cái dbt structural test không bắt được: "từng dòng hợp lệ nhưng cả lô bất thường".

- `tests/assert_rowcount_not_dropped.sql`: count fact hôm nay **≥ 70%** trung bình 7 ngày trước. Bắt upstream vỡ/mất luồng.
- `tests/assert_amount_distribution_stable.sql`: `AVG(net_amount)` các COMPLETED hôm nay nằm trong **±50%** trung bình trailing 7 ngày. Bắt vụ "đổi đơn vị đô→xu ×100" mà mọi dòng vẫn hợp lệ.
- `tests/assert_null_rate_low.sql`: NULL-rate của `merchant_id` trong fact **< 1%**. Bắt schema drift / join hỏng.

Ngưỡng (70%/±50%/1%) là mặc định minh hoạ trên data synthetic; ghi comment rõ "tuning theo lịch sử thật ở production".

## 4. Audit & time-travel (hướng A)

- **Sổ đối soát bền** = chính `audit_reconciliation` (mục 2). Append-only → **sống sót qua `expire_snapshots`** (Iceberg chỉ giữ ~10 snapshot). Trả lời "chứng minh ngày X tiền khớp" = 1 SELECT, bất kể snapshot đã hết hạn.
- **Time-travel:** script/notebook demo `SELECT * FROM gtl.bronze.transactions FOR TIMESTAMP AS OF '<T>'` + query `gtl.bronze.transactions.snapshots` / `.history`. Tài liệu hoá trong `docs/`.
- KHÔNG xây bảng audit tuỳ biến (hướng B) — Airflow + Iceberg đã ghi run/commit; không lặp.

## 5. PII hoàn chỉnh (phần lớn đã xong Phase 2)

- **Tài liệu hoá** tầng access: `docs/pii-governance.md` — bảng liệt kê từng field PII, kỹ thuật mask (hash/partial/generalize), ai thấy tầng nào (Bronze restricted → Silver masked → Gold aggregate).
- **Verify Gold sạch PII:** test `tests/assert_gold_no_raw_pii.sql` — khẳng định `dim_account` không có cột PII thô (name/national_id/full phone/email/dob), chỉ hash/mask/birth_year. Bảo vệ chống hồi quy khi ai đó sửa Silver→Gold.

## 6. Lineage

`dbt docs generate` (đã có) ra lineage source→Gold trace từng cột. Tài liệu hoá cách xem; đảm bảo mọi model/cột mới Phase 4 có `description` (giữ chuẩn "cột thiếu mô tả = fail" của verify_2).

## 7. Wiring — không đụng orchestration

- `audit_reconciliation` build trong `dbt_run` (đã có trong `gtl_transform`).
- 4 test mới (reconcile + 3 anomaly + gold-no-pii) chạy trong `dbt_test` → **tự động là cổng** (fail → push_marts skip). Không sửa DAG.

## 8. Nghiệm thu — `verify_4.sh`

1. Model `audit_reconciliation` tồn tại, incremental append-only, có ≥1 dòng sau 1 run.
2. `count_diff = 0` và `amount_diff = 0` ở run mới nhất (pipeline thật sự khớp).
3. Ép lệch giả (vd xoá 1 dòng Gold tạm) → `assert_reconciliation` **FAIL** (cổng hoạt động), rồi khôi phục.
4. 3 anomaly test có mặt và chạy (pass trên data hiện tại).
5. Time-travel: query `FOR TIMESTAMP AS OF` một snapshot cũ trả về data.
6. `assert_gold_no_raw_pii` pass; `docs/pii-governance.md` tồn tại.
7. `dbt docs generate` ra `catalog.json` có lineage.

## 9. Bẫy lường trước

- **append-only:** `audit_reconciliation` bảo vệ bằng CODE — `config(full_refresh=false)` khiến model
  phớt lờ cờ `--full-refresh` (không chỉ dặn "nhớ đừng"). `on_schema_change='append_new_columns'`,
  không unique_key. Muốn reset sổ trong dev = xoá tay có chủ đích.
- **Anomaly cần lịch sử:** 3 test so trailing 7 ngày — khi mới chạy (chưa đủ 7 ngày lịch sử) phải xử lý gracefully (coalesce/guard: chưa đủ lịch sử thì pass, không false-alarm).
- **Reconcile Bronze final-state:** phải dùng `cdc_latest_events` cho đúng trạng thái cuối, không đếm raw CDC event.

## 10. Nợ ghi nhận
- Alert khi reconcile/anomaly fail → Phase 5 (Grafana trên Prometheus sẵn có).
- Ngưỡng anomaly tuning theo phân phối thật → khi có data production.
