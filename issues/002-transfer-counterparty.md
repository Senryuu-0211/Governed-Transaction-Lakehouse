# Issue #002 — TRANSFER chưa có tài khoản đích (counterparty)

**Trạng thái:** OPEN
**Phát hiện:** Phase 1 Step 1a (2026-07-08)
**Ảnh hưởng:** Realism nghiệp vụ + logic Gold (Phase 2)
**Mức:** Low-Medium (cải tiến schema, làm khi cần chuyển khoản đúng nghĩa)

## Vấn đề
Schema `transactions` chỉ có `account_id` (tài khoản khởi tạo) + `merchant_id`. Với
`txn_type='TRANSFER'` (chuyển khoản liên tài khoản) KHÔNG có cột **tài khoản đích**.

Hiện faker mô hình TRANSFER như **dòng tiền RA** khỏi `account_id` (giống DEBIT) — nhất quán
với hàng dữ liệu, nhưng chưa phản ánh việc tiền **vào** một tài khoản khác.

## Hướng xử lý (khi cần)
- Thêm cột `counterparty_account_id BIGINT NULL REFERENCES accounts(account_id)` (NULL cho
  giao dịch merchant, NOT NULL cho TRANSFER).
- Faker: TRANSFER completed → trừ `account_id`, cộng `counterparty_account_id` (2 UPDATE accounts
  → 2 CDC event, đúng như chuyển khoản thật).
- Gold (Phase 2): đảm bảo TRANSFER không phồng tổng volume (tiền chỉ dịch chuyển, không sinh thêm).

## Ghi chú
- Chưa làm ở 1a để giữ schema tối giản. Ưu tiên thấp — nâng khi làm phần reconciliation/Gold.
