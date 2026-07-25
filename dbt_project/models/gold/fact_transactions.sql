{{ config(materialized='table') }}

-- FACT — grain: MỘT dòng cho MỖI giao dịch từng tồn tại (trừ dòng nguồn đã purge).
-- Rebuild trọn từ Silver current-state mỗi run: idempotent bẩm sinh (chạy lại ra
-- đúng cùng kết quả) — nền cho backfill Phase 3.
--
-- LUẬT TIỀN THẬT (điểm đúng-sai của cả project, không chỉ là cột):
--   * chỉ COMPLETED là tiền đã chuyển  -> is_real_money
--   * REVERSED = đã chuyển rồi ĐẢO LẠI -> net về 0, KHÔNG được đếm hai lần
--   * FAILED/PENDING chưa từng chuyển  -> net 0
-- => net_amount = amount nếu COMPLETED, ngược lại 0. Mọi tổng doanh số downstream
--    (mart, dashboard, report) BẮT BUỘC đi qua net_amount, không SUM(amount) thô.

select
    t.txn_id,
    t.account_id,
    t.merchant_id,
    cast(date_format(t.created_at, 'yyyyMMdd') as int)     as date_key,
    t.channel,
    t.txn_type,
    t.status,
    t.amount,
    t.status = 'COMPLETED'                                 as is_real_money,
    case when t.status = 'COMPLETED' then t.amount
         else cast(0 as decimal(15,2)) end                 as net_amount,
    t.created_at,
    t.updated_at
from {{ ref('transactions') }} t
where not t.is_deleted
