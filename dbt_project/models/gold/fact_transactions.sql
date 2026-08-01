{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='txn_id'
) }}

-- FACT — grain: MỘT dòng cho MỖI giao dịch từng tồn tại.
--
-- ⚠️ ĐỔI TỪ `table` SANG `incremental` (01-08). Bản cũ rebuild TRỌN từ Silver mỗi
-- run: idempotent bẩm sinh và rẻ ở quy mô demo, nhưng SAI ở hai điểm:
--   1. Trái chính tuyên bố "designed for bank-scale" — không ngân hàng nào dựng lại
--      hàng tỷ dòng giao dịch mỗi giờ. Transaction fact là loại nạp INCREMENTAL;
--      chỉ *periodic snapshot fact* (vd số dư cuối ngày) mới là snapshot theo kỳ.
--   2. Trên S3 THẬT nó là TIỀN: compute chạy on-prem nên mỗi lần đọc lại toàn bộ
--      Silver là data-transfer-out tính phí. Rebuild hourly ≈ 7TB egress/tháng.
-- MERGE (không phải append) vì `status` của giao dịch THAY ĐỔI theo vòng đời
-- (PENDING -> COMPLETED -> REVERSED) — khác transaction fact kinh điển vốn bất biến.
--
-- ⚠️ SOFT-DELETE: bản cũ lọc `where not is_deleted` nên dòng bị purge ở nguồn tự
-- biến mất khi rebuild. Với incremental thì KHÔNG: lô mới không chứa nó, MERGE
-- không đụng tới, dòng cũ ở lại vĩnh viễn -> sai số. Vì vậy fact giờ GIỮ dòng
-- soft-delete kèm cờ `is_deleted`, downstream lọc. Đúng tinh thần lakehouse có
-- kiểm toán: KHÔNG xoá dấu vết, chỉ đánh dấu — và nhất quán với Silver.
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
    t.updated_at,
    -- Cờ soft-delete: giao dịch đã bị purge ở nguồn. Downstream PHẢI lọc.
    t.is_deleted,
    -- Watermark incremental + dấu vết lineage: offset Kafka của event dựng nên dòng
    -- này. Dùng offset (không dùng thời gian) vì nó tất định — chạy lại ra đúng
    -- cùng kết quả, không phụ thuộc lúc job chạy.
    t._kafka_offset
from {{ ref('transactions') }} t

{% if is_incremental() %}
    -- Chỉ lấy giao dịch có thay đổi MỚI hơn thứ đã nằm trong fact.
    where t._kafka_offset > (select coalesce(max(_kafka_offset), -1) from {{ this }})
{% endif %}
