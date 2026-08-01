{{ config(severity='warn', warn_if='>0', error_if='>1') }}

-- ANOMALY #3 — TỶ LỆ NULL VỌT LÊN (schema drift / join hụt), hai mức độ.
--
-- Khác test `not_null` thông thường: not_null cấm TUYỆT ĐỐI mọi NULL, dùng cho khoá
-- chính. Còn ở đây `merchant_id` có thể null hợp lệ một cách lác đác (giao dịch
-- không qua merchant). Cấm tuyệt đối thì pipeline đỏ vì chuyện bình thường; không
-- kiểm gì thì bỏ lọt lúc nguồn đổi tên cột / join hỏng khiến null vọt lên 40%.
-- Vì vậy dùng NGƯỠNG TỶ LỆ: chấp nhận nhiễu tự nhiên, bắt sự cố hệ thống.
--
-- HAI MỨC: 1 dòng = >1% (WARN — theo dõi); 2 dòng = >5% (ERROR — nghi schema drift).
-- Không cần guard lịch sử: đây là ngưỡng tuyệt đối, không so với quá khứ.

with stats as (
    select
        count(*)                                              as total_rows,
        sum(case when merchant_id is null then 1 else 0 end)  as null_rows,
        round(100.0 * sum(case when merchant_id is null then 1 else 0 end)
              / nullif(count(*), 0), 2)                       as null_pct
    from {{ ref('fact_transactions') }}
    where not is_deleted   -- đo chất lượng dữ liệu ĐANG PHỤC VỤ, không tính dòng đã purge
)

select 'WARN: NULL merchant_id vượt 1%' as breach, *
from stats where null_pct > 1

union all

select 'ERROR: NULL merchant_id vượt 5% — nghi schema drift / join hỏng' as breach, *
from stats where null_pct > 5
