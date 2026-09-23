{{ config(severity='error', warn_if='>0', error_if='>1') }}

{#
  ⚠️ severity PHẢI là 'error' để hai mức hoạt động — bản cũ để 'warn' và điều đó
  biến `error_if` thành CODE CHẾT suốt từ Phase 4. dbt xử lý thế này:

      if severity == "ERROR" and result.should_error:   status = Fail
      elif result.should_warn:                          status = Warn

  Với severity='warn' thì nhánh đầu KHÔNG BAO GIỜ được vào, nên test chỉ cảnh báo
  dù `should_error` đã đúng. Bằng chứng trong log 22-09: test trả 2 dòng (tức
  2 > 1 = should_error true) mà dbt vẫn in "configured to warn if >0".

  Hệ quả: cổng chặn `push_marts` mà tài liệu mô tả CHƯA TỪNG chặn lần nào —
  đúng loại hỏng tệ nhất: cổng trông như đang gác, thật ra cửa vẫn mở.

  Giờ: 1 dòng -> WARN (1 > 1 sai)  ·  2 dòng -> ERROR (2 > 1 đúng).
#}

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

-- ⚠️ PHẠM VI THU HẸP 23-09 (issue #007). Bản cũ đo NULL trên TOÀN BỘ fact với giả
-- định "merchant_id NULL = join hỏng hoặc schema drift". Giả định đó hết đúng từ
-- khi chuyển khoản nội bộ và lương chính thức KHÔNG có merchant: ~16% NULL là
-- HỢP LỆ, và test bắn liên tục.
--
-- Cách sai là nới ngưỡng lên 20% — làm thế thì test vẫn xanh trong khi một lỗi
-- join thật ăn mất 15% merchant của giao dịch mua bán. Ngưỡng phải giữ nguyên;
-- thứ phải sửa là MẪU ĐO: chỉ những giao dịch ĐÁNG LẼ phải có merchant.
with stats as (
    select
        count(*)                                              as total_rows,
        sum(case when merchant_id is null then 1 else 0 end)  as null_rows,
        round(100.0 * sum(case when merchant_id is null then 1 else 0 end)
              / nullif(count(*), 0), 2)                       as null_pct
    from {{ ref('fact_transactions') }}
    where not is_deleted   -- đo chất lượng dữ liệu ĐANG PHỤC VỤ, không tính dòng đã purge
      and not is_internal_transfer
      and not is_payroll
)

select 'WARN: NULL merchant_id vượt 1%' as breach, *
from stats where null_pct > 1

union all

select 'ERROR: NULL merchant_id vượt 5% — nghi schema drift / join hỏng' as breach, *
from stats where null_pct > 5
