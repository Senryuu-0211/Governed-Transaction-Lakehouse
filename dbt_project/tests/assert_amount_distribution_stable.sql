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

-- ANOMALY #2 — DỊCH CHUYỂN PHÂN PHỐI SỐ TIỀN, hai mức độ.
--
-- Đây là loại lỗi tài chính NGUY HIỂM NHẤT mà mọi test cấu trúc đều cho qua:
-- hệ nguồn đổi đơn vị (đô -> xu), một tỷ giá sai, một cột bị nhân/chia nhầm.
-- Mỗi dòng vẫn dương, vẫn not-null, vẫn đúng kiểu -> 70+ test kia xanh hết.
-- Nhưng TỔNG DOANH THU sai gấp 100 lần và nó chảy thẳng lên dashboard của sếp.
-- Chỉ so hình dạng phân phối với quá khứ mới bắt được.
--
-- HAI MỨC: 1 dòng = lệch >30% (WARN — có thể do đổi cơ cấu khách/khuyến mãi);
--          2 dòng = lệch >60% (ERROR — nghi sai đơn vị tiền, CHẶN push_marts).
--
-- GUARD LỊCH SỬ: chưa đủ 7 ngày nền thì PASS, không bắn báo động giả.
-- Dải %: MINH HOẠ. Production tune từ độ lệch chuẩn thật của chính nghiệp vụ đó
-- (giá trị giao dịch ATM và giao dịch doanh nghiệp biến động rất khác nhau).

with daily as (
    select full_date, avg_completed_amount
    from {{ ref('mart_daily_volume') }}
),

latest as (
    -- LOẠI ngày hôm nay: nó LUÔN dở dang (mới chạy được vài giờ), nên đem so với
    -- các ngày đủ 24h thì lúc nào cũng "sụt". Đo thật 22-09: hôm nay 1.395 giao
    -- dịch vs ~100.000 của ngày thường -> test bắn vĩnh viễn, và một cảnh báo kêu
    -- oan thì tệ hơn không có cảnh báo. Chỉ chấm điểm ngày ĐÃ ĐÓNG SỔ.
    select max(full_date) as d from daily where full_date < current_date()
),

hist as (
    select
        avg(avg_completed_amount) as avg_7d,
        count(*)                  as history_days
    from daily, latest
    where daily.full_date <  latest.d
      and daily.full_date >= date_sub(latest.d, 7)
),

today as (
    select daily.full_date, daily.avg_completed_amount as avg_today
    from daily, latest
    where daily.full_date = latest.d
),

checked as (
    select today.full_date, today.avg_today, hist.avg_7d, hist.history_days
    from today cross join hist
    where hist.history_days >= 7      -- guard: chưa đủ nền thì im
      and hist.avg_7d > 0
)

select 'WARN: số tiền trung bình lệch hơn 30% so với 7 ngày' as breach, *
from checked
where avg_today > 1.3 * avg_7d or avg_today < 0.7 * avg_7d

union all

select 'ERROR: lệch hơn 60% — nghi sai đơn vị tiền tệ' as breach, *
from checked
where avg_today > 1.6 * avg_7d or avg_today < 0.4 * avg_7d
