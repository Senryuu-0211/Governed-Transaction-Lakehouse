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

-- ANOMALY #1 — SỤT SẢN LƯỢNG, hai mức độ.
--
-- Bắt loại lỗi mà test cấu trúc MÙ HOÀN TOÀN: mọi dòng còn lại đều hợp lệ
-- (unique/not_null/relationships đều xanh), nhưng MỘT KHỐI dữ liệu đã biến mất —
-- nguồn ngừng bơm, connector chết, một chi nhánh ngừng gửi. Không ai "sai", chỉ là
-- thiếu. Chỉ phép so với chính mình trong quá khứ mới thấy.
--
-- HAI MỨC (đổi 01-08): trả 1 dòng cho MỖI ngưỡng bị vượt, dbt map số dòng -> mức:
--   1 dòng  = giảm >20%  -> WARN  : bất thường nhưng có thể vẫn bình thường
--                                   (cuối tuần, lễ tết, hết mùa khuyến mãi)
--   2 dòng  = giảm >40%  -> ERROR : gần như chắc chắn hỏng -> CHẶN push_marts
-- Vì sao cần tách: nếu mọi sai lệch đều hard-fail thì volume tụt tự nhiên ngày lễ
-- cũng chặn báo cáo. Vài lần như vậy là người ta bắt đầu phớt lờ đèn đỏ, và cổng
-- chất lượng mất hết uy tín. Tách mức để MÀU ĐỎ LUÔN CÓ NGHĨA.
--
-- GUARD LỊCH SỬ: chỉ enforce khi đã có ĐỦ 7 ngày nền. Lúc pipeline mới dựng/vừa
-- reset thì chưa có gì để so -> phải IM, không được bắn.
--
-- Ngưỡng 20%/40%: MINH HOẠ trên data synthetic. Production phải tune từ phân phối
-- thật — sản lượng ngân hàng dao động theo thứ trong tuần / ngày lương / lễ tết.

with daily as (
    select full_date, txn_count
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
        avg(txn_count) as avg_7d,
        count(*)       as history_days
    from daily, latest
    where daily.full_date <  latest.d
      and daily.full_date >= date_sub(latest.d, 7)
),

today as (
    select daily.full_date, daily.txn_count
    from daily, latest
    where daily.full_date = latest.d
),

checked as (
    select today.full_date, today.txn_count, hist.avg_7d, hist.history_days
    from today cross join hist
    where hist.history_days >= 7      -- guard: chưa đủ nền thì im
      and hist.avg_7d > 0
)

select 'WARN: sản lượng giảm hơn 20% so với trung bình 7 ngày' as breach, *
from checked where txn_count < 0.8 * avg_7d

union all

select 'ERROR: sản lượng giảm hơn 40% — nghi mất luồng dữ liệu' as breach, *
from checked where txn_count < 0.6 * avg_7d
