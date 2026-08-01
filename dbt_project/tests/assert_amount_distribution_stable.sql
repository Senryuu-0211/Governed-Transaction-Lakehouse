{{ config(severity='warn', warn_if='>0', error_if='>1') }}

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
    select max(full_date) as d from daily
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
