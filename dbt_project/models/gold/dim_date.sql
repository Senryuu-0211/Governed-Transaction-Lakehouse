{{ config(materialized='table') }}

-- Date spine — sinh từ ngày giao dịch sớm nhất tới hôm nay (rebuild mỗi run nên
-- tự dài ra theo thời gian). Thuộc tính lịch để BI cắt theo tuần/tháng/quý mà
-- không tự chế logic ngày tháng ở dashboard.

with bounds as (
    select date(min(created_at)) as start_date
    from {{ ref('transactions') }}
),

spine as (
    select explode(sequence(start_date, current_date())) as d
    from bounds
)

select
    cast(date_format(d, 'yyyyMMdd') as int) as date_key,
    d                                       as full_date,
    year(d)                                 as year,
    quarter(d)                              as quarter,
    month(d)                                as month,
    date_format(d, 'MMMM')                  as month_name,
    day(d)                                  as day_of_month,
    dayofweek(d)                            as day_of_week,
    date_format(d, 'EEEE')                  as day_name,
    weekofyear(d)                           as week_of_year,
    dayofweek(d) in (1, 7)                  as is_weekend
from spine
