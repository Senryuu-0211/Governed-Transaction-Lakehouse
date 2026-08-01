-- Mart: khối lượng giao dịch theo NGÀY — bảng sau lưng dashboard chính.
-- Nhỏ (1 dòng/ngày) để BI click ra số trong ms. Mọi số tiền đi qua net_amount
-- (luật COMPLETED/REVERSED/FAILED đã được fact xử lý + test gác).

select
    d.full_date,
    d.year,
    d.month,
    d.month_name,
    d.day_name,
    d.is_weekend,
    count(*)                                                as txn_count,
    sum(case when f.status = 'COMPLETED' then 1 else 0 end) as completed_count,
    sum(case when f.status = 'FAILED'    then 1 else 0 end) as failed_count,
    sum(case when f.status = 'REVERSED'  then 1 else 0 end) as reversed_count,
    sum(case when f.status = 'PENDING'   then 1 else 0 end) as pending_count,
    sum(f.net_amount)                                       as net_amount,
    avg(case when f.is_real_money then f.amount end)        as avg_completed_amount,
    current_timestamp()                                     as _refreshed_at
from {{ ref('fact_transactions') }} f
join {{ ref('dim_date') }} d using (date_key)
-- Fact GIỮ dòng soft-delete (giao dịch đã purge ở nguồn) để không xoá dấu vết
-- kiểm toán -> mọi mart phục vụ báo cáo PHẢI lọc ra.
where not f.is_deleted
group by 1, 2, 3, 4, 5, 6
