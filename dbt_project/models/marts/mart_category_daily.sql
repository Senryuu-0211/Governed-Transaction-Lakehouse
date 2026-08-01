-- Mart: ngày × nhóm chi tiêu merchant — "khách đang tiêu tiền vào đâu".

select
    d.full_date,
    m.category,
    count(*)          as txn_count,
    sum(f.net_amount) as net_amount,
    current_timestamp() as _refreshed_at
from {{ ref('fact_transactions') }} f
join {{ ref('dim_date') }} d using (date_key)
join {{ ref('dim_merchant') }} m using (merchant_id)
where not f.is_deleted   -- fact giữ soft-delete để không xoá dấu vết; mart phải lọc
group by 1, 2
