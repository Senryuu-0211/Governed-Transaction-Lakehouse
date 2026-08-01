-- Mart: ngày × kênh — trả lời "tỷ trọng kênh số" mà không cần nhớ mã kênh.

select
    d.full_date,
    f.channel,
    c.is_digital,
    count(*)          as txn_count,
    sum(f.net_amount) as net_amount,
    current_timestamp() as _refreshed_at
from {{ ref('fact_transactions') }} f
join {{ ref('dim_date') }} d using (date_key)
join {{ ref('dim_channel') }} c using (channel)
where not f.is_deleted   -- fact giữ soft-delete để không xoá dấu vết; mart phải lọc
group by 1, 2, 3
