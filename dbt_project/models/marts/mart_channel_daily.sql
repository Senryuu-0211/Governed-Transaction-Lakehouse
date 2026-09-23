-- Mart: ngày × kênh — trả lời "tỷ trọng kênh số" mà không cần nhớ mã kênh.

select
    d.full_date,
    f.channel,
    c.is_digital,
    count(*)          as txn_count,
    sum(f.net_amount) as net_amount,
    -- Doanh số THẬT: loại chuyển khoản nội bộ (tiền chỉ đổi chỗ,
    -- không vào/ra khỏi ngân hàng). net_amount ở trên đo LƯU LƯỢNG.
    sum(f.external_amount) as external_amount,
    -- Offset Kafka lớn nhất đã góp vào dòng này — mọi số đều truy nguồn được.
    max(f._kafka_offset) as _kafka_offset_max,
    current_timestamp() as _refreshed_at
from {{ ref('fact_transactions') }} f
join {{ ref('dim_date') }} d using (date_key)
join {{ ref('dim_channel') }} c using (channel)
where not f.is_deleted   -- fact giữ soft-delete để không xoá dấu vết; mart phải lọc
group by 1, 2, 3
