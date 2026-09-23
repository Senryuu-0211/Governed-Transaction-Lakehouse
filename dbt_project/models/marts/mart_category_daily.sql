-- Mart: ngày × nhóm chi tiêu merchant — "khách đang tiêu tiền vào đâu".

select
    d.full_date,
    m.category,
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
join {{ ref('dim_merchant') }} m using (merchant_id)
where not f.is_deleted   -- fact giữ soft-delete để không xoá dấu vết; mart phải lọc
  -- Giữ bộ lọc này dù nguồn ĐÃ sửa (issue #007: merchant_id NULL cho chuyển khoản,
  -- nên INNER JOIN ở trên tự loại chúng). Hai lớp, có chủ đích: JOIN là hệ quả phụ
  -- của cách viết câu lệnh — ai đó đổi sang LEFT JOIN để "không đánh rơi dòng" là
  -- lỗi cũ quay lại ngay, im lặng. Dòng này nói thẳng Ý ĐỊNH.
  and not f.is_internal_transfer
group by 1, 2
