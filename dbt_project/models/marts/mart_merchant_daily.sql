-- Mart: ngày × merchant — "tiền đang dồn vào đâu, và chỗ nào vừa tắt".
-- ~200 merchant × ~90 ngày = ~18k dòng. Nhỏ, nhưng là grain DUY NHẤT trả lời được
-- câu hỏi về một đối tác cụ thể ("top 5 merchant tháng này", "merchant X có sao
-- không") — khối rộng mart_txn_daily cố tình KHÔNG có chiều merchant vì 200 giá
-- trị nhân vào 6 chiều kia sẽ làm nó phình 200 lần.
--
-- TÊN merchant để sẵn ở đây (không chỉ merchant_id): agent trả lời cho người
-- không biết kỹ thuật thì "Merchant 47" là vô nghĩa. Denormalize có chủ đích —
-- mart là tầng phục vụ, không phải tầng chuẩn hoá.
--
-- ⚠️ CHỈ GIAO DỊCH MUA BÁN (issue #007):
--   Chuyển khoản nội bộ và lương không thuộc merchant nào — ở nguồn chúng đã có
--   merchant_id NULL nên INNER JOIN dưới đây tự loại. Đây là mart mà sai chỗ này
--   gây hại nhất: trước khi sửa, TOÀN BỘ lương của 25.000 tài khoản dồn vào một
--   cửa hàng và làm nó trông to hơn thực tế 8,5% — "top merchant" lại đúng là loại
--   số người ta ra quyết định dựa trên đó.
--
-- DÙNG ĐỂ PHÁT HIỆN: merchant lớn ngừng hoạt động (txn_count tụt về 0 trong khi
-- các merchant khác bình thường) — một trong các sự cố được gieo sẵn trong thế
-- giới mô phỏng, xem CURRENT_STATUS mục 16.

select
    d.full_date,
    f.merchant_id,
    m.merchant_name,
    m.category,
    m.region,
    m.city,

    count(*)                                                as txn_count,
    sum(case when f.status = 'COMPLETED' then 1 else 0 end) as completed_count,
    -- Tỷ lệ hỏng KHÔNG lưu ở đây, chỉ lưu hai số đếm để chia ở tầng semantic:
    -- trung bình của các tỷ lệ ngày là SAI khi gộp nhiều ngày.
    sum(case when f.status = 'FAILED'    then 1 else 0 end) as failed_count,
    sum(f.net_amount)                                       as net_amount,
    sum(f.external_amount)                                  as external_amount,

    max(f._kafka_offset)                                    as _kafka_offset_max,
    current_timestamp()                                     as _refreshed_at

from {{ ref('fact_transactions') }} f
join {{ ref('dim_date') }}     d using (date_key)
join {{ ref('dim_merchant') }} m using (merchant_id)
where not f.is_deleted
  -- Chuyển khoản nội bộ không thuộc về merchant nào (xem đầu file).
  and not f.is_internal_transfer
group by 1, 2, 3, 4, 5, 6
