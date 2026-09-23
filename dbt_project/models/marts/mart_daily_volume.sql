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
    -- Doanh số THẬT: loại chuyển khoản nội bộ (tiền chỉ đổi chỗ,
    -- không vào/ra khỏi ngân hàng). net_amount ở trên đo LƯU LƯỢNG.
    sum(f.external_amount)                                       as external_amount,
    avg(case when f.is_real_money then f.amount end)        as avg_completed_amount,
    -- SỐ KHÁCH HOẠT ĐỘNG — chỉ sống được ở grain NGÀY.
    -- Đếm-phân-biệt KHÔNG cộng được: số khách hoạt động ở Metro North cộng với
    -- Coastal East KHÔNG ra số khách thật, vì một người tiêu ở cả hai nơi bị đếm
    -- hai lần. Vì vậy cột này cố tình VẮNG MẶT ở khối rộng mart_txn_daily — ở đó
    -- agent được tự do gộp nhóm, và một cột cộng-lại-sai sẽ âm thầm sinh ra số sai.
    -- Đặt ở đây thì nó đúng, vì grain ngày là grain duy nhất không ai gộp thêm.
    -- Cần cho phép PHÂN RÃ: giao dịch = khách hoạt động × giao dịch/khách × giá trị/giao dịch.
    count(distinct f.account_id)                            as active_accounts,
    -- Offset Kafka lớn nhất đã góp vào dòng này — mọi số đều truy nguồn được.
    max(f._kafka_offset)                                    as _kafka_offset_max,
    current_timestamp()                                     as _refreshed_at
from {{ ref('fact_transactions') }} f
join {{ ref('dim_date') }} d using (date_key)
-- Fact GIỮ dòng soft-delete (giao dịch đã purge ở nguồn) để không xoá dấu vết
-- kiểm toán -> mọi mart phục vụ báo cáo PHẢI lọc ra.
where not f.is_deleted
group by 1, 2, 3, 4, 5, 6
