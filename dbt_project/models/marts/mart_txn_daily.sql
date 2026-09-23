-- Mart: KHỐI RỘNG (cube) — ngày × vùng × ngành × kênh × persona × trạng thái.
-- Đây là bảng agent hỏi-đáp truy vấn nhiều nhất.
--
-- VÌ SAO MỘT BẢNG RỘNG, KHÔNG PHẢI NHIỀU BẢNG HẸP:
--   Agent phải trả lời được MỌI tổ hợp ("doanh số ngành Travel ở Coastal East qua
--   kênh MOBILE của khách CORPORATE"). Dựng một mart cho mỗi tổ hợp là lời nguyền
--   số chiều: 6 chiều -> 63 bảng phải nuôi. Một khối rộng (~300k dòng) trả lời
--   được cả 63 mà chỉ có MỘT nơi để sai.
--
-- ⚠️ MỌI ĐO LƯỜNG Ở ĐÂY BẮT BUỘC CỘNG ĐƯỢC (additive).
--   Đây là điều kiện sống còn của một cube: agent gộp nhóm theo chiều bất kỳ rồi
--   SUM, kết quả vẫn phải đúng. Vì vậy bảng này KHÔNG chứa:
--     * tỷ lệ        -> trung bình của các tỷ lệ là SAI (phải chia TỔNG cho TỔNG)
--     * trung bình   -> cùng lý do
--     * count distinct -> "khách hoạt động" ở 2 vùng cộng lại KHÔNG ra số khách thật
--   Ba thứ đó tính ở tầng semantic lúc truy vấn. Riêng số khách hoạt động (không
--   cộng được ở mọi grain) nằm ở mart_daily_volume, grain NGÀY, nơi nó đúng.
--
--   Hệ quả đẹp: `status` là một CHIỀU chứ không phải một loạt cột đếm. Muốn giá
--   trị trung bình giao dịch thành công -> lọc status='COMPLETED' rồi chia
--   net_amount cho txn_count. Không cần cột avg_* nào.
--
-- ⚠️ HAI LOẠI GIAO DỊCH KHÔNG CÓ NGÀNH HÀNG (issue #007, sửa ở NGUỒN 23-09):
--   Chuyển khoản nội bộ và lương không thuộc về merchant nào, nên `merchant_id`
--   của chúng là NULL và chúng mang NHÃN RIÊNG ở đây: INTERNAL_TRANSFER và PAYROLL.
--
--   Vì sao phải có nhãn riêng thay vì để rơi vào NULL -> 'UNKNOWN': 'UNKNOWN' phải
--   giữ đúng MỘT nghĩa — "merchant đã bị xoá ở nguồn". Trộn ba thứ khác hẳn nhau
--   vào một nhãn là làm mất khả năng trả lời, mà lại không hề báo lỗi.
--
--   Trước khi sửa nguồn: mỗi ngành hàng bị thổi ~15% bởi chuyển khoản, và toàn bộ
--   99.041 khoản lương dồn vào MỘT cửa hàng Grocery. Tiền thì không sai
--   (external_amount đã đúng từ tầng fact) nhưng SỐ ĐẾM sai — cùng họ với LUẬT
--   TIỀN #2: từng dòng đều hợp lệ, chỉ phép GOM NHÓM là sai nghĩa.

select
    d.full_date,
    -- Thuộc tính ngày để sẵn: câu hỏi mùa vụ ("cuối tuần so với ngày thường") là
    -- loại hay gặp nhất, không nên bắt agent join thêm một bảng cho mỗi câu.
    d.day_name,
    d.is_weekend,

    -- ---- 6 CHIỀU -----------------------------------------------------------
    case when f.is_internal_transfer then 'INTERNAL_TRANSFER'
         when f.is_payroll            then 'PAYROLL'
         else coalesce(m.region, 'UNKNOWN') end            as region,
    case when f.is_internal_transfer then 'INTERNAL_TRANSFER'
         when f.is_payroll            then 'PAYROLL'
         else coalesce(m.category, 'UNKNOWN') end          as category,
    f.channel,
    -- coalesce: tài khoản bị xoá mềm rơi khỏi dim_account, nhưng giao dịch của nó
    -- VẪN phải được đếm — bỏ rơi dòng fact là làm lệch tổng, đúng thứ project này
    -- tồn tại để chặn.
    coalesce(a.persona, 'UNKNOWN')                         as persona,
    f.status,

    -- ---- ĐO LƯỜNG (tất cả cộng được) ---------------------------------------
    count(*)                                               as txn_count,
    sum(f.net_amount)                                      as net_amount,
    -- Doanh số THẬT: loại tiền chỉ đổi chỗ trong nội bộ ngân hàng (LUẬT TIỀN #2).
    sum(f.external_amount)                                 as external_amount,

    -- Ba số đếm dưới đây đủ để dựng LẠI toàn bộ ma trận nhầm lẫn ở BẤT KỲ lát cắt
    -- nào, mà vẫn cộng được:
    --   tp = fraud_caught_count
    --   fp = flagged_count - fraud_caught_count
    --   fn = fraud_count   - fraud_caught_count
    --   tn = txn_count - fraud_count - flagged_count + fraud_caught_count
    -- Lưu SỐ ĐẾM chứ không lưu recall/precision chính là để phép cộng trên đúng.
    sum(case when f.fraud_label then 1 else 0 end)         as fraud_count,
    sum(case when f.is_flagged  then 1 else 0 end)         as flagged_count,
    sum(case when f.fraud_label and f.is_flagged
             then 1 else 0 end)                            as fraud_caught_count,
    -- Tiền THỰC SỰ chuyển đi trong các giao dịch gian lận. Không suy ra được từ
    -- fraud_count vì giao dịch gian lận có giá trị rất khác giao dịch thường
    -- (chiến dịch dò thẻ toàn khoản $1-30) — đếm số vụ mà không đo tiền là bỏ
    -- mất nửa câu chuyện.
    sum(case when f.fraud_label then f.net_amount
             else cast(0 as decimal(15,2)) end)            as fraud_amount,

    -- ---- TRUY NGUỒN --------------------------------------------------------
    -- Offset Kafka lớn nhất đã góp vào dòng này. Tôn chỉ "mọi số đều truy nguồn
    -- được": agent trả lời kèm được "số này tính đến offset N", và max-của-max
    -- vẫn đúng khi gộp nhóm tiếp — khác _refreshed_at (chỉ là lúc job chạy).
    max(f._kafka_offset)                                   as _kafka_offset_max,
    current_timestamp()                                    as _refreshed_at

from {{ ref('fact_transactions') }} f
join {{ ref('dim_date') }} d using (date_key)
-- LEFT JOIN cả hai dim: dim lọc soft-delete, fact thì không. Dùng INNER là âm thầm
-- đánh rơi giao dịch của merchant/tài khoản đã bị xoá ở nguồn -> tổng bị hụt.
left join {{ ref('dim_merchant') }} m using (merchant_id)
left join {{ ref('dim_account') }}  a using (account_id)
-- Fact GIỮ dòng soft-delete để không xoá dấu vết kiểm toán -> mart phục vụ báo cáo
-- PHẢI lọc ra.
where not f.is_deleted
group by 1, 2, 3, 4, 5, 6, 7, 8
