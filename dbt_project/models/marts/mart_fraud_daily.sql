-- Mart: ngày × kênh — hiệu quả PHÁT HIỆN gian lận (ma trận nhầm lẫn).
--
-- ⚠️ LƯU SỐ ĐẾM, KHÔNG LƯU TỶ LỆ. Đây là điểm dễ sai nhất của mart này:
--   recall tháng KHÔNG phải trung bình recall các ngày. Ngày có 3 vụ gian lận và
--   ngày có 130 vụ (ngày bùng phát) không được cân bằng nhau. Tỷ lệ đúng là
--   TỔNG chia TỔNG, nên bảng chỉ giữ bốn ô của ma trận và để tầng semantic chia:
--       recall    = tp / (tp + fn)
--       precision = tp / (tp + fp)
--   Cùng một cái bẫy với "trung bình của các phần trăm" trong mọi báo cáo BI.
--
-- VÌ SAO DẪN XUẤT TỪ mart_txn_daily, KHÔNG TÍNH LẠI TỪ fact:
--   Hai lối tính độc lập cho cùng một con số = hai MẶT LỆCH chờ ngày lệch nhau.
--   Khối rộng đã giữ đủ ba số đếm gốc, và vì MỌI đo lường trong đó đều cộng được
--   nên gộp lên grain thưa hơn là phép cộng thuần — không thể ra kết quả khác.
--   Đây chính là lợi tức của kỷ luật "cube chỉ chứa số cộng được".
--
-- GIAN LẬN ĐƯỢC GIEO CÓ CHỦ ĐÍCH: thế giới mô phỏng cố tình để bộ phát hiện KHÔNG
-- hoàn hảo (recall ~70%, có dương tính giả) — một bộ phát hiện hoàn hảo thì bảng
-- này vô nghĩa và agent không có gì để phát hiện. Xem CURRENT_STATUS mục 16.

select
    full_date,
    day_name,
    is_weekend,
    channel,

    sum(txn_count)                as txn_count,
    sum(fraud_count)              as fraud_count,
    sum(flagged_count)            as flagged_count,
    sum(fraud_amount)             as fraud_amount,

    -- ---- BỐN Ô CỦA MA TRẬN NHẦM LẪN ----------------------------------------
    -- Gian lận thật và ĐÃ bị chặn.
    sum(fraud_caught_count)                             as true_positive,
    -- Bị chặn nhầm: khách lương thiện bị làm phiền. Chi phí có thật, không phải 0.
    sum(flagged_count - fraud_caught_count)             as false_positive,
    -- Gian lận LỌT LƯỚI — ô đắt nhất, tiền đã đi khỏi ngân hàng.
    sum(fraud_count - fraud_caught_count)               as false_negative,
    -- Giao dịch lương thiện đi qua êm. Cần để tính được tỷ lệ dương tính giả.
    sum(txn_count - fraud_count - flagged_count
                  + fraud_caught_count)                 as true_negative,

    max(_kafka_offset_max)        as _kafka_offset_max,
    current_timestamp()           as _refreshed_at

from {{ ref('mart_txn_daily') }}
group by 1, 2, 3, 4
