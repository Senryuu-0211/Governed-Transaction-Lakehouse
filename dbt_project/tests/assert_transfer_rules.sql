-- LUẬT NGHIỆP VỤ #3: giao dịch phải đúng HÌNH DẠNG — chuyển khoản có đủ hai đầu và
-- không thành doanh số; merchant chỉ xuất hiện ở giao dịch mua bán (issue #007).
--
-- Nguồn đã có CHECK constraint, nhưng constraint chỉ gác ở Postgres. Test này gác ở
-- ĐẦU KIA của đường ống — sau khi dữ liệu đi qua CDC, Kafka, Avro, Spark, Iceberg và
-- MERGE. Đó mới là thứ cần chứng minh: ràng buộc còn nguyên sau 6 lần biến đổi, chứ
-- không phải ràng buộc tồn tại ở nguồn.
--
-- Trả về dòng = VI PHẠM -> test fail -> push_marts bị skip.

select txn_id, txn_type, status, account_id, counterparty_account_id,
       amount, net_amount, external_amount, breach
from (
    select f.*, 'TRANSFER thiếu tài khoản đích' as breach
    from {{ ref('fact_transactions') }} f
    where f.txn_type = 'TRANSFER' and f.counterparty_account_id is null

    union all
    -- Giao dịch merchant mà vẫn kèm đích thì downstream không biết cộng tiền cho ai.
    select f.*, 'Không phải TRANSFER nhưng có tài khoản đích'
    from {{ ref('fact_transactions') }} f
    where f.txn_type <> 'TRANSFER' and f.counterparty_account_id is not null

    union all
    -- Tự chuyển cho chính mình: vừa bị trừ vừa được cộng -> đối soát lệch.
    select f.*, 'Chuyển khoản cho chính tài khoản đó'
    from {{ ref('fact_transactions') }} f
    where f.counterparty_account_id = f.account_id

    union all
    -- ĐIỂM MẤU CHỐT: tiền chỉ đổi chỗ trong ngân hàng KHÔNG được tính là doanh số.
    select f.*, 'Chuyển khoản nội bộ bị tính vào doanh số'
    from {{ ref('fact_transactions') }} f
    where f.is_internal_transfer and f.external_amount <> 0

    union all
    -- Ngược lại: giao dịch bên ngoài đã hoàn tất thì doanh số phải bằng net_amount.
    select f.*, 'Giao dịch bên ngoài nhưng doanh số lệch net_amount'
    from {{ ref('fact_transactions') }} f
    where not f.is_internal_transfer and f.external_amount <> f.net_amount

    -- ---- issue #007: merchant CHỈ dành cho giao dịch mua bán -----------------
    -- Nguồn đã có CHECK hai chiều, nhưng như mọi test ở file này: cái cần chứng
    -- minh là ràng buộc CÒN NGUYÊN sau khi đi qua CDC, Kafka, Avro, Spark và MERGE.
    union all
    select f.*, 'Chuyển khoản nội bộ vẫn đeo merchant'
    from {{ ref('fact_transactions') }} f
    where f.is_internal_transfer and f.merchant_id is not null

    union all
    select f.*, 'Lương vẫn đeo merchant'
    from {{ ref('fact_transactions') }} f
    where f.is_payroll and f.merchant_id is not null

    union all
    -- Chiều ngược: mua bán mà THIẾU merchant thì mart ngành hàng âm thầm hụt dòng.
    select f.*, 'Giao dịch mua bán nhưng thiếu merchant'
    from {{ ref('fact_transactions') }} f
    where not f.is_internal_transfer and not f.is_payroll
      and f.merchant_id is null
)
