-- LUẬT NGHIỆP VỤ #2: tiền vào PHẢI bằng tiền ra qua toàn pipeline.
-- Trả về dòng = VI PHẠM -> test fail -> push_marts bị skip -> số lệch KHÔNG ra Superset.
--
-- Chỉ xét lần chạy MỚI NHẤT: các dòng cũ là lịch sử bất biến của sổ kiểm toán,
-- không phán xử lại (một lần lệch trong quá khứ đã được ghi nhận là sự thật lịch
-- sử, không được để nó chặn pipeline mãi mãi).

select
    run_ts,
    count_bronze,
    count_gold,
    count_diff,
    amount_bronze,
    amount_gold,
    amount_diff
from {{ ref('audit_reconciliation') }}
where run_ts = (select max(run_ts) from {{ ref('audit_reconciliation') }})
  and (count_diff != 0 or amount_diff != 0)
