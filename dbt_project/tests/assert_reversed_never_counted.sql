-- LUẬT NGHIỆP VỤ #1: giao dịch REVERSED không bao giờ được đếm vào tiền thật.
-- Trả về dòng = VI PHẠM -> test fail. Đây là điểm đúng-sai của cả project
-- (PLAN: "REVERSAL không double-count") nên có test riêng, không dựa vào
-- accepted_values chung chung.

select txn_id, status, amount, net_amount
from {{ ref('fact_transactions') }}
where status in ('REVERSED', 'FAILED', 'PENDING')
  and (net_amount != 0 or is_real_money)
