-- CỔNG GÁC CHO TẦNG PHỤC VỤ: mart phải CỘNG LẠI ĐÚNG BẰNG fact.
--
-- VÌ SAO CẦN: mart là nơi agent và dashboard lấy số. Một mart sai không gây lỗi —
-- nó chỉ trả về một con số khác, và không ai biết. Các test không-null/duy-nhất
-- không bắt được loại này vì từng dòng mart đều hợp lệ; chỉ TỔNG mới sai.
-- Hai kiểu hỏng cụ thể mà test này chặn:
--   1. JOIN hụt — đổi LEFT JOIN thành INNER rồi âm thầm đánh rơi giao dịch của
--      merchant/tài khoản đã xoá mềm. Tổng tụt, không ai thấy.
--   2. Bộ lọc sai chỗ — thêm/bớt một điều kiện WHERE làm lệch phạm vi đếm.
--
-- Trả về dòng = VI PHẠM -> test fail -> push_marts bị skip, số sai KHÔNG ra tới
-- Superset hay agent.

with fact_totals as (
    select
        count(*)                                                    as all_txn,
        sum(case when is_internal_transfer then 1 else 0 end)       as transfer_txn,
        sum(case when is_payroll          then 1 else 0 end)        as payroll_txn,
        -- "Có merchant thật" = không phải chuyển khoản, cũng không phải lương.
        -- Quên vế thứ hai là test xanh trong khi mart ngành hàng hụt 99k dòng.
        sum(case when is_internal_transfer or is_payroll
                 then 0 else 1 end)                                 as merchant_txn
    from {{ ref('fact_transactions') }}
    where not is_deleted
),

cube as (
    select
        sum(txn_count)                                              as all_txn,
        sum(case when category = 'INTERNAL_TRANSFER'
                 then txn_count else 0 end)                         as transfer_txn,
        sum(case when category = 'PAYROLL'
                 then txn_count else 0 end)                         as payroll_txn,
        sum(case when category in ('INTERNAL_TRANSFER', 'PAYROLL')
                 then 0 else txn_count end)                         as merchant_txn
    from {{ ref('mart_txn_daily') }}
),

checks as (
    -- Khối rộng phải chứa TRỌN VẸN fact: không thừa một giao dịch, không thiếu một.
    select 'Khối rộng lệch tổng số giao dịch so với fact' as breach,
           f.all_txn as expected, c.all_txn as actual
    from fact_totals f cross join cube c
    where f.all_txn <> c.all_txn

    union all
    -- Nhãn INTERNAL_TRANSFER phải bắt ĐÚNG tập chuyển khoản — không rộng hơn,
    -- không hẹp hơn. Sai chỗ này là ngành hàng bị thổi phồng trở lại.
    select 'Số giao dịch gắn nhãn INTERNAL_TRANSFER không khớp fact',
           f.transfer_txn, c.transfer_txn
    from fact_totals f cross join cube c
    where f.transfer_txn <> c.transfer_txn

    union all
    -- Nhãn PAYROLL phải bắt đúng tập lương — sai là 99k khoản lương chui vào một
    -- ngành hàng nào đó, đúng cái đã sinh ra issue #007.
    select 'Số giao dịch gắn nhãn PAYROLL không khớp fact',
           f.payroll_txn, c.payroll_txn
    from fact_totals f cross join cube c
    where f.payroll_txn <> c.payroll_txn

    union all
    -- Mart ngành hàng CHỈ được đếm giao dịch có merchant thật.
    select 'Mart ngành hàng lệch so với số giao dịch có merchant thật',
           f.merchant_txn, m.txn_count
    from fact_totals f
    cross join (select sum(txn_count) as txn_count
                from {{ ref('mart_category_daily') }}) m
    where f.merchant_txn <> m.txn_count

    union all
    -- Mart merchant cùng phạm vi với mart ngành hàng (cả hai loại chuyển khoản).
    select 'Mart merchant lệch so với số giao dịch có merchant thật',
           f.merchant_txn, m.txn_count
    from fact_totals f
    cross join (select sum(txn_count) as txn_count
                from {{ ref('mart_merchant_daily') }}) m
    where f.merchant_txn <> m.txn_count

    union all
    -- mart_fraud_daily là phép CỘNG thuần trên khối rộng. Lệch ở đây nghĩa là một
    -- đo lường trong khối rộng đã hết cộng-được — đúng thứ kỷ luật cube cấm.
    select 'Mart gian lận lệch so với khối rộng (đo lường hết cộng được?)',
           c.all_txn, fr.txn_count
    from cube c
    cross join (select sum(txn_count) as txn_count
                from {{ ref('mart_fraud_daily') }}) fr
    where c.all_txn <> fr.txn_count
)

select * from checks
