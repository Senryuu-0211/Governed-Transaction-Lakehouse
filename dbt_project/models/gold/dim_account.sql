{{ config(materialized='table') }}

-- Dimension tài khoản — đọc từ Silver (PII ĐÃ mask), thêm nhóm tuổi cho phân tích.
-- Natural key (account_id) thay vì surrogate key: nguồn duy nhất, không tái sinh
-- key, nên surrogate không mua thêm gì ở scale này — ghi rõ trade-off.

select
    account_id,
    customer_name_hash,
    phone_masked,
    email_masked,
    birth_year,
    year(current_date()) - birth_year          as age,
    case
        when year(current_date()) - birth_year < 25 then '18-24'
        when year(current_date()) - birth_year < 35 then '25-34'
        when year(current_date()) - birth_year < 50 then '35-49'
        when year(current_date()) - birth_year < 65 then '50-64'
        else '65+'
    end                                        as age_band,
    account_type,
    balance,
    created_at                                 as opened_at
from {{ ref('accounts') }}
where not is_deleted
