{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='account_id'
) }}

-- Current-state của accounts, PII ĐÃ MASK — tầng đầu tiên analyst được chạm.
-- Raw PII (tên, CMND, phone, email, ngày sinh) chỉ tồn tại ở Bronze (restricted);
-- mọi model downstream bắt buộc đọc từ đây nên không thể "vô tình" lộ PII.

with latest as (
    {{ cdc_latest_events('accounts') }}
)

select
    cast({{ cdc_field('account_id') }} as bigint)          as account_id,
    {{ mask_hash(cdc_field('customer_name')) }}            as customer_name_hash,
    {{ mask_hash(cdc_field('national_id')) }}              as national_id_hash,
    {{ mask_partial(cdc_field('phone')) }}                 as phone_masked,
    {{ mask_email(cdc_field('email')) }}                   as email_masked,
    {{ generalize_birth_year(cdc_field('date_of_birth')) }} as birth_year,
    {{ cdc_field('account_type') }}                        as account_type,
    -- Persona: archetype HÀNH VI (không phải PII). Đây là thứ làm cho
    -- "số tài khoản hoạt động" trở thành con số biết di chuyển.
    {{ cdc_field('persona') }}                             as persona,
    -- Vùng cư trú — độ phân giải VÙNG, không phải địa chỉ, nên không cần mask.
    -- Khác vùng của merchant = giao dịch ngoài vùng, một tín hiệu gian lận.
    {{ cdc_field('home_region') }}                         as home_region,
    cast({{ cdc_field('balance') }} as decimal(15,2))      as balance,
    cast({{ cdc_field('created_at') }} as timestamp)       as created_at,
    {{ cdc_meta_columns() }}
from latest
