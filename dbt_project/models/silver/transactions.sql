{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='txn_id'
) }}

-- Current-state của transactions: collapse CDC log -> 1 dòng/txn_id (bản mới nhất).
-- TIMESTAMPTZ đến dưới dạng chuỗi ISO-8601 (đã kiểm chứng trên envelope thật);
-- amount là STRING xuyên suốt pipeline và chỉ cast về DECIMAL đúng một lần ở đây.

with latest as (
    {{ cdc_latest_events('transactions') }}
)

select
    cast({{ cdc_field('txn_id') }}      as bigint)         as txn_id,
    cast({{ cdc_field('account_id') }}  as bigint)         as account_id,
    cast({{ cdc_field('merchant_id') }} as bigint)         as merchant_id,
    cast({{ cdc_field('amount') }}      as decimal(15,2))  as amount,
    {{ cdc_field('currency') }}                            as currency,
    {{ cdc_field('txn_type') }}                            as txn_type,
    {{ cdc_field('status') }}                              as status,
    {{ cdc_field('channel') }}                             as channel,
    cast({{ cdc_field('created_at') }}  as timestamp)      as created_at,
    cast({{ cdc_field('updated_at') }}  as timestamp)      as updated_at,
    {{ cdc_meta_columns() }}
from latest
