{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='merchant_id'
) }}

-- Current-state của merchants — dimension nhỏ, gần như tĩnh, không PII.

with latest as (
    {{ cdc_latest_events('merchants') }}
)

select
    cast({{ cdc_field('merchant_id') }} as bigint)   as merchant_id,
    {{ cdc_field('merchant_name') }}                 as merchant_name,
    {{ cdc_field('category') }}                      as category,
    {{ cdc_field('city') }}                          as city,
    cast({{ cdc_field('created_at') }} as timestamp) as created_at,
    {{ cdc_meta_columns() }}
from latest
