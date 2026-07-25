{{ config(materialized='table') }}

-- Dimension merchant — nhỏ, gần tĩnh.

select
    merchant_id,
    merchant_name,
    category,
    city,
    created_at as registered_at
from {{ ref('merchants') }}
where not is_deleted
