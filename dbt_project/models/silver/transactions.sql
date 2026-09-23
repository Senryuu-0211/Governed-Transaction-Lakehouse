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
    -- issue #002: tài khoản ĐÍCH của chuyển khoản. NULL với giao dịch merchant.
    -- Nguồn ràng buộc bằng CHECK hai chiều (TRANSFER phải có, loại khác không được),
    -- nên ở đây chỉ cần ép kiểu — không phải đoán.
    cast({{ cdc_field('counterparty_account_id') }} as bigint) as counterparty_account_id,
    -- issue #004: khoá chống trừ tiền hai lần, sinh ở phía client.
    -- Mang xuống lakehouse KHÔNG phải để khử trùng (nguồn đã UNIQUE nên không bao
    -- giờ có dòng trùng lọt xuống), mà để KIỂM CHỨNG ràng buộc đó vẫn còn đúng sau
    -- khi dữ liệu đi qua CDC -> Kafka -> Spark -> Iceberg -> MERGE. Test `unique`
    -- trên cột này là phép thử cho TOÀN BỘ đường ống, không phải cho cái cột.
    {{ cdc_field('idempotency_key') }}                    as idempotency_key,
    cast({{ cdc_field('amount') }}      as decimal(15,2))  as amount,
    {{ cdc_field('currency') }}                            as currency,
    {{ cdc_field('txn_type') }}                            as txn_type,
    {{ cdc_field('status') }}                              as status,
    {{ cdc_field('channel') }}                             as channel,
    -- HAI cột gian lận, cố ý tách bạch: cờ NGHI NGỜ của hệ thống và SỰ THẬT.
    -- Gộp một cột thì cảnh báo luôn đúng 100% — đo lường trở nên vô nghĩa.
    cast({{ cdc_field('is_flagged') }}  as boolean)        as is_flagged,
    cast({{ cdc_field('fraud_label') }} as boolean)        as fraud_label,
    cast({{ cdc_field('created_at') }}  as timestamp)      as created_at,
    cast({{ cdc_field('updated_at') }}  as timestamp)      as updated_at,
    {{ cdc_meta_columns() }}
from latest
