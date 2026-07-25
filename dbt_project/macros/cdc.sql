{#
  Khối CDC dùng chung cho MỌI model Silver — logic collapse log về current-state
  viết MỘT lần, ba model dùng chung, không thể lệch nhau.

  cdc_latest_events(source_table):
    1. Lọc bỏ tombstone (value IS NULL — tombstone là marker, không mang state)
    2. Incremental: chỉ lấy offset MỚI hơn watermark đã nằm trong bảng đích
       (topic 1 partition -> thứ tự offset CHÍNH LÀ thứ tự commit, nên watermark
       theo offset là đúng tuyệt đối; nhiều partition thì phải watermark theo
       từng partition — ghi rõ giới hạn này)
    3. Dedup: mỗi primary key giữ ĐÚNG event mới nhất (row_number theo offset desc,
       partition theo Kafka key = JSON của PK)
#}
{% macro cdc_latest_events(source_table) %}
    select kafka_offset, op, source_ts_ms, key, value
    from (
        select e.*,
               row_number() over (partition by e.key order by e.kafka_offset desc) as _rn
        from (
            select kafka_offset, op, source_ts_ms, key, value
            from {{ source('bronze', source_table) }}
            where value is not null
              and op in ('r', 'c', 'u', 'd')
            {% if is_incremental() %}
              and kafka_offset > (select coalesce(max(_kafka_offset), -1) from {{ this }})
            {% endif %}
        ) e
    )
    where _rn = 1
{% endmacro %}

{#
  Lấy 1 trường từ envelope: op=c/u/r đọc after; op=d thì after=null nên rơi về
  before (REPLICA IDENTITY FULL bảo đảm before-image đầy đủ) -> dòng soft-delete
  vẫn giữ nguyên giá trị cuối cùng của nó.
#}
{% macro cdc_field(field) -%}
coalesce(
    get_json_object(value, '$.after.{{ field }}'),
    get_json_object(value, '$.before.{{ field }}')
)
{%- endmacro %}

{# Cột metadata lineage chung cho mọi model Silver #}
{% macro cdc_meta_columns() %}
    op = 'd'                                                    as is_deleted,
    case when op = 'd' then timestamp_millis(source_ts_ms) end  as deleted_at,
    source_ts_ms                                                as _source_ts_ms,
    kafka_offset                                                as _kafka_offset,
    current_timestamp()                                         as _silver_updated_at
{% endmacro %}
