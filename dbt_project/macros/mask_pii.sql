{#
  3 kỹ thuật mask PII — chọn theo MỤC ĐÍCH dùng của từng trường, không phải một
  cỡ cho tất cả:

  1. mask_hash      — SHA-256 một chiều. Field chỉ cần JOIN/COUNT DISTINCT, không
                      bao giờ cần đọc (national_id, customer_name). Không đảo được.
  2. mask_partial   — lộ vài ký tự đầu/cuối. Field nhân viên hỗ trợ cần NHẬN RA
                      ("có phải số đuôi ...23 không ạ?") nhưng kẻ trộm không dùng
                      được (phone, email).
  3. generalize_*   — hạ độ phân giải. Field analytics cần phân bố, không cần
                      chính xác từng người (date_of_birth -> birth_year).

  Bronze giữ PII thô (restricted); Silver trở lên analyst mới được chạm.
#}

{% macro mask_hash(expr) -%}
sha2({{ expr }}, 256)
{%- endmacro %}

{% macro mask_partial(expr) -%}
concat(substring({{ expr }}, 1, 3), '*****', substring({{ expr }}, -2, 2))
{%- endmacro %}

{% macro mask_email(expr) -%}
concat(substring({{ expr }}, 1, 2), '***@', substring_index({{ expr }}, '@', -1))
{%- endmacro %}

{# Debezium serialize DATE = số ngày kể từ epoch -> đổi ra năm sinh #}
{% macro generalize_birth_year(days_expr) -%}
year(date_add(to_date('1970-01-01'), cast({{ days_expr }} as int)))
{%- endmacro %}
