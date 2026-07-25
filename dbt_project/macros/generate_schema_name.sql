{#
  Override mặc định của dbt: bình thường custom schema bị GHÉP vào target schema
  ("silver" + custom "gold" -> "silver_gold" — vô nghĩa với lakehouse này).
  Ở đây custom schema dùng NGUYÊN VĂN -> model gold nằm đúng gtl.gold.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
