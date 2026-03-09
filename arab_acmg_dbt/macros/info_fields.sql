{% macro info_scalar(info_column, key) -%}
REGEXP_EXTRACT({{ info_column }}, r'(?:^|;){{ key }}=([^;]+)')
{%- endmacro %}

{% macro info_token(info_column, key, offset_expression) -%}
SPLIT(COALESCE(REGEXP_EXTRACT({{ info_column }}, r'(?:^|;){{ key }}=([^;]+)'), ''), ',')[SAFE_OFFSET({{ offset_expression }})]
{%- endmacro %}
