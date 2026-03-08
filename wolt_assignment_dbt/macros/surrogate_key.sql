{% macro surrogate_key(columns) %}
  to_hex(md5(concat(
    {%- for col in columns -%}
      coalesce(cast({{ col }} as string), '_dbt_null_')
      {%- if not loop.last %}, '||', {% endif -%}
    {%- endfor -%}
  )))
{% endmacro %}
