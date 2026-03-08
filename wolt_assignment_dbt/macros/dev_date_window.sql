{% macro dev_date_window(column_name, column_type='timestamp') -%}
    {%- if target.name == 'dev' and var('enable_dev_sampling', true) -%}
        and {{ column_name }} >= cast('{{ var('dev_sample_start_date') }}' as {{ column_type }})
        and {{ column_name }} < cast('{{ var('dev_sample_end_date') }}' as {{ column_type }})
    {%- endif -%}
{%- endmacro %}
