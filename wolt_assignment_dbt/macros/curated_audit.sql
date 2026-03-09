{% macro backfill_last_modified_from_log_date(column_name='last_modified_utc', source_ts_column='time_log_created_utc') -%}
    update {{ this }}
    set {{ column_name }} = {{ source_ts_column }}
    where {{ column_name }} is null
{%- endmacro %}
