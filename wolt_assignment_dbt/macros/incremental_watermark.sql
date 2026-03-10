{% macro system_schema_name() -%}
    {{ target.schema }}_{{ var('system_schema_suffix', 'sys') }}
{%- endmacro %}

{% macro ensure_system_schema_exists() -%}
    create schema if not exists `{{ target.database }}`.`{{ system_schema_name() }}`
    {%- if target.location is defined and target.location -%}
        options(location='{{ target.location }}')
    {%- endif -%}
{%- endmacro %}

{% macro watermark_table_relation() -%}
    `{{ target.database }}`.`{{ system_schema_name() }}`.`{{ var('watermark_table_name', '_elt_watermarks') }}`
{%- endmacro %}

{% macro ensure_watermark_table() -%}
    {{ ensure_system_schema_exists() }};
    create table if not exists {{ watermark_table_relation() }} (
        model_name string,
        watermark_ts timestamp,
        updated_at timestamp
    )
{%- endmacro %}

{% macro watermark_lookup_expr(model_name) -%}
    coalesce(
        (
            select watermark_ts
            from {{ watermark_table_relation() }}
            where model_name = '{{ model_name }}'
            limit 1
        ),
        timestamp('2020-01-01 00:00:00+00')
    )
{%- endmacro %}

{% macro upsert_model_watermark(model_name, watermark_column, model_relation=None) -%}
    {%- set relation = model_relation if model_relation is not none else this -%}
    merge {{ watermark_table_relation() }} as tgt
    using (
        select
            '{{ model_name }}' as model_name,
            max({{ watermark_column }}) as watermark_ts
        from {{ relation }}
        having watermark_ts is not null
    ) as src
        on tgt.model_name = src.model_name
    when matched then
        update set
            watermark_ts = src.watermark_ts,
            updated_at = current_timestamp()
    when not matched then
        insert (model_name, watermark_ts, updated_at)
        values (src.model_name, src.watermark_ts, current_timestamp())
{%- endmacro %}

{% macro incremental_cutoff_expr(model_name, target_timestamp_column) -%}
    {%- if var('enable_incremental_lookback_window', true) -%}
        timestamp_sub(
            {% if var('enable_watermark_checks', true) %}
                {{ watermark_lookup_expr(model_name) }}
            {% else %}
                (
                    select coalesce(
                        max({{ target_timestamp_column }}),
                        timestamp('2020-01-01 00:00:00+00')
                    )
                    from {{ this }}
                )
            {% endif %},
            interval {{ var('incremental_lookback_days', 7) }} day
        )
    {%- else -%}
        timestamp('2020-01-01 00:00:00+00')
    {%- endif -%}
{%- endmacro %}
