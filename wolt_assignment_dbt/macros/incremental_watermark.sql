{% macro watermark_table_relation() -%}
    `{{ target.database }}`.`{{ target.schema }}`.`{{ var('watermark_table_name', '_elt_watermarks') }}`
{%- endmacro %}

{% macro ensure_watermark_table() -%}
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
        timestamp('1900-01-01 00:00:00+00')
    )
{%- endmacro %}

{% macro upsert_model_watermark(model_name, watermark_column) -%}
    merge {{ watermark_table_relation() }} as tgt
    using (
        select
            '{{ model_name }}' as model_name,
            max({{ watermark_column }}) as watermark_ts
        from {{ this }}
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
