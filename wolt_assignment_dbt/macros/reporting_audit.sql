{% macro run_id_value() -%}
    {{ return(var('run_id', invocation_id)) }}
{%- endmacro %}

{% macro run_ts_value() -%}
    {{ return(var('as_of_run_ts', run_started_at.strftime('%Y-%m-%d %H:%M:%S+00:00'))) }}
{%- endmacro %}

{% macro run_id_literal() -%}
    '{{ run_id_value() }}'
{%- endmacro %}

{% macro run_ts_literal() -%}
    timestamp('{{ run_ts_value() }}')
{%- endmacro %}

{% macro run_date_expr() -%}
    date({{ run_ts_literal() }})
{%- endmacro %}

{% macro ensure_run_metadata_table() -%}
    {{ ensure_system_schema_exists() }};
    create table if not exists `{{ target.database }}`.`{{ system_schema_name() }}`.`{{ var('run_metadata_table_name', '_run_metadata') }}` (
        run_id string,
        model_name string,
        as_of_run_ts timestamp,
        as_of_run_date date,
        target_name string,
        publish_tag string,
        incremental_lookback_days int64,
        enable_watermark_checks bool,
        enable_dev_sampling bool,
        created_at timestamp,
        updated_at timestamp
    )
    ;
    alter table `{{ target.database }}`.`{{ system_schema_name() }}`.`{{ var('run_metadata_table_name', '_run_metadata') }}`
    add column if not exists model_name string
{%- endmacro %}

{% macro upsert_run_metadata() -%}
    merge `{{ target.database }}`.`{{ system_schema_name() }}`.`{{ var('run_metadata_table_name', '_run_metadata') }}` as tgt
    using (
        select
            {{ run_id_literal() }} as run_id,
            '{{ this.identifier }}' as model_name,
            {{ run_ts_literal() }} as as_of_run_ts,
            {{ run_date_expr() }} as as_of_run_date,
            '{{ target.name }}' as target_name,
            '{{ var('publish_tag', 'scheduled') }}' as publish_tag,
            cast({{ var('incremental_lookback_days', 7) }} as int64) as incremental_lookback_days,
            cast({{ 'true' if var('enable_watermark_checks', true) else 'false' }} as bool) as enable_watermark_checks,
            cast({{ 'true' if var('enable_dev_sampling', true) else 'false' }} as bool) as enable_dev_sampling
    ) as src
        on tgt.run_id = src.run_id
        and tgt.model_name = src.model_name
    when matched then
        update set
            as_of_run_ts = src.as_of_run_ts,
            as_of_run_date = src.as_of_run_date,
            target_name = src.target_name,
            publish_tag = src.publish_tag,
            incremental_lookback_days = src.incremental_lookback_days,
            enable_watermark_checks = src.enable_watermark_checks,
            enable_dev_sampling = src.enable_dev_sampling,
            updated_at = current_timestamp()
    when not matched then
        insert (
            run_id,
            model_name,
            as_of_run_ts,
            as_of_run_date,
            target_name,
            publish_tag,
            incremental_lookback_days,
            enable_watermark_checks,
            enable_dev_sampling,
            created_at,
            updated_at
        )
        values (
            src.run_id,
            src.model_name,
            src.as_of_run_ts,
            src.as_of_run_date,
            src.target_name,
            src.publish_tag,
            src.incremental_lookback_days,
            src.enable_watermark_checks,
            src.enable_dev_sampling,
            current_timestamp(),
            current_timestamp()
        )
{%- endmacro %}
