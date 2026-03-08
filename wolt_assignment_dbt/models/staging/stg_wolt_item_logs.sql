with src as (
    select *
    from {{ source('raw', 'wolt_snack_store_item_logs') }}
),
parsed as (
    select
        log_item_id,
        item_key,
        safe.parse_timestamp('%Y-%m-%d %H:%M:%E*S Z', time_log_created_utc) as time_log_created_utc,
        safe.parse_json(payload) as payload_json,
        payload as payload_raw
    from src
),
deduped as (
    select *
    from parsed
    qualify row_number() over (
        partition by log_item_id
        order by time_log_created_utc desc, payload_raw desc
    ) = 1
)
select
    log_item_id,
    item_key,
    time_log_created_utc,
    (
        select json_value(n, '$.value')
        from unnest(json_query_array(payload_json, '$.name')) as n
        where json_value(n, '$.lang') = 'en'
        limit 1
    ) as item_name_en,
    (
        select json_value(n, '$.value')
        from unnest(json_query_array(payload_json, '$.name')) as n
        where json_value(n, '$.lang') = 'de'
        limit 1
    ) as item_name_de,
    coalesce(
        (
            select json_value(n, '$.value')
            from unnest(json_query_array(payload_json, '$.name')) as n
            where json_value(n, '$.lang') = 'en'
            limit 1
        ),
        (
            select json_value(n, '$.value')
            from unnest(json_query_array(payload_json, '$.name')) as n
            where json_value(n, '$.lang') = 'de'
            limit 1
        )
    ) as item_name_preferred,
    json_value(payload_json, '$.brand_name') as brand_name,
    coalesce(
        json_value(payload_json, '$.item_category_en'),
        json_value(payload_json, '$.item_category')
    ) as item_category,
    cast(json_value(payload_json, '$.number_of_units') as int64) as number_of_units,
    cast(json_value(payload_json, '$.weight_in_grams') as int64) as weight_in_grams,
    cast(json_value(payload_json, '$.price_attributes[0].product_base_price') as numeric) as product_base_price_gross_eur,
    cast(json_value(payload_json, '$.price_attributes[0].vat_rate_in_percent') as numeric) as vat_rate_pct,
    safe.parse_timestamp(
        '%Y-%m-%d %H:%M:%E*S Z',
        json_value(payload_json, '$.time_item_created_in_source_utc')
    ) as time_item_created_in_source_utc,
    payload_json,
    payload_raw
from deduped
