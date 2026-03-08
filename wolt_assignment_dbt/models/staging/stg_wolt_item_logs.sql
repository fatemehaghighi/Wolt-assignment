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
typed as (
    select
        *,
        cast(json_value(payload_json, '$.price_attributes[0].product_base_price') as numeric) as product_base_price_gross_eur,
        cast(json_value(payload_json, '$.price_attributes[0].vat_rate_in_percent') as numeric) as vat_rate_pct,
        -- Raw QA check showed this payload field is consistently stored without trailing timezone.
        -- Example: log_item_id = 25a4f5a95905b9c620551dd25face96c -> 2019-09-05 08:33:06.213
        -- Parse directly as UTC for efficiency and deterministic behavior.
        safe.parse_timestamp(
            '%Y-%m-%d %H:%M:%E*S',
            json_value(payload_json, '$.time_item_created_in_source_utc'),
            'UTC'
        ) as time_item_created_in_source_utc
    from parsed
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
    product_base_price_gross_eur,
    vat_rate_pct,
    time_item_created_in_source_utc,
    payload_json,
    payload_raw
from typed
