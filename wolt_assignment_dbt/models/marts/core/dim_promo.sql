{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='promo_key',
        on_schema_change='sync_all_columns',
        partition_by={'field': 'promo_start_date', 'data_type': 'date'},
        cluster_by=['item_key_sk', 'promo_type']
    )
}}

select
    {{ surrogate_key(["item_key", "promo_start_date", "promo_end_date", "promo_type", "discount_pct"]) }} as promo_key,
    {{ surrogate_key(["item_key"]) }} as item_key_sk,
    item_key,
    promo_type,
    discount_pct,
    promo_start_date,
    promo_end_date
from {{ ref('stg_wolt_promos') }}
