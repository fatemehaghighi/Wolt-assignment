{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='promo_sk',
        on_schema_change='sync_all_columns',
        partition_by={'field': 'promo_start_date', 'data_type': 'date'},
        cluster_by=['item_key_sk', 'promo_type']
    )
}}

select
    -- Promo surrogate key for this promo rule version at this grain.
    {{ surrogate_key(["item_key", "promo_start_date", "promo_end_date", "promo_type", "discount_pct"]) }} as promo_sk,
    -- Item surrogate key to join promo rules to item dimensions/facts.
    {{ surrogate_key(["item_key"]) }} as item_key_sk,
    item_key,
    promo_type,
    discount_pct,
    promo_start_date,
    promo_end_date
from {{ ref('stg_wolt_promos') }}
