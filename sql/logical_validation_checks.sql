-- Logical Validation Checks
-- Date: 2026-03-09
-- Project: wolt-assignment-489610
-- Dataset: analytics_dev
-- Purpose: Deep data-logic validation for layer consistency, joins, SCD coverage,
-- promo logic ambiguity, and financial reconciliation.

-- ==========================================================
-- 1) End-to-end layer consistency and critical integrity
-- ==========================================================
with
c1 as (
  select "stg_purchase_rows" as check_name, cast(count(*) as string) as check_value from `wolt-assignment-489610.analytics_dev.stg_wolt_purchase_logs`
  union all
  select "int_purchase_rows", cast(count(*) as string) from `wolt-assignment-489610.analytics_dev.int_wolt_purchase_logs_curated_filtered`
  union all
  select "fct_order_rows", cast(count(*) as string) from `wolt-assignment-489610.analytics_dev.fct_order`
  union all
  select "stg_order_items_rows", cast(count(*) as string) from `wolt-assignment-489610.analytics_dev.stg_wolt_order_items`
  union all
  select "fct_order_item_rows", cast(count(*) as string) from `wolt-assignment-489610.analytics_dev.fct_order_item`
),
c2 as (
  select "orphan_fct_order_item_without_order" as check_name, cast(count(*) as string) as check_value
  from `wolt-assignment-489610.analytics_dev.fct_order_item` oi
  left join `wolt-assignment-489610.analytics_dev.fct_order` o on oi.order_sk = o.order_sk
  where o.order_sk is null
  union all
  select "fct_order_without_items", cast(count(*) as string)
  from `wolt-assignment-489610.analytics_dev.fct_order` o
  left join (
    select distinct order_sk from `wolt-assignment-489610.analytics_dev.fct_order_item`
  ) oi using(order_sk)
  where oi.order_sk is null
),
c3 as (
  select
    "order_reconciliation_max_abs_diff" as check_name,
    cast(max(abs(o.total_basket_value_eur - coalesce(m.derived_basket_value_eur, 0))) as string) as check_value
  from `wolt-assignment-489610.analytics_dev.fct_order` o
  left join (
    select order_sk, sum(line_final_amount_gross_eur) as derived_basket_value_eur
    from `wolt-assignment-489610.analytics_dev.fct_order_item`
    group by order_sk
  ) m using(order_sk)
  union all
  select
    "order_reconciliation_over_0_001_cnt",
    cast(countif(abs(o.total_basket_value_eur - coalesce(m.derived_basket_value_eur, 0)) > 0.001) as string)
  from `wolt-assignment-489610.analytics_dev.fct_order` o
  left join (
    select order_sk, sum(line_final_amount_gross_eur) as derived_basket_value_eur
    from `wolt-assignment-489610.analytics_dev.fct_order_item`
    group by order_sk
  ) m using(order_sk)
),
c4 as (
  select "duplicate_promo_definitions_cnt" as check_name, cast(count(*) as string) as check_value
  from (
    select item_key, promo_start_date, promo_end_date, promo_type, discount_pct, count(*) as c
    from `wolt-assignment-489610.analytics_dev.stg_wolt_promos`
    group by 1, 2, 3, 4, 5
    having c > 1
  )
  union all
  select "items_missing_scd_match_cnt", cast(count(*) as string)
  from `wolt-assignment-489610.analytics_dev.int_wolt_order_items_with_item_price`
  where item_scd_sk is null
)
select * from c1
union all select * from c2
union all select * from c3
union all select * from c4
order by check_name;

-- ==========================================================
-- 2) Item-log quality impact (curation and source behavior)
-- ==========================================================
with stg as (
  select * from `wolt-assignment-489610.analytics_dev.stg_wolt_item_logs`
),
cur as (
  select * from `wolt-assignment-489610.analytics_dev.int_wolt_item_logs_curated_deduped`
),
dup as (
  select log_item_id, count(*) as c
  from stg
  group by 1
  having c > 1
)
select "stg_item_logs_rows" as metric, cast(count(*) as string) as value from stg
union all select "stg_distinct_log_item_id", cast(count(distinct log_item_id) as string) from stg
union all select "curated_rows", cast(count(*) as string) from cur
union all select "curated_distinct_log_item_id", cast(count(distinct log_item_id) as string) from cur
union all select "stg_invalid_price_null_or_non_positive", cast(countif(product_base_price_gross_eur is null or product_base_price_gross_eur <= 0) as string) from stg
union all select "stg_duplicate_log_item_groups", cast(count(*) as string) from dup
union all select "stg_duplicate_extra_rows", cast(coalesce(sum(c - 1), 0) as string) from dup
union all select "stg_brand_name_null_rows", cast(countif(brand_name is null) as string) from stg
union all select "curated_brand_name_null_rows", cast(countif(brand_name is null) as string) from cur
order by metric;

-- ==========================================================
-- 3) Purchase-log staging parsing and basket explode integrity
-- ==========================================================
with raw as (
  select * from `wolt-assignment-489610.raw.wolt_snack_store_purchase_logs`
),
stg as (
  select * from `wolt-assignment-489610.analytics_dev.stg_wolt_purchase_logs`
),
items as (
  select * from `wolt-assignment-489610.analytics_dev.stg_wolt_order_items`
)
select "raw_purchase_rows" as metric, cast(count(*) as string) as value from raw
union all select "stg_purchase_rows", cast(count(*) as string) from stg
union all select "stg_time_order_received_null", cast(countif(time_order_received_utc is null) as string) from stg
union all select "stg_item_basket_json_null", cast(countif(item_basket_description_json is null) as string) from stg
union all select "stg_total_basket_value_null", cast(countif(total_basket_value_eur is null) as string) from stg
union all select "stg_service_fee_null", cast(countif(wolt_service_fee_eur is null) as string) from stg
union all select "stg_courier_fee_null", cast(countif(courier_base_fee_eur is null) as string) from stg
union all select "stg_delivery_distance_null", cast(countif(delivery_distance_line_meters is null) as string) from stg
union all select "stg_orders_without_exploded_items", cast(count(*) as string)
from stg s
left join (select distinct purchase_key from items) i using(purchase_key)
where i.purchase_key is null
union all select "stg_order_items_rows", cast(count(*) as string) from items
order by metric;

-- ==========================================================
-- 4) SCD2 edge-case integrity
-- ==========================================================
with scd as (
  select * from `wolt-assignment-489610.analytics_dev.int_wolt_item_scd2`
)
select "scd_rows" as metric, cast(count(*) as string) as value from scd
union all select "scd_current_rows", cast(countif(is_current) as string) from scd
union all select "scd_zero_length_rows", cast(countif(valid_from_utc = valid_to_utc) as string) from scd
union all select "item_keys_with_same_timestamp_multi_logs", cast(count(*) as string)
from (
  select item_key, valid_from_utc, count(*) as c
  from scd
  group by 1, 2
  having c > 1
)
order by metric;

-- ==========================================================
-- 5) Promo ambiguity, timestamp ties, and value bounds
-- ==========================================================
with
same_ts as (
  select customer_key, time_order_received_utc, count(*) as c
  from `wolt-assignment-489610.analytics_dev.int_wolt_purchase_logs_curated_filtered`
  group by 1, 2
  having c > 1
),
promo_multi_match as (
  select p.order_item_sk, count(*) as promo_matches
  from `wolt-assignment-489610.analytics_dev.int_wolt_order_items_with_item_price` p
  join `wolt-assignment-489610.analytics_dev.stg_wolt_promos` pr
    on p.item_key = pr.item_key
   and p.order_date_berlin >= pr.promo_start_date
   and p.order_date_berlin < pr.promo_end_date
  group by 1
  having count(*) > 1
),
promo_unknown_items as (
  select count(*) as c
  from `wolt-assignment-489610.analytics_dev.stg_wolt_promos` pr
  left join (
    select distinct item_key from `wolt-assignment-489610.analytics_dev.dim_item_history`
  ) i using(item_key)
  where i.item_key is null
)
select "customers_with_same_timestamp_multi_orders" as metric, cast(count(*) as string) as value from same_ts
union all select "max_orders_same_customer_same_ts", cast(coalesce(max(c), 0) as string) from same_ts
union all select "order_items_with_multiple_promo_matches", cast(count(*) as string) from promo_multi_match
union all select "promo_rows_for_unknown_items", cast((select c from promo_unknown_items) as string)
union all select "fct_order_item_discount_gt_base_rows", cast(countif(line_discount_amount_gross_eur - line_base_amount_gross_eur > 0.000001) as string)
from `wolt-assignment-489610.analytics_dev.fct_order_item`;

-- ==========================================================
-- 6) Watermark and run metadata observability
-- ==========================================================
select model_name, watermark_ts, updated_at
from `wolt-assignment-489610.analytics_dev._elt_watermarks`
order by model_name;

select model_name, count(*) as run_rows, max(as_of_run_ts) as latest_run_ts
from `wolt-assignment-489610.analytics_dev._run_metadata`
where model_name in ("rpt_category_monthly_kpi_long", "rpt_customer_promo_behavior", "rpt_cross_sell_product_pairs")
group by model_name
order by model_name;
