-- Data quality assertion for curated item logs.
-- Assumption documented in modeling notes: valid item prices must be positive and non-null.
select *
from {{ ref('int_wolt_item_logs_curated_deduped') }}
where product_base_price_gross_eur is null
   or product_base_price_gross_eur <= 0
