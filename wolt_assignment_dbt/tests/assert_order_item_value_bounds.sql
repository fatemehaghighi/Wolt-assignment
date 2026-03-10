select *
from {{ ref('fct_order_item') }}
where (vat_rate_pct < 0 or vat_rate_pct > 100)
    or (order_item_row_discount_amount_gross_eur - order_item_row_base_amount_gross_eur > 0.01)
