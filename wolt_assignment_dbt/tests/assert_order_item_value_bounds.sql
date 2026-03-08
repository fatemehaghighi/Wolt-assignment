select *
from {{ ref('fct_order_item') }}
where (vat_rate_pct < 0 or vat_rate_pct > 100)
    or (line_discount_amount_gross_eur - line_base_amount_gross_eur > 0.01)
