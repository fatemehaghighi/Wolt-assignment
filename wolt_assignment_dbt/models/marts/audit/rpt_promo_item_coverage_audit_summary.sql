select
    count(*) as promo_row_count,
    countif(in_dim_item_current_flag) as promo_rows_mapped_to_item_dim_count,
    countif(not in_dim_item_current_flag) as promo_rows_missing_item_dim_count
from {{ ref('rpt_promo_item_coverage_audit') }}
