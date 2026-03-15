# Task 2 Q1 Dashboard Pack (QuickSight)

Business question:
- Which categories are performing better within Wolt Snacks and why?
- What are the star products?

## Query files
- `q1_category_monthly_performance.sql`
  - Trend dataset by month x category.
- `q1_category_scorecard_and_controls.sql`
  - Scorecard + control flags at category grain.
- `q1_star_products_detail.sql`
  - Top-10 products per category with concentration shares.

## Recommended visuals
1. KPI tiles (from scorecard)
- `total_revenue_eur`, `total_units_sold`, `total_orders_with_category`.

2. Monthly trend line (from monthly performance)
- x: `period_month`
- y: `revenue_eur`
- color: `item_category`

3. Growth scatter
- x: `unit_growth_ratio_first_to_last`
- y: `revenue_growth_ratio_first_to_last`
- bubble: `total_revenue_eur`
- color: `item_category`

4. Quality vs risk scatter
- x: `repeat_customer_mix_ratio`
- y: `promo_unit_share`
- bubble: `total_revenue_eur`
- color: `item_category`

5. Category risk/control heatmap
- rows: `item_category`
- columns: control flags
- values: `control_*_flag`

6. Star product table/bar
- from `q1_star_products_detail.sql`
- show `revenue_rank_in_category <= 3` for executive view.

## Interpretation guidance
- High performance should not rely on only one metric.
- Use revenue + growth + repeat mix + promo dependency + concentration + volatility together.
