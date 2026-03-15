# Task 2 - Q1 Dashboard Deep Dive (Business Readout)

Analysis scope:
- Question: Which categories are performing better and why? What are the star products?
- Window: 2023-01-01 to 2023-12-31 (Berlin-local order date)
- Data sources: `analytics_dev_rpt.rpt_category_monthly_kpi_long`, `analytics_dev_core.fct_order_item`

## What we checked (beyond revenue)
- Scale: total revenue, units, orders with category.
- Growth quality: first-to-last month growth in revenue, units, and orders.
- Pricing/mix: weighted ASP.
- Promo dependence: promo unit share.
- Customer quality: repeat-customer mix ratio.
- Stability: monthly revenue coefficient of variation (`revenue_cv`).
- Concentration: star-product revenue share in category.
- Classification drift risk: same `item_key` appearing in multiple categories.

## Current category situation (2023)

1. Crisp & Snacks
- Strongest category by scale: ~154.4k EUR revenue, ~61.8k units, ~44.8k orders.
- Growth is strong and broad-based (revenue +91%, units +87%, orders +83%).
- Risk: very high star concentration (~69.8% of category revenue from one SKU).
- Main driver: category demand expansion + one dominant hero item.
- Action: protect hero item availability, but reduce concentration with bundles and second-hero development.

2. Chocolate
- Second largest by scale: ~130.4k EUR revenue.
- Very high growth (+733% revenue first-to-last month), but with high volatility (`revenue_cv` ~0.89).
- High star concentration (~65.3%).
- Main driver: explosive demand around one hero product and likely category reclassification effect.
- Action: treat as growth engine but add volatility guardrails (weekly anomaly monitoring, stock buffer policy).

3. Other Confectionary
- Large category (~82.0k EUR) but strongly declining in period-end comparison (~-92% revenue).
- Extreme concentration (~93.2% on one product).
- Main driver: category migration/relabeling effect, not only demand collapse.
- Evidence: the same item_key `7aef490acb1ca55f113afe02977b9e8f` appears in both `Chocolate` and `Other Confectionary`.
- Action: for executive reporting, add a stable category bridge (dominant/current category mapping) to avoid false decline narratives.

4. Cheese Crackers, Breadsticks & Dipping
- Mid-scale (~35.5k EUR), strong healthy growth (+72% revenue).
- Lower volatility (`revenue_cv` ~0.21) than Chocolate.
- Moderately high concentration (~63.8%) near risk threshold.
- Main driver: stable repeat demand with one lead SKU.
- Action: optimize assortment depth around top item and test cross-sell with Crisp & Snacks.

5. Cookies
- Smaller category (~18.3k EUR) but healthy growth (+151% revenue).
- Very low concentration (~16.5%): broad product mix, lower single-SKU risk.
- Main driver: diversified portfolio expansion rather than one hero SKU.
- Action: continue breadth strategy; prioritize top quartile SKUs by margin/velocity.

6. Chocolate & Sweet Spreads
- Niche scale (~4.9k EUR), moderate growth (+45% revenue), low volatility.
- Mid concentration (~48.5%).
- Main driver: steady base demand with one visible lead product.
- Action: monitor for scale potential; promote in targeted baskets rather than broad discounts.

7. Toffee, Fudge & Nougat
- Smallest scale (~2.37k EUR), high growth (+184% revenue) from small base.
- Concentration ~65.1% (risk threshold crossed).
- Main driver: one leading SKU with low base effect.
- Action: avoid over-interpreting growth; keep as incubating category with concentration watch.

## Control framework by category

High-priority controls:
- `control_high_star_concentration_flag` (threshold: star revenue share >= 65%).
  Affects: Crisp & Snacks, Chocolate, Other Confectionary, Toffee/Fudge/Nougat.
- `control_high_revenue_volatility_flag` (threshold: `revenue_cv` >= 0.60).
  Affects: Chocolate, Other Confectionary.

Current non-triggered controls:
- `control_high_promo_dependency_flag` (promo unit share >= 12%): no category triggered.
- `control_low_repeat_mix_flag` (repeat mix <= 95%): no category triggered.

## Recommended Q1 dashboard structure in QuickSight
Use these SQL datasets:
- `sql/task_answers/task2/dashboard_q1/q1_category_monthly_performance.sql`
- `sql/task_answers/task2/dashboard_q1/q1_category_scorecard_and_controls.sql`
- `sql/task_answers/task2/dashboard_q1/q1_star_products_detail.sql`

Charts:
1. Category monthly revenue trend (line)
2. Growth matrix (revenue growth vs unit growth, bubble=size)
3. Quality matrix (repeat mix vs promo share)
4. Star-product concentration bars
5. Category control heatmap (risk flags)
6. Category scorecard table with drill-through to top-10 products

## Interpretation caveat
- Category performance can be affected by historical category label changes (SCD2-consistent modeling).
- For management reporting, add a stable category mapping layer to separate true demand movement from classification drift.
