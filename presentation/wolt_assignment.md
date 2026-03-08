# Wolt Assignment - Category Growth and Promo Insights

## 1) Scope and Questions

- Build a reliable data model to explain category growth in Berlin (2023).
- Identify:
  - best/worst performing categories
  - star products
  - products bought together
  - role of promotions in first-time customer acquisition

## 2) Data and Modeling Approach

- Sources: item logs, purchase logs, promotions.
- Warehouse flow:
  - `staging`: source-conformed parsing
  - `intermediate`: curation, SCD2, promo assignment, pricing
  - `marts`: core facts + reporting marts
- Facts:
  - `fct_order` (order grain)
  - `fct_order_item` (order-item grain)

## 3) Data Issues and Assumptions

- Duplicate item-log events with conflicting prices observed.
- Rule applied in curated item logs:
  - keep positive/non-null price event
  - discard non-business-relevant null/negative duplicates
- Basket JSON is defensively aggregated by `(purchase_key, item_key)` to avoid duplicate item rows in one order.

## 4) Category Growth Findings (Q1 vs Q4 2023 Revenue)

Top positive movers:

- `Chocolate`: +62.0k EUR (`+664.8%`)
- `Crisp & Snacks`: +18.6k EUR (`+59.9%`)
- `Cheese Crackers, Breadsticks & Dipping`: +4.2k EUR (`+60.3%`)
- `Cookies`: +3.1k EUR (`+104.6%`)

Weak/declining signal:

- `Other Confectionary`: -28.2k EUR (`-92.9%`)

## 5) Star Products (2023 Revenue)

- `Funny-Frisch Kettle Chips Sweet Chilli & Red Pepper, 120 g`: 107.8k EUR
- `Tony’s Chocolonely Dark Milk Brownie Chocolate, 180 g`: 85.1k EUR (Chocolate label)
- `Tony’s Chocolonely Dark Milk Brownie Chocolate, 180 g`: 76.4k EUR (Other Confectionary label in part of history)
- `Fuego salsa dip, mild, 200 ml`: 22.6k EUR

## 6) Promo and First-Time Customer Behavior

From `rpt_customer_promo_behavior` latest snapshot:

- Total customers: `2,001`
- First order had any promo item: `44` customers (`2.20%`)
- First order had only promo items: `19` customers (`0.95%`)

Interpretation:

- Promotions contribute to acquisition, but only a small share of first orders are promo-driven.

## 7) Item Affinity Signals

Examples of top-lift pairs (latest snapshot):

- `Veganz Chocolate Cookie` + `Griesson Soft Cake cherry`: lift `47.75`
- `Reese’s Minis` + `Griesson Soft Cake cherry`: lift `39.79`
- `Storck Super Dickmann’s` + `Storck Mini Dickmann’s`: lift `38.16`

These are strong cross-sell candidates for bundle placement and recommendation ranking.

## 8) Limitations and Next Steps

- Historical category/name changes can affect label interpretation; model now uses deterministic month-end labels in affinity.
- Add store/service-area dimensions for localized growth decomposition.
- Add margin/cost signals to distinguish revenue growth from profitability growth.
