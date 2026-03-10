# Task 2 Findings (Data-Backed)

Date of extraction: 2026-03-10  
Source: `analytics_dev_core` + `analytics_dev_rpt` (latest `snapshot_date`)

## 1) Best-performing categories and star products
- Top categories by 2023 revenue:
  - `Crisp & Snacks`: **154,370.45 EUR**, 61,844 units
  - `Chocolate`: **130,380.92 EUR**, 44,647 units
  - `Other Confectionary`: **81,952.21 EUR**, 25,104 units
- Example star products:
  - `Funny-Frisch Kettle Chips Sweet Chilli & Red Pepper, 120 g` (Crisp & Snacks)
  - `Tony’s Chocolonely Dark Milk Brownie Chocolate, 180 g` (Chocolate)

## 2) Categories not improving
- Clear negative Jan->Dec direction in 2023:
  - `Other Confectionary`: **-8,312.19 EUR** (Jan to Dec monthly revenue delta)
- Low-scale categories (small absolute contribution):
  - `Toffee, Fudge & Nougat`
  - `Chocolate & Sweet Spreads`

## 3) Product affinity (bought together)
- Strong pair example (high lift and sufficient frequency):
  - `Pringles Sour Cream & Onion` + `Crunchips Cheese & Onion`
  - lift: **24.29**, orders together: **51** (Apr 2023)
- Repeated chips+dip pair signals across months:
  - `Chio Tortillas Original Salted` + `Fuego salsa dip, mild`
  - `Funny-Frisch Kettle Chips Sweet Chilli` + `Chio Dip Hot Cheese`

## 4) Time-based category consumption
- Peak hour across all major categories: **18:00 Berlin time**.
- Highest demand weekdays by units/revenue:
  - **Wednesday**, then **Tuesday**.

## 5) First-time customers and promotions
- Total customers: **2,001**
- First order had any promo units: **44** (**2.20%**)
- First order had only promo units: **19** (**0.95%**)
- Lifetime promo-only customers: **0**

## Interpretation for business
- Growth is strongly concentrated in snack/chocolate demand.
- Affinity suggests cross-sell bundles (chips + dip) are a real lever.
- Promo is not a major first-customer acquisition driver in this dataset.
- Evening operations (around 18:00) are critical for availability and assortment.
