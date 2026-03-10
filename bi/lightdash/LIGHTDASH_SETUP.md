# Lightdash Setup (Open Source BI)

This folder provides a local self-hosted Lightdash setup so you can explore the dbt models with a semantic layer style workflow.

## 1) Start Lightdash

```bash
cd bi/lightdash
cp .env.example .env
# set a strong LIGHTDASH_SECRET in .env
docker compose up -d
```

Open: http://localhost:8080

## 2) Create admin user in UI

On first launch, create your Lightdash admin account.

## 3) Connect your warehouse (BigQuery)

In Lightdash UI, create a project and choose BigQuery.
Use a service account with read access to your analytics datasets:

- `analytics_dev_core`
- `analytics_dev_rpt`
- (optional) `analytics_dev_audit`

## 4) Connect dbt project metadata

Point Lightdash project to this dbt project so model-level metadata is available.
Recommended branch: `main`.

Project path: `wolt_assignment_dbt`

## 5) Start with these models

- `fct_order`
- `fct_order_item`
- `rpt_category_daily`
- `rpt_customer_promo_behavior`
- `rpt_item_pair_affinity`

These already map to assignment questions and business dashboards.

## Semantic Layer Note

This repository contains dbt semantic definitions (`models/marts/metrics/semantic_models.yml`) and reporting marts.
In practice for OSS BI, Lightdash is the easiest way to consume centralized metric/dimension logic from the dbt project and avoid repeated SQL in dashboards.

## Stop / cleanup

```bash
cd bi/lightdash
docker compose down
# remove containers + volume
docker compose down -v
```
