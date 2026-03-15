# Lightdash Setup (Open Source BI)

This folder provides a local self-hosted Lightdash setup so you can explore the dbt models with a semantic layer style workflow.
It includes MinIO as S3-compatible storage so SQL/table native pagination works with BigQuery.

## 1) Start Lightdash

```bash
cd bi/lightdash
cp .env.example .env
# set a strong LIGHTDASH_SECRET in .env
docker compose up -d
```

Open: http://localhost:8080
MinIO Console (optional): http://localhost:9001

## 2) Create admin user in UI

On first launch, create your Lightdash admin account.

## 3) Connect your warehouse (BigQuery)

In Lightdash UI, create a project and choose BigQuery.
Use a service account with read access to your analytics datasets:

- `analytics_dev_core`
- `analytics_dev_rpt`
- (optional) `analytics_dev_audit`

## 4) Connect dbt semantic metadata (manifest)

This repo now uses a manifest-backed Lightdash project for semantic Explore usage.
`make lightdash-connect-semantic` compiles dbt and uploads `wolt_assignment_dbt/target/manifest.json`.

## 5) Start with these models

- `fct_order`
- `fct_order_item`
- `rpt_category_monthly_kpi_long`
- `rpt_customer_promo_behavior`
- `rpt_cross_sell_product_pairs`

These already map to assignment questions and business dashboards.

## Quick Auto-Connect (already wired in this repo)

After login once in Lightdash UI, run:

```bash
make lightdash-connect
```

This command will:
- create/reuse Lightdash project `Wolt Assignment Dev`,
- connect it to BigQuery using values from root `.env`,
- run a SQL Runner table-discovery smoke test.

For semantic layer + dashboards:

```bash
make lightdash-connect-semantic
make lightdash-dashboards
```

This will:
- create/reuse `Wolt Assignment Dev Semantic` (dbt connection type `manifest`),
- refresh project compilation in Lightdash,
- create both Task 1 and Task 2 dashboards.

Dashboard maintenance behavior:
- Task dashboards are generated with chart + guide pairs.
- Guide tiles are markdown (not SQL tables) and include:
  - what the chart says,
  - main metric,
  - metric calculation logic,
  - how to use the chart.
- Existing dashboard layout edits made in UI are kept by default when rerunning:
  - `make lightdash-task1`
  - `make lightdash-task2`
- Force a full dashboard recreation only when needed:
  - `LIGHTDASH_RECREATE_DASHBOARD=1 make lightdash-task1`
  - `LIGHTDASH_RECREATE_DASHBOARD=1 make lightdash-task2`

Optional layout controls when (re)creating dashboards:
- `LIGHTDASH_CHART_TILE_WIDTH` (default: `24`, total grid width: `48`)
- `LIGHTDASH_ROW_TILE_HEIGHT` (default: `10`)

## Semantic Layer Note

This repository contains dbt semantic definitions at:
- `wolt_assignment_dbt/models/marts/metrics/semantic_models.yml`

In Lightdash, open the semantic project and go to `Explore`.
You will see dbt model metadata (dimensions/measures) compiled from the manifest, then use dashboards for curated business views.

## Stop / cleanup

```bash
cd bi/lightdash
docker compose down
# remove containers + volume
docker compose down -v
```
