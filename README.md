# Lakehouse Lite

Python ingestion → Postgres raw layer → dbt transformations → Streamlit dashboard

## Live Demo
Streamlit app: https://lakehouse-lite-g99et945rfj2ob3iry9qzh.streamlit.app/

## What it does
Lakehouse Lite is a small end-to-end data engineering project that shows the full flow from raw data to analytics and a dashboard:
1) Ingest a raw CSV into Postgres (`raw.penguins`)
2) Transform and test models with dbt (`analytics.stg_penguins`, `analytics.mart_penguin_summary`)
3) Serve results in a Streamlit dashboard (KPIs, tables, charts)

## Key features
- Raw → Staging → Mart structure (clean warehouse-style layers)
- dbt models + data quality tests (not_null checks)
- Simple analytics mart: summary by species + sex (counts + averages)
- Dashboard with:
  - Total groups, total penguins, species count
  - Summary table + charts
  - Raw data sample preview

## Tech stack
- Python: pandas, SQLAlchemy, psycopg2, python-dotenv
- Database: Postgres (Neon)
- Transformations: dbt (dbt-postgres)
- Dashboard: Streamlit + Plotly

## Repo structure
- `src/ingest/` → ingestion script that loads CSV into Postgres `raw` schema
- `dbt/lakehouse_lite/` → dbt project (staging + mart + tests)
- `app/` → Streamlit dashboard
- `data/raw/` → sample raw dataset (CSV)

## Raw Data (GitHub Table View)
GitHub renders this CSV as a table:
- Raw CSV: [data/raw/penguins.csv](data/raw/penguins.csv)

## Outputs (what to look at)
- Raw table: `raw.penguins`
- Staging view: `analytics.stg_penguins`
- Mart table: `analytics.mart_penguin_summary`
- Dashboard: KPIs + summary breakdown + charts + raw sample

## Why this project?
This repo is built to show the “real job” workflow end-to-end:
- ingest raw data
- model it cleanly (dbt)
- add basic tests
- deliver something visible (dashboard)

## Quick highlights
- Built an end-to-end analytics pipeline: **Python ingestion → Postgres raw → dbt transforms/tests → Streamlit dashboard**
- Implemented **warehouse layering** (raw/staging/mart) with clear ownership and naming
- Added **data quality checks** in dbt (not_null tests) to prevent bad data from reaching analytics
- Produced a **business-ready mart** (`analytics.mart_penguin_summary`) with counts and averages used by the dashboard
- Deployed a **live dashboard** so anyone can review results without running code locally


