# Lakehouse Lite 🧊🐧
A small, recruiter-friendly end-to-end data platform project.

Python ingestion → Postgres raw layer → dbt transformations → Streamlit dashboard

## Live Demo
- Streamlit Dashboard: https://lakehouse-lite-g99et945rfj2ob3iry9qzh.streamlit.app/

## What it does
This repo shows a complete analytics workflow:
1) Python ingests a real dataset into Postgres (`raw.penguins`)
2) dbt builds a staging model + a mart model in the `analytics` schema
3) Streamlit queries the mart table and displays KPIs, tables, and charts

## Features
- End-to-end pipeline: ingest → transform → serve
- Clear layer separation: raw schema + analytics schema
- dbt models:
  - `analytics.stg_penguins` (staging view)
  - `analytics.mart_penguin_summary` (mart table)
- dbt tests included (ex: not_null checks)
- Streamlit dashboard:
  - KPIs (total groups, total penguins, species count)
  - Penguin summary by species and sex
  - Charts (counts by species, counts by species/sex)
  - Raw sample preview

## Tech Stack
- Python: pandas, SQLAlchemy, psycopg2, python-dotenv
- Database: Postgres (Neon or any Postgres)
- Transform: dbt (dbt-postgres)
- Dashboard: Streamlit + Plotly
- Deployment: Streamlit Cloud

## Raw Data (GitHub Table View)
GitHub renders this CSV as a table (easy to click and view):
- Raw CSV: [data/raw/penguins.csv](data/raw/penguins.csv)

## Repo Structure
```text
lakehouse-lite/
  app/
    streamlit_app.py
  data/
    raw/
      penguins.csv
  dbt/
    lakehouse_lite/
      dbt_project.yml
      models/
        staging/
          stg_penguins.sql
        marts/
          mart_penguin_summary.sql
        schema.yml
  src/
    ingest/
      ingest_penguins.py
      db.py
  requirements.txt
  requirements-dev.txt