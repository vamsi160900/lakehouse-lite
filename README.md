# Lakehouse Lite (Postgres + dbt + Streamlit)

Live Demo: https://lakehouse-lite-g99et945rfj2ob3iry9qzh.streamlit.app/#lakehouse-lite
GitHub Repo: https://github.com/vamsi160900/lakehouse-lite

## What this project does
Lakehouse Lite is a simple end-to-end data engineering project:
- Downloads a public CSV dataset (Penguins)
- Loads it into Postgres as a raw layer (raw.penguins)
- Transforms it using dbt into analytics tables (analytics.*)
- Shows results in a live Streamlit dashboard

## Features
- Docker Postgres for local development
- Python ingestion script loads data into raw.penguins
- dbt staging model + mart model with tests
- Streamlit dashboard with KPIs, table, and chart
- Deployed publicly (Neon Postgres + Streamlit Community Cloud)

## Tech stack
- Python: pandas, SQLAlchemy, psycopg2, python-dotenv
- Postgres: Docker locally, Neon in cloud
- dbt: dbt-core, dbt-postgres
- Streamlit: dashboard
- GitHub: version control + hosting

## Screenshot
![Dashboard](screenshots/dashboard.png)

## Folder structure
- src/ingest/         Python ingestion scripts
- dbt/lakehouse_lite/ dbt models + tests
- app/                Streamlit app
- screenshots/         README images

## How to run locally (Windows 11)

1) Start Postgres (Docker)
powershell:
docker rm -f lakehouse-postgres
docker run --name lakehouse-postgres `
  -e POSTGRES_USER=lakehouse `
  -e POSTGRES_PASSWORD=lakehouse123 `
  -e POSTGRES_DB=lakehouse_db `
  -p 5432:5432 `
  -v lakehouse_pgdata:/var/lib/postgresql/data `
  -d postgres:16

2) Setup Python environment
powershell:
cd E:\MyProjects\lakehouse-lite
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements-dev.txt

3) Create .env (local only, do not commit)
DB_HOST=localhost
DB_PORT=5432
DB_NAME=lakehouse_db
DB_USER=lakehouse
DB_PASSWORD=lakehouse123
DB_SSLMODE=

4) Ingest raw data
powershell:
cd src\ingest
python ingest_penguins.py

5) Run dbt models + tests
powershell:
cd ..\..\dbt\lakehouse_lite
dbt run --select stg_penguins mart_penguin_summary
dbt test --select stg_penguins mart_penguin_summary

6) Run the dashboard
powershell:
cd ..\..
streamlit run app\streamlit_app.py

## Deployment notes
- Streamlit app is deployed on Streamlit Community Cloud
- Database is Neon Postgres
- DB credentials are stored in Streamlit Secrets (not in GitHub)
