# Lakehouse Lite

Python ingestion → Postgres raw layer → dbt transformations → Streamlit dashboard

## Live Dashboard
- Streamlit app: https://lakehouse-lite-g99et945rfj2ob3iry9qzh.streamlit.app/

## Project Flow
1) Ingest CSV → load into Postgres `raw.penguins`
2) Transform with dbt → `analytics.stg_penguins` and `analytics.mart_penguin_summary`
3) Visualize in Streamlit (KPIs, charts, tables)

## Repository Structure
- `src/ingest/` → ingestion scripts
- `data/raw/` → raw dataset files (tracked)
- `dbt/lakehouse_lite/` → dbt models + tests
- `app/` → Streamlit dashboard

## Raw Data (GitHub Table View)
- Raw CSV: [data/raw/penguins.csv](data/raw/penguins.csv)

## dbt Models
- Staging model: [dbt/lakehouse_lite/models/staging/stg_penguins.sql](dbt/lakehouse_lite/models/staging/stg_penguins.sql)
- Mart model: [dbt/lakehouse_lite/models/marts/mart_penguin_summary.sql](dbt/lakehouse_lite/models/marts/mart_penguin_summary.sql)
- Tests: [dbt/lakehouse_lite/models/schema.yml](dbt/lakehouse_lite/models/schema.yml)

## Run Locally (Windows PowerShell)
```powershell
cd "E:\MyProjects\lakehouse-lite"
.\.venv\Scripts\Activate.ps1

# load .env into this PowerShell session
Get-Content .env | ForEach-Object {
  if ($_ -match '^\s*$' -or $_ -match '^\s*#') { return }
  $k, $v = $_ -split '=', 2
  $k = $k.Trim()
  $v = $v.Trim().Trim('"').Trim("'")
  [System.Environment]::SetEnvironmentVariable($k, $v, "Process")
}

# ingest raw data
python .\src\ingest\ingest_penguins.py

# run dbt
cd ".\dbt\lakehouse_lite"
dbt debug
dbt run --select stg_penguins mart_penguin_summary
dbt test --select stg_penguins mart_penguin_summary

# run Streamlit
cd "..\.."
streamlit run .\app\streamlit_app.py
