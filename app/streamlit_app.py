import os
from urllib.parse import quote_plus

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text


# ----------------------------
# Config helpers
# ----------------------------
def _try_get_streamlit_secret(key: str):
    """
    Safely read st.secrets without crashing when secrets.toml doesn't exist locally.
    """
    try:
        # Accessing st.secrets can throw StreamlitSecretNotFoundError if no secrets file exists
        return st.secrets.get(key, None)
    except Exception:
        return None


def get_db_config() -> dict:
    """
    Priority:
    1) Environment variables (local .env loaded into process OR set in terminal)
    2) Streamlit secrets (Streamlit Cloud)
    """
    cfg = {
        "DB_HOST": os.getenv("DB_HOST") or _try_get_streamlit_secret("DB_HOST"),
        "DB_PORT": os.getenv("DB_PORT") or _try_get_streamlit_secret("DB_PORT") or "5432",
        "DB_NAME": os.getenv("DB_NAME") or _try_get_streamlit_secret("DB_NAME"),
        "DB_USER": os.getenv("DB_USER") or _try_get_streamlit_secret("DB_USER"),
        "DB_PASSWORD": os.getenv("DB_PASSWORD") or _try_get_streamlit_secret("DB_PASSWORD"),
        "DB_SSLMODE": os.getenv("DB_SSLMODE") or _try_get_streamlit_secret("DB_SSLMODE") or "require",
    }

    missing = [k for k, v in cfg.items() if not v and k != "DB_SSLMODE"]
    if missing:
        st.error(
            "Missing DB config: " + ", ".join(missing) +
            "\n\nFix: set env vars (DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DB_SSLMODE) "
            "or add them to Streamlit Secrets."
        )
        st.stop()

    return cfg


def make_engine():
    cfg = get_db_config()
    user = cfg["DB_USER"]
    pwd = quote_plus(cfg["DB_PASSWORD"] or "")
    host = cfg["DB_HOST"]
    port = cfg["DB_PORT"]
    db = cfg["DB_NAME"]
    ssl = cfg["DB_SSLMODE"] or "require"

    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}?sslmode={ssl}"
    return create_engine(url, pool_pre_ping=True)


def _fix_for_streamlit(df: pd.DataFrame) -> pd.DataFrame:
    """
    Avoid Streamlit frontend Arrow errors like: LargeUtf8 not recognized.
    Force all text-like columns to plain Python strings.
    """
    df = df.copy()

    for c in df.columns:
        dt = str(df[c].dtype)
        # handle pandas string[pyarrow] or any pyarrow-backed dtypes
        if "pyarrow" in dt or "Arrow" in dt or dt.startswith("string[pyarrow]"):
            df[c] = df[c].astype("string")

    # also ensure object columns are clean strings (helps prevent LargeUtf8 issues)
    for c in df.columns:
        if df[c].dtype == "object":
            # only convert if it looks like strings (not dict/list)
            sample = df[c].dropna().head(5).tolist()
            if all(isinstance(x, (str, int, float, bool)) for x in sample):
                df[c] = df[c].astype("string")

    return df


# ----------------------------
# Data loaders
# ----------------------------
@st.cache_data(ttl=300)
def load_summary() -> pd.DataFrame:
    engine = make_engine()
    q = """
    select
      species,
      sex,
      penguin_count,
      avg_body_mass_g,
      avg_flipper_length_mm
    from analytics.mart_penguin_summary
    order by species, sex;
    """
    df = pd.read_sql(q, engine)
    return _fix_for_streamlit(df)


@st.cache_data(ttl=300)
def load_raw_sample(limit: int = 10) -> pd.DataFrame:
    engine = make_engine()
    q = text(f"""
    select
      species,
      island,
      bill_length_mm,
      bill_depth_mm,
      flipper_length_mm,
      body_mass_g,
      sex
    from raw.penguins
    limit :limit;
    """)
    with engine.begin() as conn:
        df = pd.read_sql(q, conn, params={"limit": int(limit)})
    return _fix_for_streamlit(df)


# ----------------------------
# UI
# ----------------------------
st.set_page_config(page_title="Lakehouse Lite", layout="wide")

st.title("Lakehouse Lite")
st.caption("Python ingestion → Postgres raw layer → dbt transformations → Streamlit dashboard")

summary = load_summary()

# KPIs
col1, col2, col3 = st.columns(3)

total_groups = int(len(summary))
total_penguins = int(summary["penguin_count"].sum()) if "penguin_count" in summary.columns else 0
species_count = int(summary["species"].nunique()) if "species" in summary.columns else 0

col1.metric("Total groups", total_groups)
col2.metric("Total penguins", total_penguins)
col3.metric("Species count", species_count)

st.divider()

# Table
st.subheader("Penguin summary by species and sex")
st.dataframe(summary, width="stretch")

# Charts
st.subheader("Counts by species")
by_species = (
    summary.groupby("species", as_index=False)["penguin_count"]
    .sum()
    .sort_values("penguin_count", ascending=False)
)
fig1 = px.bar(by_species, x="species", y="penguin_count")
st.plotly_chart(fig1, width="stretch")

st.subheader("Counts by species and sex")
fig2 = px.bar(summary, x="species", y="penguin_count", color="sex", barmode="group")
st.plotly_chart(fig2, width="stretch")

st.divider()

# Raw sample
st.subheader("Raw penguins sample")
limit = st.slider("Rows to show", min_value=5, max_value=50, value=10, step=5)
raw_df = load_raw_sample(limit=limit)
st.dataframe(raw_df, width="stretch")
