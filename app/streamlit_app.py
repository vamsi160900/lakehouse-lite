import os
from urllib.parse import quote_plus

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text


def _get_secret(key: str, default: str | None = None) -> str | None:
    # Streamlit Cloud: st.secrets works
    try:
        if hasattr(st, "secrets") and key in st.secrets:
            return str(st.secrets[key])
    except Exception:
        pass

    # Local: fall back to env vars
    return os.getenv(key, default)


def get_db_config() -> dict:
    return {
        "DB_HOST": _get_secret("DB_HOST", ""),
        "DB_PORT": _get_secret("DB_PORT", "5432"),
        "DB_NAME": _get_secret("DB_NAME", ""),
        "DB_USER": _get_secret("DB_USER", ""),
        "DB_PASSWORD": _get_secret("DB_PASSWORD", ""),
        "DB_SSLMODE": _get_secret("DB_SSLMODE", "require"),
    }


def make_engine():
    cfg = get_db_config()

    user = cfg["DB_USER"]
    pwd = quote_plus(cfg["DB_PASSWORD"] or "")
    host = cfg["DB_HOST"]
    port = cfg["DB_PORT"] or "5432"
    db = cfg["DB_NAME"]
    ssl = cfg["DB_SSLMODE"] or "require"

    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}?sslmode={ssl}"
    return create_engine(url, pool_pre_ping=True)


@st.cache_data(ttl=300)
def load_metrics():
    engine = make_engine()
    q = """
    SELECT
      COUNT(*)::int AS total_groups,
      SUM(penguin_count)::int AS total_penguins,
      COUNT(DISTINCT species)::int AS species_count
    FROM analytics.mart_penguin_summary
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(q), conn)
    return df.iloc[0].to_dict()


@st.cache_data(ttl=300)
def load_summary():
    engine = make_engine()
    q = """
    SELECT
      species,
      sex,
      penguin_count,
      avg_body_mass_g,
      avg_flipper_length_mm
    FROM analytics.mart_penguin_summary
    ORDER BY species, sex
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(q), conn)

    # ---- IMPORTANT CLEANUP (fix missing Adelie bars + Arrow issues) ----
    df = df.copy()
    df["species"] = df["species"].astype(str).str.strip()
    df["sex"] = df["sex"].fillna("unknown").astype(str).str.strip()

    # force numbers
    df["penguin_count"] = pd.to_numeric(df["penguin_count"], errors="coerce").fillna(0).astype(int)
    df["avg_body_mass_g"] = pd.to_numeric(df["avg_body_mass_g"], errors="coerce")
    df["avg_flipper_length_mm"] = pd.to_numeric(df["avg_flipper_length_mm"], errors="coerce")

    # keep only known species (optional safety)
    df = df[df["species"].isin(["Adelie", "Chinstrap", "Gentoo"])]

    return df


@st.cache_data(ttl=300)
def load_raw_sample(n=10):
    engine = make_engine()
    q = f"""
    SELECT
      species,
      island,
      bill_length_mm,
      bill_depth_mm,
      flipper_length_mm,
      body_mass_g,
      sex
    FROM analytics.stg_penguins
    LIMIT {int(n)}
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(q), conn)

    # cleanup (avoid Arrow weird types)
    df = df.copy()
    for c in df.columns:
        if df[c].dtype == "object":
            df[c] = df[c].astype(str)

    return df


st.set_page_config(page_title="Lakehouse Lite", layout="wide")

st.title("Lakehouse Lite")
st.caption("Python ingestion → Postgres raw layer → dbt transformations → Streamlit dashboard")

# KPI cards
m = load_metrics()
c1, c2, c3 = st.columns(3)
c1.metric("Total groups", int(m["total_groups"]))
c2.metric("Total penguins", int(m["total_penguins"]))
c3.metric("Species count", int(m["species_count"]))

st.markdown("---")

summary = load_summary()

st.subheader("Penguin summary by species and sex")

# show summary table (use_container_width=True is safe on cloud)
st.dataframe(summary, use_container_width=True)

st.subheader("Counts by species and sex")

fig = px.bar(
    summary,
    x="species",
    y="penguin_count",
    color="sex",
    barmode="group",
    title="Counts by species and sex",
)
st.plotly_chart(fig, use_container_width=True)

st.subheader("Raw penguins sample")
raw_df = load_raw_sample(10)
st.dataframe(raw_df, use_container_width=True)
