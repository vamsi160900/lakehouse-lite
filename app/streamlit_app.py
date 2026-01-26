# app/streamlit_app.py
from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

# Optional (works locally; safe on Streamlit Cloud too)
try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None


# ----------------------------
# Helpers
# ----------------------------
def _repo_root() -> Path:
    # app/streamlit_app.py -> repo root is 1 level up from "app"
    return Path(__file__).resolve().parents[1]


def _load_local_env_if_present() -> None:
    """Load .env only for local runs (Streamlit Cloud uses secrets)."""
    if load_dotenv is None:
        return
    env_path = _repo_root() / ".env"
    if env_path.exists():
        load_dotenv(env_path)


def _get_cfg(key: str, default: str | None = None) -> str | None:
    # 1) Streamlit secrets (Cloud)
    try:
        if hasattr(st, "secrets") and key in st.secrets:
            return str(st.secrets[key])
    except Exception:
        # If no secrets.toml exists locally, Streamlit may raise here
        pass

    # 2) Environment variables (local .env or OS env)
    return os.getenv(key, default)


def get_db_config() -> dict:
    _load_local_env_if_present()

    cfg = {
        "DB_HOST": _get_cfg("DB_HOST"),
        "DB_PORT": _get_cfg("DB_PORT", "5432"),
        "DB_NAME": _get_cfg("DB_NAME"),
        "DB_USER": _get_cfg("DB_USER"),
        "DB_PASSWORD": _get_cfg("DB_PASSWORD"),
        "DB_SSLMODE": _get_cfg("DB_SSLMODE", "require"),
    }

    missing = [k for k, v in cfg.items() if v is None or str(v).strip() == ""]
    if missing:
        st.error(
            "Missing DB config: "
            + ", ".join(missing)
            + "\n\nSet them in Streamlit Cloud Secrets or in a local .env file."
        )
        st.stop()

    return cfg  # type: ignore[return-value]


@st.cache_resource(show_spinner=False)
def make_engine():
    cfg = get_db_config()
    user = cfg["DB_USER"]
    pwd = quote_plus(cfg["DB_PASSWORD"])
    host = cfg["DB_HOST"]
    port = cfg["DB_PORT"]
    db = cfg["DB_NAME"]
    ssl = cfg["DB_SSLMODE"]

    # SQLAlchemy URL (psycopg2)
    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}?sslmode={ssl}"

    # pool_pre_ping helps avoid stale connections on Cloud
    return create_engine(url, pool_pre_ping=True)


def _safe_for_streamlit_arrow(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fix Streamlit/Arrow issues like:
    - Unrecognized type: "LargeUtf8"
    by forcing string columns to plain Python objects.
    """
    df = df.copy()

    for col in df.columns:
        dt = df[col].dtype
        # pandas "string" dtype can become LargeUtf8 via pyarrow
        if str(dt).startswith("string"):
            df[col] = df[col].astype("object")
        # object columns that contain non-serializable types
        elif dt == "object":
            # convert None/NaN safely and keep as object
            df[col] = df[col].map(lambda x: None if pd.isna(x) else x).astype("object")

    return df


@st.cache_data(show_spinner=False, ttl=300)
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
    order by species, sex
    """
    df = pd.read_sql(q, engine)
    return _safe_for_streamlit_arrow(df)


@st.cache_data(show_spinner=False, ttl=300)
def load_raw_sample(limit: int = 10) -> pd.DataFrame:
    engine = make_engine()
    q = text(
        """
        select
          species,
          island,
          bill_length_mm,
          bill_depth_mm,
          flipper_length_mm,
          body_mass_g,
          sex
        from analytics.stg_penguins
        order by species, island
        limit :limit
        """
    )
    df = pd.read_sql(q, engine, params={"limit": limit})
    return _safe_for_streamlit_arrow(df)


# ----------------------------
# UI
# ----------------------------
st.set_page_config(page_title="Lakehouse Lite", layout="wide")

st.title("Lakehouse Lite")
st.caption("Python ingestion → Postgres raw layer → dbt transformations → Streamlit dashboard")

summary = load_summary()
raw_df = load_raw_sample(10)

# Metrics
total_groups = int(summary.shape[0])
total_penguins = int(pd.to_numeric(summary["penguin_count"], errors="coerce").fillna(0).sum())
species_count = int(summary["species"].nunique())

c1, c2, c3 = st.columns(3)
c1.metric("Total groups", total_groups)
c2.metric("Total penguins", total_penguins)
c3.metric("Species count", species_count)

st.markdown("---")

# Table
st.subheader("Penguin summary by species and sex")
st.dataframe(summary, use_container_width=True)

st.markdown("---")

# ONLY ONE BAR CHART (species + sex)
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

st.markdown("---")

# Raw sample
st.subheader("Raw penguins sample")
st.dataframe(raw_df, use_container_width=True)
