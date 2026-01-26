import os
import pandas as pd
import streamlit as st

import plotly.express as px
import plotly.graph_objects as go

from sqlalchemy import create_engine
from urllib.parse import quote_plus

# Local only (Streamlit Cloud won't need this)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


def get_config(key: str, default: str | None = None) -> str | None:
    # Streamlit Cloud secrets
    try:
        if hasattr(st, "secrets") and key in st.secrets:
            return str(st.secrets[key])
    except Exception:
        pass

    # Local env vars / .env
    return os.getenv(key, default)


def make_engine():
    host = get_config("DB_HOST")
    port = get_config("DB_PORT", "5432")
    db = get_config("DB_NAME")
    user = get_config("DB_USER")
    pwd = get_config("DB_PASSWORD")
    sslmode = get_config("DB_SSLMODE", "require")

    if not all([host, port, db, user, pwd]):
        raise RuntimeError(
            "Missing DB config. Set DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DB_SSLMODE "
            "in Streamlit secrets (Cloud) or .env (local)."
        )

    pwd_enc = quote_plus(pwd)
    url = f"postgresql+psycopg2://{user}:{pwd_enc}@{host}:{port}/{db}?sslmode={sslmode}"
    return create_engine(url, pool_pre_ping=True)


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
    order by species, sex
    """
    return pd.read_sql(q, engine)


@st.cache_data(ttl=300)
def load_raw(limit: int = 25) -> pd.DataFrame:
    engine = make_engine()
    q = f"""
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
    limit {int(limit)}
    """
    return pd.read_sql(q, engine)


def df_to_plotly_table(df: pd.DataFrame, max_rows: int = 50, title: str | None = None):
    d = df.copy()
    if max_rows:
        d = d.head(max_rows)

    # Clean NaN/None and make safe strings
    d = d.where(pd.notnull(d), "")
    for c in d.columns:
        if d[c].dtype == object:
            d[c] = d[c].astype(str)

    header = dict(values=list(d.columns))
    cells = dict(values=[d[c].tolist() for c in d.columns])

    fig = go.Figure(data=[go.Table(header=header, cells=cells)])
    if title:
        fig.update_layout(title=title)
    fig.update_layout(margin=dict(l=0, r=0, t=40 if title else 10, b=0))
    return fig


# ---------------- UI ----------------
st.set_page_config(page_title="Lakehouse Lite", layout="wide")

st.title("Lakehouse Lite")
st.caption("Python ingestion → Postgres raw layer → dbt transformations → Streamlit dashboard")

summary = load_summary()
raw_df = load_raw(25)

# KPIs
total_groups = int(summary.shape[0])
total_penguins = int(summary["penguin_count"].sum()) if "penguin_count" in summary.columns else 0
species_count = int(summary["species"].nunique()) if "species" in summary.columns else 0

c1, c2, c3 = st.columns(3)
c1.metric("Total groups", total_groups)
c2.metric("Total penguins", total_penguins)
c3.metric("Species count", species_count)

st.subheader("Penguin summary by species and sex")
st.plotly_chart(df_to_plotly_table(summary, max_rows=50), use_container_width=True)

# ONE chart only
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
st.plotly_chart(df_to_plotly_table(raw_df, max_rows=25), use_container_width=True)
