import os
import pandas as pd
import streamlit as st
import plotly.express as px

import pyarrow as pa
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# Only used for local runs (Streamlit Cloud won't need it)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


# ----------------------------
# Helpers
# ----------------------------
def get_config(key: str, default: str | None = None) -> str | None:
    # Streamlit Cloud: secrets
    try:
        if hasattr(st, "secrets") and key in st.secrets:
            return str(st.secrets[key])
    except Exception:
        pass

    # Local: env vars / .env
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


def to_arrow_safe(df: pd.DataFrame) -> pa.Table:
    # Make a clean copy
    df = df.copy()

    # Replace NaN with None
    df = df.where(pd.notnull(df), None)

    # Force string/object columns into plain Python strings
    for c in df.columns:
        if pd.api.types.is_string_dtype(df[c]) or df[c].dtype == object:
            df[c] = df[c].apply(lambda x: None if x is None else str(x))

    # Convert to Arrow table
    t = pa.Table.from_pandas(df, preserve_index=False)

    # Cast LargeUtf8 -> Utf8 (normal string)
    new_fields = []
    for f in t.schema:
        if pa.types.is_large_string(f.type):
            new_fields.append(pa.field(f.name, pa.string()))
        else:
            new_fields.append(f)

    try:
        t = t.cast(pa.schema(new_fields))
    except Exception:
        # fallback: cast column-by-column
        cols = []
        for i, f in enumerate(t.schema):
            col = t.column(i)
            if pa.types.is_large_string(f.type):
                cols.append(col.cast(pa.string()))
            else:
                cols.append(col)
        t = pa.Table.from_arrays(cols, names=t.schema.names)

    return t


# ----------------------------
# Queries
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


# ----------------------------
# UI
# ----------------------------
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

# Table (Arrow-safe)
st.dataframe(to_arrow_safe(summary), use_container_width=True)

# ONE chart only: species + sex
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
st.dataframe(to_arrow_safe(raw_df), use_container_width=True)
