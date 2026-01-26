import os
from urllib.parse import quote_plus

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine
import plotly.express as px


def _get_cfg_value(key: str, default: str | None = None) -> str | None:
    # Streamlit Cloud uses st.secrets, local can use env vars
    try:
        if hasattr(st, "secrets") and key in st.secrets:
            return str(st.secrets[key])
    except Exception:
        pass
    return os.getenv(key, default)


def get_db_config() -> dict:
    return {
        "DB_HOST": _get_cfg_value("DB_HOST"),
        "DB_PORT": _get_cfg_value("DB_PORT", "5432"),
        "DB_NAME": _get_cfg_value("DB_NAME"),
        "DB_USER": _get_cfg_value("DB_USER"),
        "DB_PASSWORD": _get_cfg_value("DB_PASSWORD"),
        "DB_SSLMODE": _get_cfg_value("DB_SSLMODE", "require"),
    }


def make_engine():
    cfg = get_db_config()
    missing = [k for k, v in cfg.items() if not v and k != "DB_SSLMODE"]
    if missing:
        st.error(
            "Missing DB config: " + ", ".join(missing) +
            ". Add them in Streamlit Cloud -> App settings -> Secrets (or set env vars locally)."
        )
        st.stop()

    user = cfg["DB_USER"]
    pwd = quote_plus(cfg["DB_PASSWORD"])
    host = cfg["DB_HOST"]
    port = cfg["DB_PORT"]
    db = cfg["DB_NAME"]
    ssl = cfg["DB_SSLMODE"] or "require"

    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}?sslmode={ssl}"
    return create_engine(url, pool_pre_ping=True)


def make_streamlit_safe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fix Streamlit Cloud 'LargeUtf8' error by forcing string/object columns
    into normal python object strings before st.dataframe().
    """
    df = df.copy()
    for col in df.columns:
        if pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col]):
            # convert to python strings, keep NaN as empty for clean display
            df[col] = df[col].astype("object").where(df[col].notna(), "")
    return df


@st.cache_data(ttl=300)
def load_summary() -> pd.DataFrame:
    engine = make_engine()
    q = """
    select species, sex, penguin_count, avg_body_mass_g, avg_flipper_length_mm
    from analytics.mart_penguin_summary
    order by species, sex
    """
    df = pd.read_sql(q, engine)
    return make_streamlit_safe(df)


@st.cache_data(ttl=300)
def load_counts_by_species() -> pd.DataFrame:
    engine = make_engine()
    q = """
    select species, sum(penguin_count)::int as penguin_count
    from analytics.mart_penguin_summary
    group by species
    order by species
    """
    df = pd.read_sql(q, engine)
    return make_streamlit_safe(df)


@st.cache_data(ttl=300)
def load_counts_by_species_sex() -> pd.DataFrame:
    engine = make_engine()
    q = """
    select species, sex, penguin_count
    from analytics.mart_penguin_summary
    order by species, sex
    """
    df = pd.read_sql(q, engine)
    return make_streamlit_safe(df)


@st.cache_data(ttl=300)
def load_raw_sample(limit: int = 10) -> pd.DataFrame:
    engine = make_engine()
    q = f"""
    select species, island, bill_length_mm, bill_depth_mm, flipper_length_mm, body_mass_g, sex
    from analytics.stg_penguins
    order by species, island
    limit {int(limit)}
    """
    df = pd.read_sql(q, engine)
    return make_streamlit_safe(df)


st.set_page_config(page_title="Lakehouse Lite", layout="wide")

st.title("Lakehouse Lite")
st.caption("Python ingestion → Postgres raw layer → dbt transformations → Streamlit dashboard")

summary = load_summary()

total_groups = int(len(summary))
total_penguins = int(pd.to_numeric(summary["penguin_count"], errors="coerce").fillna(0).sum())
species_count = int(summary["species"].nunique())

c1, c2, c3 = st.columns(3)
c1.metric("Total groups", total_groups)
c2.metric("Total penguins", total_penguins)
c3.metric("Species count", species_count)

# divider (safe across versions)
if hasattr(st, "divider"):
    st.divider()
else:
    st.markdown("---")

st.subheader("Penguin summary by species and sex")
st.dataframe(summary, use_container_width=True)

# charts
counts_species = load_counts_by_species()
fig1 = px.bar(counts_species, x="species", y="penguin_count", title="Counts by species")
st.plotly_chart(fig1, use_container_width=True)

counts_species_sex = load_counts_by_species_sex()
fig2 = px.bar(counts_species_sex, x="species", y="penguin_count", color="sex", barmode="group",
              title="Counts by species and sex")
st.plotly_chart(fig2, use_container_width=True)

st.subheader("Raw penguins sample")
raw_df = load_raw_sample(10)
st.dataframe(raw_df, use_container_width=True)
