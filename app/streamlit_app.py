import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import plotly.express as px


# ---------- helpers (fixes Streamlit Cloud LargeUtf8) ----------
def _to_display_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert Arrow-backed / nullable pandas dtypes into plain Python objects.
    This avoids Streamlit Cloud errors like: Unrecognized type: "LargeUtf8"
    """
    df = df.copy()

    # Turn pandas nullable values into None
    df = df.where(pd.notnull(df), None)

    # Force any string-like dtype to normal Python strings (object)
    for c in df.columns:
        dt = str(df[c].dtype)
        if "string" in dt or dt == "object":
            df[c] = df[c].apply(lambda x: None if x is None else str(x))

    return df


def _get_secret(key: str, default: str | None = None) -> str | None:
    # Streamlit Cloud: st.secrets
    try:
        if hasattr(st, "secrets") and key in st.secrets:
            return st.secrets[key]
    except Exception:
        pass
    # Local: env var
    return os.getenv(key, default)


def get_db_config() -> dict:
    return {
        "DB_HOST": _get_secret("DB_HOST"),
        "DB_PORT": _get_secret("DB_PORT", "5432"),
        "DB_NAME": _get_secret("DB_NAME"),
        "DB_USER": _get_secret("DB_USER"),
        "DB_PASSWORD": _get_secret("DB_PASSWORD"),
        "DB_SSLMODE": _get_secret("DB_SSLMODE", "require"),
    }


def make_engine():
    cfg = get_db_config()
    missing = [k for k, v in cfg.items() if v is None]
    if missing:
        raise RuntimeError(f"Missing DB config keys: {missing}")

    user = cfg["DB_USER"]
    pwd = quote_plus(cfg["DB_PASSWORD"])
    host = cfg["DB_HOST"]
    port = cfg["DB_PORT"]
    db = cfg["DB_NAME"]
    sslmode = cfg["DB_SSLMODE"]

    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}?sslmode={sslmode}"
    return create_engine(url, pool_pre_ping=True)


@st.cache_data(ttl=300)
def load_summary() -> pd.DataFrame:
    engine = make_engine()
    q = """
    select species, sex, penguin_count, avg_body_mass_g, avg_flipper_length_mm
    from analytics.mart_penguin_summary
    order by species, sex
    """
    return pd.read_sql(q, engine)


@st.cache_data(ttl=300)
def load_raw_sample() -> pd.DataFrame:
    engine = make_engine()
    q = """
    select species, island, bill_length_mm, bill_depth_mm, flipper_length_mm, body_mass_g, sex
    from analytics.stg_penguins
    limit 10
    """
    return pd.read_sql(q, engine)


# ---------- UI ----------
st.set_page_config(page_title="Lakehouse Lite", layout="wide")

st.title("Lakehouse Lite")
st.caption("Python ingestion → Postgres raw layer → dbt transformations → Streamlit dashboard")

summary = load_summary()
raw_df = load_raw_sample()

summary_disp = _to_display_df(summary)
raw_disp = _to_display_df(raw_df)

# KPIs
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Total groups", int(summary_disp.shape[0]))
with col2:
    st.metric("Total penguins", int(pd.to_numeric(summary["penguin_count"], errors="coerce").fillna(0).sum()))
with col3:
    st.metric("Species count", int(summary["species"].nunique()))

st.subheader("Penguin summary by species and sex")
st.dataframe(summary_disp, use_container_width=True)

# Only ONE chart: species + sex
st.subheader("Counts by species and sex")
fig = px.bar(
    summary_disp,
    x="species",
    y="penguin_count",
    color="sex",
    barmode="group",
)
st.plotly_chart(fig, use_container_width=True)

st.subheader("Raw penguins sample")
st.dataframe(raw_disp, use_container_width=True)
