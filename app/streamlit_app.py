import os
from urllib.parse import quote_plus

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text


def _get_secret(key: str, default: str | None = None) -> str | None:
    # Streamlit Cloud: st.secrets
    try:
        if hasattr(st, "secrets") and key in st.secrets:
            return str(st.secrets[key])
    except Exception:
        pass
    # Local: env vars
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


def render_table_html(df: pd.DataFrame, height_px: int = 520):
    # Convert to safe strings to avoid any Arrow/typing issues
    safe = df.copy()
    for c in safe.columns:
        if pd.api.types.is_datetime64_any_dtype(safe[c]):
            safe[c] = safe[c].astype(str)
        elif pd.api.types.is_numeric_dtype(safe[c]):
            # keep numeric as-is
            pass
        else:
            safe[c] = safe[c].fillna("").astype(str)

    html = safe.to_html(index=False, escape=True)

    # Scrollable container
    st.markdown(
        f"""
        <div style="height:{height_px}px; overflow:auto; border:1px solid #e6e6e6; border-radius:8px; padding:8px;">
            {html}
        </div>
        """,
        unsafe_allow_html=True,
    )


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

    # Basic cleanup
    df = df.copy()
    df["species"] = df["species"].astype(str).str.strip()
    df["sex"] = df["sex"].fillna("unknown").astype(str).str.strip()
    df["penguin_count"] = pd.to_numeric(df["penguin_count"], errors="coerce").fillna(0).astype(int)
    df["avg_body_mass_g"] = pd.to_numeric(df["avg_body_mass_g"], errors="coerce")
    df["avg_flipper_length_mm"] = pd.to_numeric(df["avg_flipper_length_mm"], errors="coerce")
    return df


@st.cache_data(ttl=300)
def load_raw_all():
    engine = make_engine()
    q = """
    SELECT
      species,
      island,
      bill_length_mm,
      bill_depth_mm,
      flipper_length_mm,
      body_mass_g,
      sex
    FROM raw.penguins
    ORDER BY species, island, sex NULLS LAST
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(q), conn)

    # Make sure types are simple
    df = df.copy()
    for c in ["bill_length_mm", "bill_depth_mm", "flipper_length_mm", "body_mass_g"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    for c in ["species", "island", "sex"]:
        if c in df.columns:
            df[c] = df[c].fillna("").astype(str)

    return df


st.set_page_config(page_title="Lakehouse Lite", layout="wide")

st.title("Lakehouse Lite")
st.caption("Python ingestion → Postgres raw layer → dbt transformations → Streamlit dashboard")

m = load_metrics()
c1, c2, c3 = st.columns(3)
c1.metric("Total groups", int(m["total_groups"]))
c2.metric("Total penguins", int(m["total_penguins"]))
c3.metric("Species count", int(m["species_count"]))

st.markdown("---")

st.subheader("Penguin summary by species and sex")
summary = load_summary()
render_table_html(summary, height_px=320)

st.markdown("---")

st.subheader("Raw penguins (all rows)")
raw_df = load_raw_all()
st.caption(f"Rows: {len(raw_df)}")
render_table_html(raw_df, height_px=650)
