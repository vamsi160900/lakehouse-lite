import os
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Force pandas to store strings as normal Python strings (avoid Arrow LargeUtf8 issues in Streamlit UI)
try:
    pd.options.mode.string_storage = "python"
except Exception:
    pass

# Local dev: reads .env if present
load_dotenv()

st.set_page_config(page_title="Lakehouse Lite", layout="wide")

def get_config():
    # Streamlit Cloud secrets first
    if "DB_HOST" in st.secrets:
        return {
            "host": st.secrets["DB_HOST"],
            "port": str(st.secrets.get("DB_PORT", "5432")),
            "name": st.secrets["DB_NAME"],
            "user": st.secrets["DB_USER"],
            "password": st.secrets["DB_PASSWORD"],
            "sslmode": st.secrets.get("DB_SSLMODE", "require"),
        }

    # Local env fallback
    host = os.getenv("DB_HOST")
    name = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    if not all([host, name, user, password]):
        return None

    return {
        "host": host,
        "port": os.getenv("DB_PORT", "5432"),
        "name": name,
        "user": user,
        "password": password,
        "sslmode": os.getenv("DB_SSLMODE", ""),
    }

def get_engine():
    cfg = get_config()
    if not cfg:
        st.error("Database secrets are missing. Open Manage app → Settings → Secrets and add DB_HOST, DB_NAME, DB_USER, DB_PASSWORD.")
        st.stop()

    url = f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['name']}"
    if cfg.get("sslmode"):
        url += f"?sslmode={cfg['sslmode']}"

    return create_engine(url)

@st.cache_data(ttl=60)
def load_summary():
    engine = get_engine()
    q = "select * from analytics.mart_penguin_summary order by species, sex"
    df = pd.read_sql(q, engine)

    # Extra safety: force string columns to plain python strings
    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].astype(str)

    return df

st.title("Lakehouse Lite")
st.caption("Python ingestion → Postgres raw layer → dbt transformations → Streamlit dashboard")

df = load_summary()

left, mid, right = st.columns(3)
left.metric("Total groups", int(df.shape[0]))
mid.metric("Total penguins", int(df["penguin_count"].sum()))
right.metric("Species count", int(df["species"].nunique()))

st.subheader("Penguin summary by species and sex")
st.dataframe(df, use_container_width=True)

st.subheader("Counts by species")
pivot = df.groupby("species", as_index=False)["penguin_count"].sum()
pivot["species"] = pivot["species"].astype(str)
st.bar_chart(pivot.set_index("species"))
