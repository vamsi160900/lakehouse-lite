import os
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

st.set_page_config(page_title="Lakehouse Lite", layout="wide")

def get_engine():
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME", "lakehouse_db")
    user = os.getenv("DB_USER", "lakehouse")
    password = os.getenv("DB_PASSWORD", "lakehouse123")
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}"
    return create_engine(url)

@st.cache_data(ttl=60)
def load_summary():
    engine = get_engine()
    q = "select * from analytics.mart_penguin_summary order by species, sex"
    return pd.read_sql(q, engine)

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
pivot = df.pivot_table(index="species", values="penguin_count", aggfunc="sum").reset_index()
st.bar_chart(pivot.set_index("species"))
