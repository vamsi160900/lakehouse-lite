import pandas as pd
from sqlalchemy import text
from pathlib import Path

from db import get_engine

PENGUINS_CSV_URL = "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/penguins.csv"
LOCAL_CSV_PATH = Path(r"E:\MyProjects\lakehouse-lite\data\raw\penguins.csv")


def load_penguins() -> pd.DataFrame:
    # Prefer local file if it exists, else download from GitHub
    if LOCAL_CSV_PATH.exists():
        print(f"Loading local dataset: {LOCAL_CSV_PATH}")
        df = pd.read_csv(LOCAL_CSV_PATH)
    else:
        print("Downloading dataset...")
        df = pd.read_csv(PENGUINS_CSV_URL)

    # basic cleanup
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df


def ensure_table(conn) -> None:
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
    conn.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS raw.penguins (
                species text,
                island text,
                bill_length_mm double precision,
                bill_depth_mm double precision,
                flipper_length_mm double precision,
                body_mass_g double precision,
                sex text
            );
            """
        )
    )


def main():
    engine = get_engine()

    df = load_penguins()

    with engine.begin() as conn:
        print("Creating schema/table if not exists...")
        ensure_table(conn)

        # IMPORTANT: do NOT drop/replace the table (dbt views depend on it)
        print("Clearing old rows (keeping table)...")
        conn.execute(text("TRUNCATE TABLE raw.penguins;"))

    print(f"Loading {len(df)} rows into raw.penguins ...")
    df.to_sql("penguins", engine, schema="raw", if_exists="append", index=False)

    print("Done.")


if __name__ == "__main__":
    main()
