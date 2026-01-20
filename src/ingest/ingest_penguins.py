import pandas as pd
from sqlalchemy import text
from db import get_engine

PENGUINS_CSV = "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/penguins.csv"

def main():
    engine = get_engine()

    print("Downloading dataset...")
    df = pd.read_csv(PENGUINS_CSV)

    # basic cleanup
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # load into Postgres
    with engine.begin() as conn:
        print("Creating schema if not exists...")
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))

    print(f"Loading {len(df)} rows into raw.penguins ...")
    df.to_sql("penguins", engine, schema="raw", if_exists="replace", index=False)

    print("Done.")

if __name__ == "__main__":
    main()
