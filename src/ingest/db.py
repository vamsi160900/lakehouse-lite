import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

def get_engine():
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME", "lakehouse_db")
    user = os.getenv("DB_USER", "lakehouse")
    password = os.getenv("DB_PASSWORD", "lakehouse123")
    sslmode = os.getenv("DB_SSLMODE", "")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}"
    if sslmode:
        url = url + f"?sslmode={sslmode}"

    return create_engine(url)
