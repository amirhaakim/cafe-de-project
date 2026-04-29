import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


PROJECT_ROOT = Path(__file__).resolve().parent
CSV_PATH = PROJECT_ROOT / "dirty_cafe_sales.csv"
TARGET_SCHEMA = "public"
TARGET_TABLE = "cafe_sales"


def get_engine():
    load_dotenv(PROJECT_ROOT / ".env")

    engine_credentials = os.getenv("engine_credentials")
    if not engine_credentials:
        raise ValueError("Missing engine_credentials in .env file")

    return create_engine(engine_credentials)


def check_postgres_connection(engine):
    try:
        with engine.connect() as connection:
            connection.execute(text("select 1"))
    except SQLAlchemyError as error:
        raise ConnectionError(f"Unable to connect to Postgres: {error}") from error


def load_csv(csv_path):
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    df = pd.read_csv(csv_path)
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    return df


def ingest_to_postgres(df, engine):
    try:
        df.to_sql(
            TARGET_TABLE,
            engine,
            schema=TARGET_SCHEMA,
            if_exists="replace",
            index=False,
        )
    except SQLAlchemyError as error:
        raise RuntimeError(f"Failed to ingest data into Postgres: {error}") from error


def main():
    engine = get_engine()

    try:
        check_postgres_connection(engine)
        df = load_csv(CSV_PATH)
        ingest_to_postgres(df, engine)
        print(f"Loaded {len(df)} rows into cafe_db.{TARGET_SCHEMA}.{TARGET_TABLE}")
    except (ConnectionError, FileNotFoundError, RuntimeError, ValueError) as error:
        print(error)
        raise
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
