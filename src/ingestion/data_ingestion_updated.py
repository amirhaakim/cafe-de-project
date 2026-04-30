import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


REPO_ROOT = Path(__file__).resolve().parents[2]
CSV_PATH = REPO_ROOT / "dirty_cafe_sales.csv"
TARGET_SCHEMA = "public"
TARGET_TABLE = "cafe_sales"
REQUIRED_COLUMNS = [
    "transaction_id",
    "item",
    "quantity",
    "price_per_unit",
    "total_spent",
    "payment_method",
    "location",
    "transaction_date",
]


def get_engine():
    load_dotenv(REPO_ROOT / ".env")

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
    missing_columns = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing_columns:
        raise ValueError(f"CSV is missing required columns: {missing_columns}")
    return df


def ingest_to_postgres(df, engine):
    try:
        selected_df = df[REQUIRED_COLUMNS]
        if selected_df["transaction_id"].isna().any() or (
            selected_df["transaction_id"].astype(str).str.strip() == ""
        ).any():
            raise ValueError("transaction_id contains null or blank values")
        upsert_records = selected_df.where(pd.notnull(selected_df), None).to_dict(orient="records")
        if not upsert_records:
            print("No rows found in CSV. Nothing to load.")
            return

        create_table_sql = text(
            f"""
            create table if not exists {TARGET_SCHEMA}.{TARGET_TABLE} (
                transaction_id text primary key,
                item text,
                quantity text,
                price_per_unit text,
                total_spent text,
                payment_method text,
                location text,
                transaction_date text
            )
            """
        )
        upsert_sql = text(
            f"""
            insert into {TARGET_SCHEMA}.{TARGET_TABLE} (
                transaction_id,
                item,
                quantity,
                price_per_unit,
                total_spent,
                payment_method,
                location,
                transaction_date
            ) values (
                :transaction_id,
                :item,
                :quantity,
                :price_per_unit,
                :total_spent,
                :payment_method,
                :location,
                :transaction_date
            )
            on conflict (transaction_id) do update set
                item = excluded.item,
                quantity = excluded.quantity,
                price_per_unit = excluded.price_per_unit,
                total_spent = excluded.total_spent,
                payment_method = excluded.payment_method,
                location = excluded.location,
                transaction_date = excluded.transaction_date
            """
        )

        with engine.begin() as connection:
            connection.execute(create_table_sql)
            connection.execute(upsert_sql, upsert_records)
    except SQLAlchemyError as error:
        raise RuntimeError(f"Failed to ingest data into Postgres: {error}") from error


def main():
    engine = get_engine()

    try:
        check_postgres_connection(engine)
        df = load_csv(CSV_PATH)
        ingest_to_postgres(df, engine)
        print(f"Upsert completed for {len(df)} input rows into cafe_db.{TARGET_SCHEMA}.{TARGET_TABLE}")
    except (ConnectionError, FileNotFoundError, RuntimeError, ValueError) as error:
        print(error)
        raise
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
