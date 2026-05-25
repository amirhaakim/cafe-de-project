from prefect import flow, task
import sys
from pathlib import Path

# Go 2 level up to reach root level
sys.path.append(str(Path(__file__).resolve().parents[1]))

# Call ingestion code from ingestion folder after reaching 2 levels higher from this file dir
from ingestion import data_ingestion_updated

ingestion = data_ingestion_updated

@task
def run_ingestion():
    engine = ingestion.get_engine()
    ingestion.check_postgres_connection(engine)
    df = ingestion.load_csv(ingestion.CSV_PATH)
    ingestion.ingest_to_postgres(df, engine)
    engine.dispose()

@flow
def cafe_pipeline():
    run_ingestion()

if __name__ == "__main__":
    cafe_pipeline()


