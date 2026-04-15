import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

engine = create_engine(os.getenv('engine_credentials'))

df = pd.read_csv('dirty_cafe_sales.csv')

df.columns = df.columns.str.lower()

df.to_sql('cafe_sales', engine, schema='public', if_exists='replace', index='False')

print(f"Loaded {len(df)} rows into cafe_db.public.cafe_sales")