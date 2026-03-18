import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv

from src.pipeline.base import insert_transactions
from src.pipeline.train import train

load_dotenv()

DB_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/db")
CSV_PATH = 'data/initial_transactions.csv'

def seed():
    df = pd.read_csv(CSV_PATH)

    con = psycopg2.connect(DB_URL)
    cur = con.cursor()

    try:
        insert_transactions(df, cur, CSV_PATH)
        con.commit()
        print("Seed complete.")
    except Exception as e:
        con.rollback()
        print(f"Error: {e}")

    cur.close()
    con.close()

if __name__ == "__main__":
    seed()
    train()
