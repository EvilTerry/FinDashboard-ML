import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

from src.preprocessing.normalize import normalize_merchant
from src.pipeline.train import train

DB_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/db")
CSV_PATH = 'data/initial_transactions.csv'

def load_data():
    df = pd.read_csv(CSV_PATH)

    con = psycopg2.connect(DB_URL)
    cur = con.cursor()

    df['normalized_merchant'] = df['merchant'].apply(normalize_merchant)

    cur.execute('SELECT name, id FROM categories')
    cat_map = {name: cat_id for name, cat_id in cur.fetchall()}

    df['category_id'] = df['category'].map(cat_map)

    unknowns = df[df['category_id'].isna() & df['category'].notna()]
    if not unknowns.empty:
        print('Found unmapped categories')
        print(unknowns['category'].unique())

    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')

    cur.execute("""
        INSERT INTO import_batches (source_file, transaction_count)
        VALUES (%s, %s)
        RETURNING id
    """, (CSV_PATH, len(df)))
    row = cur.fetchone()
    assert row is not None, "Failed to create import batch"
    batch_id = row[0]
    print(f"Created import batch id={batch_id}")

    rows = [
        (
            r['date'], r['amount'], r['description'], r['merchant'],
            r['normalized_merchant'], r['source_account'],
            int(r['category_id']) if pd.notna(r['category_id']) else None,
            r.get('currency', 'EUR'),
            batch_id
        )
        for _, r in df.iterrows()
    ]

    print(f"Inserting {len(rows)} rows...")

    try:
        execute_values(cur, """
            INSERT INTO transactions
            (date, amount, description, merchant, normalized_merchant, source_account, category_id, currency, import_batch_id)
            VALUES %s
            ON CONFLICT (date, amount, COALESCE(merchant, ''), COALESCE(description, '')) DO NOTHING
        """, rows)
        con.commit()
        print("Import Complete!")
    except Exception as e:
        con.rollback()
        print(f"Error: {e}")

    cur.close()
    con.close()

if __name__ == "__main__":
    load_data()
    train()
