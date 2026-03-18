import pandas as pd
from psycopg2.extras import execute_values

from src.preprocessing.normalize import normalize_merchant


def insert_transactions(df: pd.DataFrame, cur, source_file: str) -> int:
    """
    Normalizes, maps categories, creates an import batch, and inserts transactions.
    Returns the import batch id.
    Expects df to have columns: date, amount, merchant, description, currency, source_account.
    category_id is optional — present in seed data, absent in fresh fetches.
    """
    df = df.copy()
    df['normalized_merchant'] = df['merchant'].apply(normalize_merchant)
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')

    # Map category strings to IDs if present
    if 'category' in df.columns:
        cur.execute('SELECT name, id FROM categories')
        cat_map = {name: cat_id for name, cat_id in cur.fetchall()}
        df['category_id'] = df['category'].map(cat_map)

        unknowns = df[df['category_id'].isna() & df['category'].notna()]
        if not unknowns.empty:
            print('Found unmapped categories:')
            print(unknowns['category'].unique())
    else:
        df['category_id'] = None

    cur.execute("""
        INSERT INTO import_batches (source_file, transaction_count)
        VALUES (%s, %s)
        RETURNING id
    """, (source_file, len(df)))
    row = cur.fetchone()
    assert row is not None, "Failed to create import batch"
    batch_id = row[0]
    print(f"Created import batch id={batch_id} ({len(df)} transactions)")

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

    execute_values(cur, """
        INSERT INTO transactions
        (date, amount, description, merchant, normalized_merchant, source_account, category_id, currency, import_batch_id)
        VALUES %s
        ON CONFLICT (date, amount, COALESCE(merchant, ''), COALESCE(description, '')) DO NOTHING
    """, rows)

    return batch_id
