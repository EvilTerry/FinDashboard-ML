import os
import psycopg2
from psycopg2 import sql, errors
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/db")

def init_db():
    print(f"Initializing database at: {DB_URL}")

    con = psycopg2.connect(DB_URL)
    cur = con.cursor()

    # Create models table first (transactions references it)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS models (
            id                  SERIAL PRIMARY KEY,
            version             TEXT NOT NULL UNIQUE,   -- e.g. '2026-03'
            algorithm           TEXT,                   -- e.g. 'LinearSVC'
            model_path          TEXT NOT NULL,          -- path to .joblib
            accuracy            DOUBLE PRECISION,
            f1_score            DOUBLE PRECISION,
            training_data_size  INTEGER,
            hyperparameters     JSONB,
            notes               TEXT,
            trained_at          TIMESTAMP DEFAULT NOW()
        );
    """)

    # Create categories table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS categories (
            id          SERIAL PRIMARY KEY,
            name        TEXT NOT NULL UNIQUE
        );
    """)

    # Create import_batches table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS import_batches (
            id              SERIAL PRIMARY KEY,
            imported_at     TIMESTAMP DEFAULT NOW(),
            source_file     TEXT,
            transaction_count INTEGER
        );
    """)

    # Create transactions table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id                      SERIAL PRIMARY KEY,

            -- Core bank data
            date                    DATE NOT NULL,
            amount                  DECIMAL(10, 2) NOT NULL,
            currency                TEXT DEFAULT 'EUR',
            description             TEXT,
            merchant                TEXT,               -- Raw merchant name
            normalized_merchant     TEXT,               -- Cleaned name
            source_account          TEXT,               -- e.g. 'ING Main', 'Credit Card'
            import_batch_id         INTEGER REFERENCES import_batches(id),

            -- Classification
            predicted_category_id   INTEGER REFERENCES categories(id),  -- model output, never changes
            category_id             INTEGER REFERENCES categories(id),  -- verified ground truth

            -- MLOps metadata
            model_id                INTEGER REFERENCES models(id),
            confidence_score        DOUBLE PRECISION,

            -- Audit
            created_at              TIMESTAMP DEFAULT NOW()
        );
    """)

    # Dedup index using COALESCE to handle NULL merchant/description safely
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS transactions_dedup
        ON transactions (date, amount, COALESCE(merchant, ''), COALESCE(description, ''));
    """)

    # Seed categories
    categories = [
        'Groceries',
        'Dining & Takeout',
        'Housing & Rent',
        'Subscriptions',
        'Shopping',
        'Travel & Transport',
        'Petrol Station',
        'Health & Drugstore',
        'Insurance',
        'Income',
        'Transfers (P2P)',
        'Tobacco',
        'Withdrawal',
        'Entertainment & Gaming',
        'Other/Unknown',
    ]

    print("Seeding categories...")
    for name in categories:
        cur.execute("""
            INSERT INTO categories (name)
            VALUES (%s)
            ON CONFLICT (name) DO NOTHING;
        """, (name,))

    con.commit()

    cur.execute("SELECT COUNT(*) FROM categories")
    result = cur.fetchone()
    if result:
        print(f"Database initialized successfully with {result[0]} categories.")

    cur.close()
    con.close()


if __name__ == "__main__":
    init_db()
