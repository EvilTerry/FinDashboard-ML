import duckdb
import os

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data', 'data.duckdb')

def init_db():
    print(f"Initializing database at: {DB_PATH}")
    
    con = duckdb.connect(DB_PATH)
    
    # Create Sequences
    con.execute("CREATE SEQUENCE IF NOT EXISTS seq_category_id;")
    con.execute("CREATE SEQUENCE IF NOT EXISTS seq_transaction_id;")
    con.execute("CREATE SEQUENCE IF NOT EXISTS seq_model_id;")
    
    # Create Categories Table
    con.execute("""
        CREATE TABLE IF NOT EXISTS categories (
            id INTEGER DEFAULT nextval('seq_category_id') PRIMARY KEY,
            name VARCHAR NOT NULL UNIQUE,
            is_income BOOLEAN DEFAULT FALSE
        );
    """)
    
    # Create Transactions Table
    con.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER DEFAULT nextval('seq_transaction_id') PRIMARY KEY,
            
            -- Core Bank Data
            date DATE NOT NULL,
            amount DECIMAL(10, 2) NOT NULL,
            currency VARCHAR DEFAULT 'EUR',
            description VARCHAR,
            merchant VARCHAR,                -- Raw merchant name
            normalized_merchant VARCHAR,     -- Cleaned name
            source_account VARCHAR,          -- e.g. 'ING Main', 'Credit Card'
            
            -- Classification
            category_id INTEGER REFERENCES categories(id),
            
            -- MLOps Metadata
            is_verified BOOLEAN DEFAULT FALSE,
            model_version VARCHAR,
            confidence_score DOUBLE,
            
            -- Audit
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            -- Constraints: Prevent duplicate imports
            UNIQUE(date, amount, description)
        );
    """)
    
    # Seed Categories
    categories = [
        ('Groceries', False),
        ('Dining & Takeout', False),
        ('Housing & Rent', False),
        ('Bills & Utilities', False),
        ('Subscriptions', False),
        ('Shopping', False),
        ('Public Transport', False),
        ('Petrol Station', False),
        ('Health & Drugstore', False),
        ('Insurance', False),
        ('Income', True),
        ('Transfers (P2P)', False),
        ('Financial & Savings', False),
        ('Tobacco', False),
        ('Withdrawal', False),
        ('Other/Unknown', False)
    ]

    print("Seeding categories...")
    for name, is_income in categories:
        try:
            con.execute("INSERT INTO categories (name, is_income) VALUES (?, ?)", [name, is_income])
        except duckdb.ConstraintException:
            pass

    # Create a model metric table
    con.execute("""
        CREATE TABLE IF NOT EXISTS model_experiments (
            id INTEGER DEFAULT nextval('seq_model_id') PRIMARY KEY,
            
            -- Versioning
            model_version VARCHAR NOT NULL UNIQUE,  -- e.g. "v1_20240101"
            algorithm VARCHAR,                      -- e.g. "LinearSVC"
            
            -- Metrics (The Report Card)
            accuracy DOUBLE,
            precision DOUBLE,
            recall DOUBLE,
            f1_score DOUBLE,
            
            -- Context (Metadata)
            training_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            training_data_size INTEGER,
            notes VARCHAR
        );
    """)
            
    # Verify
    result = con.execute("SELECT COUNT(*) FROM categories").fetchone()

    if result:
        print(f"Database initialized successfully with {result[0]} categories.")
    else:
        print("Could not verify category count.")
    
    con.close()

if __name__ == "__main__":
    # Ensure the data directory exists
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    init_db()