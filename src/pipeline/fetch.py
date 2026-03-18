"""
Monthly fetch pipeline. Run manually or via cron.

Usage:
    python -m src.pipeline.fetch --swedbank data/raw/2026-03_swedbank.csv
                                 --bunq     data/raw/2026-03_bunq.csv
                                 --output   data/backups/2026-03_transactions.csv

Each raw bank CSV is parsed by its adapter into the standard shape,
combined into one DataFrame, saved as a backup, then inserted into the DB.
Inference is run automatically on the new transactions.
"""
import os
import argparse
import pandas as pd
import psycopg2
from dotenv import load_dotenv

from src.banks.swedbank import SwedbankAdapter
from src.banks.bunq import BunqAdapter
from src.pipeline.base import insert_transactions
from src.pipeline.predict import run_inference

load_dotenv()

DB_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/db")

ADAPTERS = {
    "swedbank": SwedbankAdapter(),
    "bunq": BunqAdapter(),
}

def fetch(bank_files: dict[str, str], output_path: str):
    """
    bank_files: dict of bank name -> raw CSV path, e.g. {"swedbank": "data/raw/..."}
    output_path: where to save the combined normalized CSV
    """
    frames = []

    for bank, filepath in bank_files.items():
        adapter = ADAPTERS[bank]
        print(f"Parsing {bank} export: {filepath}")
        df = adapter.parse(filepath)
        frames.append(df)

    if not frames:
        print("No bank files provided.")
        return

    combined = pd.concat(frames, ignore_index=True)
    combined.to_csv(output_path, index=False)
    print(f"Combined {len(combined)} transactions → {output_path}")

    con = psycopg2.connect(DB_URL)
    cur = con.cursor()

    try:
        insert_transactions(combined, cur, output_path)
        con.commit()
        print("Import complete.")
    except Exception as e:
        con.rollback()
        print(f"Error during import: {e}")
        return
    finally:
        cur.close()
        con.close()

    run_inference()

    # TODO: send notification

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--swedbank", type=str, help="Path to raw Swedbank CSV")
    parser.add_argument("--bunq", type=str, help="Path to raw Bunq CSV")
    parser.add_argument("--output", type=str, required=True, help="Path for combined output CSV")
    args = parser.parse_args()

    bank_files = {}
    if args.swedbank:
        bank_files["swedbank"] = args.swedbank
    if args.bunq:
        bank_files["bunq"] = args.bunq

    fetch(bank_files, args.output)
