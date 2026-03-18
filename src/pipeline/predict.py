import os
import numpy as np
import psycopg2
import psycopg2.extras
import joblib
from dotenv import load_dotenv

load_dotenv()
import pandas as pd

DB_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/db")

def get_confidence(model, X_input):
    """
    Attempts to get probability, falls back to decision function distance.
    Returns the max confidence score for the predicted class.
    """
    if hasattr(model, "predict_proba"):
        try:
            probs = model.predict_proba(X_input)
            return np.max(probs, axis=1)
        except Exception:
            pass

    if hasattr(model, "decision_function"):
        scores = model.decision_function(X_input)
        if scores.ndim == 1:
            return np.abs(scores)
        return np.max(scores, axis=1)

    return None

def run_inference():
    con = psycopg2.connect(DB_URL)

    df = pd.read_sql("""
        SELECT id, normalized_merchant
        FROM transactions
        WHERE predicted_category_id IS NULL AND category_id IS NULL
    """, con)

    if df.empty:
        print('No pending transactions to classify.')
        con.close()
        return

    model_row = pd.read_sql("SELECT id, model_path, version FROM models ORDER BY trained_at DESC LIMIT 1", con)

    if model_row.empty:
        print('No trained model found in DB.')
        con.close()
        return

    model_id = int(model_row.iloc[0]['id'])
    model_path = model_row.iloc[0]['model_path']
    model_version = model_row.iloc[0]['version']
    print(f"Using model: {model_version} ({model_path})")

    try:
        model = joblib.load(model_path)
    except FileNotFoundError:
        print(f'Model file not found: {model_path}')
        con.close()
        return

    print(f'Predicting {len(df)} transactions...')

    X_input = df[['normalized_merchant']]
    predictions = model.predict(X_input)
    confidence_scores = get_confidence(model, X_input)

    cur = con.cursor()
    rows = [
        (
            int(predictions[i]),
            float(confidence_scores[i]) if confidence_scores is not None else None,
            model_id,
            int(df.iloc[i]['id'])
        )
        for i in range(len(df))
    ]

    psycopg2.extras.execute_values(cur, """
        UPDATE transactions AS t SET
            predicted_category_id = v.predicted_category_id,
            confidence_score = v.confidence_score,
            model_id = v.model_id
        FROM (VALUES %s) AS v(predicted_category_id, confidence_score, model_id, id)
        WHERE t.id = v.id
    """, rows)

    con.commit()
    cur.close()
    con.close()
    print(f"Updated {len(df)} transactions with predictions.")

if __name__ == '__main__':
    run_inference()
