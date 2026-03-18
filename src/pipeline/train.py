import os
import json
import argparse
import psycopg2
import pandas as pd
import joblib
import datetime
from dotenv import load_dotenv
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.svm import LinearSVC
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.model_selection import StratifiedKFold, cross_val_score

load_dotenv()

DB_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/db")

def train(notes: str = None):
    con = psycopg2.connect(DB_URL)

    df = pd.read_sql("""
        SELECT normalized_merchant, category_id
        FROM transactions
        WHERE category_id IS NOT NULL
    """, con)

    con.close()

    print(f'Training on {len(df)} verified transactions.')

    X = df[['normalized_merchant']]
    y = df['category_id']

    hyperparameters = {
        "word_ngram_range": (1, 2),
        "char_ngram_range": (3, 5),
        "min_df": 2,
        "C": 1.0,
        "class_weight": "balanced",
        "max_iter": 5000,
    }

    word_vec = TfidfVectorizer(analyzer='word', ngram_range=hyperparameters["word_ngram_range"], min_df=hyperparameters["min_df"])
    char_vec = TfidfVectorizer(analyzer='char', ngram_range=hyperparameters["char_ngram_range"])

    pipeline = Pipeline([
        ('features', ColumnTransformer([
            ('word', word_vec, 'normalized_merchant'),
            ('char', char_vec, 'normalized_merchant'),
        ])),
        ('classifier', LinearSVC(class_weight=hyperparameters["class_weight"], random_state=42, max_iter=hyperparameters["max_iter"]))
    ])

    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    accuracy_scores = cross_val_score(pipeline, X, y, cv=cv, scoring='accuracy')
    f1_scores = cross_val_score(pipeline, X, y, cv=cv, scoring='f1_weighted')
    mean_accuracy = accuracy_scores.mean()
    mean_f1 = f1_scores.mean()

    print(f"Model Evaluation: Accuracy {mean_accuracy:.2%} (+/- {accuracy_scores.std() * 2:.2%}), F1 {mean_f1:.2%}")

    version_name = f"v{datetime.datetime.now().strftime('%Y%m%d_%H%M')}"
    model_path = f"models/{version_name}_LinearSVC.joblib"

    pipeline.fit(X, y)

    joblib.dump(pipeline, model_path)
    print(f"Model saved to {model_path}")

    con = psycopg2.connect(DB_URL)
    cur = con.cursor()
    cur.execute("""
        INSERT INTO models (version, algorithm, model_path, accuracy, f1_score, training_data_size, hyperparameters, notes)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (version_name, 'LinearSVC', model_path, float(mean_accuracy), float(mean_f1), len(df), json.dumps(hyperparameters), notes))
    con.commit()
    cur.close()
    con.close()
    print(f"Model record saved to DB as {version_name}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--notes", type=str, default=None, help="Notes about this training run")
    args = parser.parse_args()
    train(notes=args.notes)
