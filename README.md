# FinDashboard-ML

A self-hosted, automated personal finance platform with ML-powered transaction categorisation.

Transactions are ingested from bank CSV exports, automatically classified by a trained model, reviewed and corrected via a Streamlit dashboard, and used to retrain the model over time.

### Key Features
- **Analytics Dashboard:** Spending breakdowns and category summaries via Streamlit
- **Auto-Classification:** TF-IDF + LinearSVC pipeline that categorises transactions on ingest
- **Human Verification:** Review predicted categories, correct mistakes, and confirm them in the UI
- **Continuous Improvement:** Retrain the model on verified transactions with one button click — each cycle makes it smarter
- **Telegram Notifications:** Get a message when a new batch is fetched and ready for review
- **Private:** Self-hosted on your own machine — financial data never leaves your server

### Tech Stack
- **Core:** Python 3.14, Pandas, Scikit-Learn
- **Database:** PostgreSQL (Docker)
- **Dashboard:** Streamlit
- **Infrastructure:** Docker Compose, Caddy (reverse proxy)