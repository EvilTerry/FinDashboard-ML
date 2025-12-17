# FinDashboard-ML

A self-hosted, automated personal finance platform that replaces manual spreadsheet tracking with Machine Learning.

This system ingests raw bank transactions, automatically classifies them and presents them in a dashboard for insights and verification.

### Key Features
- **Analytics Dashboard:** Visualizing financial data with charts, spending breakdowns and historical trends using Streamlit
- **Auto-Classification:** Categorization of transactions using Scikit-Learn
- **Human Verification:** A Streamlit dashboard to audit low-confidence predictions and correct errors
- **Continous Improvements:** The Orchestrator periodically retrains the model on newly verified data, making it smarter over time
- **Private:** Self-hosted DuckDB database ensuring financial data never leaves the server

### Tech Stack
- **Core:** Python 3.14.0, Pandas, Scikit-Learn
- **Database:** DuckDB
- **Services:** Streamlit (Dashboard), Docker Compose