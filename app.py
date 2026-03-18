import os
import streamlit as st
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/db")

st.set_page_config(layout="wide", page_title="FinDashboard")

def get_connection():
    return psycopg2.connect(DB_URL)

def get_categories(con):
    df = pd.read_sql("SELECT id, name FROM categories ORDER BY name", con)
    return dict(zip(df['name'], df['id'])), list(df['name'])

# ─────────────────────────────────────────────
# DASHBOARD
# ─────────────────────────────────────────────
def page_dashboard():
    st.title("Dashboard")
    con = get_connection()

    # Metrics
    col1, col2, col3 = st.columns(3)

    cur = con.cursor()

    cur.execute("SELECT SUM(amount) FROM transactions WHERE amount < 0")
    total_spent = abs((cur.fetchone() or [0])[0] or 0)

    cur.execute("SELECT SUM(amount) FROM transactions WHERE amount > 0")
    total_income = (cur.fetchone() or [0])[0] or 0

    cur.execute("SELECT COUNT(*) FROM transactions WHERE predicted_category_id IS NOT NULL AND category_id IS NULL")
    pending_review = (cur.fetchone() or [0])[0] or 0

    col1.metric("Total Spent", f"€{total_spent:,.2f}")
    col2.metric("Total Income", f"€{total_income:,.2f}")
    col3.metric("Pending Review", int(pending_review))

    cur.close()

    # Spending by category
    st.subheader("Spending by Category")
    df_cat = pd.read_sql("""
        SELECT c.name AS category, SUM(ABS(t.amount)) AS total
        FROM transactions t
        JOIN categories c ON t.category_id = c.id
        WHERE t.amount < 0
        GROUP BY c.name
        ORDER BY total DESC
    """, con)

    if not df_cat.empty:
        st.bar_chart(df_cat.set_index("category"))
    else:
        st.info("No verified transactions yet.")

    con.close()

# ─────────────────────────────────────────────
# TRANSACTIONS
# ─────────────────────────────────────────────
def page_transactions():
    st.title("Transactions")
    con = get_connection()
    name_to_id, category_names = get_categories(con)

    # Filters
    col1, col2, col3 = st.columns(3)
    search = col1.text_input("Search merchant / description")
    category_filter = col2.selectbox("Category", ["All"] + category_names)
    unverified_only = col3.checkbox("Unverified only")

    filters = []
    params = []

    if search:
        filters.append("(t.merchant ILIKE %s OR t.description ILIKE %s)")
        params += [f"%{search}%", f"%{search}%"]
    if category_filter != "All":
        filters.append("c.name = %s")
        params.append(category_filter)
    if unverified_only:
        filters.append("t.category_id IS NULL")

    where = ("WHERE " + " AND ".join(filters)) if filters else ""

    # Pagination
    if "tx_page" not in st.session_state:
        st.session_state.tx_page = 0
    rows_per_page = 25
    offset = st.session_state.tx_page * rows_per_page

    df = pd.read_sql(f"""
        SELECT t.id, t.date, t.merchant, t.amount, t.currency, c.name AS category
        FROM transactions t
        LEFT JOIN categories c ON t.category_id = c.id
        {where}
        ORDER BY t.date DESC
        LIMIT %s OFFSET %s
    """, con, params=params + [rows_per_page, offset])

    edited_df = st.data_editor(
        df,
        column_config={
            "id": st.column_config.NumberColumn(disabled=True),
            "date": st.column_config.DateColumn(disabled=True),
            "merchant": st.column_config.TextColumn(disabled=True),
            "amount": st.column_config.NumberColumn(disabled=True, format="%.2f"),
            "currency": st.column_config.TextColumn(disabled=True),
            "category": st.column_config.SelectboxColumn("Category", options=category_names, required=False),
        },
        hide_index=True,
        use_container_width=True,
        key="tx_editor"
    )

    col_save, col_prev, col_next, col_page = st.columns([2, 1, 1, 6])

    if col_save.button("Save", type="primary"):
        cur = con.cursor()
        for _, row in edited_df.iterrows():
            new_cat_id = name_to_id.get(row["category"]) if row["category"] else None
            if new_cat_id is not None:
                cur.execute("UPDATE transactions SET category_id = %s WHERE id = %s", (new_cat_id, row["id"]))
            else:
                cur.execute("UPDATE transactions SET category_id = NULL WHERE id = %s", (row["id"],))
        con.commit()
        cur.close()
        st.success("Saved.")
        st.rerun()

    if col_prev.button("← Prev"):
        if st.session_state.tx_page > 0:
            st.session_state.tx_page -= 1
            st.rerun()
    if col_next.button("Next →"):
        st.session_state.tx_page += 1
        st.rerun()
    col_page.caption(f"Page {st.session_state.tx_page + 1}")

    con.close()

# ─────────────────────────────────────────────
# REVIEW PREDICTIONS
# ─────────────────────────────────────────────
def page_review():
    st.title("Review Predictions")
    con = get_connection()
    name_to_id, category_names = get_categories(con)

    df = pd.read_sql("""
        SELECT t.id, t.date, t.merchant, t.amount,
               pc.name AS predicted_category,
               t.confidence_score
        FROM transactions t
        JOIN categories pc ON t.predicted_category_id = pc.id
        WHERE t.category_id IS NULL
        ORDER BY t.confidence_score ASC
    """, con)

    if df.empty:
        st.success("Nothing to review.")
        con.close()
        return

    st.caption(f"{len(df)} transactions awaiting review. Sorted by lowest confidence first.")

    df["confirm"] = df["predicted_category"]

    edited_df = st.data_editor(
        df,
        column_config={
            "id": st.column_config.NumberColumn(disabled=True),
            "date": st.column_config.DateColumn(disabled=True),
            "merchant": st.column_config.TextColumn(disabled=True),
            "amount": st.column_config.NumberColumn(disabled=True, format="%.2f"),
            "predicted_category": st.column_config.TextColumn("Predicted", disabled=True),
            "confidence_score": st.column_config.NumberColumn("Confidence", disabled=True, format="%.3f"),
            "confirm": st.column_config.SelectboxColumn("Confirm / Override", options=category_names, required=True),
        },
        hide_index=True,
        use_container_width=True,
        key="review_editor"
    )

    col1, _ = st.columns([2, 8])

    if col1.button("Confirm All", type="primary"):
        cur = con.cursor()
        for _, row in edited_df.iterrows():
            cat_id = name_to_id.get(row["confirm"])
            if cat_id:
                cur.execute("UPDATE transactions SET category_id = %s WHERE id = %s", (cat_id, row["id"]))
        con.commit()
        cur.close()
        st.success("All confirmed.")
        st.rerun()

    con.close()

# ─────────────────────────────────────────────
# MODELS
# ─────────────────────────────────────────────
def page_models():
    st.title("Models")
    con = get_connection()

    df = pd.read_sql("""
        SELECT version, algorithm, accuracy, f1_score, training_data_size,
               hyperparameters, notes, trained_at
        FROM models
        ORDER BY trained_at DESC
    """, con)
    con.close()

    if df.empty:
        st.info("No models trained yet.")
        return

    st.dataframe(df, use_container_width=True, hide_index=True)

# ─────────────────────────────────────────────
# NAV
# ─────────────────────────────────────────────
page = st.sidebar.radio("", ["Dashboard", "Transactions", "Review Predictions", "Models"])

if page == "Dashboard":
    page_dashboard()
elif page == "Transactions":
    page_transactions()
elif page == "Review Predictions":
    page_review()
elif page == "Models":
    page_models()
