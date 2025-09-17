
import os, sqlite3
import pandas as pd
import streamlit as st

APP_DB_PATH = os.environ.get("TAKEOUT_DB_PATH", "app.db")

def _conn():
    c = sqlite3.connect(APP_DB_PATH, check_same_thread=False)
    c.execute("PRAGMA foreign_keys=ON;")
    c.execute("""CREATE TABLE IF NOT EXISTS expenses (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        category TEXT,
        description TEXT,
        amount REAL NOT NULL,
        vendor TEXT,
        ref_no TEXT
    );""")
    return c

def render(user=None):
    st.title("Expense Reports")
    conn = _conn()

    try:
        df = pd.read_sql_query("SELECT date, category, amount FROM expenses;", conn)
    except Exception as e:
        st.error(f"Reading expenses failed: {e}")
        return

    if df.empty:
        st.info("No expenses yet.")
        return

    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    min_d, max_d = df['date'].min().date(), df['date'].max().date()
    s, e = st.date_input("Date range", value=(min_d, max_d))
    mask = (df['date'] >= pd.to_datetime(s)) & (df['date'] <= pd.to_datetime(e))
    f = df.loc[mask]

    st.metric("Total (filtered)", f"{f['amount'].sum():,.2f}")
    st.dataframe(f, use_container_width=True, height=320)

app = render
main = render
