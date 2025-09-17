
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
    st.title("Expenses")
    conn = _conn()

    st.subheader("Add Expense")
    c1, c2, c3 = st.columns(3)
    with c1:
        date = st.date_input("Date")
        category = st.text_input("Category")
    with c2:
        amount = st.number_input("Amount", min_value=0.0, step=0.01)
        vendor = st.text_input("Vendor")
    with c3:
        ref_no = st.text_input("Reference No.")
    desc = st.text_area("Description")

    if st.button("Save Expense", type="primary", key="exp_save"):
        with conn:
            conn.execute("INSERT INTO expenses(date, category, description, amount, vendor, ref_no) VALUES(?,?,?,?,?,?)",
                         (str(date), category, desc, float(amount), vendor, ref_no))
        st.success("Saved.")
        st.experimental_rerun()

    st.subheader("Recent Expenses")
    df = pd.read_sql_query("SELECT date, category, amount, vendor, ref_no FROM expenses ORDER BY date DESC, id DESC LIMIT 200;", conn)
    st.dataframe(df, use_container_width=True, height=320)

app = render
main = render
