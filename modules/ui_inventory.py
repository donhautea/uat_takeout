
import os, sqlite3
import pandas as pd
import streamlit as st

APP_DB_PATH = os.environ.get("TAKEOUT_DB_PATH", "app.db")

def _conn():
    c = sqlite3.connect(APP_DB_PATH, check_same_thread=False)
    c.execute("PRAGMA foreign_keys=ON;")
    c.execute("""CREATE TABLE IF NOT EXISTS inventory (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        item_code TEXT,
        item_name TEXT,
        unit TEXT,
        quantity REAL DEFAULT 0,
        cost REAL DEFAULT 0,
        price REAL DEFAULT 0,
        updated_at TEXT DEFAULT (datetime('now'))
    );""")
    return c

def render(user=None):
    st.title("Inventory")
    conn = _conn()

    st.subheader("Items")
    try:
        df = pd.read_sql_query("SELECT * FROM inventory ORDER BY item_name;", conn)
    except Exception:
        df = pd.DataFrame(columns=["item_code","item_name","unit","quantity","cost","price"])

    st.dataframe(df, use_container_width=True, height=360)

    st.subheader("Add / Update Item")
    c1, c2, c3 = st.columns(3)
    with c1:
        code = st.text_input("Item Code", key="inv_code")
        name = st.text_input("Item Name", key="inv_name")
    with c2:
        unit = st.text_input("Unit", key="inv_unit")
        qty = st.number_input("Quantity", min_value=0.0, step=1.0, key="inv_qty")
    with c3:
        cost = st.number_input("Cost", min_value=0.0, step=0.01, key="inv_cost")
        price = st.number_input("Price", min_value=0.0, step=0.01, key="inv_price")

    if st.button("Save Item", type="primary"):
        with conn:
            cur = conn.execute("SELECT id FROM inventory WHERE item_code=?;", (code,))
            row = cur.fetchone()
            if row:
                conn.execute("UPDATE inventory SET item_name=?, unit=?, quantity=?, cost=?, price=?, updated_at=datetime('now') WHERE item_code=?;",
                             (name, unit, float(qty), float(cost), float(price), code))
            else:
                conn.execute("INSERT INTO inventory(item_code,item_name,unit,quantity,cost,price) VALUES(?,?,?,?,?,?);",
                             (code, name, unit, float(qty), float(cost), float(price)))
        st.success("Saved.")
        st.experimental_rerun()

app = render
main = render
