
import os, sqlite3
import pandas as pd
import streamlit as st

APP_DB_PATH = os.environ.get("TAKEOUT_DB_PATH", "app.db")

def _conn():
    c = sqlite3.connect(APP_DB_PATH, check_same_thread=False)
    c.execute("PRAGMA foreign_keys=ON;")
    c.execute("""CREATE TABLE IF NOT EXISTS supplies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT DEFAULT (date('now')),
        supplier TEXT,
        sku TEXT,
        item_name TEXT,
        unit TEXT,
        quantity REAL DEFAULT 0,
        unit_cost REAL DEFAULT 0,
        total_cost REAL DEFAULT 0
    );""")
    c.execute("""CREATE TABLE IF NOT EXISTS inventory (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sku TEXT UNIQUE,
        item_name TEXT,
        unit TEXT,
        quantity REAL DEFAULT 0,
        cost REAL DEFAULT 0,
        price REAL DEFAULT 0,
        updated_at TEXT DEFAULT (datetime('now'))
    );""")
    return c

def render(user=None):
    st.title("Supplies")
    conn = _conn()

    st.subheader("Add Supply Entry")
    c1, c2, c3 = st.columns(3)
    with c1:
        supplier = st.text_input("Supplier")
        sku = st.text_input("SKU")
    with c2:
        item_name = st.text_input("Item Name")
        unit = st.text_input("Unit", value="pc")
    with c3:
        qty = st.number_input("Quantity", min_value=0.0, step=1.0)
        unit_cost = st.number_input("Unit Cost", min_value=0.0, step=0.01)
    if st.button("Save Supply", type="primary"):
        total = float(qty) * float(unit_cost)
        with conn:
            conn.execute("INSERT INTO supplies(supplier, sku, item_name, unit, quantity, unit_cost, total_cost) VALUES(?,?,?,?,?,?,?)",
                         (supplier, sku, item_name, unit, float(qty), float(unit_cost), total))
            cur = conn.execute("SELECT id, quantity, cost FROM inventory WHERE sku=?;", (sku,))
            row = cur.fetchone()
            if row:
                inv_id, inv_qty, inv_cost = row
                new_qty = float(inv_qty or 0) + float(qty)
                new_cost = ((inv_qty or 0)*(inv_cost or 0) + float(qty)*float(unit_cost)) / (new_qty or 1)
                conn.execute("UPDATE inventory SET item_name=?, unit=?, quantity=?, cost=?, updated_at=datetime('now') WHERE id=?;",
                             (item_name, unit, new_qty, new_cost, inv_id))
            else:
                conn.execute("INSERT INTO inventory(sku, item_name, unit, quantity, cost) VALUES(?,?,?,?,?);",
                             (sku, item_name, unit, float(qty), float(unit_cost)))
        st.success("Supply recorded and inventory updated.")
        st.experimental_rerun()

    st.subheader("Recent Supplies")
    df = pd.read_sql_query("SELECT date, supplier, sku, item_name, quantity, unit_cost, total_cost FROM supplies ORDER BY date DESC, id DESC LIMIT 200;", conn)
    st.dataframe(df, use_container_width=True, height=320)

app = render
main = render
