
import os
import sqlite3
from datetime import date
import pandas as pd
import streamlit as st

DEFAULT_DB_PATH = os.environ.get("TAKEOUT_DB_PATH", "app.db")

def _connect(db_path: str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def _load_date_bounds(conn):
    try:
        df = pd.read_sql_query("SELECT invoice_date FROM sales WHERE invoice_date IS NOT NULL AND invoice_date <> ''", conn)
        if df.empty:
            return None, None
        dates = pd.to_datetime(df["invoice_date"], errors="coerce").dropna()
        if dates.empty:
            return None, None
        return dates.min().date(), dates.max().date()
    except Exception:
        return None, None

def render(user=None):
    st.title("📊 Sales Reports")
    conn = _connect()

    # Ensure columns exist (in case of older DBs)
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(sales)").fetchall()}
        if "invoice_date" not in cols:
            conn.execute("ALTER TABLE sales ADD COLUMN invoice_date TEXT;")
        if "subtotal" not in cols:
            conn.execute("ALTER TABLE sales ADD COLUMN subtotal REAL;")
        if "discount_amount" not in cols:
            conn.execute("ALTER TABLE sales ADD COLUMN discount_amount REAL;")
        if "total" not in cols:
            conn.execute("ALTER TABLE sales ADD COLUMN total REAL;")
        if "financial_status" not in cols:
            conn.execute("ALTER TABLE sales ADD COLUMN financial_status TEXT;")
        conn.commit()
    except Exception:
        pass

    # Sidebar filters
    dmin, dmax = _load_date_bounds(conn)
    st.caption("Filter by date range and financial status.")
    left, right = st.columns(2)
    with left:
        start_date = st.date_input("Start date", value=dmin or date(2000,1,1), key="sr_start")
    with right:
        end_date = st.date_input("End date", value=dmax or date.today(), key="sr_end")

    status_options = ["Paid", "Pending", "Voided"]
    sel_status = st.multiselect("Financial Status", options=status_options, default=["Paid"], help="Choose one or more statuses to include. Default: Paid only.")

    # Build query depending on filters
    params = {}
    where = []
    if start_date:
        where.append("date(invoice_date) >= :start")
        params["start"] = str(start_date)
    if end_date:
        where.append("date(invoice_date) <= :end")
        params["end"] = str(end_date)
    if sel_status:
        placeholders = ", ".join([f":st{i}" for i, _ in enumerate(sel_status)])
        where.append(f"COALESCE(financial_status,'') IN ({placeholders})")
        for i, s in enumerate(sel_status):
            params[f"st{i}"] = s

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    base_sql = f"""    SELECT
        invoice_date,
        invoice_no,
        customer,
        financial_status,
        vendor,
        subtotal,
        discount_amount,
        total
    FROM sales
    {where_sql}
    ORDER BY COALESCE(invoice_date, '') DESC
    """

    try:
        df = pd.read_sql_query(base_sql, conn, params=params if params else None)
    except Exception as e:
        st.error(f"Failed to load sales: {e}")
        return

    if df.empty:
        st.info("No sales found for the selected filters.")
        return

    # Calculate Net Sales = subtotal - discount_amount
    df["subtotal"] = pd.to_numeric(df["subtotal"], errors="coerce").fillna(0.0)
    df["discount_amount"] = pd.to_numeric(df["discount_amount"], errors="coerce").fillna(0.0)
    df["net_sales"] = df["subtotal"] - df["discount_amount"]

    # Totals
    subtotal_sum = float(df["subtotal"].sum())
    discount_sum = float(df["discount_amount"].sum())
    net_sum = float(df["net_sales"].sum())

    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.metric("Records", f"{len(df):,}")
    with m2:
        st.metric("Subtotal (₱)", f"{subtotal_sum:,.2f}")
    with m3:
        st.metric("Discount (₱)", f"{discount_sum:,.2f}")
    with m4:
        st.metric("Net Sales (₱)", f"{net_sum:,.2f}")

    # Format numeric columns for display
    df_display = df.copy()
    for col in ["subtotal", "discount_amount", "net_sales", "total"]:
        if col in df_display.columns:
            df_display[col] = pd.to_numeric(df_display[col], errors="coerce").fillna(0.0).apply(lambda v: f"{v:,.2f}")

    # Reorder / rename columns for clarity
    rename = {
        "invoice_date":"Date",
        "invoice_no":"Invoice #",
        "customer":"Customer",
        "financial_status":"Status",
        "vendor":"Vendor",
        "subtotal":"Subtotal",
        "discount_amount":"Discount",
        "net_sales":"Net Sales",
        "total":"Original Total"
    }
    cols_order = ["Date","Invoice #","Customer","Status","Vendor","Subtotal","Discount","Net Sales","Original Total"]
    df_display = df_display.rename(columns=rename)
    for c in cols_order:
        if c not in df_display.columns:
            df_display[c] = ""
    st.dataframe(df_display[cols_order], use_container_width=True, height=500)

def app(user=None):
    render(user)

def main():
    st.set_page_config(page_title="Sales Reports", layout="wide")
    render()

if __name__ == "__main__":
    main()
