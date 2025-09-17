
import os
import sqlite3
from datetime import datetime, date
from typing import Optional

import pandas as pd
import streamlit as st

APP_DB_PATH = os.environ.get("TAKEOUT_DB_PATH", "app.db")

# ---------------- DB ----------------
def _conn():
    c = sqlite3.connect(APP_DB_PATH, check_same_thread=False)
    c.execute("PRAGMA foreign_keys=ON;")
    return c

# ---------------- Helpers ----------------
def _to_date(s: Optional[str]):
    if s is None or pd.isna(s) or str(s).strip() == "":
        return None
    try:
        return pd.to_datetime(str(s).split(" ")[0], errors="coerce")
    except Exception:
        return None

def _safe_read_sql(conn, query, cols):
    try:
        df = pd.read_sql_query(query, conn)
        if df.empty:
            return pd.DataFrame(columns=cols)
        return df
    except Exception as e:
        st.warning(f"Query issue: {e}")
        return pd.DataFrame(columns=cols)

def _daterange_df(df: pd.DataFrame, col: str, start: Optional[date], end: Optional[date]) -> pd.DataFrame:
    if df.empty:
        return df
    m = pd.Series(True, index=df.index)
    if start:
        m &= df[col] >= pd.Timestamp(start)
    if end:
        m &= df[col] <= pd.Timestamp(end)
    return df[m]

def _sum_or_0(series: pd.Series) -> float:
    try:
        return float(pd.to_numeric(series, errors="coerce").fillna(0).sum())
    except Exception:
        return 0.0

def _fmt(n: float) -> str:
    try:
        return f"{float(n):,.2f}"
    except Exception:
        return "0.00"

def _expenses_date_column(conn) -> Optional[str]:
    """Return a usable date column from expenses, preferring 'date', then 'created_at'. None if neither exists."""
    try:
        cols = pd.read_sql_query("PRAGMA table_info(expenses);", conn)
        names = set(col for col in cols["name"].astype(str).tolist())
        if "date" in names:
            return "date"
        if "created_at" in names:
            return "created_at"
    except Exception:
        pass
    return None

# ---------------- UI ----------------
def render(user=None):
    st.title("📊 Business Dashboard")

    conn = _conn()

    # Sales with status/vendor/customer + amounts
    df_sales = _safe_read_sql(
        conn,
        "SELECT "
        "COALESCE(subtotal,0) AS subtotal, "
        "COALESCE(discount_amount,0) AS discount_amount, "
        "COALESCE(total,0) AS total, "
        "COALESCE(invoice_date, created_at) AS inv_date, "
        "customer, vendor, COALESCE(financial_status,'') AS financial_status "
        "FROM sales;",
        ["subtotal","discount_amount","total","inv_date","customer","vendor","financial_status"]
    )
    if not df_sales.empty:
        df_sales["inv_date"] = df_sales["inv_date"].apply(_to_date)
        df_sales = df_sales.dropna(subset=["inv_date"])
        df_sales["inv_date"] = pd.to_datetime(df_sales["inv_date"]).dt.normalize()
        df_sales["subtotal"] = pd.to_numeric(df_sales["subtotal"], errors="coerce").fillna(0.0)
        df_sales["discount_amount"] = pd.to_numeric(df_sales["discount_amount"], errors="coerce").fillna(0.0)
        # --- Net Sales = Subtotal - Discount ---
        df_sales["net"] = df_sales["subtotal"] - df_sales["discount_amount"]

    # Expenses (schema-robust: pick available date column)
    exp_date_col = _expenses_date_column(conn)
    if exp_date_col:
        df_exp = _safe_read_sql(
            conn,
            f"SELECT COALESCE(amount,0) AS amount, {exp_date_col} AS exp_date, COALESCE(category,'') AS category FROM expenses;",
            ["amount","exp_date","category"]
        )
    else:
        st.info("No usable date column found in 'expenses' (expected 'date' or 'created_at'). Skipping expense analytics.")
        df_exp = pd.DataFrame(columns=["amount","exp_date","category"])

    if not df_exp.empty:
        df_exp["exp_date"] = df_exp["exp_date"].apply(_to_date)
        df_exp = df_exp.dropna(subset=["exp_date"])
        df_exp["exp_date"] = pd.to_datetime(df_exp["exp_date"]).dt.normalize()
        df_exp["amount"] = pd.to_numeric(df_exp["amount"], errors="coerce").fillna(0.0)

    # Sidebar filters
    with st.sidebar:
        st.header("Filters")
        today = date.today()
        default_start = today.replace(day=1)
        dr = st.date_input("Date range", (default_start, today))
        try:
            start, end = dr
        except Exception:
            start, end = default_start, today

        # Status filter (default to Pending + Paid)
        status_base = ["Pending","Paid","Voided"]
        data_statuses = sorted(df_sales["financial_status"].dropna().unique().tolist()) if not df_sales.empty else []
        status_options = list(dict.fromkeys(status_base + data_statuses))  # de-dup preserving order
        default_sel = [s for s in ["Pending","Paid"] if s in status_options] or status_options  # default both; fallback all
        sel_status = st.multiselect("Financial Status", options=status_options, default=default_sel, help="Filter metrics by order status. Default: Pending + Paid.")

        show_expenses = st.checkbox("Show expenses & profit", value=True)
        group_granularity = st.radio("Time granularity", options=["Daily","Weekly","Monthly"], index=2)

    # Apply filters
    dfs = df_sales.copy()
    if not dfs.empty:
        dfs = _daterange_df(dfs, "inv_date", start, end)
        if sel_status:
            dfs = dfs[dfs["financial_status"].isin(sel_status)]

    dfe = df_exp.copy()
    if show_expenses and not dfe.empty:
        dfe = _daterange_df(dfe, "exp_date", start, end)

    # --- KPI tiles (use NET everywhere) ---
    today_dt = pd.Timestamp(date.today()).normalize()
    start_of_week = today_dt - pd.to_timedelta(today_dt.weekday(), unit="D")
    start_of_month = today_dt.replace(day=1)

    sales_today = _sum_or_0(dfs.loc[dfs["inv_date"] == today_dt, "net"]) if not dfs.empty else 0.0
    sales_week = _sum_or_0(dfs.loc[(dfs["inv_date"] >= start_of_week) & (dfs["inv_date"] <= today_dt), "net"]) if not dfs.empty else 0.0
    sales_month = _sum_or_0(dfs.loc[(dfs["inv_date"] >= start_of_month) & (dfs["inv_date"] <= today_dt), "net"]) if not dfs.empty else 0.0
    total_sales = _sum_or_0(dfs["net"]) if not dfs.empty else 0.0

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Today (Sales)", _fmt(sales_today))
    col2.metric("This Week (Sales)", _fmt(sales_week))
    col3.metric("This Month (Sales)", _fmt(sales_month))
    col4.metric("Selected Range (Sales)", _fmt(total_sales))

    if show_expenses:
        total_expenses = _sum_or_0(dfe["amount"]) if dfe is not None and not dfe.empty else 0.0
        st.metric("Selected Range (Expenses)", _fmt(total_expenses))
        st.metric("Selected Range (Profit est.)", _fmt(total_sales - total_expenses))

    # --- Trend chart (NET) ---
    if not dfs.empty:
        s = dfs.copy()
        if group_granularity == "Daily":
            s["bucket"] = s["inv_date"].dt.date
        elif group_granularity == "Weekly":
            s["bucket"] = s["inv_date"].dt.to_period("W").apply(lambda p: p.start_time.date())
        else:
            s["bucket"] = s["inv_date"].dt.to_period("M").dt.to_timestamp().dt.date
        chart_df = s.groupby("bucket", as_index=False)["net"].sum().sort_values("bucket")
        st.subheader(f"Sales Trend ({group_granularity})")
        st.line_chart(chart_df.set_index("bucket"))

    # --- Top lists (NET) ---
    c1, c2 = st.columns(2)
    if not dfs.empty:
        with c1:
            st.subheader("Top Customers")
            top_cust = (dfs.groupby("customer", dropna=False, as_index=False)["net"]
                        .sum().sort_values("net", ascending=False).head(10))
            top_cust["Sales"] = top_cust["net"].apply(_fmt)
            top_cust = top_cust.rename(columns={"customer":"Customer"})[["Customer","Sales"]]
            st.dataframe(top_cust, use_container_width=True, height=290)
        with c2:
            st.subheader("Top Vendors")
            top_vend = (dfs.groupby("vendor", dropna=False, as_index=False)["net"]
                        .sum().sort_values("net", ascending=False).head(10))
            top_vend["Sales"] = top_vend["net"].apply(_fmt)
            top_vend = top_vend.rename(columns={"vendor":"Vendor"})[["Vendor","Sales"]]
            st.dataframe(top_vend, use_container_width=True, height=290)

    # --- Recent orders (show Net) ---
    try:
        recent = _safe_read_sql(conn, "SELECT invoice_no, COALESCE(invoice_date, created_at) AS inv_date, customer, subtotal, discount_amount, vendor, financial_status FROM sales ORDER BY id DESC LIMIT 25;", ["invoice_no","inv_date","customer","subtotal","discount_amount","vendor","financial_status"])
        if not recent.empty:
            recent["inv_date"] = pd.to_datetime(recent["inv_date"], errors="coerce").dt.strftime("%Y-%m-%d")
            recent["subtotal"] = pd.to_numeric(recent["subtotal"], errors="coerce").fillna(0.0)
            recent["discount_amount"] = pd.to_numeric(recent["discount_amount"], errors="coerce").fillna(0.0)
            recent["Net"] = (recent["subtotal"] - recent["discount_amount"]).apply(_fmt)
            recent = recent.rename(columns={
                "invoice_no":"Invoice #","inv_date":"Date","customer":"Customer","vendor":"Vendor",
                "financial_status":"Status"
            })[["Invoice #","Date","Customer","Vendor","Status","Net"]]
            st.subheader("Recent Orders")
            st.dataframe(recent, use_container_width=True, height=320)
    except Exception as e:
        st.caption(f"Recent orders unavailable: {e}")

    # --- Expense breakdown ---
    if show_expenses and dfe is not None and not dfe.empty:
        st.subheader("Expense Breakdown (by Category)")
        b = dfe.groupby("category", dropna=False, as_index=False)["amount"].sum().sort_values("amount", ascending=False)
        b = b.rename(columns={"category":"Category","amount":"Amount"})
        b_fmt = b.copy()
        b_fmt["Amount"] = b_fmt["Amount"].apply(_fmt)
        st.dataframe(b_fmt, use_container_width=True, height=260)
        st.bar_chart(b.set_index("Category"))

app = render
main = render
