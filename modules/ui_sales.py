# modules/ui_sales.py
import streamlit as st
import pandas as pd
from .db import get_conn, record_audit
from .utils import spaced_title

def render(user):
    spaced_title("Sales & Invoicing")
    conn = get_conn()
    with st.form("sale_form"):
        date = st.date_input("Date")
        invoice_no = st.text_input("Invoice No.")
        product_code = st.text_input("Product Code")
        product_name = st.text_input("Product Name")
        unit = st.text_input("Unit", value="pc")
        quantity = st.number_input("Quantity", min_value=0.0, step=1.0)
        price = st.number_input("Price", min_value=0.0, step=1.0)
        customer = st.text_input("Customer")
        submitted = st.form_submit_button("Record Sale")
    if submitted:
        total = quantity * price
        conn.execute("""INSERT INTO sales(date, invoice_no, product_code, product_name, unit, quantity, price, total_amount, customer)
                        VALUES(?,?,?,?,?,?,?,?,?)""", (str(date), invoice_no, product_code, product_name, unit, quantity, price, total, customer))
        conn.commit()
        record_audit(user["username"], user["role"], "CREATE", f"Sale {invoice_no} for {product_name} x{quantity}")
        st.success("Sale recorded.")
        st.rerun()

    df = pd.read_sql_query("SELECT * FROM sales ORDER BY id DESC", conn)
    st.dataframe(df, use_container_width=True)
