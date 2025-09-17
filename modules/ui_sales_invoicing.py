
import os
import sqlite3
from datetime import date
import pandas as pd
import streamlit as st

__version__ = "S&I-2025-09-16d (full-detail)"

DEFAULT_DB_PATH = os.environ.get("TAKEOUT_DB_PATH", "app.db")

# ---------------- DB Helpers ----------------
def _connect(db_path: str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def _init_db(conn: sqlite3.Connection):
    # Create base tables if missing
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sales (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            invoice_date TEXT,
            invoice_no TEXT NOT NULL UNIQUE,
            customer TEXT,
            email TEXT,
            shipping_method TEXT,
            branch_location TEXT,
            financial_status TEXT CHECK (financial_status IN ('Pending','Paid','Voided')) DEFAULT 'Pending',
            discount_code TEXT,
            discount_amount REAL DEFAULT 0.0,
            billing_name TEXT,
            billing_street TEXT,
            billing_address1 TEXT,
            billing_address2 TEXT,
            billing_city TEXT,
            billing_zip TEXT,
            billing_phone TEXT,
            payment_method TEXT,
            vendor TEXT CHECK (vendor IN ('Takeout Store','Lola Tindeng','Swiss Proli')),
            payment_id TEXT,
            payment_reference TEXT,
            subtotal REAL DEFAULT 0.0,
            total REAL DEFAULT 0.0,
            notes TEXT
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sales_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sale_id INTEGER NOT NULL,
            line_no INTEGER NOT NULL,
            product_code TEXT,
            product_name TEXT,
            unit TEXT,
            quantity REAL DEFAULT 0,
            price REAL DEFAULT 0,
            total_amount REAL DEFAULT 0,
            FOREIGN KEY (sale_id) REFERENCES sales(id) ON DELETE CASCADE
        );
        """
    )
    # Safe auto-migrations for legacy DBs
    required_cols = [
        ("created_at", "TEXT"),
        ("invoice_date", "TEXT"),
        ("email", "TEXT"),
        ("shipping_method", "TEXT"),
        ("branch_location", "TEXT"),
        ("financial_status", "TEXT"),
        ("discount_code", "TEXT"),
        ("discount_amount", "REAL"),
        ("billing_name", "TEXT"),
        ("billing_street", "TEXT"),
        ("billing_address1", "TEXT"),
        ("billing_address2", "TEXT"),
        ("billing_city", "TEXT"),
        ("billing_zip", "TEXT"),
        ("billing_phone", "TEXT"),
        ("payment_method", "TEXT"),
        ("vendor", "TEXT"),
        ("payment_id", "TEXT"),
        ("payment_reference", "TEXT"),
        ("subtotal", "REAL"),
        ("total", "REAL"),
        ("notes", "TEXT"),
    ]
    have = {r[1] for r in conn.execute("PRAGMA table_info(sales)").fetchall()}
    for col, typ in required_cols:
        if col not in have:
            conn.execute(f"ALTER TABLE sales ADD COLUMN {col} {typ};")
    # Backfill created_at if empty
    try:
        conn.execute("UPDATE sales SET created_at = datetime('now') WHERE created_at IS NULL OR created_at = ''")
    except Exception:
        pass
    conn.commit()

def _save_invoice(conn: sqlite3.Connection, header: dict, items_df: pd.DataFrame) -> int:
    df = items_df.copy()
    # Normalize numbers
    df["quantity"] = pd.to_numeric(df.get("quantity", 0), errors="coerce").fillna(0.0)
    df["price"] = pd.to_numeric(df.get("price", 0), errors="coerce").fillna(0.0)
    df["total_amount"] = df["quantity"] * df["price"]
    subtotal = float(df["total_amount"].sum()) if not df.empty else 0.0
    discount_amt = float(header.get("discount_amount") or 0.0)
    total = max(subtotal - discount_amt, 0.0)
    header["subtotal"] = subtotal
    header["total"] = total

    with conn:
        cur = conn.execute("SELECT id FROM sales WHERE invoice_no = ?;", (header["invoice_no"],))
        row = cur.fetchone()
        if row:
            sale_id = row[0]
            cols = list(header.keys())
            conn.execute(
                f"UPDATE sales SET {', '.join([f'{c}=?' for c in cols])} WHERE id = ?;",
                [header[c] for c in cols] + [sale_id],
            )
            conn.execute("DELETE FROM sales_items WHERE sale_id = ?;", (sale_id,))
        else:
            cols = ", ".join(header.keys())
            qs = ", ".join(["?"] * len(header))
            cur = conn.execute(f"INSERT INTO sales ({cols}) VALUES ({qs});", list(header.values()))
            sale_id = cur.lastrowid

        if not df.empty:
            payload = [
                (
                    sale_id,
                    i + 1,
                    str(r.get("product_code") or ""),
                    str(r.get("product_name") or ""),
                    str(r.get("unit") or ""),
                    float(r.get("quantity") or 0),
                    float(r.get("price") or 0),
                    float(r.get("total_amount") or 0),
                )
                for i, r in df.iterrows()
            ]
            conn.executemany(
                "INSERT INTO sales_items (sale_id, line_no, product_code, product_name, unit, quantity, price, total_amount) VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
                payload,
            )
    return sale_id

def _load_invoice(conn: sqlite3.Connection, invoice_no: str):
    cur = conn.execute("SELECT * FROM sales WHERE invoice_no = ?;", (invoice_no,))
    header_row = cur.fetchone()
    if not header_row:
        return None, None
    cols = [c[1] for c in conn.execute("PRAGMA table_info(sales)").fetchall()]
    header = dict(zip(cols, header_row))
    sale_id = header["id"]
    items = pd.read_sql_query(
        "SELECT line_no, product_code, product_name, unit, quantity, price, total_amount FROM sales_items WHERE sale_id = ? ORDER BY line_no;",
        conn,
        params=(sale_id,),
    )
    return header, items

# ---------------- UI ----------------
def sales_invoicing_app(user=None):
    st.title("🧾 Sales & Invoicing")
    st.caption(f"Version: {__version__}") 

# ----| Module: {__file__}")

    conn = _connect()
    _init_db(conn)

    # Sidebar loader
    with st.sidebar:
        st.subheader("Find Invoice")
        s_no = st.text_input("Invoice No.", key="si_search_no")
        if st.button("Load Invoice", key="si_load"):
            if s_no.strip():
                h, i = _load_invoice(conn, s_no.strip())
                if h:
                    st.session_state["si_loaded_header"] = h
                    st.session_state["si_loaded_items"] = i
                    st.success(f"Loaded invoice {s_no}")
                else:
                    st.warning("Invoice not found.")

    # Defaults
    if "si_loaded_items" not in st.session_state:
        st.session_state["si_loaded_items"] = pd.DataFrame(
            [{"product_code": "", "product_name": "", "unit": "", "quantity": 1, "price": 0.0, "total_amount": 0.0}]
        )
    if "si_loaded_header" not in st.session_state:
        st.session_state["si_loaded_header"] = {}

    # Header fields
    cA, cB, cC, cD = st.columns(4)
    with cA:
        invoice_date = st.date_input("Invoice Date", value=date.today(), format="YYYY-MM-DD", key="si_inv_date")
    with cB:
        invoice_no = st.text_input("Invoice No.", value=st.session_state["si_loaded_header"].get("invoice_no",""), key="si_inv_no")
    with cC:
        customer = st.text_input("Customer", value=st.session_state["si_loaded_header"].get("customer",""), key="si_inv_customer")
    with cD:
        email = st.text_input("Email", value=st.session_state["si_loaded_header"].get("email",""), key="si_inv_email")

    c1, c2, c3 = st.columns(3)
    with c1:
        shipping_method = st.text_input("Shipping Method", value=st.session_state["si_loaded_header"].get("shipping_method",""), key="si_inv_ship_method")
    with c2:
        branch_location = st.text_input("Branch Location", value=st.session_state["si_loaded_header"].get("branch_location",""), key="si_inv_branch")
    with c3:
        financial_status = st.selectbox(
            "Financial Status",
            ["Pending", "Paid", "Voided"],
            index=["Pending", "Paid", "Voided"].index(st.session_state["si_loaded_header"].get("financial_status","Pending")),
            key="si_inv_fin_status"
        )

    c4, c5 = st.columns(2)
    with c4:
        discount_code = st.text_input("Discount Code", value=st.session_state["si_loaded_header"].get("discount_code",""), key="si_inv_disc_code")
    with c5:
        discount_amount = st.number_input("Discount Amount", min_value=0.0, step=0.01, value=float(st.session_state["si_loaded_header"].get("discount_amount",0.0)), key="si_inv_disc_amt")

    st.markdown("**Billing Information**")
    b1, b2 = st.columns(2)
    with b1:
        billing_name = st.text_input("Billing Name", value=st.session_state["si_loaded_header"].get("billing_name",""), key="si_inv_bill_name")
        billing_street = st.text_input("Billing Street", value=st.session_state["si_loaded_header"].get("billing_street",""), key="si_inv_bill_street")
        billing_address1 = st.text_input("Billing Address1", value=st.session_state["si_loaded_header"].get("billing_address1",""), key="si_inv_bill_addr1")
        billing_address2 = st.text_input("Billing Address2", value=st.session_state["si_loaded_header"].get("billing_address2",""), key="si_inv_bill_addr2")
    with b2:
        billing_city = st.text_input("Billing City", value=st.session_state["si_loaded_header"].get("billing_city",""), key="si_inv_bill_city")
        billing_zip = st.text_input("Billing Zip", value=st.session_state["si_loaded_header"].get("billing_zip",""), key="si_inv_bill_zip")
        billing_phone = st.text_input("Billing Phone", value=st.session_state["si_loaded_header"].get("billing_phone",""), key="si_inv_bill_phone")

    c6, c7, c8, c9 = st.columns(4)
    with c6:
        payment_method = st.text_input("Payment Method", value=st.session_state["si_loaded_header"].get("payment_method",""), key="si_inv_pay_method")
    with c7:
        vendor_list = ["Takeout Store", "Lola Tindeng", "Swiss Proli"]
        current_vendor = st.session_state["si_loaded_header"].get("vendor","Takeout Store")
        initial_idx = vendor_list.index(current_vendor) if current_vendor in vendor_list else 0
        vendor = st.selectbox("Vendor", vendor_list, index=initial_idx, key="si_inv_vendor")
    with c8:
        payment_id = st.text_input("Payment ID", value=st.session_state["si_loaded_header"].get("payment_id",""), key="si_inv_pay_id")
    with c9:
        payment_reference = st.text_input("Payment Reference", value=st.session_state["si_loaded_header"].get("payment_reference",""), key="si_inv_pay_ref")

    # Line items
    st.markdown("---")
    st.markdown("**Line Items**")
    items_df = st.data_editor(
        st.session_state["si_loaded_items"],
        num_rows="dynamic",
        use_container_width=True,
        key="si_items_editor",
        column_config={
            "product_code": st.column_config.TextColumn("Product Code"),
            "product_name": st.column_config.TextColumn("Product Name"),
            "unit": st.column_config.TextColumn("Unit"),
            "quantity": st.column_config.NumberColumn("Quantity", min_value=0.0, step=1.0),
            "price": st.column_config.NumberColumn("Price", min_value=0.0, step=0.01),
            "total_amount": st.column_config.NumberColumn("Total Amount", disabled=True),
        }
    )
    # Live totals
    tmp = items_df.copy()
    tmp["quantity"] = pd.to_numeric(tmp.get("quantity", 0), errors="coerce").fillna(0.0)
    tmp["price"] = pd.to_numeric(tmp.get("price", 0), errors="coerce").fillna(0.0)
    tmp["total_amount"] = tmp["quantity"] * tmp["price"]
    subtotal_live = float(tmp["total_amount"].sum()) if not tmp.empty else 0.0
    discount_live = float(discount_amount or 0.0)
    total_live = max(subtotal_live - discount_live, 0.0)
    m1, m2, m3 = st.columns(3)
    m1.metric("Subtotal", f"{subtotal_live:,.2f}")
    m2.metric("Discount", f"{discount_live:,.2f}")
    m3.metric("Total", f"{total_live:,.2f}")

    # Notes
    notes = st.text_area("Notes", value=st.session_state["si_loaded_header"].get("notes",""), height=80, key="si_inv_notes")

    # Save/Update
    if st.button("Save / Update Invoice", type="primary", use_container_width=True, key="si_inv_save"):
        if not invoice_no.strip():
            st.error("Invoice No. is required.")
        else:
            header = dict(
                invoice_date=str(invoice_date),
                invoice_no=invoice_no.strip(),
                customer=customer.strip(),
                email=email.strip(),
                shipping_method=shipping_method.strip(),
                branch_location=branch_location.strip(),
                financial_status=financial_status,
                discount_code=discount_code.strip(),
                discount_amount=float(discount_amount or 0.0),
                billing_name=billing_name.strip(),
                billing_street=billing_street.strip(),
                billing_address1=billing_address1.strip(),
                billing_address2=billing_address2.strip(),
                billing_city=billing_city.strip(),
                billing_zip=billing_zip.strip(),
                billing_phone=billing_phone.strip(),
                payment_method=payment_method.strip(),
                vendor=vendor,
                payment_id=payment_id.strip(),
                payment_reference=payment_reference.strip(),
                notes=notes.strip(),
            )
            sale_id = _save_invoice(conn, header, items_df)
            st.success(f"Saved invoice #{invoice_no} (ID: {sale_id}).")
            st.session_state["si_loaded_header"] = header
            st.session_state["si_loaded_items"] = items_df

    # Recent Invoices (limited columns per your request)
    st.markdown("---")
    st.subheader("Recent Invoices")
    try:
        recent = pd.read_sql_query(
            "SELECT COALESCE(invoice_date,'') AS invoice_date, invoice_no, COALESCE(customer,'') AS customer, COALESCE(email,'') AS email, COALESCE(branch_location,'') AS branch_location, COALESCE(financial_status,'') AS financial_status FROM sales ORDER BY date(COALESCE(invoice_date,'1970-01-01')) DESC, invoice_no DESC LIMIT 100;",
            conn
        )
        st.dataframe(recent, use_container_width=True, height=320)
    except Exception as e:
        st.error(f"Failed to load Recent Invoices: {e}")

# Router compatibility
def app(user=None): 
    return sales_invoicing_app(user)

def render(user=None): 
    return sales_invoicing_app(user)

def main():
    st.set_page_config(page_title="Sales & Invoicing", layout="wide")
    return sales_invoicing_app()

if __name__ == "__main__":
    main()
