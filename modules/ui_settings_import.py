
import os
import json
import sqlite3
from collections import defaultdict
from datetime import datetime
from typing import List, Optional

import pandas as pd
import streamlit as st
import re

# ---------------- Settings ----------------
DEFAULT_DB_PATH = os.environ.get("TAKEOUT_DB_PATH", "app.db")
AUDIT_DB_PATH = os.environ.get("TAKEOUT_AUDIT_DB_PATH", os.environ.get("TAKEOUT_DB_PATH", "audit.db"))
DOC_PREFIX = os.environ.get("TAKEOUT_DOC_PREFIX", "INV")  # document number prefix
DOC_PAD = int(os.environ.get("TAKEOUT_DOC_PAD", "4"))     # e.g., 0001

# ---------------- DB helpers ----------------
def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def _ensure_schema(conn: sqlite3.Connection):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sales (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            invoice_date TEXT,
            invoice_no TEXT UNIQUE,
            customer TEXT,
            email TEXT,
            shipping_method TEXT,
            branch_location TEXT,
            financial_status TEXT,
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
            vendor TEXT,
            payment_id TEXT,
            payment_reference TEXT,
            subtotal REAL DEFAULT 0.0,
            total REAL DEFAULT 0.0,
            notes TEXT,
            currency TEXT,
            shipping_cost REAL,
            taxes REAL,
            paid_at TEXT,
            fulfilled_at TEXT,
            fulfillment_status TEXT,
            accepts_marketing TEXT,
            shipping_name TEXT,
            shipping_street TEXT,
            shipping_address1 TEXT,
            shipping_address2 TEXT,
            shipping_city TEXT,
            shipping_zip TEXT,
            shipping_phone TEXT,
            billing_company TEXT,
            billing_province TEXT,
            billing_country TEXT,
            shipping_company TEXT,
            shipping_province TEXT,
            shipping_country TEXT,
            cancelled_at TEXT,
            refunded_amount REAL,
            outstanding_balance REAL,
            employee TEXT,
            location TEXT,
            device_id TEXT,
            tags TEXT,
            risk_level TEXT,
            source TEXT,
            receipt_number TEXT,
            duties REAL,
            billing_province_name TEXT,
            shipping_province_name TEXT,
            payment_terms_name TEXT,
            next_payment_due_at TEXT,
            extra_json TEXT
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
    # sequence table for document numbers
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sequences (
            name TEXT PRIMARY KEY,
            value INTEGER NOT NULL
        );
        """
    )
    conn.commit()

def _ensure_audit_schema(conn: sqlite3.Connection):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL DEFAULT (datetime('now')),
            module TEXT NOT NULL,
            action TEXT NOT NULL,
            invoice_no TEXT,
            document_no TEXT,
            user TEXT,
            public_ip TEXT,
            details TEXT
        );
        """
    )
    conn.commit()

# ---------------- Utility: public IP ----------------
def _get_public_ip() -> str:
    # Best-effort; return 'unknown' if blocked
    try:
        import requests
    except Exception:
        return "unknown"
    urls = [
        "https://api.ipify.org",
        "https://ifconfig.me/ip",
        "https://ipinfo.io/ip",
    ]
    for u in urls:
        try:
            r = requests.get(u, timeout=2)
            if r.ok:
                return r.text.strip()
        except Exception:
            continue
    return "unknown"

# ---------------- Document number ----------------
def _next_doc_no(conn: sqlite3.Connection, when: Optional[datetime] = None) -> str:
    """Generate next document number like INV-YYYYMM-0001 using sequences table."""
    when = when or datetime.utcnow()
    yyyymm = when.strftime("%Y%m")
    seq_name = f"{DOC_PREFIX}-{yyyymm}"
    with conn:
        row = conn.execute("SELECT value FROM sequences WHERE name=?;", (seq_name,)).fetchone()
        if row is None:
            conn.execute("INSERT INTO sequences(name, value) VALUES (?, ?);", (seq_name, 1))
            seq = 1
        else:
            seq = int(row[0]) + 1
            conn.execute("UPDATE sequences SET value=? WHERE name=?;", (seq, seq_name))
    return f"{DOC_PREFIX}-{yyyymm}-{str(seq).zfill(DOC_PAD)}"

# ---------------- Audit log ----------------
def _audit_log(audit_conn: sqlite3.Connection, action: str, invoice_no: str, document_no: str, user: Optional[str], public_ip: str, details: dict):
    _ensure_audit_schema(audit_conn)
    payload = ( "settings_import", action, invoice_no, document_no, str(user) if user else None, public_ip, json.dumps(details) )
    with audit_conn:
        audit_conn.execute(
            "INSERT INTO audit_log (module, action, invoice_no, document_no, user, public_ip, details) VALUES (?,?,?,?,?,?,?);",
            payload
        )

# ---------------- Normalizers ----------------
ALLOWED_STATUS = {"Pending", "Paid", "Voided"}
STATUS_MAP = {
    "unpaid":"Pending","new":"Pending","open":"Pending","partially paid":"Pending","partial":"Pending",
    "awaiting payment":"Pending","due":"Pending","cancelled":"Voided","canceled":"Voided","void":"Voided",
    "voided":"Voided","closed - void":"Voided","complete":"Paid","completed":"Paid","done":"Paid",
    "paid/closed":"Paid","settled":"Paid"
}
def _clean_status(x):
    if x is None: return "Pending"
    s = str(x).strip()
    if s == "" or s.lower() in {"nan","none","null"}: return "Pending"
    low = s.lower()
    if low in STATUS_MAP: return STATUS_MAP[low]
    return s.title().strip()

def _clean_invoice_no(x) -> str:
    s = str(x).strip()
    if s.endswith(".0"): s = s[:-2]
    return re.sub(r"\s+","", s)

ALLOWED_VENDORS = {"Takeout Store", "Lola Tindeng", "Swiss Proli"}
VENDOR_MAP = {
    "takeout store":"Takeout Store","lola tindeng":"Lola Tindeng","swiss proli":"Swiss Proli",
    "swiss proli.":"Swiss Proli","swiss-proli":"Swiss Proli","swissproli":"Swiss Proli",
    "swiss prolife":"Swiss Proli",
    "takeoutstore":"Takeout Store","takeoutstoreph":"Takeout Store","takeout-store":"Takeout Store",
    "takeout store ph":"Takeout Store","takeoutstore-ph":"Takeout Store",
}
def _clean_vendor(x) -> str:
    s = str(x).strip()
    if s == "" or s.lower() in {"nan","none","null"}: return ""
    low = s.lower().strip()
    if low in VENDOR_MAP: return VENDOR_MAP[low]
    alnum = re.sub(r"[^a-z0-9]+","", low)
    if alnum.startswith("takeout"): return "Takeout Store"
    if "lola" in alnum and "tindeng" in alnum: return "Lola Tindeng"
    if "swiss" in alnum and "proli" in alnum: return "Swiss Proli"
    return s.title().strip()

def _f(x):
    try:
        if pd.isna(x): return 0.0
        if isinstance(x,str): x = x.replace(",","").strip()
        return float(x)
    except Exception: return 0.0

def _s(x):
    if pd.isna(x): return ""
    return str(x).strip()

COLMAP = {
    "Name":"invoice_no","Email":"email","Financial Status":"financial_status","Paid at":"paid_at",
    "Fulfillment Status":"fulfillment_status","Fulfilled at":"fulfilled_at","Accepts Marketing":"accepts_marketing",
    "Currency":"currency","Subtotal":"subtotal","Shipping":"shipping_cost","Taxes":"taxes","Total":"total",
    "Discount Code":"discount_code","Discount Amount":"discount_amount","Shipping Method":"shipping_method",
    "Created at":"invoice_date","Billing Name":"billing_name","Billing Street":"billing_street",
    "Billing Address1":"billing_address1","Billing Address2":"billing_address2","Billing Company":"billing_company",
    "Billing City":"billing_city","Billing Zip":"billing_zip","Billing Province":"billing_province",
    "Billing Country":"billing_country","Billing Phone":"billing_phone","Shipping Name":"shipping_name",
    "Shipping Street":"shipping_street","Shipping Address1":"shipping_address1","Shipping Address2":"shipping_address2",
    "Shipping Company":"shipping_company","Shipping City":"shipping_city","Shipping Zip":"shipping_zip",
    "Shipping Province":"shipping_province","Shipping Country":"shipping_country","Shipping Phone":"shipping_phone",
    "Notes":"notes","Cancelled at":"cancelled_at","Payment Method":"payment_method","Payment Reference":"payment_reference",
    "Refunded Amount":"refunded_amount","Vendor":"vendor","Outstanding Balance":"outstanding_balance","Employee":"employee",
    "Location":"location","Device ID":"device_id","Id":"external_id","Tags":"tags","Risk Level":"risk_level",
    "Source":"source","Phone":"billing_phone","Receipt Number":"receipt_number","Duties":"duties",
    "Billing Province Name":"billing_province_name","Shipping Province Name":"shipping_province_name",
    "Payment ID":"payment_id","Payment Terms Name":"payment_terms_name","Next Payment Due At":"next_payment_due_at",
    "Payment References":"payment_reference",
}
LINE_COLS = [
    "Lineitem quantity","Lineitem name","Lineitem price","Lineitem compare at price","Lineitem sku",
    "Lineitem requires shipping","Lineitem taxable","Lineitem fulfillment status","Lineitem discount",
    "Tax 1 Name","Tax 1 Value","Tax 2 Name","Tax 2 Value","Tax 3 Name","Tax 3 Value","Tax 4 Name","Tax 4 Value","Tax 5 Name","Tax 5 Value",
]

def _aggregate_items(items: List[dict]) -> List[dict]:
    bucket = defaultdict(lambda: {"quantity":0.0, "total_amount":0.0, "unit":""})
    meta = {}
    for it in items:
        key = (it.get("product_code",""), it.get("product_name",""), float(it.get("price",0.0)), it.get("unit",""))
        bucket[key]["quantity"] += float(it.get("quantity",0.0))
        bucket[key]["total_amount"] += float(it.get("total_amount",0.0))
        meta[key] = {"product_code": key[0], "product_name": key[1], "price": key[2], "unit": key[3]}
    out = []
    for key, agg in bucket.items():
        base = meta[key].copy()
        base["quantity"] = agg["quantity"]
        base["total_amount"] = agg["total_amount"]
        out.append(base)
    return out

def _header_from_group(g: pd.DataFrame, combine_items: bool = True):
    first = g.iloc[0].to_dict()
    header = {}
    for src, dst in COLMAP.items():
        if src in first:
            if dst in {"discount_amount","subtotal","shipping_cost","taxes","total","refunded_amount","outstanding_balance","duties"}:
                header[dst] = _f(first[src])
            else:
                header[dst] = _s(first[src])

    raw_name = first.get("Name") or first.get("Id") or ""
    header.setdefault("invoice_no", _s(raw_name))
    header["invoice_no"] = _clean_invoice_no(header["invoice_no"])

    header.setdefault("customer", _s(first.get("Shipping Name") or first.get("Billing Name") or first.get("Name") or ""))

    if "invoice_date" in header:
        header["invoice_date"] = _s(header["invoice_date"]).split(" ")[0]

    header["financial_status"] = _clean_status(header.get("financial_status", "Pending"))

    header["vendor"] = _clean_vendor(header.get("vendor", first.get("Vendor","")))

    items = []
    for _, row in g.iterrows():
        qty = _f(row.get("Lineitem quantity"))
        if qty == 0 and pd.isna(row.get("Lineitem name")):
            continue
        price = _f(row.get("Lineitem price"))
        items.append({
            "product_code": _s(row.get("Lineitem sku")),
            "product_name": _s(row.get("Lineitem name")),
            "unit": "",
            "quantity": qty,
            "price": price,
            "total_amount": qty * price,
        })

    if combine_items and items:
        items = _aggregate_items(items)

    if not header.get("subtotal"):
        header["subtotal"] = float(sum(i["total_amount"] for i in items))
    if not header.get("discount_amount"):
        header["discount_amount"] = _f(first.get("Lineitem discount"))
    if not header.get("total"):
        header["total"] = max(header.get("subtotal", 0.0) - header.get("discount_amount", 0.0), 0.0)

    return header, items

def _save_order(conn: sqlite3.Connection, header: dict, items: List[dict]):
    _ensure_schema(conn)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(sales)").fetchall()}
    clean_header = {k: v for k, v in header.items() if k in cols}

    with conn:
        inv = clean_header.get("invoice_no")
        cur = conn.execute("SELECT id FROM sales WHERE invoice_no = ?;", (inv,))
        row = cur.fetchone()
        if row:
            sid = row[0]
            set_clause = ", ".join([f"{k}=?" for k in clean_header.keys()])
            conn.execute(f"UPDATE sales SET {set_clause} WHERE id=?;", list(clean_header.values()) + [sid])
            conn.execute("DELETE FROM sales_items WHERE sale_id=?;", (sid,))
        else:
            keys = ", ".join(clean_header.keys())
            qs = ", ".join(["?"] * len(clean_header))
            cur = conn.execute(f"INSERT INTO sales ({keys}) VALUES ({qs});", list(clean_header.values()))
            sid = cur.lastrowid

        if items:
            payload = [(sid, i+1, it.get("product_code",""), it.get("product_name",""), it.get("unit",""),
                        float(it.get("quantity",0)), float(it.get("price",0)), float(it.get("total_amount",0)))
                       for i, it in enumerate(items)]
            conn.executemany(
                "INSERT INTO sales_items (sale_id, line_no, product_code, product_name, unit, quantity, price, total_amount) VALUES (?,?,?,?,?,?,?,?);",
                payload
            )
    return sid

# ---------------- UI ----------------
def render(user=None):
    st.title("⚙️ Settings / Import")
    st.subheader("Excel Order Import (Shopify-style)")

    file = st.file_uploader("Choose .xlsx file", type=["xlsx"], key="imp_file")
    if not file:
        st.info("Awaiting file...")
        return

    try:
        df = pd.read_excel(file, engine="openpyxl")
    except Exception as e:
        st.error(f"Failed to read Excel: {e}")
        return

    df.columns = [str(c).strip() for c in df.columns]

    # Drop exact duplicate rows (idempotent re-uploads)
    before = len(df)
    df = df.dropna(how="all").drop_duplicates()
    if len(df) < before:
        st.info(f"De-duplication: removed {before - len(df)} exact duplicate row(s).")

    combine_items = st.toggle("Combine duplicate line items (same SKU/Name/Price)", value=True)

    st.write("Preview:", df.head(10))
    st.write(f"Rows: {len(df):,} | Columns: {len(df.columns)}")

    # Pre-checks
    if "Financial Status" in df.columns:
        raw_fs = df["Financial Status"].astype(str).fillna("").str.strip()
        fs_norm = raw_fs.apply(_clean_status)
        bad_fs = ~fs_norm.isin(ALLOWED_STATUS)
        if int(bad_fs.sum()):
            st.warning("Some rows have invalid financial status even after normalization. They will be skipped.")
            st.dataframe(pd.DataFrame({"Raw": raw_fs[bad_fs].head(50), "Normalized": fs_norm[bad_fs].head(50)}))

    if "Vendor" in df.columns:
        raw_v = df["Vendor"].astype(str).fillna("").str.strip()
        v_norm = raw_v.apply(_clean_vendor)
        bad_v = ~v_norm.isin(ALLOWED_VENDORS)
        if int(bad_v.sum()):
            st.warning("Some rows have invalid vendor even after normalization. They will be skipped.")
            st.dataframe(pd.DataFrame({"Raw": raw_v[bad_v].head(50), "Normalized": v_norm[bad_v].head(50)}))

    group_key = "Id" if "Id" in df.columns else "Name"
    st.caption(f"Grouping by: **{group_key}**")

    if not st.button("Import to Database", type="primary", use_container_width=True):
        return

    # open connections
    conn = _connect(DEFAULT_DB_PATH)
    _ensure_schema(conn)
    audit_conn = _connect(AUDIT_DB_PATH)
    _ensure_audit_schema(audit_conn)

    imported, updated, skipped = 0, 0, 0
    issues = []

    public_ip = _get_public_ip()
    user_str = None
    try:
        # if your app passes a user object/string, we can show it; else keep None
        user_str = str(user) if user is not None else None
    except Exception:
        user_str = None

    for gkey, sub in df.groupby(group_key, dropna=False):
        try:
            header, items = _header_from_group(sub, combine_items=combine_items)

            if header["financial_status"] not in ALLOWED_STATUS:
                skipped += 1
                msg = f"Invalid financial_status '{header['financial_status']}' after normalization"
                issues.append((str(gkey), msg))
                _audit_log(audit_conn, "skip_invalid_status", header.get("invoice_no",""), None, user_str, public_ip, {"reason": msg})
                continue

            if header.get("vendor","") not in ALLOWED_VENDORS:
                skipped += 1
                msg = f"Invalid vendor '{header.get('vendor','')}' after normalization"
                issues.append((str(gkey), msg))
                _audit_log(audit_conn, "skip_invalid_vendor", header.get("invoice_no",""), None, user_str, public_ip, {"reason": msg})
                continue

            inv = header.get("invoice_no")
            if not inv:
                skipped += 1
                msg = "Missing invoice_no after normalization"
                issues.append((str(gkey), msg))
                _audit_log(audit_conn, "skip_missing_invoice_no", "", None, user_str, public_ip, {"reason": msg})
                continue

            # assign document number if missing (receipt_number)
            if not header.get("receipt_number"):
                header["receipt_number"] = _next_doc_no(conn)

            # check if exists
            exists = False
            with conn:
                cur = conn.execute("SELECT id, receipt_number FROM sales WHERE invoice_no=?;", (inv,))
                row = cur.fetchone()
                if row:
                    exists = True
                    # keep existing receipt_number if already set
                    if row[1]:
                        header["receipt_number"] = row[1]

            sale_id = _save_order(conn, header, items)

            if exists:
                updated += 1
                _audit_log(audit_conn, "update", inv, header.get("receipt_number"), user_str, public_ip, {"sale_id": sale_id})
            else:
                imported += 1
                _audit_log(audit_conn, "insert", inv, header.get("receipt_number"), user_str, public_ip, {"sale_id": sale_id})

        except Exception as e:
            skipped += 1
            msg = str(e)
            issues.append((str(gkey), msg))
            _audit_log(audit_conn, "error", str(gkey), None, user_str, public_ip, {"error": msg})

    st.success(f"Done. Imported: {imported:,} | Updated: {updated:,} | Skipped: {skipped:,}")
    if issues:
        with st.expander("Issues"):
            for k, msg in issues[:300]:
                st.text(f"{k}: {msg}")

def app(user=None):
    return render(user)

def main():
    st.set_page_config(page_title="Settings / Import", layout="wide")
    render()

if __name__ == "__main__":
    main()
