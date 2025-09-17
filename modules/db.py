
# db.py — Streamlit Cloud + Google Cloud Storage–ready DB bootstrap
# Strategy:
#   - On import, pull DBs from GCS to a LOCAL data dir (fast, safe for SQLite)
#   - App reads/writes locally with WAL mode
#   - Provide helpers to push back to GCS on demand (e.g., admin button)
#
# Secrets expected:
#   [gcp]
#   project   = "..."
#   bucket    = "..."
#   prefix    = "takeout_ms/db"
#   credentials_json = "{...}"   # service account JSON (recommended on Streamlit Cloud)
#
#   [paths]
#   local_data_dir = "/app/data" # optional; defaults to "/app/data" on Streamlit Cloud
#
import os
import sqlite3
from typing import Optional, Dict, Any

try:
    import streamlit as st
except Exception:
    class _Dummy(dict):
        def get(self, *a, **k): return None
    st = _Dummy()

# Local runtime dir for SQLite files (ephemeral on Streamlit Cloud, but fine)
LOCAL_DATA_DIR = (st.secrets.get("paths", {}) or {}).get("local_data_dir") or "/app/data"
os.makedirs(LOCAL_DATA_DIR, exist_ok=True)

APP_DB = os.path.join(LOCAL_DATA_DIR, "app.db")
USER_DB = os.path.join(LOCAL_DATA_DIR, "user.db")
AUDIT_DB = os.path.join(LOCAL_DATA_DIR, "audit.db")

# ---- GCS bootstrap (download on cold start) ----
def _bootstrap_from_gcs():
    try:
        from gcs_sync import pull_from_gcs
        pull_from_gcs(["app.db","user.db","audit.db"], local_dir=LOCAL_DATA_DIR)
    except Exception as e:
        # Non-fatal: we will create fresh DBs if missing
        pass

_bootstrap_from_gcs()

def _apply_pragmas(c: sqlite3.Connection):
    c.execute("PRAGMA foreign_keys=ON;")
    c.execute("PRAGMA busy_timeout=5000;")
    try:
        c.execute("PRAGMA journal_mode=WAL;")
        c.execute("PRAGMA synchronous=NORMAL;")
    except Exception:
        pass

def _connect(path: str) -> sqlite3.Connection:
    c = sqlite3.connect(path, check_same_thread=False)
    _apply_pragmas(c)
    return c

def conn_app():
    c = _connect(APP_DB)
    _ensure_app_schema(c)
    return c

def conn_user():
    c = _connect(USER_DB)
    _ensure_user_schema(c)
    return c

def conn_audit():
    c = _connect(AUDIT_DB)
    _ensure_audit_schema(c)
    return c

# ---- Schemas ----
def _ensure_app_schema(c):
    c.execute("""CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sku TEXT UNIQUE,
        name TEXT,
        unit TEXT,
        cost REAL DEFAULT 0,
        price REAL DEFAULT 0,
        active INTEGER DEFAULT 1,
        created_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now'))
    );""")
    c.execute("""CREATE TABLE IF NOT EXISTS inventory (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id INTEGER NOT NULL,
        quantity REAL DEFAULT 0,
        last_update TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
        FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE
    );""")
    c.execute("""CREATE TABLE IF NOT EXISTS sales (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        invoice_no TEXT,
        customer TEXT,
        total_amount REAL DEFAULT 0,
        created_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now'))
    );""")
    c.execute("""CREATE TABLE IF NOT EXISTS sale_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sale_id INTEGER NOT NULL,
        product_id INTEGER NOT NULL,
        unit TEXT,
        quantity REAL NOT NULL,
        price REAL NOT NULL,
        total_amount REAL NOT NULL,
        FOREIGN KEY(sale_id) REFERENCES sales(id) ON DELETE CASCADE,
        FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE RESTRICT
    );""")
    c.execute("""CREATE TABLE IF NOT EXISTS expenses (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        category TEXT,
        description TEXT,
        amount REAL NOT NULL,
        created_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now'))
    );""")
    c.execute("""CREATE TABLE IF NOT EXISTS supplies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        unit TEXT,
        quantity REAL DEFAULT 0,
        cost REAL DEFAULT 0,
        last_update TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now'))
    );""")
    c.commit()

def _ensure_user_schema(c):
    c.execute("""CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT UNIQUE NOT NULL,
        username TEXT UNIQUE,
        password_hash BLOB NOT NULL,
        salt BLOB NOT NULL,
        role TEXT DEFAULT 'User',
        is_active INTEGER DEFAULT 1,
        created_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now'))
    );""")
    c.execute("""CREATE TABLE IF NOT EXISTS password_tokens (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        token TEXT NOT NULL,
        purpose TEXT NOT NULL,
        expires_at TEXT NOT NULL,
        used_at TEXT,
        created_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
        FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
    );""")
    cols = _table_columns(c, "users")
    _ensure_column(c, "users", "username", "TEXT UNIQUE", cols)
    _ensure_column(c, "users", "role", "TEXT DEFAULT 'User'", cols)
    _ensure_column(c, "users", "is_active", "INTEGER DEFAULT 1", cols)
    _ensure_column(c, "users", "salt", "BLOB NOT NULL DEFAULT X''", cols)
    _ensure_column(c, "users", "password_hash", "BLOB NOT NULL DEFAULT X''", cols)
    c.commit()

def _ensure_audit_schema(c):
    c.execute("""CREATE TABLE IF NOT EXISTS audit_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
        actor TEXT,
        action TEXT,
        payload TEXT
    );""")
    c.commit()

def _table_columns(c, table_name: str):
    return {r[1] for r in c.execute(f"PRAGMA table_info({table_name});").fetchall()}

def _ensure_column(c, table: str, column: str, ddl: str, existing_cols: set):
    if column not in existing_cols:
        c.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl};")

# ---- Public helpers ----
def ensure_user_table_columns():
    c = conn_user()
    c.close()

def get_user_min_by_login(login: str) -> Optional[Dict[str, Any]]:
    c = conn_user()
    try:
        row = c.execute(
            "SELECT id, email, username, role, is_active FROM users WHERE email=? OR username=?",
            (login, login)
        ).fetchone()
        if not row:
            return None
        return {"id": row[0], "email": row[1], "username": row[2], "role": row[3], "is_active": bool(row[4])}
    finally:
        c.close()

def get_user_min(email: str) -> Optional[Dict[str, Any]]:
    return get_user_min_by_login(email)

def current_paths():
    return {"app_db_path": APP_DB, "user_db_path": USER_DB, "audit_db_path": AUDIT_DB}

def fetchall_df(query: str, params: tuple = (), *, which: str = "app"):
    import pandas as pd
    c = conn_app() if which == "app" else (conn_user() if which == "user" else conn_audit())
    try:
        df = pd.read_sql_query(query, c, params=params)
        return df
    finally:
        c.close()

def execute(query: str, params: tuple = (), *, which: str = "app"):
    c = conn_app() if which == "app" else (conn_user() if which == "user" else conn_audit())
    try:
        c.execute(query, params)
        c.commit()
    finally:
        c.close()

# ---- Sync back to GCS on demand ----
def push_all_to_gcs():
    try:
        from gcs_sync import push_to_gcs
        push_to_gcs(["app.db","user.db","audit.db"], local_dir=LOCAL_DATA_DIR, make_backup=True)
        return True, "Databases pushed to GCS."
    except Exception as e:
        return False, f"GCS push failed: {e}"
