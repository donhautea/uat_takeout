
"""
auth.py — Auth helpers with email-verified password change.

Highlights
- Uses the correct user DB path (secrets→env→local).
- Accepts login by email OR username.
- Robust handling of legacy rows (salt/password_hash as TEXT).
- Auto-upgrades plaintext passwords to hashed on successful login.
- Token-based password change with email delivery.
"""

import os
import hmac
import smtplib
import sqlite3
import hashlib
import secrets
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.utils import formatdate

try:
    import streamlit as st
except Exception:
    class _Dummy:
        def __getattr__(self, k):
            raise RuntimeError("Streamlit not available; UI helpers require Streamlit.")
    st = _Dummy()

# ------------------------
# Paths
# ------------------------
def _user_db_path():
    try:
        p = st.secrets.get("paths", {}).get("user_db_path")
        if p:
            return p
    except Exception:
        pass
    return os.environ.get("TAKEOUT_USER_DB_PATH", "user.db")

USER_DB_PATH = _user_db_path()

# ------------------------
# DB & migrations
# ------------------------
def _conn():
    conn = sqlite3.connect(USER_DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("""CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT UNIQUE NOT NULL,
        username TEXT UNIQUE,
        password_hash BLOB NOT NULL,
        salt BLOB NOT NULL,
        role TEXT DEFAULT 'User',
        is_active INTEGER DEFAULT 1,
        created_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now'))
    );""")
    conn.execute("""CREATE TABLE IF NOT EXISTS password_tokens (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        token TEXT NOT NULL,
        purpose TEXT NOT NULL,
        expires_at TEXT NOT NULL,
        used_at TEXT,
        created_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
        FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
    );""")
    # Migrations
    cols = {r[1] for r in conn.execute("PRAGMA table_info(users)").fetchall()}
    if "username" not in cols:
        conn.execute("ALTER TABLE users ADD COLUMN username TEXT UNIQUE;")
    if "role" not in cols:
        conn.execute("ALTER TABLE users ADD COLUMN role TEXT DEFAULT 'User';")
    if "is_active" not in cols:
        conn.execute("ALTER TABLE users ADD COLUMN is_active INTEGER DEFAULT 1;")
    if "salt" not in cols:
        conn.execute("ALTER TABLE users ADD COLUMN salt BLOB NOT NULL DEFAULT X'';")
    if "password_hash" not in cols:
        conn.execute("ALTER TABLE users ADD COLUMN password_hash BLOB NOT NULL DEFAULT X'';")
    conn.commit()
    return conn

# ------------------------
# Bytes helpers
# ------------------------
def _to_bytes(v):
    if v is None:
        return b""
    if isinstance(v, (bytes, bytearray)):
        return bytes(v)
    if isinstance(v, memoryview):
        return v.tobytes()
    if isinstance(v, str):
        s = v.strip()
        # Try hex for legacy storage
        try:
            if len(s) % 2 == 0 and all(c in "0123456789abcdefABCDEF" for c in s):
                return bytes.fromhex(s)
        except Exception:
            pass
        return s.encode("utf-8", "ignore")
    try:
        return bytes(v)
    except Exception:
        return b""

# ------------------------
# Password hashing
# ------------------------
def _hash_password(password: str, salt: bytes) -> bytes:
    salt_b = _to_bytes(salt)
    return hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt_b, 200_000)

def set_password(user_id: int, new_password: str) -> None:
    if not new_password or len(new_password) < 8:
        raise ValueError("Password must be at least 8 characters.")
    conn = _conn()
    try:
        salt = secrets.token_bytes(16)
        pwh = _hash_password(new_password, salt)
        conn.execute("UPDATE users SET password_hash=?, salt=? WHERE id=?", (pwh, salt, user_id))
        conn.commit()
    finally:
        conn.close()

def check_password(login: str, password: str) -> bool:
    """
    Accepts login by email OR username.
    Auto-upgrade legacy plaintext passwords (salt empty, stored==password).
    """
    conn = _conn()
    try:
        row = conn.execute(
            "SELECT id, password_hash, salt, is_active FROM users WHERE (email=? OR username=?)",
            (login, login)
        ).fetchone()
        if not row:
            return False
        user_id, password_hash, salt, is_active = row
        if not is_active:
            return False

        stored = _to_bytes(password_hash)
        salt_b = _to_bytes(salt)

        # Legacy plaintext
        if (salt_b in (b"", b"\x00"*16)) and stored and stored == password.encode("utf-8"):
            new_salt = secrets.token_bytes(16)
            new_hash = _hash_password(password, new_salt)
            conn.execute("UPDATE users SET password_hash=?, salt=? WHERE id=?", (new_hash, new_salt, user_id))
            conn.commit()
            return True

        if not salt_b or not stored:
            return False

        computed = _hash_password(password, salt_b)
        return hmac.compare_digest(stored, computed)
    finally:
        conn.close()

# ------------------------
# User utilities
# ------------------------
def _get_user_by_login(login: str):
    conn = _conn()
    try:
        row = conn.execute(
            "SELECT id, email, username, is_active FROM users WHERE (email=? OR username=?)",
            (login, login)
        ).fetchone()
        if not row:
            return None
        return {"id": row[0], "email": row[1], "username": row[2], "is_active": bool(row[3])}
    finally:
        conn.close()

def ensure_user(email: str, password: str = None, *, username: str = None, role: str = "User", is_active: bool = True):
    conn = _conn()
    try:
        if username:
            row = conn.execute("SELECT id FROM users WHERE email=? OR username=?", (email, username)).fetchone()
        else:
            row = conn.execute("SELECT id FROM users WHERE email=?", (email,)).fetchone()
        if row:
            return row[0]
        if password is None:
            password = secrets.token_urlsafe(10)
        salt = secrets.token_bytes(16)
        pwh = _hash_password(password, salt)
        conn.execute(
            "INSERT INTO users(email, username, password_hash, salt, role, is_active) VALUES(?,?,?,?,?,?)",
            (email, username, pwh, salt, role, 1 if is_active else 0),
        )
        conn.commit()
        return conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    finally:
        conn.close()

# ------------------------
# SMTP / Email
# ------------------------
def _smtp_client_from_secrets():
    cfg = st.secrets.get("smtp", None)
    if not cfg:
        raise RuntimeError("Missing [smtp] in st.secrets. Please configure SMTP settings.")
    server = cfg.get("server")
    port = int(cfg.get("port", 587))
    username = cfg.get("username")
    password = cfg.get("password")
    sender = cfg.get("sender") or username
    if not server or not port or not username or not password or not sender:
        raise RuntimeError("Incomplete SMTP config: server, port, username, password, sender required.")
    return server, port, username, password, sender

def _send_email(to_email: str, subject: str, body: str):
    server, port, username, password, sender = _smtp_client_from_secrets()
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = to_email
    msg["Date"] = formatdate(localtime=True)

    with smtplib.SMTP(server, port) as smtp:
        smtp.starttls()
        smtp.login(username, password)
        smtp.sendmail(sender, [to_email], msg.as_string())

# ------------------------
# Token workflow
# ------------------------
def _create_token(user_id: int, purpose: str = "password_change", minutes_valid: int = 15) -> str:
    token = secrets.token_urlsafe(24)
    expires_at = (datetime.utcnow() + timedelta(minutes=minutes_valid)).strftime("%Y-%m-%d %H:%M:%S")
    conn = _conn()
    try:
        conn.execute(
            "INSERT INTO password_tokens(user_id, token, purpose, expires_at) VALUES(?,?,?,?)",
            (user_id, token, purpose, expires_at),
        )
        conn.commit()
    finally:
        conn.close()
    return token

def request_password_change(login: str, *, app_name: str = "Takeout MS", link_base_url: str = None):
    user = _get_user_by_login(login)
    if not user or not user["is_active"]:
        return True, "If the account is registered and active, a confirmation token has been sent."
    token = _create_token(user["id"], "password_change", minutes_valid=15)
    subject = f"[{app_name}] Confirm password change"
    link = f"{link_base_url.rstrip('/')}/change-password?token={token}" if link_base_url else None
    body_lines = [
        "Hello,",
        "",
        f"We received a request to change the password for your {app_name} account ({user['email']}).",
        "",
        "Your confirmation token is:",
        "",
        f"    {token}",
        "",
        "This token will expire in 15 minutes."
    ]
    if link:
        body_lines += ["", "Alternatively, click the link below to proceed:", link]
    body_lines += ["", "If you did not request this change, you can safely ignore this email."]
    try:
        _send_email(user["email"], subject, "\n".join(body_lines))
    except Exception as e:
        return False, f"Failed to send token email: {e}"
    return True, "If the account is registered and active, a confirmation token has been sent."

def verify_token_and_change_password(token: str, new_password: str):
    if not token or not new_password:
        return False, "Token and new password are required."
    conn = _conn()
    try:
        row = conn.execute(
            "SELECT id, user_id, purpose, expires_at, used_at FROM password_tokens WHERE token=?",
            (token,)
        ).fetchone()
        if not row:
            return False, "Invalid or expired token."
        token_id, user_id, purpose, expires_at, used_at = row
        if purpose != "password_change":
            return False, "Invalid token purpose."
        if used_at is not None:
            return False, "This token has already been used."
        try:
            exp_dt = datetime.strptime(expires_at, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return False, "Token expiry is malformed."
        if datetime.utcnow() > exp_dt:
            return False, "This token has expired."
        if len(new_password) < 8:
            return False, "Password must be at least 8 characters."
        salt = secrets.token_bytes(16)
        pwh = hashlib.pbkdf2_hmac("sha256", new_password.encode("utf-8"), salt, 200_000)
        conn.execute("UPDATE users SET password_hash=?, salt=? WHERE id=?", (pwh, salt, user_id))
        conn.execute("UPDATE password_tokens SET used_at=strftime('%Y-%m-%d %H:%M:%S','now') WHERE id=?", (token_id,))
        conn.commit()
        return True, "Password changed successfully."
    finally:
        conn.close()

# ------------------------
# Optional Streamlit UI (tabs but safe — explicit if/else)
# ------------------------
def render_change_password_ui(app_name: str = "Takeout MS", link_base_url: str = None):
    st.subheader("Change Password (Email Verification)")
    tabs = st.tabs(["Request Token", "Confirm & Change Password"])
    with tabs[0]:
        st.write("Enter your registered email or username. We'll send a confirmation token to your email.")
        who = st.text_input("Email or Username", key="pw_email")
        if st.button("Send Token", use_container_width=True):
            ok, msg = request_password_change(who, app_name=app_name, link_base_url=link_base_url)
            if ok:
                st.success(msg)
            else:
                st.error(msg)
    with tabs[1]:
        st.write("Paste the token from your email, then set a new password.")
        token = st.text_input("Token", key="pw_token")
        new_pw = st.text_input("New Password", type="password", key="pw_new")
        new_pw2 = st.text_input("Confirm New Password", type="password", key="pw_new2")
        if st.button("Change Password", type="primary", use_container_width=True):
            if new_pw != new_pw2:
                st.error("Passwords do not match.")
            else:
                ok, msg = verify_token_and_change_password(token, new_pw)
                if ok:
                    st.success(msg)
                else:
                    st.error(msg)
