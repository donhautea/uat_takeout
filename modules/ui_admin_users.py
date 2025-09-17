
import streamlit as st
import os, sqlite3

USER_DB_PATH = os.environ.get("TAKEOUT_USER_DB_PATH", "user.db")

def _conn():
    c = sqlite3.connect(USER_DB_PATH, check_same_thread=False)
    c.execute("PRAGMA foreign_keys=ON;")
    c.execute("""CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        email TEXT UNIQUE,
        password_hash TEXT NOT NULL,
        salt TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'User',
        status TEXT NOT NULL DEFAULT 'active',
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );""")
    return c

def _list_users(conn):
    cur = conn.execute("SELECT id, username, email, role, status, created_at FROM users ORDER BY id;")
    rows = cur.fetchall()
    return rows

def render(user=None):
    st.title("Admin / Users")
    conn = _conn()
    rows = _list_users(conn)
    if not rows:
        st.info("No users yet.")
        return None  # be explicit, avoid returning a Streamlit element

    # Use a page-specific prefix to guarantee widget-key uniqueness across the app
    KEY_PREFIX = "admin_users"

    for uid, uname, email, role, status, created_at in rows:
        with st.expander(f"{uname} • {email or ''}", expanded=False):
            col1, col2, col3 = st.columns(3)

            with col1:
                st.write(f"Role: **{role}**")
                roles = ["Owner","Admin","Sales","User"]
                try:
                    idx = roles.index(role) if role in roles else roles.index("User")
                except ValueError:
                    idx = 3
                new_role = st.selectbox(
                    "Change Role",
                    roles,
                    index=idx,
                    key=f"{KEY_PREFIX}_role_select_{uid}"
                )
                if st.button("Update Role", key=f"{KEY_PREFIX}_role_update_{uid}"):
                    with conn:
                        conn.execute("UPDATE users SET role=? WHERE id=?", (new_role, uid))
                    st.success("Role updated.")
                    st.rerun()

            with col2:
                st.write(f"Status: **{status}**")
                act_col, deact_col = st.columns(2)
                with act_col:
                    if st.button("Activate", key=f"{KEY_PREFIX}_activate_{uid}"):
                        with conn:
                            conn.execute("UPDATE users SET status='active' WHERE id=?", (uid,))
                        st.success("Activated.")
                        st.rerun()
                with deact_col:
                    if st.button("Deactivate", key=f"{KEY_PREFIX}_deactivate_{uid}"):
                        with conn:
                            conn.execute("UPDATE users SET status='disabled' WHERE id=?", (uid,))
                        st.success("Deactivated.")
                        st.rerun()

            with col3:
                if st.button("Delete User", key=f"{KEY_PREFIX}_delete_{uid}"):
                    with conn:
                        conn.execute("DELETE FROM users WHERE id=?", (uid,))
                    st.warning("User deleted.")
                    st.rerun()

app = render
main = render
