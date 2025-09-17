
import os, sqlite3
import pandas as pd
import streamlit as st

APP_DB_PATH = os.environ.get("TAKEOUT_DB_PATH", "app.db")

def _conn():
    c = sqlite3.connect(APP_DB_PATH, check_same_thread=False)
    c.execute("PRAGMA foreign_keys=ON;")
    c.execute("""CREATE TABLE IF NOT EXISTS audit (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL DEFAULT (datetime('now')),
        user TEXT,
        action TEXT,
        detail TEXT
    );""")
    return c

def render(user=None):
    st.title("Audit Trail")
    conn = _conn()

    # Filters
    st.subheader("Filters")
    c1, c2, c3 = st.columns(3)
    with c1:
        user_f = st.text_input("User contains", key="au_user")
    with c2:
        action_f = st.text_input("Action contains", key="au_action")
    with c3:
        date_range = st.date_input("Date range (optional)", value=None, key="au_range")

    # Build query safely
    base = "SELECT id, ts, COALESCE(user,'') AS user, COALESCE(action,'') AS action, COALESCE(detail,'') AS detail FROM audit"
    where = []
    params = []
    if user_f:
        where.append("user LIKE ?")
        params.append(f"%{user_f}%")
    if action_f:
        where.append("action LIKE ?")
        params.append(f"%{action_f}%")
    if isinstance(date_range, tuple) and len(date_range) == 2:
        where.append("date(ts) BETWEEN date(?) AND date(?)")
        params.extend([str(date_range[0]), str(date_range[1])])
    if where:
        base += " WHERE " + " AND ".join(where)
    base += " ORDER BY ts DESC, id DESC LIMIT 2000;"

    try:
        df = pd.read_sql_query(base, conn, params=params)
    except Exception as e:
        st.error(f"Failed to read audit: {e}")
        return

    st.subheader("Recent Activity")
    st.dataframe(df, use_container_width=True, height=420)

    # Export
    if not df.empty:
        csv = df.to_csv(index=False).encode('utf-8-sig')
        st.download_button("Download CSV", csv, file_name="audit.csv", mime="text/csv")

    # Manual test logger (optional)
    with st.expander("Quick Test Log"):
        test_action = st.text_input("Action", key="au_test_action", value="TEST")
        test_detail = st.text_input("Detail", key="au_test_detail", value="Manual log from Audit page")
        if st.button("Insert Test Log", key="au_test_btn"):
            with conn:
                conn.execute("INSERT INTO audit(user, action, detail) VALUES(?,?,?)",
                             (user.get("username") if isinstance(user, dict) else None, test_action, test_detail))
            st.success("Inserted.")
            st.experimental_rerun()

app = render
main = render
