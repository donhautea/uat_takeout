
# ui_admin_storage.py — Admin-only page to push DBs to GCS (Streamlit Cloud)
import streamlit as st
import db

def render(user=None):
    if not user or user.get("role") not in ("Admin", "Owner"):
        st.error("Admins only.")
        return

    st.title("Storage / Backups")
    st.caption("Pulls happen automatically on startup. Use this to push latest DBs to Google Cloud Storage.")

    paths = db.current_paths()
    st.info(f"**Local paths**\n\n- app: `{paths['app_db_path']}`\n- user: `{paths['user_db_path']}`\n- audit: `{paths['audit_db_path']}`")

    if st.button("Push all DBs to GCS", type="primary", use_container_width=True):
        ok, msg = db.push_all_to_gcs()
        if ok:
            st.success(msg)
        else:
            st.error(msg)
