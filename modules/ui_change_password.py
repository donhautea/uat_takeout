
# ui_change_password.py — Single-page flow for email-token password change (explicit if/else).
import streamlit as st
from auth import request_password_change, verify_token_and_change_password

def render(user=None):
    st.title("Change Password")

    st.markdown("1) **Request a token** to your registered email.  \n2) **Paste the token** and set a new password.")

    # Step 1: Request token
    st.subheader("Step 1: Request Token")
    who = st.text_input("Email or Username", key="cp_who")
    if st.button("Send Token", key="cp_send", use_container_width=True):
        ok, msg = request_password_change(
            who, app_name=st.secrets.get("app_name", "Takeout MS"),
            link_base_url=st.secrets.get("app_base_url")
        )
        if ok:
            st.success(msg)
            st.session_state["cp_token_sent"] = True
        else:
            st.error(msg)

    # Step 2: Confirm & change password
    st.subheader("Step 2: Confirm & Change Password")
    st.caption("Paste the token from the email you received.")
    token = st.text_input("Token", key="cp_token")
    new_pw = st.text_input("New Password", type="password", key="cp_new_pw")
    new_pw2 = st.text_input("Confirm New Password", type="password", key="cp_new_pw2")

    if st.button("Change Password", key="cp_change", type="primary", use_container_width=True):
        if new_pw != new_pw2:
            st.error("Passwords do not match.")
        elif len(new_pw) < 8:
            st.error("Password must be at least 8 characters.")
        else:
            ok, msg = verify_token_and_change_password(token, new_pw)
            if ok:
                st.success(msg)
                for k in ("cp_token", "cp_new_pw", "cp_new_pw2"):
                    if k in st.session_state:
                        del st.session_state[k]
            else:
                st.error(msg)
