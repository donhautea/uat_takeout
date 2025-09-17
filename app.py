
# app.py — Login/Register + Forgot Password (token sender) with explicit if/else Streamlit calls.
import importlib
import streamlit as st
import sqlite3

st.set_page_config(page_title="Takeout MS", layout="wide")

def _try_import(module_name):
    try:
        return importlib.import_module(module_name)
    except ModuleNotFoundError:
        return importlib.import_module(f"modules.{module_name}")

auth = _try_import("auth")
db = _try_import("db")

def _maybe(name):
    try:
        return _try_import(name)
    except Exception:
        return None

ui_dashboard         = _maybe("ui_dashboard")
ui_sales_invoicing   = _maybe("ui_sales_invoicing")
ui_sales_reports     = _maybe("ui_sales_reports")
ui_inventory         = _maybe("ui_inventory")
ui_products_pricing  = _maybe("ui_products_pricing")
ui_expenses          = _maybe("ui_expenses")
ui_expense_reports   = _maybe("ui_expense_reports")
ui_supplies          = _maybe("ui_supplies")
ui_admin_users       = _maybe("ui_admin_users")
ui_audit             = _maybe("ui_audit")
ui_settings_import   = _maybe("ui_settings_import")
ui_change_password   = _maybe("ui_change_password")

APP_NAME = st.secrets.get("app_name", "Takeout MS")
DEBUG_UI = bool(st.secrets.get("debug", False))

if "user" not in st.session_state:
    st.session_state.user = None
if "login_error" not in st.session_state:
    st.session_state.login_error = ""

def _debug_paths_badge():
    if not DEBUG_UI:
        return
    paths = db.current_paths()
    st.sidebar.info(
        f"**DB Paths**\n\n"
        f"- app: `{paths['app_db_path']}`\n"
        f"- user: `{paths['user_db_path']}`"
    )

def _do_login(login_id, password):
    if not login_id or not password:
        st.session_state.login_error = "Enter your email/username and password."
        return
    db.ensure_user_table_columns()
    ok = auth.check_password(login_id, password)
    if not ok:
        st.session_state.login_error = "Invalid credentials or inactive account."
        return
    row = db.get_user_min_by_login(login_id)
    if not row or not row.get("is_active", False):
        st.session_state.login_error = "Account pending approval. Please wait for an admin to activate your access."
        return
    st.session_state.user = {"id": row["id"], "email": row["email"], "username": row.get("username"), "role": row["role"]}
    st.session_state.login_error = ""
    st.rerun()

def _logout():
    st.session_state.user = None
    st.rerun()

ALL_PAGES = {
    "Dashboard": ui_dashboard,
    "Sales & Invoicing": ui_sales_invoicing,
    "Sales Reports": ui_sales_reports,
    "Inventory": ui_inventory,
    "Products & Pricing": ui_products_pricing,
    "Expenses": ui_expenses,
    "Expense Reports": ui_expense_reports,
    "Supplies": ui_supplies,
    "Settings / Import": ui_settings_import,
    "Admin / Users": ui_admin_users,
    "Audit Trail": ui_audit,
    "Change Password": ui_change_password,
}

ROLE_MENUS = {
    "Owner": [
        "Dashboard","Sales & Invoicing","Sales Reports","Inventory","Products & Pricing",
        "Expenses","Expense Reports","Supplies","Settings / Import","Audit Trail","Change Password"
    ],
    "Admin": [
        "Dashboard","Sales & Invoicing","Sales Reports","Inventory","Products & Pricing",
        "Expenses","Expense Reports","Supplies","Settings / Import","Admin / Users","Audit Trail","Change Password"
    ],
    "User": [
        "Dashboard","Sales & Invoicing","Sales Reports","Settings / Import","Supplies",
        "Inventory","Products & Pricing","Expense Reports","Expenses","Change Password"
    ],
    "Sales": [
        "Sales & Invoicing","Sales Reports","Supplies","Inventory","Change Password"
    ],
}

def _render_menu():
    user = st.session_state.user
    if not user:
        return None
    role = user.get("role", "User") or "User"
    allowed = ROLE_MENUS.get(role, ROLE_MENUS["User"])
    st.sidebar.subheader("Menu")
    _debug_paths_badge()
    return st.sidebar.selectbox("Go to", allowed, index=0)


def _login_register_panel():
    c1, c2 = st.columns(2, gap="large")
    with c1:
        st.subheader("Sign in")
        login_id = st.text_input("Email or Username", key="login_id")
        pwd = st.text_input("Password", type="password", key="login_pwd")
        if st.button("Login", type="primary", use_container_width=True):
            _do_login(login_id, pwd)
        if st.session_state.login_error:
            st.error(st.session_state.login_error)

        with st.expander("Forgot password?"):
            st.caption("Step 1: Send a token to your registered email; Step 2: paste token and set a new password.")
            # --- Step 1: Send token ---
            who = st.text_input("Email or Username", key="fp_login")
            if st.button("Send token", key="fp_send", use_container_width=True):
                ok, msg = auth.request_password_change(
                    who, app_name=APP_NAME, link_base_url=st.secrets.get("app_base_url")
                )
                if ok:
                    st.success(msg)
                else:
                    st.error(msg)

            st.divider()

            # --- Step 2: Confirm & change password ---
            st.caption("Enter the token you received via email, then choose a new password.")
            fp_token = st.text_input("Token", key="fp_token")
            fp_new   = st.text_input("New Password", type="password", key="fp_new")
            fp_new2  = st.text_input("Confirm New Password", type="password", key="fp_new2")
            if st.button("Change Password", key="fp_change", type="primary", use_container_width=True):
                if fp_new != fp_new2:
                    st.error("Passwords do not match.")
                elif len(fp_new) < 8:
                    st.error("Password must be at least 8 characters.")
                else:
                    ok, msg = auth.verify_token_and_change_password(fp_token, fp_new)
                    if ok:
                        st.success(msg)
                        # optional: clear fields
                        for k in ("fp_token", "fp_new", "fp_new2"):
                            if k in st.session_state:
                                del st.session_state[k]
                    else:
                        st.error(msg)

    with c2:
        st.subheader("Register")
        st.caption("New accounts require admin approval before you can sign in.")
        r_username = st.text_input("Username (unique)", key="reg_username")
        r_email = st.text_input("Email", key="reg_email")
        r_pwd1 = st.text_input("Password", type="password", key="reg_pwd1")
        r_pwd2 = st.text_input("Confirm Password", type="password", key="reg_pwd2")
        if st.button("Register", use_container_width=True):
            if not r_email or not r_pwd1 or not r_username:
                st.error("Username, email, and password are required.")
            elif r_pwd1 != r_pwd2:
                st.error("Passwords do not match.")
            elif len(r_pwd1) < 8:
                st.error("Password must be at least 8 characters.")
            else:
                try:
                    db.ensure_user_table_columns()
                    auth.ensure_user(r_email, r_pwd1, username=r_username, role="User", is_active=False)
                    st.success("Registration received. Awaiting admin approval before you can sign in.")
                except sqlite3.IntegrityError:
                    st.warning("Email or username already registered. If you forgot your password, use 'Forgot password?'")
                except Exception as e:
                    st.error(f"Registration failed: {e}")


def _route(choice):
    user = st.session_state.user
    if not choice:
        return
    page = ALL_PAGES.get(choice)
    if page and hasattr(page, "render"):
        try:
            page.render(user=user)
        except Exception as e:
            st.error(f"Module call failed: {e}")
    else:
        st.warning("Module not available.")

def main():
    st.title(APP_NAME)
    if not st.session_state.user:
        _login_register_panel()
        return
    choice = _render_menu()
    _route(choice)

if __name__ == "__main__":
    main()
