# modules/utils.py
import streamlit as st

# Roles: Admin, Owner, User, Sales
ROLE_PERMS = {
    "Admin": {"Dashboard","Sales & Invoicing","Sales Reports","Settings / Import","Supplies","Inventory","Products & Pricing","Expense Reports","Expenses","Admin / Users","Audit Trail"},
    "Owner": {"Dashboard","Sales & Invoicing","Sales Reports","Settings / Import","Supplies","Inventory","Products & Pricing","Expense Reports","Expenses","Audit Trail"}, # no Admin/Users
    "User": {"Dashboard","Sales & Invoicing","Sales Reports","Settings / Import","Supplies","Inventory","Products & Pricing","Expense Reports","Expenses"},
    "Sales": {"Sales & Invoicing","Sales Reports","Supplies","Inventory"}
}

def can_access(role: str, menu_label: str) -> bool:
    allowed = ROLE_PERMS.get(role, set())
    return (menu_label in allowed)

def require_login():
    if "user" not in st.session_state or st.session_state.get("user") is None:
        st.stop()

def menu_options_for(role: str):
    return sorted(list(ROLE_PERMS.get(role, set())))

def spaced_title(txt: str):
    st.markdown(f"### {txt}")
