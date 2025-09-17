
import os, sqlite3
import pandas as pd
import streamlit as st

APP_DB_PATH = os.environ.get("TAKEOUT_DB_PATH", "app.db")

# ---------------- DB ----------------
def _conn():
    c = sqlite3.connect(APP_DB_PATH, check_same_thread=False)
    c.execute("PRAGMA foreign_keys=ON;")
    c.execute("""CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sku TEXT UNIQUE,
        name TEXT,
        unit TEXT,
        cost REAL DEFAULT 0,
        price REAL DEFAULT 0,
        active INTEGER DEFAULT 1
    );""")
    return c

def _load_products(conn) -> pd.DataFrame:
    try:
        return pd.read_sql_query("SELECT id, sku, name, unit, cost, price, active FROM products ORDER BY name;", conn)
    except Exception:
        return pd.DataFrame(columns=["id","sku","name","unit","cost","price","active"])

def _fmt_money(v):
    try:
        return f"{float(v):,.2f}"
    except Exception:
        return "0.00"

# ---------------- UI ----------------
def render(user=None):
    st.title("Products & Pricing")
    conn = _conn()
    KEY = "pp"  # page key prefix for Streamlit widget keys

    # ---------- Add / Retrieve / Update / Delete (single item) ----------
    st.subheader("Add / Retrieve / Update / Delete Product")

    df_all = _load_products(conn)
    mode = st.radio("Mode", ["Add New", "Edit Existing"], horizontal=True, key=f"{KEY}_mode")

    # Select existing by SKU or ID
    selected_id = None
    selected_sku = None
    if mode == "Edit Existing":
        sel_type = st.radio("Select by", ["SKU", "ID"], index=0, horizontal=True, key=f"{KEY}_sel_type")
        if sel_type == "SKU":
            sku_opts = ["— select —"] + (df_all["sku"].dropna().astype(str).tolist() if not df_all.empty else [])
            selected_sku = st.selectbox("Choose SKU", options=sku_opts, key=f"{KEY}_sel_sku")
        else:
            id_opts = ["— select —"] + ([str(i) for i in df_all["id"].astype(int).tolist()] if not df_all.empty else [])
            selected_id = st.selectbox("Choose ID", options=id_opts, key=f"{KEY}_sel_id")

    # Prefill values
    init = {"sku":"", "name":"", "unit":"", "cost":0.0, "price":0.0, "active":True}
    if mode == "Edit Existing" and not df_all.empty:
        row = None
        if selected_sku and selected_sku != "— select —":
            row = df_all.loc[df_all["sku"] == selected_sku].head(1)
        if selected_id and selected_id != "— select —":
            try:
                sid = int(selected_id)
                row = df_all.loc[df_all["id"] == sid].head(1)
            except Exception:
                row = None
        if row is not None and not row.empty:
            r = row.iloc[0]
            init = {
                "sku": str(r["sku"] or ""),
                "name": str(r["name"] or ""),
                "unit": str(r["unit"] or ""),
                "cost": float(r["cost"] or 0.0),
                "price": float(r["price"] or 0.0),
                "active": bool(int(r["active"] or 0))
            }

    # Form fields
    c1, c2, c3 = st.columns(3)
    with c1:
        sku = st.text_input("SKU", value=init["sku"], key=f"{KEY}_sku")
        name = st.text_input("Name", value=init["name"], key=f"{KEY}_name")
    with c2:
        unit = st.text_input("Unit", value=init["unit"], key=f"{KEY}_unit")
        cost = st.number_input("Cost", min_value=0.0, step=0.01, value=float(init["cost"]), key=f"{KEY}_cost")
    with c3:
        price = st.number_input("Price", min_value=0.0, step=0.01, value=float(init["price"]), key=f"{KEY}_price")
        active = st.checkbox("Active", value=bool(init["active"]), key=f"{KEY}_active")

    # Action buttons
    colA, colB, colC = st.columns(3)
    with colA:
        save_label = "Update Product" if mode == "Edit Existing" else "Save Product"
        if st.button(save_label, type="primary", key=f"{KEY}_save"):
            if not sku:
                st.error("SKU is required.")
            else:
                with conn:
                    if mode == "Edit Existing":
                        # Prefer updating by selected ID if chosen
                        if selected_id and selected_id != "— select —":
                            conn.execute(
                                "UPDATE products SET sku=?, name=?, unit=?, cost=?, price=?, active=? WHERE id=?;",
                                (sku, name, unit, float(cost), float(price), 1 if active else 0, int(selected_id))
                            )
                            st.success("Product updated by ID.")
                        else:
                            # Upsert by SKU
                            cur = conn.execute("SELECT id FROM products WHERE sku=?;", (sku,))
                            row = cur.fetchone()
                            if row:
                                conn.execute("UPDATE products SET name=?, unit=?, cost=?, price=?, active=? WHERE sku=?;",
                                             (name, unit, float(cost), float(price), 1 if active else 0, sku))
                                st.success("Product updated.")
                            else:
                                conn.execute("INSERT INTO products(sku,name,unit,cost,price,active) VALUES(?,?,?,?,?,?);",
                                             (sku, name, unit, float(cost), float(price), 1 if active else 0))
                                st.success("Product added.")
                    else:
                        # Add New (upsert by SKU)
                        cur = conn.execute("SELECT id FROM products WHERE sku=?;", (sku,))
                        row = cur.fetchone()
                        if row:
                            conn.execute("UPDATE products SET name=?, unit=?, cost=?, price=?, active=? WHERE sku=?;",
                                         (name, unit, float(cost), float(price), 1 if active else 0, sku))
                            st.success("Product updated.")
                        else:
                            conn.execute("INSERT INTO products(sku,name,unit,cost,price,active) VALUES(?,?,?,?,?,?);",
                                         (sku, name, unit, float(cost), float(price), 1 if active else 0))
                            st.success("Product added.")
                st.rerun()

    with colB:
        # Delete requires edit mode and a selected product
        can_delete = (mode == "Edit Existing") and (
            (selected_id and selected_id != "— select —") or (selected_sku and selected_sku != "— select —")
        )
        st.checkbox("Confirm delete", key=f"{KEY}_confirm_del", value=False)
        if st.button("Delete Product", type="secondary", disabled=not can_delete, key=f"{KEY}_delete"):
            if st.session_state.get(f"{KEY}_confirm_del"):
                with conn:
                    if selected_id and selected_id != "— select —":
                        conn.execute("DELETE FROM products WHERE id=?;", (int(selected_id),))
                    elif selected_sku and selected_sku != "— select —":
                        conn.execute("DELETE FROM products WHERE sku=?;", (selected_sku,))
                st.warning("Product deleted.")
                st.rerun()
            else:
                st.error("Please tick 'Confirm delete' before deleting.")

    # ---------- Product List (view + optional bulk update) ----------
    st.subheader("Product List")

    # Simple search
    search = st.text_input("Search by SKU/Name/Unit", key=f"{KEY}_search")
    base_sql = "SELECT id, sku, name, unit, cost, price, active FROM products"
    params = []
    if search:
        base_sql += " WHERE sku LIKE ? OR name LIKE ? OR unit LIKE ?"
        s = f"%{search}%"
        params = [s, s, s]
    base_sql += " ORDER BY name;"

    df = pd.read_sql_query(base_sql, conn, params=params if params else None)

    if df.empty:
        st.info("No products found.")
        return

    # Pretty read-only table
    view = df.copy()
    view["cost"] = pd.to_numeric(view["cost"], errors="coerce").fillna(0.0).map(_fmt_money)
    view["price"] = pd.to_numeric(view["price"], errors="coerce").fillna(0.0).map(_fmt_money)
    view["active"] = view["active"].map({1:"Yes", 0:"No"})
    st.dataframe(view, use_container_width=True, height=300)

    # Optional bulk editor
    st.caption("Bulk edit (optional): update values below and click **Apply Changes**.")
    editable = df.copy()
    edited = st.data_editor(
        editable,
        num_rows="dynamic",
        use_container_width=True,
        key=f"{KEY}_editor",
        column_config={
            "id": st.column_config.NumberColumn("ID", disabled=True),
            "sku": st.column_config.TextColumn("SKU", required=True),
            "name": st.column_config.TextColumn("Name"),
            "unit": st.column_config.TextColumn("Unit"),
            "cost": st.column_config.NumberColumn("Cost", min_value=0.0, step=0.01),
            "price": st.column_config.NumberColumn("Price", min_value=0.0, step=0.01),
            "active": st.column_config.CheckboxColumn("Active"),
        }
    )

    if st.button("Apply Changes", key=f"{KEY}_apply"):
        try:
            ser = edited["sku"].astype(str).str.strip()
            if ser.duplicated().any():
                dups = ser[ser.duplicated()].unique().tolist()
                st.error(f"Duplicate SKU(s) found in edits: {', '.join(dups)}")
            else:
                with conn:
                    for _, r in edited.iterrows():
                        id_v = int(r["id"]) if pd.notna(r["id"]) else None
                        sku_v = str(r["sku"]).strip()
                        name_v = str(r.get("name") or "").strip()
                        unit_v = str(r.get("unit") or "").strip()
                        cost_v = float(r.get("cost") or 0.0)
                        price_v = float(r.get("price") or 0.0)
                        active_v = 1 if bool(r.get("active")) else 0

                        if id_v:
                            conn.execute("UPDATE products SET sku=?, name=?, unit=?, cost=?, price=?, active=? WHERE id=?;",
                                         (sku_v, name_v, unit_v, cost_v, price_v, active_v, id_v))
                        else:
                            conn.execute("INSERT INTO products(sku,name,unit,cost,price,active) VALUES(?,?,?,?,?,?);",
                                         (sku_v, name_v, unit_v, cost_v, price_v, active_v))
                st.success("Changes applied.")
                st.rerun()
        except Exception as e:
            st.error(f"Failed to apply changes: {e}")

app = render
main = render
