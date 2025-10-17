# streamlit_app.py (with login)
import os
import datetime as dt
import pandas as pd
import psycopg2
import streamlit as st
import streamlit_authenticator as stauth
from dotenv import load_dotenv

# ---------- Config & secrets ----------
load_dotenv()  # local dev support

# Read DB URL from Streamlit secrets first, else .env
DATABASE_URL = st.secrets.get("DATABASE_URL", os.getenv("DATABASE_URL"))
if not DATABASE_URL:
    st.stop()

# Auth: read users & cookie settings from secrets
import copy
CREDENTIALS = copy.deepcopy(st.secrets["credentials"])
AUTH = st.secrets.get("auth", {})
COOKIE_NAME = AUTH.get("cookie_name", "mirakl_auth")
COOKIE_KEY = AUTH.get("cookie_key", "change-this-key")
COOKIE_EXPIRY = int(AUTH.get("cookie_expiry_days", 30))

st.set_page_config(page_title="Mirakl Profitability v1", layout="wide")

# ---------- Auth ----------
authenticator = stauth.Authenticate(
    credentials=CREDENTIALS,
    cookie_name=COOKIE_NAME,
    key=COOKIE_KEY,
    cookie_expiry_days=COOKIE_EXPIRY,
)

# Render login form in the main area (returns None; values are in st.session_state)
authenticator.login(location="main")

auth_status = st.session_state.get("authentication_status", None)
username = st.session_state.get("username", None)
name = st.session_state.get("name", username or "")

if auth_status is True:
    st.sidebar.write(f"Signed in as **{name}**")
    authenticator.logout("Logout", location="sidebar")
elif auth_status is False:
    st.error("Incorrect username or password.")
    st.stop()
else:
    st.info("Please enter your username and password.")
    st.stop()

# If we’re here → authenticated
st.sidebar.write(f"Signed in as **{name}**")
authenticator.logout("Logout", "sidebar")

st.title("Mirakl Profitability — v1 (GMV, Refunds, Fees, Contribution)")

# ---------- DB connection (cached, persistent) ----------
@st.cache_resource
def get_conn():
    # Keep-alives help on Neon
    return psycopg2.connect(
        DATABASE_URL,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )

# ---------- Cached queries ----------
@st.cache_data(ttl=300)
def get_marketplaces():
    conn = get_conn()
    return pd.read_sql(
        "select code as marketplace_code, name from mirakl.marketplaces order by code",
        conn,
    )

@st.cache_data(ttl=300)
def get_date_bounds():
    conn = get_conn()
    df = pd.read_sql(
        "select min(created_at) as min_dt, max(created_at) as max_dt from mirakl.orders",
        conn,
    )
    return (df.loc[0, "min_dt"], df.loc[0, "max_dt"])

@st.cache_data(ttl=300)
def kpis(start_dt, end_dt, mkt_codes=None, sku_filter=None):
    sql = """
    with lines as (
      select
        o.marketplace_code,
        o.created_at::date as day,
        ol.sku,
        ol.qty::numeric as qty,
        coalesce(ol.price_tax_excl,0)::numeric as price_ex,
        coalesce(ol.tax_amount,0)::numeric  as tax,
        coalesce(ol.shipping_price,0)::numeric as ship_price,
        coalesce(ol.fees_total,0)::numeric as fees_total,
        ol.order_id, ol.line_id
      from mirakl.orders o
      join mirakl.order_lines ol
        on ol.order_id = o.order_id and ol.marketplace_code = o.marketplace_code
      where o.created_at >= %(start)s and o.created_at < %(end)s
    ),
    refunds as (
      select order_id, line_id, marketplace_code,
             sum(coalesce(amount_tax_excl,0) + coalesce(tax_amount,0))::numeric as refund_amount
      from mirakl.refunds
      where created_at >= %(start)s and created_at < %(end)s
      group by order_id, line_id, marketplace_code
    ),
    joined as (
      select
        l.marketplace_code, l.day, l.sku,
        (l.qty * (l.price_ex + l.tax))::numeric as line_gmv,
        coalesce(r.refund_amount,0)::numeric as refunds,
        l.fees_total::numeric as fees
      from lines l
      left join refunds r
        on r.order_id = l.order_id and r.line_id = l.line_id and r.marketplace_code = l.marketplace_code
      where (%(sku)s is null OR l.sku = %(sku)s)
        and (%(mkt)s is null OR l.marketplace_code = any(%(mkt)s))
    )
    select
      marketplace_code,
      day,
      sum(line_gmv) as gmv,
      sum(refunds)  as refunds,
      sum(fees)     as fees,
      sum(line_gmv) - sum(refunds) - sum(fees) as contribution
    from joined
    group by marketplace_code, day
    order by day, marketplace_code;
    """
    params = {
        "start": start_dt,
        "end": end_dt + dt.timedelta(days=1),
        "mkt": mkt_codes if mkt_codes else None,
        "sku": sku_filter if sku_filter else None,
    }
    conn = get_conn()
    return pd.read_sql(sql, conn, params=params)

@st.cache_data(ttl=300)
def top_skus(start_dt, end_dt, mkt_codes=None, sku_filter=None):
    sql = """
    with lines as (
      select
        o.marketplace_code,
        ol.sku,
        ol.qty::numeric as qty,
        coalesce(ol.price_tax_excl,0)::numeric as price_ex,
        coalesce(ol.tax_amount,0)::numeric as tax,
        coalesce(ol.fees_total,0)::numeric as fees,
        ol.order_id, ol.line_id
      from mirakl.orders o
      join mirakl.order_lines ol
        on ol.order_id = o.order_id and ol.marketplace_code = o.marketplace_code
      where o.created_at >= %(start)s and o.created_at < %(end)s
    ),
    refunds as (
      select order_id, line_id, marketplace_code,
             sum(coalesce(amount_tax_excl,0) + coalesce(tax_amount,0))::numeric as refund_amount
      from mirakl.refunds
      where created_at >= %(start)s and created_at < %(end)s
      group by order_id, line_id, marketplace_code
    ),
    joined as (
      select
        l.marketplace_code, l.sku,
        (l.qty * (l.price_ex + l.tax))::numeric as line_gmv,
        coalesce(r.refund_amount,0)::numeric as refunds,
        l.fees::numeric as fees
      from lines l
      left join refunds r
        on r.order_id = l.order_id and r.line_id = l.line_id and r.marketplace_code = l.marketplace_code
      where (%(sku)s is null OR l.sku = %(sku)s)
        and (%(mkt)s is null OR l.marketplace_code = any(%(mkt)s))
    )
    select marketplace_code, sku,
           sum(line_gmv) as gmv,
           sum(refunds)  as refunds,
           sum(fees)     as fees,
           sum(line_gmv) - sum(refunds) - sum(fees) as contribution
    from joined
    group by marketplace_code, sku
    order by contribution desc
    limit 25;
    """
    params = {
        "start": start_dt,
        "end": end_dt + dt.timedelta(days=1),
        "mkt": mkt_codes if mkt_codes else None,
        "sku": sku_filter if sku_filter else None,
    }
    conn = get_conn()
    return pd.read_sql(sql, conn, params=params)

# ---------- UI ----------
mkt_df = get_marketplaces()
left, right = st.columns([2, 3])

with left:
    st.subheader("Filters")

    mkt_codes = mkt_df["marketplace_code"].tolist()
    selected = st.multiselect("Marketplace", mkt_codes, default=mkt_codes)

    min_dt, max_dt = get_date_bounds()
    if pd.isna(min_dt) or pd.isna(max_dt):
        st.info("No orders found yet. Ingest data first.")
        st.stop()

    default_start = max((max_dt.date() - dt.timedelta(days=30)), min_dt.date())
    start_date = st.date_input("Start date", value=default_start, min_value=min_dt.date(), max_value=max_dt.date())
    end_date   = st.date_input("End date", value=max_dt.date(), min_value=min_dt.date(), max_value=max_dt.date())
    if start_date > end_date:
        st.error("Start date must be before end date.")
        st.stop()

    sku_filter = st.text_input("SKU (optional, exact match)").strip() or None

with right:
    st.subheader("Daily totals")
    df = kpis(start_date, end_date, selected, sku_filter)
    if df.empty:
        st.warning("No data for the selected filters.")
    else:
        summary = df.groupby("marketplace_code", as_index=False)[["gmv","refunds","fees","contribution"]].sum()
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("GMV (sum)", f"£{summary['gmv'].sum():,.2f}")
        c2.metric("Refunds (sum)", f"£{summary['refunds'].sum():,.2f}")
        c3.metric("Fees (sum)", f"£{summary['fees'].sum():,.2f}")
        c4.metric("Contribution (sum)", f"£{summary['contribution'].sum():,.2f}")

        # Trend of contribution by day/marketplace
        pivot_contrib = df.pivot_table(index="day", columns="marketplace_code", values="contribution", aggfunc="sum").fillna(0)
        st.line_chart(pivot_contrib)

st.subheader("Top SKUs by Contribution (limit 25)")
sku_df = top_skus(start_date, end_date, selected, sku_filter)
st.dataframe(sku_df, width="stretch")
