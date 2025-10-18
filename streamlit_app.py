# streamlit_app.py (with login)
import os
import datetime as dt
import pandas as pd
import psycopg2
import streamlit as st
import streamlit_authenticator as stauth
from dotenv import load_dotenv
from sqlalchemy import create_engine

# ---------- Config & secrets ----------
load_dotenv()  # local dev support

# Read DB URL from Streamlit secrets first, else .env
DATABASE_URL = st.secrets.get("DATABASE_URL", os.getenv("DATABASE_URL"))
if not DATABASE_URL:
    st.stop()

# Auth: read users & cookie settings from secrets
_creds = st.secrets.get("credentials", {})
_users = _creds.get("usernames", {})

# Make a normal Python dict copy so authenticator can modify it
CREDENTIALS = {"usernames": {}}
for uname, u in _users.items():
    CREDENTIALS["usernames"][str(uname)] = {
        "name": str(u.get("name", "")),
        "email": str(u.get("email", "")),
        "password": str(u.get("password", "")),
    }

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
    pass
elif auth_status is False:
    st.error("Incorrect username or password.")
    st.stop()
else:
    st.info("Please enter your username and password.")
    st.stop()

# If we’re here → authenticated. Always render sidebar.
with st.sidebar:
    st.markdown(f"Signed in as **{name}**")
    authenticator.logout("Logout", location="sidebar", key="logout-btn")

st.title("Mirakl Profitability — v1 (GMV, Refunds, Fees, Contribution)")

# ---------- DB engine (cached, resilient) ----------
@st.cache_resource
def get_engine():
    # Clean up the URL in case secrets had extra quotes/newlines
    url = DATABASE_URL.strip().strip("'").strip('"').replace("\n", "")
    return create_engine(
        url,
        pool_pre_ping=True,       # auto-checks connection health
        pool_recycle=900,         # recycle every 15 min (Neon safe)
        connect_args={
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 10,
            "keepalives_count": 5,
        },
    )

# ---------- Cached queries ----------
@st.cache_data(ttl=300)
def get_marketplaces():
    engine = get_engine()
    return pd.read_sql(
        "select code as marketplace_code, name from mirakl.marketplaces order by code",
        con=engine,
    )

@st.cache_data(ttl=300)
def get_date_bounds():
    engine = get_engine()
    df = pd.read_sql(
        "select min(created_at) as min_dt, max(created_at) as max_dt from mirakl.orders",
        con=engine,
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
        (l.qty * coalesce(l.price_incl, l.price_ex + l.tax))::numeric as line_gmv,
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
    engine = get_engine()
    return pd.read_sql(sql, con=engine, params=params)

@st.cache_data(ttl=300)
def top_skus(start_dt, end_dt, mkt_codes=None, sku_filter=None):
    sql = """
    with lines as (
      select
        o.marketplace_code,
        ol.sku,
        ol.qty::numeric as qty,
        coalesce(ol.price_tax_excl,0)::numeric as price_ex,
	coalesce(ol.price_tax_incl, null)::numeric as price_incl,
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
        l.qty::numeric as qty,
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
           sum(qty) as units,
           sum(line_gmv) as gmv,
           sum(refunds)  as refunds,
           sum(fees)     as fees,
           sum(line_gmv) - sum(refunds) - sum(fees) as contribution
    from joined
    group by marketplace_code, sku
    order by contribution desc;
    """
    params = {
        "start": start_dt,
        "end": end_dt + dt.timedelta(days=1),
        "mkt": mkt_codes if mkt_codes else None,
        "sku": sku_filter if sku_filter else None,
    }
    engine = get_engine()
    return pd.read_sql(sql, con=engine, params=params)

def order_lines_table(start_date, end_date, sku=None, marketplaces=None, page=1, page_size=100):
    """
    Returns a paginated order-line table and the total row count.
    Includes Order # (order_id), SKU, qty, unit gross (incl VAT), line GMV, refunds, fees, day, marketplace.
    """
    # compute bounds
    start = start_date
    end = end_date + dt.timedelta(days=1)  # make end exclusive

    # calculate offset from page
    page = max(1, int(page))
    limit = int(page_size)
    offset = (page - 1) * limit

    sql = """
    with lines as (
      select
        o.created_at::date as day,
        o.marketplace_code,
        ol.order_id,
        ol.line_id,
        ol.sku,
        ol.qty,
        coalesce(ol.price_tax_incl, ol.price_tax_excl + ol.tax_amount)::numeric as unit_gross,
        (ol.qty * coalesce(ol.price_tax_incl, ol.price_tax_excl + ol.tax_amount))::numeric as line_gmv,
        coalesce(ol.fees_total, 0)::numeric as fees
      from mirakl.orders o
      join mirakl.order_lines ol
        on ol.order_id = o.order_id
       and ol.marketplace_code = o.marketplace_code
      where o.created_at >= %(start)s
        and o.created_at < %(end)s
        and (%(sku)s is null or ol.sku = %(sku)s)
        and (%(mkt)s is null or o.marketplace_code = any(%(mkt)s))
    ),
    refunds as (
      select
        r.order_id, r.marketplace_code, r.line_id,
        sum(coalesce(r.amount_tax_excl,0) + coalesce(r.tax_amount,0))::numeric as refund_amount
      from mirakl.refunds r
      where r.created_at >= %(start)s
        and r.created_at < %(end)s
      group by 1,2,3
    ),
    joined as (
      select
        l.day, l.marketplace_code, l.order_id, l.line_id, l.sku, l.qty,
        l.unit_gross,
        l.line_gmv,
        coalesce(r.refund_amount, 0)::numeric as refunds,
        l.fees
      from lines l
      left join refunds r
        on r.order_id = l.order_id
       and r.marketplace_code = l.marketplace_code
       and r.line_id = l.line_id
    )
    select
      *,
      count(*) over() as total_count
    from joined
    order by day desc, order_id desc, line_id desc
    offset %(offset)s limit %(limit)s;
    """

    engine = get_engine()
    params = {
        "start": start,
        "end": end,
        "sku": sku,
        "mkt": marketplaces if marketplaces else None,
        "offset": offset,
        "limit": limit,
    }
    df = pd.read_sql(sql, con=engine, params=params)

    total = int(df["total_count"].iloc[0]) if not df.empty else 0
    # make visible column names friendly
    if not df.empty:
        df = df.drop(columns=["total_count"])
        df = df.rename(columns={
            "marketplace_code": "Marketplace",
            "day": "Day",
            "order_id": "Order #",
            "sku": "SKU",
            "qty": "Qty",
            "unit_gross": "Unit (inc VAT)",
            "line_gmv": "Line GMV",
            "refunds": "Refunds",
            "fees": "Fees"
        })
    return df, total


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

st.subheader("SKU Performance by Contribution")
# ---- Order-level toggle + pagination ----
show_orders = st.checkbox("Show order-level rows (with Order #)", value=False)

if show_orders:
    # Pagination state
    if "order_page" not in st.session_state:
        st.session_state.order_page = 1

    col_a, col_b, col_c, col_d = st.columns([1,1,4,4])
    with col_a:
        if st.button("◀ Prev", use_container_width=True):
            st.session_state.order_page = max(1, st.session_state.order_page - 1)
    with col_b:
        if st.button("Next ▶", use_container_width=True):
            st.session_state.order_page = st.session_state.order_page + 1

    PAGE_SIZE = 100
    orders_df, total_rows = order_lines_table(
        start_date=start_date,
        end_date=end_date,
        sku=sku if sku else None,
        marketplaces=marketplaces if marketplaces else None,
        page=st.session_state.order_page,
        page_size=PAGE_SIZE
    )

    # clamp page if user clicks next beyond last
    max_page = max(1, int((total_rows + PAGE_SIZE - 1) // PAGE_SIZE))
    if st.session_state.order_page > max_page:
        st.session_state.order_page = max_page
        # re-fetch with clamped page
        orders_df, total_rows = order_lines_table(
            start_date=start_date,
            end_date=end_date,
            sku=sku if sku else None,
            marketplaces=marketplaces if marketplaces else None,
            page=st.session_state.order_page,
            page_size=PAGE_SIZE
        )

    # header + info
    left_i, right_i = st.columns([3, 2])
    with left_i:
        st.subheader("Order lines")
    with right_i:
        st.write(f"Page {st.session_state.order_page} of {max_page} • {total_rows} rows total")

    st.dataframe(orders_df, use_container_width=True, height=520)

    # stop here so the SKU table below doesn't also render
    st.stop()
sku_df = top_skus(start_date, end_date, selected, sku_filter)
st.dataframe(sku_df, width="stretch")
