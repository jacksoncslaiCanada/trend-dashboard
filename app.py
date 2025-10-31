# -----------------------------------------------------------
# Tech Trend Dashboard (Supabase → Streamlit)
# -----------------------------------------------------------
import os
from urllib.parse import quote_plus
from datetime import datetime
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text as sql_text

st.set_page_config(page_title="Tech Trend Dashboard", layout="wide")

# ---------- 1) Secrets / Env loader (supports DB_* keys and PGURL_VIEW) ----------
def _get_secret(name: str, default: str = "") -> str:
    # Prefer Streamlit Cloud secrets; fall back to environment (local dev)
    try:
        v = st.secrets[name]
        if isinstance(v, (int, float)): v = str(v)
        return (v or "").strip()
    except Exception:
        return (os.environ.get(name, default) or "").strip()

DB_HOST = _get_secret("DB_HOST")
DB_PORT = _get_secret("DB_PORT", "6543") or "6543"
DB_NAME = _get_secret("DB_NAME", "postgres") or "postgres"
DB_USER = _get_secret("DB_USER", "analytics_ro") or "analytics_ro"
DB_PASSWORD_RAW = _get_secret("DB_PASSWORD")
DB_SSLMODE = _get_secret("DB_SSLMODE", "require") or "require"
PGURL_DIRECT = _get_secret("PGURL_VIEW")  # optional single-URL fallback

# Build URL safely
if PGURL_DIRECT:
    PGURL = PGURL_DIRECT.strip()
else:
    if not (DB_HOST and DB_USER and DB_PASSWORD_RAW):
        st.error("Missing DB_* secrets. Set DB_HOST/DB_USER/DB_PASSWORD in Settings → Secrets (TOML).")
        st.stop()
    DB_PASSWORD = quote_plus(DB_PASSWORD_RAW)  # URL-encode password only
    PGURL = f"postgresql+psycopg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{int(DB_PORT)}/{DB_NAME}?sslmode={DB_SSLMODE}"

# Create engine
engine = create_engine(PGURL, pool_pre_ping=True)

# ---------- 2) TEMP: visibility + connection debug (remove when green) ----------
with st.expander("🔧 Secrets & DB connection debug (temporary)", expanded=False):
    st.write("st.secrets keys:", list(getattr(st, "secrets", {}).keys()))
    st.write("DB_HOST:", DB_HOST, "DB_PORT:", DB_PORT, "DB_NAME:", DB_NAME, "DB_USER:", DB_USER, "SSL:", DB_SSLMODE)
    st.write("Using PGURL_VIEW (single URL):", bool(PGURL_DIRECT))
    # DNS + raw connect
    import socket, psycopg
    # Parse host from DB_HOST or PGURL
    host_for_dns = DB_HOST
    if not host_for_dns and PGURL_DIRECT:
        try:
            from urllib.parse import urlsplit
            sp = urlsplit(PGURL_DIRECT.replace("postgresql+psycopg", "postgresql"))
            host_for_dns = (sp.hostname or "").strip()
        except Exception:
            host_for_dns = ""
    try:
        if host_for_dns:
            socket.gethostbyname(host_for_dns)
            st.success(f"DNS OK for {host_for_dns}")
        else:
            st.info("No host parsed for DNS test.")
    except Exception as e:
        st.error(f"DNS failed: {e!r}")
    try:
        psycopg.connect(PGURL, connect_timeout=5).close()
        st.success("✅ psycopg.connect(): SUCCESS")
    except Exception as e:
        st.error(f"❌ psycopg.connect() failed: {type(e).__name__}: {e}")

# ---------- 3) Query helper ----------
@st.cache_data(ttl=300)
def q(sql: str, **params) -> pd.DataFrame:
    with engine.begin() as cx:
        return pd.read_sql(sql_text(sql), cx, params=params)

# ---------- 4) UI ----------
st.title("Daily Tech Trends")
st.caption("Interactive view on your Supabase dataset")

# Controls
c1, c2, c3 = st.columns([1, 1, 2])
with c1:
    days = st.selectbox("Window (days)", [1, 7, 14, 30], index=1)
with c2:
    src_df = q("select distinct source from items order by 1")
    sources = src_df["source"].tolist()
    sel_src = st.multiselect("Sources", options=sources, default=sources)
with c3:
    qtext = st.text_input("Search in title/body (optional)")

where = ["ts >= now() - interval :days"]
params = {"days": f"{days} day"}
if sel_src and len(sel_src) != len(sources):
    where.append("source = ANY(:src)")
    params["src"] = sel_src
if qtext:
    where.append("(lower(title) like :q or lower(body) like :q)")
    params["q"] = f"%{qtext.lower()}%"

sql_where = " AND ".join(where)

# KPIs
kpi = q(f"""
select
  count(*)::int as n_items,
  round(avg(trend_score)::numeric,3) as avg_score
from items
where {sql_where}
""", **params)
k1, k2, k3 = st.columns(3)
k1.metric("Items", int(kpi["n_items"][0] or 0))
k2.metric("Avg trend score", float(kpi["avg_score"][0] or 0))
k3.metric("Last refresh (UTC)", datetime.utcnow().strftime("%Y-%m-%d %H:%M"))

# Top items table
st.subheader("Top items")
top = q(f"""
select ts, source, title, url, trend_score
from items
where {sql_where}
order by trend_score desc
limit 100
""", **params)

if not top.empty:
    top = top.assign(TitleLink=top.apply(lambda r: f"[{r['title']}]({r['url']})", axis=1))
    top_display = top[["ts", "source", "TitleLink", "trend_score"]].rename(
        columns={"ts": "Time (UTC)", "source": "Source", "TitleLink": "Title", "trend_score": "Trend score"}
    )
    st.dataframe(
        top_display,
        use_container_width=True,
        column_config={"Title": st.column_config.LinkColumn("Title")},
        hide_index=True
    )
else:
    st.info("No items match your filters.")

# Volume by source
st.subheader("Volume by source")
by_src = q(f"""
select source, count(*)::int as n
from items
where {sql_where}
group by 1
order by n desc
""", **params)
if not by_src.empty:
    st.bar_chart(by_src.set_index("source"))
else:
    st.info("No data for selected window/sources.")

# Daily volume
st.subheader(f"Daily volume (last {days}d)")
by_day = q(f"""
select date_trunc('day', ts) as day, count(*)::int as n
from items
where {sql_where}
group by 1
order by 1
""", **params)
if not by_day.empty:
    by_day = by_day.rename(columns={"day": "date"}).set_index("date")
    st.line_chart(by_day["n"])
else:
    st.info("No daily data.")

