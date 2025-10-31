import os
from datetime import datetime
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Tech Trend Dashboard", layout="wide")

# --- DB connection (read-only) ---
PGURL = os.environ.get("PGURL_VIEW") or os.environ.get("PGURL")
if not PGURL:
    st.error("Missing PGURL_VIEW/PGURL environment variable."); st.stop()
if "postgresql+psycopg" not in PGURL:
    PGURL = PGURL.replace("postgresql://", "postgresql+psycopg://")

# new block for fixing the string
PGURL = (os.environ.get("PGURL_VIEW") or os.environ.get("PGURL") or "").strip()  # <-- trim
if not PGURL:
    raise SystemExit("Missing PGURL_VIEW/PGURL env var")

# force psycopg driver if missing
if "postgresql+psycopg" not in PGURL:
    PGURL = PGURL.replace("postgresql://", "postgresql+psycopg://")

# end of new block for fixing the string


# ---- TEMP DEBUG: inspect connection string & connectivity ----
# ---- robust parse of host/port ----
from urllib.parse import urlsplit
import re, streamlit as st

def _mask(url: str) -> str:
    try:
        scheme, rest = url.split("://", 1)
        creds, hostpart = rest.split("@", 1)
        user = creds.split(":", 1)[0]
        return f"{scheme}://{user}:***@{hostpart}"
    except Exception:
        return url

sp = urlsplit(PGURL.replace("postgresql+psycopg", "postgresql"))
netloc = sp.netloc or ""              # e.g. "analytics_ro:***@abc.pooler.supabase.com:6543"
hostpart = netloc.split("@")[-1]      # drop credentials if present
hostpart = hostpart.strip()

# extract host and a numeric port if present
m = re.match(r"^(?P<host>[^:\s]+)(?::(?P<port>\d+))?$", hostpart)
host = m.group("host") if m else hostpart
port = int(m.group("port")) if (m and m.group("port")) else None

st.write("Engine URL:", _mask(PGURL))
st.write("Parsed host:", host)
st.write("Parsed port:", port)
st.write("SSL param present:", "sslmode=require" in (sp.query or ""))


# Quick checks:
st.write("✅ Driver ok:", PGURL.startswith("postgresql+psycopg://"))
st.write("✅ Pooled host (.pooler.supabase.com):", host.endswith(".pooler.supabase.com"))
st.write("✅ Port 6543:", port == 6543)
st.write("✅ sslmode=require:", "sslmode=require" in ssl)

# 2) DNS check
try:
    dns = socket.gethostbyname_ex(host)
    st.success(f"DNS OK: {dns[0]} -> {dns[2][:2]} (showing up to 2 IPs)")
except Exception as e:
    st.error(f"DNS lookup FAILED for '{host}': {e!r}")

# 3) Raw psycopg connect (5s timeout) to surface the exact DB error
try:
    # psycopg accepts the same URL used by SQLAlchemy's create_engine
    psycopg.connect(PGURL, connect_timeout=5).close()
    st.success("psycopg.connect(): SUCCESS")
except Exception as e:
    st.error(f"psycopg.connect(): FAILED -> {type(e).__name__}: {e}")
# ---- END TEMP DEBUG ----

engine = create_engine(PGURL, pool_pre_ping=True)

@st.cache_data(ttl=300)
def q(sql: str, **params) -> pd.DataFrame:
    with engine.begin() as cx:
        return pd.read_sql(text(sql), cx, params=params)

st.title("Daily Tech Trends")
st.caption("Interactive view on your Supabase dataset")

# --- Controls ---
c1, c2, c3 = st.columns([1,1,2])
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

# --- KPIs ---
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

# --- Top items table ---
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
    top_display = top[["ts","source","TitleLink","trend_score"]].rename(
        columns={"ts":"Time (UTC)","source":"Source","TitleLink":"Title","trend_score":"Trend score"})
    st.dataframe(
        top_display,
        use_container_width=True,
        column_config={"Title": st.column_config.LinkColumn("Title")},
        hide_index=True
    )
else:
    st.info("No items match your filters.")

# --- Volume by source ---
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

# --- Daily volume ---
st.subheader(f"Daily volume (last {days}d)")
by_day = q(f"""
select date_trunc('day', ts) as day, count(*)::int as n
from items
where {sql_where}
group by 1
order by 1
""", **params)
if not by_day.empty:
    by_day = by_day.rename(columns={"day":"date"}).set_index("date")
    st.line_chart(by_day["n"])
else:
    st.info("No daily data.")
