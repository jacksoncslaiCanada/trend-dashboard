import os
import streamlit as st
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Tech Trend Dashboard", layout="wide")

def _get(name: str, default: str = "") -> str:
    # Prefer Streamlit secrets; fall back to env for local dev
    try:
        v = st.secrets[name]
        if isinstance(v, (int, float)): v = str(v)
        return (v or "").strip()
    except Exception:
        return (os.environ.get(name, default) or "").strip()

host = _get("DB_HOST")
port = int(_get("DB_PORT", "6543") or "6543")
name = _get("DB_NAME", "postgres") or "postgres"
user = _get("DB_USER", "analytics_ro") or "analytics_ro"
pwd_raw = _get("DB_PASSWORD")
ssl = _get("DB_SSLMODE", "require") or "require"

if not (host and user and pwd_raw):
    st.error("Missing DB_* secrets. Set DB_HOST/DB_USER/DB_PASSWORD in Settings → Secrets."); st.stop()

# URL-encode password only
pwd = quote_plus(pwd_raw)

PGURL = f"postgresql+psycopg://{user}:{pwd}@{host}:{port}/{name}?sslmode={ssl}"
engine = create_engine(PGURL, pool_pre_ping=True)

@st.cache_data(ttl=300)
def q(sql: str, **params):
    with engine.begin() as cx:
        return st.session_state.get("_pd_read_sql", __import__("pandas")).read_sql(text(sql), cx, params=params)



# ---- TEMP DEBUG: inspect connection string & connectivity ----
import streamlit as st
from urllib.parse import urlsplit, parse_qs
import socket, psycopg, re

st.markdown("### 🔧 DB Connection Debug (temporary)")

PGURL = (os.environ.get("PGURL_VIEW") or os.environ.get("PGURL") or "").strip()
if not PGURL:
    st.error("❌ PGURL_VIEW/PGURL is empty or not set. Check Streamlit secrets."); st.stop()

# Force psycopg driver if missing
if "postgresql+psycopg" not in PGURL:
    PGURL = PGURL.replace("postgresql://", "postgresql+psycopg://")

# --- Safe parse host/port/sslmode ---
sp = urlsplit(PGURL.replace("postgresql+psycopg", "postgresql"))
netloc = sp.netloc or ""
hostpart = netloc.split("@")[-1].strip()
m = re.match(r"^(?P<host>[^:\s]+)(?::(?P<port>\d+))?$", hostpart)
host = m.group("host") if m else hostpart
port = int(m.group("port")) if (m and m.group("port")) else None
qs = parse_qs(sp.query or "")
sslmode = qs.get("sslmode", [""])[0]

st.write("Engine URL (masked):", PGURL.split("@")[0] + ":***@" + host)
st.write("Parsed host:", host)
st.write("Parsed port:", port)
st.write("Parsed database:", sp.path.lstrip("/") or "(none)")
st.write("sslmode param:", sslmode or "(none)")

# --- Quick validations ---
st.write("✅ Driver ok:", PGURL.startswith("postgresql+psycopg://"))
st.write("✅ Host ends with .pooler.supabase.com:", host.endswith(".pooler.supabase.com"))
st.write("✅ Port = 6543:", port == 6543)
st.write("✅ sslmode=require:", sslmode == "require")

# --- DNS check ---
try:
    ips = socket.gethostbyname_ex(host)[2]
    st.success(f"DNS OK: {host} -> {ips[:2]} (showing first 2 IPs)")
except Exception as e:
    st.error(f"❌ DNS lookup failed for '{host}': {e!r}")

# --- Raw psycopg connection test ---
try:
    psycopg.connect(PGURL, connect_timeout=5).close()
    st.success("✅ psycopg.connect(): SUCCESS")
except Exception as e:
    st.error(f"❌ psycopg.connect() failed: {type(e).__name__}: {e}")
# ---- END DEBUG ----


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
