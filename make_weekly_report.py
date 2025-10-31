import os, io, base64
from datetime import datetime, timezone
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table
from reportlab.lib.units import inch
import requests

# --- DB connection ---
PGURL = os.environ.get("PGURL_VIEW") or os.environ.get("PGURL")
if not PGURL:
    raise SystemExit("PGURL_VIEW/PGURL not set")
if "postgresql+psycopg" not in PGURL:
    PGURL = PGURL.replace("postgresql://", "postgresql+psycopg://")
engine = create_engine(PGURL, pool_pre_ping=True)

def fetch_df(sql: str, **params):
    with engine.begin() as cx:
        return pd.read_sql(text(sql), cx, params=params)

# last 7 days
by_day = fetch_df("""
select date_trunc('day', ts) as day, count(*)::int as n
from items
where ts >= now() - interval '7 days'
group by 1 order by 1;
""")
by_src = fetch_df("""
select source, count(*)::int as n
from items
where ts >= now() - interval '7 days'
group by 1 order by n desc
limit 10;
""")
top = fetch_df("""
select ts, source, title, url, trend_score
from items
where ts >= now() - interval '7 days'
order by trend_score desc
limit 20;
""")

def fig_to_png_bytes(make_plot):
    fig = plt.figure(figsize=(7,3.2), dpi=150)
    try:
        make_plot(fig)
        buf = io.BytesIO()
        fig.savefig(buf, format="png", bbox_inches="tight")
        buf.seek(0)
        return buf
    finally:
        plt.close(fig)

daily_png = fig_to_png_bytes(lambda fig: (
    plt.plot(by_day["day"], by_day["n"]),
    plt.title("Daily volume (7 days)"),
    plt.xlabel("Day"), plt.ylabel("# Items")
))
src_png = fig_to_png_bytes(lambda fig: (
    plt.bar(by_src["source"], by_src["n"]),
    plt.title("Top sources (7 days)"),
    plt.xticks(rotation=45, ha="right"),
    plt.xlabel("Source"), plt.ylabel("# Items")
))

today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
pdf_path = f"weekly_trends_{today}.pdf"
styles = getSampleStyleSheet()
story = []
story.append(Paragraph(f"Weekly Tech Trends — {today}", styles["Title"]))
story.append(Spacer(1, 0.2*inch))
story.append(Paragraph("Overview", styles["Heading2"]))
story.append(Paragraph(
    "This report summarizes the last 7 days of items across your sources. "
    "Charts show daily volume and top sources; the table lists the top 20 items by trend score.",
    styles["BodyText"])
)
story.append(Spacer(1, 0.2*inch))
story.append(Paragraph("Daily volume", styles["Heading3"]))
story.append(Image(daily_png, width=6.5*inch, height=3.0*inch))
story.append(Spacer(1, 0.2*inch))
story.append(Paragraph("Top sources", styles["Heading3"]))
story.append(Image(src_png, width=6.5*inch, height=3.0*inch))
story.append(Spacer(1, 0.2*inch))
story.append(Paragraph("Top items (by trend score)", styles["Heading3"]))
table_data = [["Time (UTC)","Source","Title","Score"]]
for _, r in top.iterrows():
    table_data.append([str(r["ts"])[:16], r["source"], r["title"], f'{r["trend_score"]:.3f}'])
story.append(Table(table_data, colWidths=[1.5*inch, 1.0*inch, 3.5*inch, 0.5*inch]))
doc = SimpleDocTemplate(pdf_path, pagesize=LETTER, leftMargin=0.6*inch, rightMargin=0.6*inch)
doc.build(story)
print(f"[report] wrote {pdf_path}")

# Email via SendGrid (optional)
SG_KEY = os.environ.get("SENDGRID_API_KEY")
TO_EMAIL = os.environ.get("REPORT_TO_EMAIL")
FROM_EMAIL = os.environ.get("REPORT_FROM_EMAIL")
if SG_KEY and TO_EMAIL and FROM_EMAIL:
    with open(pdf_path, "rb") as f:
        pdf_b64 = base64.b64encode(f.read()).decode("ascii")
    payload = {
      "personalizations": [{"to": [{"email": TO_EMAIL}]}],
      "from": {"email": FROM_EMAIL, "name": "Trend Reports"},
      "subject": f"Weekly Tech Trends — {today}",
      "content": [{"type": "text/plain", "value": "Attached: weekly trend report (PDF)."}],
      "attachments": [{
          "content": pdf_b64,
          "type": "application/pdf",
          "filename": pdf_path,
          "disposition": "attachment"
      }]
    }
    r = requests.post(
        "https://api.sendgrid.com/v3/mail/send",
        headers={"Authorization": f"Bearer {SG_KEY}", "Content-Type": "application/json"},
        json=payload, timeout=20
    )
    print("[email] status:", r.status_code, r.text[:200])
else:
    print("[email] skipped (missing SENDGRID_API_KEY or emails)")
