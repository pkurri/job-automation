"""
Optional email digest via SendGrid.
Requires SENDGRID_KEY, NOTIFY_EMAIL, FROM_EMAIL secrets in GitHub.
"""
import os, json, requests

SENDGRID_KEY = os.environ.get("SENDGRID_KEY", "")
NOTIFY_EMAIL = os.environ.get("NOTIFY_EMAIL", "")
FROM_EMAIL   = os.environ.get("FROM_EMAIL", "jobbot@noreply.com")

if not SENDGRID_KEY or not NOTIFY_EMAIL:
    print("Email skipped — secrets not configured")
    exit(0)

with open("run_summary.json") as f:
    s = json.load(f)

rows = ""
for j in s["top_jobs"]:
    em = "🔥" if j["score"] >= 9.5 else "✅" if j["score"] >= 8.5 else "👍"
    rows += f"""<tr>
      <td style="padding:8px">{em} {j["score"]}/10</td>
      <td style="padding:8px"><a href="{j["url"]}">{j["title"]}</a></td>
      <td style="padding:8px">{j["company"]}</td>
      <td style="padding:8px">{j["source"]}</td>
    </tr>"""

html = f"""<html><body style="font-family:Arial,sans-serif;max-width:700px;margin:auto">
  <h2 style="color:#1F3864">🤖 Daily Job Scan — {s["date"]}</h2>
  <p><strong>{s["added"]}</strong> new jobs added to Notion from
     <strong>{s["total_scraped"]}</strong> scraped listings.</p>
  <table border="1" cellpadding="0" cellspacing="0"
         style="border-collapse:collapse;width:100%;border-color:#ccc">
    <tr style="background:#2E75B6;color:white;text-align:left">
      <th style="padding:10px">Score</th>
      <th style="padding:10px">Title</th>
      <th style="padding:10px">Company</th>
      <th style="padding:10px">Source</th>
    </tr>{rows}
  </table>
  <p style="margin-top:24px">
    <a href="https://notion.so/2cf8236ea4f947ae9d9093e48eb724cf"
       style="background:#2E75B6;color:white;padding:12px 24px;
              text-decoration:none;border-radius:6px;font-weight:bold">
      Open Notion Tracker →
    </a>
  </p>
</body></html>"""

r = requests.post(
    "https://api.sendgrid.com/v3/mail/send",
    headers={"Authorization": f"Bearer {SENDGRID_KEY}", "Content-Type": "application/json"},
    json={
        "personalizations": [{"to": [{"email": NOTIFY_EMAIL}]}],
        "from":    {"email": FROM_EMAIL, "name": "Job Intelligence Bot"},
        "subject": f"🤖 {s['added']} New Job Matches — {s['date']}",
        "content": [{"type": "text/html", "value": html}],
    }, timeout=15
)
print("Email sent ✓" if r.status_code == 202 else f"Email failed {r.status_code}: {r.text}")
