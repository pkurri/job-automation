# 🤖 Job Intelligence Automation

Daily job scraper → scores against resume → auto-posts to **Notion Master Job Tracker**

[![Daily Job Scan](https://github.com/pkurri/job-automation/actions/workflows/daily_job_scan.yml/badge.svg)](https://github.com/pkurri/job-automation/actions/workflows/daily_job_scan.yml)

---

## ⏰ Schedule
| Days | Time |
|---|---|
| Monday – Friday | **7:00 AM EST** |
| Saturday – Sunday | **9:00 AM EST** |

---

## 📡 Sources Scraped
LinkedIn · Indeed · Himalayas · RemoteOK · (+ Glassdoor/ZipRecruiter via Apify)

## 📍 Locations
- **Remote** (all roles)
- **Jacksonville, FL** (local roles)

---

## 🚀 One-Time Setup

### 1. Add GitHub Secrets
Go to **Settings → Secrets → Actions → New repository secret**

| Secret | Value |
|---|---|
| `APIFY_API_KEY` | From [apify.com/account/integrations](https://apify.com/account/integrations) |
| `NOTION_API_KEY` | From [notion.so/my-integrations](https://notion.so/my-integrations) |
| `NOTION_DB_ID` | `d69bdba9-5cc8-4066-9ee6-b6e9f73d1743` |
| `NOTIFY_EMAIL` | *(optional)* Your email for daily digest |
| `SENDGRID_KEY` | *(optional)* SendGrid free API key |

### 2. Connect Notion Integration
1. Go to [notion.so/my-integrations](https://notion.so/my-integrations) → **+ New integration** → name it **"Job Bot"**
2. Copy the token → use as `NOTION_API_KEY`
3. Open **Master Job Tracker** → `...` menu → **Connections** → Add **Job Bot**

### 3. Run Manually to Test
**Actions tab → 🤖 Daily Job Intelligence Scan → Run workflow**

---

## ⚙️ Customize

Edit `SEARCH_QUERIES` in `scripts/job_scraper.py` to change keywords/locations:
```python
SEARCH_QUERIES = [
    ("Principal Software Engineer", "remote"),
    ("AI Engineer LLM RAG",         "remote"),
    ("Senior Engineer Java",         "Jacksonville FL"),
    # Add your own titles here
]
```

---

## 📊 Scoring
| Tier | Score | Action |
|---|---|---|
| 🔥 Elite | 9.5–10.0 | Apply immediately |
| ✅ Strong | 8.5–9.4 | Apply this week |
| 👍 Good | 7.0–8.4 | Apply if bandwidth allows |

AI/ML skills weighted **3×**, Cloud/Core stack **2×**, Tools **1×**

---

## 💰 Cost
| Service | Cost |
|---|---|
| GitHub Actions | **Free** |
| Apify | ~$1–2/month |
| Himalayas + RemoteOK | **Free** |
| SendGrid digest | **Free** (100/day) |
