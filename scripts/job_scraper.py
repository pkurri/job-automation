"""
Job Intelligence Agent
Scrapes LinkedIn, Indeed, Himalayas, RemoteOK, Glassdoor, ZipRecruiter
Deduplicates -> Scores against resume profile -> Posts to Notion Master Job Tracker
Schedule: 7AM EST Mon-Fri via GitHub Actions
"""

import os, json, time, hashlib, requests
from datetime import datetime, timezone
from typing import Optional

# ── Config ────────────────────────────────────────────────────────────────────
APIFY_API_KEY   = os.environ["APIFY_API_KEY"]
NOTION_API_KEY  = os.environ["NOTION_API_KEY"]
NOTION_DB_ID    = os.environ["NOTION_DB_ID"]
SCORE_THRESHOLD = float(os.getenv("SCORE_THRESHOLD", "7.0"))
DRY_RUN         = os.getenv("DRY_RUN", "false").lower() == "true"
TODAY           = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# ── Search queries: (keyword, location) ──────────────────────────────────────
SEARCH_QUERIES = [
    # Remote
    ("Principal Software Engineer",          "remote"),
    ("Staff Software Engineer Java AWS",      "remote"),
    ("AI Engineer LLM RAG",                  "remote"),
    ("Senior Software Engineer FastAPI",      "remote"),
    ("Full Stack Engineer React Java",        "remote"),
    ("Cloud Engineer AWS Azure Kubernetes",   "remote"),
    ("GenAI Engineer LangChain",             "remote"),
    ("Senior Backend Engineer Python",        "remote"),
    # Jacksonville FL
    ("Software Engineer Java Spring Boot",   "Jacksonville FL"),
    ("Cloud Engineer AWS",                   "Jacksonville FL"),
    ("Principal Engineer",                   "Jacksonville FL"),
    ("Full Stack Developer React",           "Jacksonville FL"),
    ("AI ML Engineer",                       "Jacksonville FL"),
]

# Build LinkedIn search URLs (f_TPR=r86400 = last 24 hours)
LINKEDIN_URLS = []
for keyword, location in SEARCH_QUERIES:
    kw  = requests.utils.quote(keyword)
    loc = requests.utils.quote(location)
    wt  = "f_WT=2" if "remote" in location.lower() else "f_WT=1,2,3"
    LINKEDIN_URLS.append(
        f"https://www.linkedin.com/jobs/search/?keywords={kw}"
        f"&location={loc}&f_TPR=r86400&{wt}&position=1&pageNum=0"
    )

# ── Resume skill weights for scoring ─────────────────────────────────────────
SKILL_WEIGHTS = {
    # AI/ML — weight 3 (highest)
    "generative ai": 3, "llm": 3, "large language model": 3,
    "rag": 3, "retrieval augmented": 3, "pgvector": 3,
    "vector database": 3, "hugging face": 3, "langchain": 3,
    "llamaindex": 3, "prompt engineering": 3, "bert": 3,
    "openai": 3, "agentic": 3, "mlops": 3, "ai platform": 3,
    # Core stack — weight 2
    "fastapi": 2, "spring boot": 2, "java": 2, "python": 2,
    "react": 2, "node.js": 2, "nodejs": 2, "typescript": 2,
    "microservices": 2, "rest api": 2, "kafka": 2, "spark": 2,
    "scala": 2, "pydantic": 2,
    # Cloud/DevOps — weight 2
    "aws": 2, "azure": 2, "kubernetes": 2, "docker": 2,
    "lambda": 2, "s3": 2, "ec2": 2, "api gateway": 2,
    "github actions": 2, "ci/cd": 2, "openshift": 2, "jenkins": 2,
    # Security/Observability — weight 1
    "oauth2": 1, "jwt": 1, "sso": 1, "opentelemetry": 1,
    "prometheus": 1, "splunk": 1, "elasticsearch": 1,
    "application insights": 1,
    # General engineering — weight 1
    "distributed systems": 1, "postgresql": 1, "redis": 1,
    "principal engineer": 1, "staff engineer": 1,
    "team lead": 1, "mentoring": 1, "architecture": 1,
    "full stack": 1, "mongodb": 1, "hadoop": 1,
}

SENIORITY_KEYWORDS = ["principal", "staff", "senior", "lead", "architect", "sr.", "sr "]

def score_job(title: str, description: str) -> tuple:
    text        = (title + " " + description).lower()
    total, max_s = 0, 0
    matched     = []
    for skill, weight in SKILL_WEIGHTS.items():
        max_s += weight
        if skill in text:
            total += weight
            matched.append(skill)
    if any(k in title.lower() for k in SENIORITY_KEYWORDS):
        total += 2
    score = min(round((total / max_s) * 10, 1) if max_s else 0, 10.0)
    return score, matched[:6]

def tier(score):
    if score >= 9.5: return "🔥 Elite"
    if score >= 8.5: return "✅ Strong"
    return "👍 Good"

def priority(score):
    return "High" if score >= 8.5 else "Medium"

# ── Apify runner ──────────────────────────────────────────────────────────────
def run_apify_actor(actor_id: str, input_data: dict, timeout: int = 180) -> list:
    headers = {
        "Authorization": f"Bearer {APIFY_API_KEY}",
        "Content-Type": "application/json"
    }
    base = "https://api.apify.com/v2"
    print(f"  ▶ Running {actor_id}...")
    r = requests.post(
        f"{base}/acts/{actor_id}/runs?timeout={timeout}&memory=512",
        headers=headers, json=input_data, timeout=30
    )
    if r.status_code not in (200, 201):
        print(f"  ✗ Actor start failed: {r.status_code}")
        return []
    run_id = r.json()["data"]["id"]
    for _ in range(timeout // 5):
        time.sleep(5)
        sr  = requests.get(f"{base}/actor-runs/{run_id}", headers=headers, timeout=10)
        status = sr.json()["data"]["status"]
        if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
            break
    if status != "SUCCEEDED":
        print(f"  ✗ Actor ended: {status}")
        return []
    ds_id   = sr.json()["data"]["defaultDatasetId"]
    items_r = requests.get(f"{base}/datasets/{ds_id}/items?limit=200", headers=headers, timeout=30)
    items   = items_r.json()
    result  = items if isinstance(items, list) else []
    print(f"  ✓ {len(result)} items")
    return result

def scrape_linkedin() -> list:
    return run_apify_actor(
        "curious_coder/linkedin-jobs-scraper",
        {"urls": LINKEDIN_URLS, "count": 100, "scrapeCompany": False},
        timeout=300,
    )

def scrape_indeed() -> list:
    results = []
    queries = [
        ("Principal Software Engineer remote",  "remote"),
        ("AI Engineer LLM Python",              "remote"),
        ("Senior Software Engineer Java AWS",   "Jacksonville, FL"),
        ("Full Stack React Node",               "Jacksonville, FL"),
    ]
    for kw, loc in queries:
        items = run_apify_actor(
            "curious_coder/indeed-scraper",
            {"queries": [{"query": kw, "location": loc, "maxItems": 25}],
             "maxItems": 25, "saveOnlyUniqueItems": True},
        )
        results.extend(items)
    return results

def scrape_himalayas() -> list:
    results = []
    for q in ["software+engineer+ai", "java+spring+boot+remote", "llm+rag+engineer"]:
        try:
            r = requests.get(
                f"https://himalayas.app/jobs/api?q={q}&limit=30",
                timeout=15, headers={"User-Agent": "JobBot/1.0"}
            )
            if r.ok:
                data = r.json()
                jobs = data.get("jobs", data) if isinstance(data, dict) else data
                for j in (jobs if isinstance(jobs, list) else []):
                    co = j.get("company", {})
                    results.append({
                        "title":       j.get("title", ""),
                        "companyName": co.get("name", "") if isinstance(co, dict) else str(co),
                        "location":    j.get("location", "Remote"),
                        "jobUrl":      j.get("url", j.get("applyUrl", "")),
                        "description": j.get("description", ""),
                        "source":      "Himalayas",
                    })
        except Exception as e:
            print(f"  Himalayas error: {e}")
    return results

def scrape_remoteok() -> list:
    results = []
    for tag in ["software-engineer", "python", "java"]:
        try:
            r = requests.get(
                f"https://remoteok.com/api?tag={tag}",
                timeout=15, headers={"User-Agent": "JobBot/1.0"}
            )
            if r.ok:
                data = r.json()
                for j in (data[1:] if isinstance(data, list) else []):
                    results.append({
                        "title":       j.get("position", ""),
                        "companyName": j.get("company", ""),
                        "location":    "Remote",
                        "jobUrl":      j.get("url", ""),
                        "description": " ".join(j.get("tags", [])),
                        "source":      "RemoteOK",
                    })
        except Exception as e:
            print(f"  RemoteOK error: {e}")
    return results

# ── Normalise raw items ───────────────────────────────────────────────────────
def normalise(item: dict, default_source: str) -> Optional[dict]:
    title   = (item.get("title") or item.get("position") or item.get("jobTitle") or "").strip()
    company = (item.get("companyName") or item.get("company") or item.get("employer") or "").strip()
    url     = (item.get("jobUrl") or item.get("url") or item.get("applyUrl") or "").strip()
    desc    = item.get("description") or item.get("jobDescription") or ""
    if isinstance(desc, list): desc = " ".join(str(d) for d in desc)
    location = (item.get("location") or item.get("formattedLocation") or "Remote").strip()
    source   = item.get("source", default_source)
    if not title or not url:
        return None
    dedup_key = hashlib.md5(f"{title.lower()}|{company.lower()}".encode()).hexdigest()
    return {
        "title": title, "company": company, "url": url,
        "description": str(desc)[:2000], "location": location,
        "source": source, "dedup_key": dedup_key,
    }

# ── Notion ────────────────────────────────────────────────────────────────────
NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Notion-Version": "2022-06-28",
    "Content-Type":   "application/json",
}

def get_existing_urls() -> set:
    existing, cursor = set(), None
    while True:
        body = {"page_size": 100,
                "filter": {"property": "URL", "url": {"is_not_empty": True}}}
        if cursor:
            body["start_cursor"] = cursor
        r    = requests.post(
            f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query",
            headers=NOTION_HEADERS, json=body, timeout=15
        )
        data = r.json()
        for page in data.get("results", []):
            u = page["properties"].get("URL", {}).get("url", "")
            if u: existing.add(u)
        if not data.get("has_more"): break
        cursor = data.get("next_cursor")
    return existing

def add_to_notion(job: dict, score: float, matched: list) -> bool:
    if DRY_RUN:
        print(f"  [DRY RUN] Would add: {job['title']} @ {job['company']} ({score})")
        return True
    alignment = ", ".join(matched[:5]) or "General engineering match"
    payload = {
        "parent": {"database_id": NOTION_DB_ID},
        "properties": {
            "Job Title":       {"title": [{"text": {"content": f"{job['title']} — {job['company']}"[:200]}}]},
            "URL":             {"url": job["url"]},
            "Status":          {"status": {"name": "Wishlist"}},
            "Role":            {"select": {"name": "Software Engineer"}},
            "Priority":        {"select": {"name": priority(score)}},
            "Match Score":     {"number": score},
            "Score Tier":      {"select": {"name": tier(score)}},
            "Source Platform": {"rich_text": [{"text": {"content": job["source"]}}]},
            "Key Alignment":   {"rich_text": [{"text": {"content": alignment[:100]}}]},
            "Notes":           {"rich_text": [{"text": {"content":
                f"Company: {job['company']} | {tier(score)} {score}/10 | {job['location']}"
            }}]},
            "Date Applied":    {"date": {"start": TODAY}},
        }
    }
    r = requests.post("https://api.notion.com/v1/pages",
                      headers=NOTION_HEADERS, json=payload, timeout=15)
    if r.status_code == 200:
        return True
    print(f"  ✗ Notion {r.status_code}: {r.text[:150]}")
    return False

# ── Main pipeline ─────────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*60}")
    print(f"🤖 Job Intelligence Agent — {TODAY}{' [DRY RUN]' if DRY_RUN else ''}")
    print(f"{'='*60}\n")

    # 1. Scrape
    print("📡 Scraping all sources...")
    raw_jobs = []

    for item in scrape_linkedin():
        n = normalise(item, "LinkedIn"); 
        if n: raw_jobs.append(n)

    for item in scrape_himalayas():
        n = normalise(item, "Himalayas")
        if n: raw_jobs.append(n)

    for item in scrape_remoteok():
        n = normalise(item, "RemoteOK")
        if n: raw_jobs.append(n)

    try:
        for item in scrape_indeed():
            n = normalise(item, "Indeed")
            if n: raw_jobs.append(n)
    except Exception as e:
        print(f"  Indeed skipped: {e}")

    print(f"\n📦 Raw jobs collected: {len(raw_jobs)}")

    # 2. Deduplicate
    seen, unique = set(), []
    for job in raw_jobs:
        if job["dedup_key"] not in seen:
            seen.add(job["dedup_key"])
            unique.append(job)
    print(f"🔍 After deduplication: {len(unique)}")

    # 3. Score & filter
    scored = []
    for job in unique:
        score, matched = score_job(job["title"], job["description"])
        if score >= SCORE_THRESHOLD:
            scored.append((score, matched, job))
    scored.sort(key=lambda x: x[0], reverse=True)
    print(f"⭐ Above threshold ({SCORE_THRESHOLD}): {len(scored)}")

    # 4. Fetch existing Notion URLs
    print("\n📋 Checking Notion for existing entries...")
    existing_urls = get_existing_urls()
    print(f"   {len(existing_urls)} existing entries found")

    # 5. Post new jobs to Notion
    print("\n✍️  Posting new jobs to Notion...")
    added = skipped = 0
    for score, matched, job in scored:
        if job["url"] in existing_urls:
            skipped += 1
            continue
        if add_to_notion(job, score, matched):
            added += 1
            existing_urls.add(job["url"])
            print(f"  ✅ [{score}/10] {job['title']} @ {job['company']} ({job['source']})")
        time.sleep(0.35)

    # 6. Summary
    print(f"\n{'='*60}")
    print(f"✅ Done — Added: {added} | Skipped: {skipped} | Scored: {len(scored)}")
    print(f"{'='*60}\n")

    summary = {
        "date": TODAY, "added": added, "skipped": skipped,
        "total_scored": len(scored), "total_scraped": len(raw_jobs),
        "top_jobs": [
            {"score": s, "title": j["title"], "company": j["company"],
             "source": j["source"], "url": j["url"]}
            for s, _, j in scored[:10]
        ]
    }
    with open("run_summary.json", "w") as f:
        json.dump(summary, f, indent=2)
    print("📄 run_summary.json saved")

if __name__ == "__main__":
    main()
