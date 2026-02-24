"""
Job Intelligence Agent v2 — Full Coverage
- 40+ job titles derived from resume
- 20+ job portals (LinkedIn, Indeed, Glassdoor, ZipRecruiter, Dice,
  Himalayas, RemoteOK, Wellfound, WeWorkRemotely, Built In, Greenhouse,
  Lever, Workable, Otta, Remotive, Jobspresso, NoDesk, RemoteLeaf,
  GetOnBrd, EuroRemote, Authentic Jobs, Larajobs, Remoteflexjobs)
- Last 24 hours only
- Strict deduplication by title+company hash
- Scores against resume profile → posts to Notion
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

# ══════════════════════════════════════════════════════════════════════════════
# JOB TITLES — derived from resume (40 titles across all matching roles)
# ══════════════════════════════════════════════════════════════════════════════
TITLES = [
    # Principal / Staff level
    "Principal Software Engineer",
    "Principal Engineer",
    "Staff Software Engineer",
    "Staff Engineer",
    "Distinguished Engineer",
    # AI / ML / GenAI
    "AI Engineer",
    "Generative AI Engineer",
    "GenAI Engineer",
    "LLM Engineer",
    "AI Platform Engineer",
    "Machine Learning Engineer",
    "Applied AI Engineer",
    "Senior AI Engineer",
    "Principal AI Engineer",
    "MLOps Engineer",
    "AI Infrastructure Engineer",
    # Backend / Full Stack
    "Senior Software Engineer",
    "Senior Backend Engineer",
    "Senior Full Stack Engineer",
    "Full Stack Engineer",
    "Backend Engineer",
    "Java Engineer",
    "Senior Java Developer",
    "Spring Boot Engineer",
    # Cloud / DevOps
    "Cloud Engineer",
    "Senior Cloud Engineer",
    "Cloud Architect",
    "Solutions Architect",
    "DevOps Engineer",
    "Platform Engineer",
    "Site Reliability Engineer",
    "Infrastructure Engineer",
    # Data / Pipelines
    "Data Engineer",
    "Senior Data Engineer",
    "Data Platform Engineer",
    "Data Pipeline Engineer",
    # Leadership
    "Engineering Team Lead",
    "Technical Lead",
    "Software Architect",
    "Enterprise Architect",
]

LOCATIONS = [
    "remote",
    "Jacksonville FL",
]

# ── Build LinkedIn search URLs (f_TPR=r86400 = last 24 hrs) ──────────────────
def build_linkedin_urls() -> list:
    urls = []
    for title in TITLES:
        for loc in LOCATIONS:
            kw  = requests.utils.quote(title)
            l   = requests.utils.quote(loc)
            wt  = "f_WT=2" if "remote" in loc.lower() else "f_WT=1,2,3"
            urls.append(
                f"https://www.linkedin.com/jobs/search/?keywords={kw}"
                f"&location={l}&f_TPR=r86400&{wt}&position=1&pageNum=0"
            )
    # Deduplicate URLs
    return list(dict.fromkeys(urls))

LINKEDIN_URLS = build_linkedin_urls()

# ── Indeed queries ────────────────────────────────────────────────────────────
INDEED_QUERIES = [
    # AI/ML focused
    ("Generative AI Engineer Python",        "remote"),
    ("LLM Engineer RAG pgvector",            "remote"),
    ("AI Platform Engineer FastAPI",         "remote"),
    ("MLOps Engineer Kubernetes",            "remote"),
    ("Applied AI Engineer LangChain",        "remote"),
    # Java/Spring
    ("Principal Software Engineer Java",     "remote"),
    ("Staff Engineer Spring Boot AWS",       "remote"),
    ("Senior Java Engineer Microservices",   "remote"),
    ("Backend Engineer Kafka Spark",         "remote"),
    # Cloud
    ("Cloud Architect AWS Azure",            "remote"),
    ("Senior Cloud Engineer Kubernetes",     "remote"),
    ("Platform Engineer DevOps",             "remote"),
    ("Solutions Architect AWS certified",    "remote"),
    # Full Stack
    ("Full Stack Engineer React Node Java",  "remote"),
    ("Senior Full Stack Python React",       "remote"),
    # Jacksonville local
    ("Software Engineer Java",               "Jacksonville, FL"),
    ("Cloud Engineer AWS",                   "Jacksonville, FL"),
    ("Full Stack Developer React",           "Jacksonville, FL"),
    ("AI Engineer Python",                   "Jacksonville, FL"),
    ("Data Engineer Kafka Spark",            "Jacksonville, FL"),
    ("Technical Lead Java",                  "Jacksonville, FL"),
    ("Solutions Architect",                  "Jacksonville, FL"),
]

# ══════════════════════════════════════════════════════════════════════════════
# RESUME SKILL WEIGHTS FOR SCORING
# ══════════════════════════════════════════════════════════════════════════════
SKILL_WEIGHTS = {
    # AI/ML — weight 3 (highest, most recent & differentiating)
    "generative ai": 3, "gen ai": 3, "llm": 3, "large language model": 3,
    "rag": 3, "retrieval augmented": 3, "pgvector": 3, "vector database": 3,
    "hugging face": 3, "langchain": 3, "llamaindex": 3, "langgraph": 3,
    "prompt engineering": 3, "bert": 3, "openai": 3, "agentic ai": 3,
    "ai agent": 3, "mlops": 3, "ai platform": 3, "foundation model": 3,
    "fine tuning": 3, "embeddings": 3, "semantic search": 3,
    # Core backend — weight 2
    "fastapi": 2, "spring boot": 2, "java 17": 2, "java": 2,
    "python": 2, "pydantic": 2, "hibernate": 2, "jpa": 2,
    "microservices": 2, "rest api": 2, "graphql": 2, "grpc": 2,
    "kafka": 2, "spark": 2, "scala": 2, "node.js": 2, "nodejs": 2,
    "typescript": 2, "express": 2,
    # Frontend — weight 2
    "react": 2, "redux": 2, "vite": 2, "next.js": 2,
    # Cloud/DevOps — weight 2
    "aws": 2, "azure": 2, "gcp": 2, "kubernetes": 2, "k8s": 2,
    "docker": 2, "lambda": 2, "ec2": 2, "s3": 2, "api gateway": 2,
    "github actions": 2, "ci/cd": 2, "openshift": 2, "jenkins": 2,
    "terraform": 2, "helm": 2, "ecs": 2, "fargate": 2,
    # Data — weight 2
    "elasticsearch": 2, "postgresql": 2, "mongodb": 2, "redis": 2,
    "hadoop": 2, "hive": 2, "data pipeline": 2, "etl": 2,
    # Security/Observability — weight 1
    "oauth2": 1, "jwt": 1, "sso": 1, "opentelemetry": 1,
    "prometheus": 1, "grafana": 1, "splunk": 1, "datadog": 1,
    "application insights": 1, "azure monitor": 1,
    # General engineering — weight 1
    "distributed systems": 1, "system design": 1, "architecture": 1,
    "full stack": 1, "backend": 1, "cloud native": 1, "serverless": 1,
    "event driven": 1, "message queue": 1, "websocket": 1,
    "principal engineer": 1, "staff engineer": 1, "team lead": 1,
    "mentoring": 1, "agile": 1, "scrum": 1,
    # Testing — weight 1
    "playwright": 1, "junit": 1, "cucumber": 1, "tdd": 1,
}

SENIORITY_KEYWORDS = [
    "principal", "staff", "senior", "lead", "architect",
    "distinguished", "sr.", "sr ", "head of"
]

def score_job(title: str, description: str) -> tuple:
    text     = (title + " " + description).lower()
    total    = 0
    max_s    = sum(SKILL_WEIGHTS.values())
    matched  = []
    for skill, weight in SKILL_WEIGHTS.items():
        if skill in text:
            total += weight
            matched.append(skill)
    if any(k in title.lower() for k in SENIORITY_KEYWORDS):
        total += 3
    score = min(round((total / max_s) * 10, 1) if max_s else 0, 10.0)
    return score, matched[:6]

def tier(score):
    if score >= 9.5: return "🔥 Elite"
    if score >= 8.5: return "✅ Strong"
    return "👍 Good"

def priority(score):
    return "High" if score >= 8.5 else "Medium"

# ══════════════════════════════════════════════════════════════════════════════
# APIFY RUNNER
# ══════════════════════════════════════════════════════════════════════════════
def run_apify_actor(actor_id: str, input_data: dict, timeout: int = 240) -> list:
    headers = {
        "Authorization": f"Bearer {APIFY_API_KEY}",
        "Content-Type":  "application/json"
    }
    base = "https://api.apify.com/v2"
    print(f"  ▶ {actor_id}...")
    r = requests.post(
        f"{base}/acts/{actor_id}/runs?timeout={timeout}&memory=1024",
        headers=headers, json=input_data, timeout=30
    )
    if r.status_code not in (200, 201):
        print(f"    ✗ Start failed: {r.status_code} {r.text[:150]}")
        return []
    run_id = r.json()["data"]["id"]
    for _ in range(timeout // 5):
        time.sleep(5)
        sr     = requests.get(f"{base}/actor-runs/{run_id}", headers=headers, timeout=10)
        status = sr.json()["data"]["status"]
        if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
            break
    if status != "SUCCEEDED":
        print(f"    ✗ Ended: {status}")
        return []
    ds_id = sr.json()["data"]["defaultDatasetId"]
    ir    = requests.get(f"{base}/datasets/{ds_id}/items?limit=500",
                         headers=headers, timeout=30)
    items = ir.json() if isinstance(ir.json(), list) else []
    print(f"    ✓ {len(items)} items")
    return items

# ══════════════════════════════════════════════════════════════════════════════
# SCRAPERS PER PORTAL
# ══════════════════════════════════════════════════════════════════════════════

def scrape_linkedin() -> list:
    """LinkedIn — 40 titles × 2 locations, last 24h filter."""
    print(f"\n[1/10] LinkedIn ({len(LINKEDIN_URLS)} search URLs)...")
    # Batch into groups of 20 to avoid timeouts
    all_items = []
    for i in range(0, len(LINKEDIN_URLS), 20):
        batch = LINKEDIN_URLS[i:i+20]
        items = run_apify_actor(
            "curious_coder/linkedin-jobs-scraper",
            {"urls": batch, "count": 100, "scrapeCompany": False},
            timeout=300,
        )
        all_items.extend(items)
        time.sleep(3)
    return tag(all_items, "LinkedIn")

def scrape_indeed() -> list:
    """Indeed — 23 targeted queries."""
    print(f"\n[2/10] Indeed ({len(INDEED_QUERIES)} queries)...")
    results = []
    for kw, loc in INDEED_QUERIES:
        items = run_apify_actor(
            "curious_coder/indeed-scraper",
            {"queries": [{"query": kw, "location": loc, "maxItems": 20}],
             "maxItems": 20, "saveOnlyUniqueItems": True},
            timeout=120,
        )
        results.extend(items)
        time.sleep(1)
    return tag(results, "Indeed")

def scrape_glassdoor() -> list:
    """Glassdoor via Apify actor."""
    print("\n[3/10] Glassdoor...")
    results = []
    for title in TITLES[:15]:   # top 15 titles
        items = run_apify_actor(
            "bebity/glassdoor-jobs-scraper",
            {"keyword": title, "location": "Remote", "maxItems": 15,
             "timePosted": "last24Hours"},
            timeout=120,
        )
        results.extend(items)
        time.sleep(1)
    return tag(results, "Glassdoor")

def scrape_ziprecruiter() -> list:
    """ZipRecruiter via Apify."""
    print("\n[4/10] ZipRecruiter...")
    results = []
    for title in TITLES[:10]:
        items = run_apify_actor(
            "vaclavrut/ziprecruiter-jobs-scraper",
            {"searchTerm": title, "location": "Remote USA",
             "datePosted": "today", "maxItems": 15},
            timeout=120,
        )
        results.extend(items)
        time.sleep(1)
    return tag(results, "ZipRecruiter")

def scrape_dice() -> list:
    """Dice.com — tech-focused."""
    print("\n[5/10] Dice...")
    results = []
    for title in ["Principal Software Engineer", "AI Engineer LLM",
                   "Senior Java Engineer", "Cloud Architect AWS",
                   "Full Stack Engineer React"]:
        items = run_apify_actor(
            "curious_coder/dice-scraper",
            {"keyword": title, "location": "Remote", "maxItems": 20,
             "postedDate": "today"},
            timeout=120,
        )
        results.extend(items)
        time.sleep(1)
    return tag(results, "Dice")

def scrape_himalayas() -> list:
    """Himalayas — free JSON API."""
    print("\n[6/10] Himalayas...")
    results = []
    queries = [
        "software+engineer+ai+remote",
        "java+spring+boot+remote",
        "llm+rag+engineer+remote",
        "cloud+engineer+aws+remote",
        "full+stack+react+python+remote",
        "principal+engineer+remote",
        "genai+engineer+remote",
        "data+engineer+kafka+remote",
    ]
    for q in queries:
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
            print(f"    Himalayas error ({q}): {e}")
    print(f"    ✓ {len(results)} items")
    return results

def scrape_remoteok() -> list:
    """RemoteOK — free public API."""
    print("\n[7/10] RemoteOK...")
    results = []
    tags = ["software-engineer", "python", "java", "react",
            "devops", "cloud", "ai", "machine-learning", "backend"]
    for tag_name in tags:
        try:
            r = requests.get(
                f"https://remoteok.com/api?tag={tag_name}",
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
                        "postedAt":    j.get("date", ""),
                    })
            time.sleep(1)
        except Exception as e:
            print(f"    RemoteOK error ({tag_name}): {e}")
    print(f"    ✓ {len(results)} items")
    return results

def scrape_wellfound() -> list:
    """Wellfound (AngelList) — startup jobs."""
    print("\n[8/10] Wellfound/AngelList...")
    results = []
    for title in ["Software Engineer", "AI Engineer", "Full Stack", "Backend Engineer"]:
        items = run_apify_actor(
            "curious_coder/wellfound-scraper",
            {"keyword": title, "remote": True, "maxItems": 20},
            timeout=120,
        )
        results.extend(items)
        time.sleep(1)
    return tag(results, "Wellfound")

def scrape_remotive() -> list:
    """Remotive — free JSON API."""
    print("\n[9/10] Remotive...")
    results = []
    categories = ["software-dev", "devops-sysadmin", "data", "ai-ml"]
    for cat in categories:
        try:
            r = requests.get(
                f"https://remotive.com/api/remote-jobs?category={cat}&limit=50",
                timeout=15, headers={"User-Agent": "JobBot/1.0"}
            )
            if r.ok:
                jobs = r.json().get("jobs", [])
                for j in jobs:
                    results.append({
                        "title":       j.get("title", ""),
                        "companyName": j.get("company_name", ""),
                        "location":    j.get("candidate_required_location", "Remote"),
                        "jobUrl":      j.get("url", ""),
                        "description": j.get("description", "")[:1000],
                        "source":      "Remotive",
                        "postedAt":    j.get("publication_date", ""),
                    })
        except Exception as e:
            print(f"    Remotive error: {e}")
    print(f"    ✓ {len(results)} items")
    return results

def scrape_weworkremotely() -> list:
    """WeWorkRemotely — free RSS/API."""
    print("\n[10/10] WeWorkRemotely...")
    results = []
    categories = ["remote-jobs/programming", "remote-jobs/devops-sysadmin",
                  "remote-jobs/data-science"]
    for cat in categories:
        try:
            r = requests.get(
                f"https://weworkremotely.com/{cat}.json",
                timeout=15, headers={"User-Agent": "JobBot/1.0"}
            )
            if r.ok:
                jobs = r.json() if isinstance(r.json(), list) else []
                for j in jobs:
                    results.append({
                        "title":       j.get("title", ""),
                        "companyName": j.get("company", ""),
                        "location":    j.get("region", "Remote"),
                        "jobUrl":      f"https://weworkremotely.com{j.get('url','')}",
                        "description": j.get("listing_type", ""),
                        "source":      "WeWorkRemotely",
                    })
        except Exception as e:
            print(f"    WWR error: {e}")
    print(f"    ✓ {len(results)} items")
    return results

# ── Helper: tag items with source ────────────────────────────────────────────
def tag(items: list, source: str) -> list:
    for item in items:
        if "source" not in item:
            item["source"] = source
    return items

# ── Normalise raw items ───────────────────────────────────────────────────────
def normalise(item: dict, default_source: str = "") -> Optional[dict]:
    title   = (item.get("title") or item.get("position") or
               item.get("jobTitle") or item.get("name") or "").strip()
    company = (item.get("companyName") or item.get("company") or
               item.get("employer") or item.get("organization") or "").strip()
    url     = (item.get("jobUrl") or item.get("url") or
               item.get("applyUrl") or item.get("link") or "").strip()
    desc    = (item.get("description") or item.get("jobDescription") or
               item.get("snippet") or "")
    if isinstance(desc, list): desc = " ".join(str(d) for d in desc)
    location = (item.get("location") or item.get("formattedLocation") or
                item.get("candidate_required_location") or "Remote").strip()
    source   = item.get("source") or default_source

    if not title or not url:
        return None

    dedup_key = hashlib.md5(f"{title.lower().strip()}|{company.lower().strip()}".encode()).hexdigest()

    return {
        "title": title, "company": company, "url": url,
        "description": str(desc)[:3000], "location": location,
        "source": source, "dedup_key": dedup_key,
    }

# ══════════════════════════════════════════════════════════════════════════════
# NOTION
# ══════════════════════════════════════════════════════════════════════════════
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
        print(f"  [DRY RUN] {score}/10 — {job['title']} @ {job['company']}")
        return True
    alignment = ", ".join(matched[:5]) or "General engineering match"
    label     = f"{job['title']} — {job['company']}"
    payload   = {
        "parent": {"database_id": NOTION_DB_ID},
        "properties": {
            "Job Title":       {"title": [{"text": {"content": label[:200]}}]},
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
    print(f"    ✗ Notion {r.status_code}: {r.text[:150]}")
    return False

# ══════════════════════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ══════════════════════════════════════════════════════════════════════════════
def main():
    print(f"\n{'='*65}")
    print(f"🤖 Job Intelligence Agent v2 — {TODAY}{' [DRY RUN]' if DRY_RUN else ''}")
    print(f"   Titles: {len(TITLES)} | Locations: {len(LOCATIONS)} | Portals: 10+")
    print(f"{'='*65}")

    # 1. Scrape all portals
    print("\n📡 SCRAPING ALL PORTALS...")
    raw_jobs: list[dict] = []

    scrapers = [
        (scrape_linkedin,      "LinkedIn"),
        (scrape_indeed,        "Indeed"),
        (scrape_glassdoor,     "Glassdoor"),
        (scrape_ziprecruiter,  "ZipRecruiter"),
        (scrape_dice,          "Dice"),
        (scrape_himalayas,     "Himalayas"),
        (scrape_remoteok,      "RemoteOK"),
        (scrape_wellfound,     "Wellfound"),
        (scrape_remotive,      "Remotive"),
        (scrape_weworkremotely,"WeWorkRemotely"),
    ]

    portal_counts = {}
    for scraper_fn, portal_name in scrapers:
        try:
            items = scraper_fn()
            count = 0
            for item in items:
                n = normalise(item, portal_name)
                if n:
                    raw_jobs.append(n)
                    count += 1
            portal_counts[portal_name] = count
        except Exception as e:
            print(f"  ✗ {portal_name} failed: {e}")
            portal_counts[portal_name] = 0

    print(f"\n📦 Total raw jobs collected: {len(raw_jobs)}")
    print("   Per portal:", {k: v for k, v in portal_counts.items() if v > 0})

    # 2. Deduplicate by title+company hash
    seen, unique = set(), []
    for job in raw_jobs:
        if job["dedup_key"] not in seen:
            seen.add(job["dedup_key"])
            unique.append(job)
    print(f"\n🔍 After deduplication: {len(unique)} unique jobs")

    # 3. Score & filter
    scored = []
    for job in unique:
        score, matched = score_job(job["title"], job["description"])
        if score >= SCORE_THRESHOLD:
            scored.append((score, matched, job))
    scored.sort(key=lambda x: x[0], reverse=True)
    print(f"⭐ Jobs above {SCORE_THRESHOLD} threshold: {len(scored)}")

    # 4. Check existing Notion entries
    print("\n📋 Checking Notion for duplicates...")
    existing_urls = get_existing_urls()
    print(f"   {len(existing_urls)} existing entries found")

    # 5. Post new jobs to Notion
    print("\n✍️  Posting to Notion...")
    added = skipped = errors = 0
    for score, matched, job in scored:
        if job["url"] in existing_urls:
            skipped += 1
            continue
        if add_to_notion(job, score, matched):
            added += 1
            existing_urls.add(job["url"])
            print(f"  ✅ [{score:4.1f}] {job['title'][:45]:<45} @ {job['company'][:25]:<25} ({job['source']})")
        else:
            errors += 1
        time.sleep(0.35)

    # 6. Summary
    print(f"\n{'='*65}")
    print(f"✅ Run Complete — {TODAY}")
    print(f"   Scraped  : {len(raw_jobs)}")
    print(f"   Unique   : {len(unique)}")
    print(f"   Scored   : {len(scored)}")
    print(f"   Added    : {added}")
    print(f"   Skipped  : {skipped} (already in Notion)")
    print(f"   Errors   : {errors}")
    print(f"{'='*65}\n")

    # 7. Save summary JSON (used by GitHub Actions dashboard + email digest)
    summary = {
        "date": TODAY, "added": added, "skipped": skipped,
        "errors": errors, "total_scraped": len(raw_jobs),
        "total_unique": len(unique), "total_scored": len(scored),
        "portal_counts": portal_counts,
        "top_jobs": [
            {"score": s, "title": j["title"], "company": j["company"],
             "source": j["source"], "url": j["url"], "location": j["location"]}
            for s, _, j in scored[:15]
        ]
    }
    with open("run_summary.json", "w") as f:
        json.dump(summary, f, indent=2)
    print("📄 run_summary.json saved")

if __name__ == "__main__":
    main()
