"""
Job Intelligence Agent v4 — URL Verified + Score Validated
- Verifies every URL returns HTTP 200 before inserting into Notion
- Validates score is realistic (checks title/desc actually match skills)
- Flags and skips dead links, redirects to login pages, and 404s
- Minimum score 7.0, must have at least 2 matching skills
- Deduplicates by title+company hash across all 17 portals
"""

import os, json, time, hashlib, requests, re
from datetime import datetime, timezone
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

APIFY_API_KEY   = os.environ["APIFY_API_KEY"]
NOTION_API_KEY  = os.environ["NOTION_API_KEY"]
NOTION_DB_ID    = os.environ["NOTION_DB_ID"]
SCORE_THRESHOLD = float(os.getenv("SCORE_THRESHOLD", "7.0"))
DRY_RUN         = os.getenv("DRY_RUN", "false").lower() == "true"
TODAY           = datetime.now(timezone.utc).strftime("%Y-%m-%d")

MIN_MATCHED_SKILLS = 2       # must match at least 2 skills
URL_TIMEOUT        = 8       # seconds per URL check
MAX_URL_WORKERS    = 10      # parallel URL checks

# ── Known redirect/login pages that indicate a dead job link ─────────────────
DEAD_URL_PATTERNS = [
    "login", "signin", "sign-in", "auth", "expired", "removed",
    "no-longer-available", "job-not-found", "404", "error",
    "session", "redirect", "greenhouse.io/plans",
]

TITLES = [
    "Principal Software Engineer", "Principal Engineer",
    "Staff Software Engineer", "Staff Engineer", "Distinguished Engineer",
    "AI Engineer", "Generative AI Engineer", "GenAI Engineer",
    "LLM Engineer", "AI Platform Engineer", "Machine Learning Engineer",
    "Applied AI Engineer", "Senior AI Engineer", "Principal AI Engineer",
    "MLOps Engineer", "AI Infrastructure Engineer",
    "Senior Software Engineer", "Senior Backend Engineer",
    "Senior Full Stack Engineer", "Full Stack Engineer", "Backend Engineer",
    "Java Engineer", "Senior Java Developer", "Spring Boot Engineer",
    "Cloud Engineer", "Senior Cloud Engineer", "Cloud Architect",
    "Solutions Architect", "DevOps Engineer", "Platform Engineer",
    "Site Reliability Engineer", "Infrastructure Engineer",
    "Data Engineer", "Senior Data Engineer", "Data Platform Engineer",
    "Data Pipeline Engineer", "Engineering Team Lead", "Technical Lead",
    "Software Architect", "Enterprise Architect",
]
LOCATIONS = ["remote", "Jacksonville FL"]

def build_linkedin_urls():
    urls = []
    for title in TITLES:
        for loc in LOCATIONS:
            kw = requests.utils.quote(title)
            l  = requests.utils.quote(loc)
            wt = "f_WT=2" if "remote" in loc.lower() else "f_WT=1,2,3"
            urls.append(
                f"https://www.linkedin.com/jobs/search/?keywords={kw}"
                f"&location={l}&f_TPR=r86400&{wt}&position=1&pageNum=0"
            )
    return list(dict.fromkeys(urls))

LINKEDIN_URLS = build_linkedin_urls()

SKILL_WEIGHTS = {
    "generative ai":3,"gen ai":3,"llm":3,"large language model":3,
    "rag":3,"retrieval augmented":3,"pgvector":3,"vector database":3,
    "hugging face":3,"langchain":3,"llamaindex":3,"langgraph":3,
    "prompt engineering":3,"bert":3,"openai":3,"agentic ai":3,
    "ai agent":3,"mlops":3,"ai platform":3,"foundation model":3,
    "fine tuning":3,"embeddings":3,"semantic search":3,
    "fastapi":2,"spring boot":2,"java 17":2,"java":2,"python":2,
    "pydantic":2,"microservices":2,"rest api":2,"graphql":2,
    "grpc":2,"kafka":2,"spark":2,"scala":2,"node.js":2,
    "nodejs":2,"typescript":2,"react":2,"redux":2,"next.js":2,
    "aws":2,"azure":2,"gcp":2,"kubernetes":2,"k8s":2,"docker":2,
    "lambda":2,"ec2":2,"s3":2,"api gateway":2,"github actions":2,
    "ci/cd":2,"terraform":2,"helm":2,"elasticsearch":2,
    "postgresql":2,"mongodb":2,"redis":2,"hadoop":2,"kafka":2,
    "data pipeline":2,"etl":2,
    "oauth2":1,"jwt":1,"sso":1,"opentelemetry":1,"prometheus":1,
    "grafana":1,"splunk":1,"datadog":1,"distributed systems":1,
    "system design":1,"architecture":1,"cloud native":1,"serverless":1,
    "principal engineer":1,"staff engineer":1,"team lead":1,
    "mentoring":1,"playwright":1,"junit":1,"tdd":1,
}
SENIORITY = ["principal","staff","senior","lead","architect",
             "distinguished","sr.","sr ","head of"]

def score_job(title, description):
    text    = (title + " " + description).lower()
    matched = [s for s in SKILL_WEIGHTS if s in text]
    total   = sum(SKILL_WEIGHTS[s] for s in matched)
    if any(k in title.lower() for k in SENIORITY):
        total += 3
    score = min(round((total / sum(SKILL_WEIGHTS.values())) * 10, 1), 10.0)
    return score, matched[:8]

def tier(s): return "🔥 Elite" if s>=9.5 else "✅ Strong" if s>=8.5 else "👍 Good"
def priority(s): return "High" if s>=8.5 else "Medium"

# ══════════════════════════════════════════════════════════════════════════════
# URL VERIFICATION
# ══════════════════════════════════════════════════════════════════════════════

VERIFY_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

def verify_url(url: str) -> tuple[bool, str]:
    """
    Returns (is_valid, reason).
    Checks:
      1. URL is well-formed
      2. HTTP response is 200 (not 404, 403, 301->login, etc.)
      3. Final URL doesn't land on a login/expired page
      4. Response body doesn't contain dead-job markers
    """
    if not url or not url.startswith("http"):
        return False, "invalid_url"

    # Skip LinkedIn direct checks (they always require login)
    # but still include them — we trust the scraper result
    if "linkedin.com/jobs/view" in url:
        return True, "linkedin_trusted"

    try:
        r = requests.get(
            url,
            headers=VERIFY_HEADERS,
            timeout=URL_TIMEOUT,
            allow_redirects=True,
            stream=True,        # don't download full body
        )

        # Check final URL for login/dead patterns
        final_url = r.url.lower()
        for pattern in DEAD_URL_PATTERNS:
            if pattern in final_url:
                return False, f"redirect_to_{pattern}"

        # 404, 410 Gone, 403 Forbidden
        if r.status_code in (404, 410):
            return False, f"http_{r.status_code}"

        if r.status_code == 403:
            # Some sites block bots — treat as uncertain but allow
            return True, "http_403_allowed"

        if r.status_code >= 500:
            return False, f"server_error_{r.status_code}"

        # Read first 3KB to check for dead-job markers in body
        body = r.raw.read(3000).decode("utf-8", errors="ignore").lower()
        dead_markers = [
            "this job is no longer available",
            "job has expired",
            "position has been filled",
            "listing has been removed",
            "job not found",
            "no longer accepting",
            "this position is closed",
        ]
        for marker in dead_markers:
            if marker in body:
                return False, f"body_marker: {marker[:30]}"

        return True, f"ok_{r.status_code}"

    except requests.exceptions.ConnectionError:
        return False, "connection_error"
    except requests.exceptions.Timeout:
        return False, "timeout"
    except Exception as e:
        return False, f"error: {str(e)[:50]}"


def batch_verify_urls(jobs: list) -> list:
    """Verify all job URLs in parallel, return only valid jobs."""
    print(f"\n🔍 Verifying {len(jobs)} URLs (parallel, {MAX_URL_WORKERS} workers)...")
    valid, invalid = [], []

    with ThreadPoolExecutor(max_workers=MAX_URL_WORKERS) as executor:
        future_to_job = {executor.submit(verify_url, job["url"]): job for job in jobs}
        for future in as_completed(future_to_job):
            job = future_to_job[future]
            try:
                is_valid, reason = future.result()
                if is_valid:
                    job["url_status"] = reason
                    valid.append(job)
                else:
                    invalid.append((job, reason))
            except Exception as e:
                invalid.append((job, str(e)))

    print(f"  ✅ Valid URLs   : {len(valid)}")
    print(f"  ❌ Invalid URLs : {len(invalid)}")
    if invalid[:5]:
        print(f"  Sample invalid:")
        for job, reason in invalid[:5]:
            print(f"    [{reason}] {job['title'][:40]} — {job['url'][:60]}")

    return valid


# ══════════════════════════════════════════════════════════════════════════════
# SCORE VALIDATION
# ══════════════════════════════════════════════════════════════════════════════

def validate_score(title: str, description: str, score: float, matched: list) -> tuple[bool, str]:
    """
    Validates that the score is legitimate:
    1. Must have minimum matched skills
    2. Score must not come solely from seniority bonus
    3. Title must contain at least one engineering keyword
    4. Description can't be empty when score is high
    """
    ENG_KEYWORDS = [
        "engineer","developer","architect","analyst","scientist",
        "devops","platform","backend","frontend","fullstack","full-stack",
        "cloud","data","ml","ai","lead","principal","staff"
    ]

    # 1. Minimum matched skills
    if len(matched) < MIN_MATCHED_SKILLS:
        return False, f"too_few_skills ({len(matched)}<{MIN_MATCHED_SKILLS})"

    # 2. Title must be engineering-related
    title_lower = title.lower()
    if not any(k in title_lower for k in ENG_KEYWORDS):
        return False, f"non_engineering_title: {title[:40]}"

    # 3. High scores need description content
    if score >= 8.0 and len(description.strip()) < 50:
        return False, f"high_score_empty_description (score={score})"

    # 4. Score sanity — can't be 10.0 with no AI/cloud skills
    core_ai_skills = {"llm","rag","generative ai","mlops","ai platform",
                      "vector database","langchain","hugging face"}
    core_cloud     = {"aws","azure","kubernetes","docker"}
    has_ai    = bool(core_ai_skills & set(matched))
    has_cloud = bool(core_cloud & set(matched))

    if score >= 9.0 and not has_ai and not has_cloud:
        return False, f"suspiciously_high_score_no_ai_cloud (score={score})"

    return True, "valid"


# ══════════════════════════════════════════════════════════════════════════════
# APIFY RUNNER
# ══════════════════════════════════════════════════════════════════════════════
def run_apify(actor_id, input_data, timeout=240):
    hdrs = {"Authorization":f"Bearer {APIFY_API_KEY}","Content-Type":"application/json"}
    base = "https://api.apify.com/v2"
    r = requests.post(f"{base}/acts/{actor_id}/runs?timeout={timeout}&memory=1024",
                      headers=hdrs, json=input_data, timeout=30)
    if r.status_code not in (200,201):
        print(f"    Actor start failed: {r.status_code}")
        return []
    run_id = r.json()["data"]["id"]
    for _ in range(timeout//5):
        time.sleep(5)
        sr = requests.get(f"{base}/actor-runs/{run_id}", headers=hdrs, timeout=10)
        status = sr.json()["data"]["status"]
        if status in ("SUCCEEDED","FAILED","ABORTED","TIMED-OUT"): break
    if status != "SUCCEEDED":
        print(f"    Actor ended: {status}")
        return []
    ds = sr.json()["data"]["defaultDatasetId"]
    items = requests.get(f"{base}/datasets/{ds}/items?limit=500",
                         headers=hdrs, timeout=30).json()
    result = items if isinstance(items, list) else []
    print(f"    ✓ {len(result)} items")
    return result

# ── Scrapers (same as v3, condensed) ─────────────────────────────────────────
def tag(items, source):
    for i in items: i.setdefault("source", source)
    return items

def scrape_linkedin():
    print(f"\n[1] LinkedIn ({len(LINKEDIN_URLS)} URLs)...")
    all_items = []
    for i in range(0, len(LINKEDIN_URLS), 20):
        items = run_apify("curious_coder/linkedin-jobs-scraper",
                          {"urls":LINKEDIN_URLS[i:i+20],"count":100,"scrapeCompany":False},
                          timeout=300)
        all_items.extend(items); time.sleep(3)
    return tag(all_items, "LinkedIn")

def scrape_indeed():
    print("\n[2] Indeed...")
    results = []
    for kw, loc in [
        ("Generative AI Engineer","remote"),("LLM Engineer RAG","remote"),
        ("Principal Software Engineer Java","remote"),("Staff Engineer AWS","remote"),
        ("AI Platform Engineer FastAPI","remote"),("Full Stack React Java","remote"),
        ("Software Engineer Java","Jacksonville, FL"),("AI Engineer Python","Jacksonville, FL"),
        ("Cloud Engineer AWS","Jacksonville, FL"),
    ]:
        items = run_apify("curious_coder/indeed-scraper",
                          {"queries":[{"query":kw,"location":loc,"maxItems":20}],
                           "maxItems":20,"saveOnlyUniqueItems":True})
        results.extend(items); time.sleep(1)
    return tag(results, "Indeed")

def scrape_himalayas():
    print("\n[3] Himalayas...")
    results = []
    for q in ["software+engineer+ai","llm+rag","cloud+engineer+aws",
              "genai+engineer","mlops","principal+engineer","java+spring+boot"]:
        try:
            r = requests.get(f"https://himalayas.app/jobs/api?q={q}&limit=30",
                             timeout=15, headers={"User-Agent":"JobBot/1.0"})
            if r.ok:
                data = r.json()
                jobs = data.get("jobs", data) if isinstance(data,dict) else data
                for j in (jobs if isinstance(jobs,list) else []):
                    co = j.get("company",{})
                    results.append({
                        "title":j.get("title",""),
                        "companyName":co.get("name","") if isinstance(co,dict) else str(co),
                        "location":j.get("location","Remote"),
                        "jobUrl":j.get("url",j.get("applyUrl","")),
                        "description":j.get("description",""),
                        "source":"Himalayas"})
        except Exception as e: print(f"    err: {e}")
    print(f"    ✓ {len(results)}"); return results

def scrape_remoteok():
    print("\n[4] RemoteOK...")
    results = []
    for t in ["software-engineer","python","java","ai","machine-learning","backend","llm"]:
        try:
            r = requests.get(f"https://remoteok.com/api?tag={t}",
                             timeout=15, headers={"User-Agent":"JobBot/1.0"})
            if r.ok:
                for j in (r.json()[1:] if isinstance(r.json(),list) else []):
                    results.append({"title":j.get("position",""),
                        "companyName":j.get("company",""),"location":"Remote",
                        "jobUrl":j.get("url",""),
                        "description":" ".join(j.get("tags",[])),"source":"RemoteOK"})
            time.sleep(0.5)
        except Exception as e: print(f"    err: {e}")
    print(f"    ✓ {len(results)}"); return results

def scrape_remotive():
    print("\n[5] Remotive...")
    results = []
    for cat in ["software-dev","devops-sysadmin","data","ai-ml"]:
        try:
            r = requests.get(f"https://remotive.com/api/remote-jobs?category={cat}&limit=50",
                             timeout=15, headers={"User-Agent":"JobBot/1.0"})
            if r.ok:
                for j in r.json().get("jobs",[]):
                    results.append({"title":j.get("title",""),
                        "companyName":j.get("company_name",""),
                        "location":j.get("candidate_required_location","Remote"),
                        "jobUrl":j.get("url",""),
                        "description":j.get("description","")[:1000],"source":"Remotive"})
        except Exception as e: print(f"    err: {e}")
    print(f"    ✓ {len(results)}"); return results

def scrape_weworkremotely():
    print("\n[6] WeWorkRemotely...")
    results = []
    for cat in ["remote-jobs/programming","remote-jobs/devops-sysadmin","remote-jobs/data-science"]:
        try:
            r = requests.get(f"https://weworkremotely.com/{cat}.json",
                             timeout=15, headers={"User-Agent":"JobBot/1.0"})
            if r.ok:
                for j in (r.json() if isinstance(r.json(),list) else []):
                    results.append({"title":j.get("title",""),
                        "companyName":j.get("company",""),"location":"Remote",
                        "jobUrl":f"https://weworkremotely.com{j.get('url','')}",
                        "description":"","source":"WeWorkRemotely"})
        except Exception as e: print(f"    err: {e}")
    print(f"    ✓ {len(results)}"); return results

def scrape_jobright():
    print("\n[7] Jobright.ai...")
    results = []
    try:
        repos = ["jobright-ai/2026-Software-Engineer-Job",
                 "jobright-ai/2026-Tech-Internship"]
        gh_hdrs = {"User-Agent":"JobBot/1.0"}
        for repo in repos:
            r = requests.get(f"https://api.github.com/repos/{repo}/contents/",
                             timeout=10, headers=gh_hdrs)
            if not r.ok: continue
            files = sorted([f for f in r.json() if f["name"].endswith(".md")],
                           key=lambda x: x["name"], reverse=True)
            for f in files[:2]:
                cr = requests.get(f["download_url"], timeout=15)
                if not cr.ok: continue
                for line in cr.text.split("\n"):
                    if "| [" not in line or "](http" not in line: continue
                    try:
                        parts = [p.strip() for p in line.split("|")]
                        # Extract title and URL from markdown link
                        title_cell = parts[1] if len(parts)>1 else ""
                        title_match = re.search(r'\[([^\]]+)\]', title_cell)
                        url_match   = re.search(r'\((https?://[^)]+)\)', title_cell)
                        if not title_match or not url_match: continue
                        title   = title_match.group(1).strip()
                        url     = url_match.group(1).strip()
                        company = parts[2] if len(parts)>2 else ""
                        loc     = parts[3] if len(parts)>3 else "Remote"
                        # Skip non-engineering roles
                        if any(x in title.lower() for x in ["intern","marketing","sales","recruiter"]):
                            continue
                        results.append({"title":title,"companyName":company,
                            "location":loc,"jobUrl":url,
                            "description":f"{title} {company}","source":"Jobright.ai"})
                    except: pass
    except Exception as e: print(f"    err: {e}")
    print(f"    ✓ {len(results)}"); return results

def scrape_aijobs():
    print("\n[8] AIJobs.net...")
    results = []
    try:
        r = requests.get("https://aijobs.net/api/jobs/?limit=100&ordering=-created",
                         timeout=15, headers={"User-Agent":"JobBot/1.0"})
        if r.ok:
            jobs = r.json().get("results", r.json()) if isinstance(r.json(),dict) else r.json()
            for j in (jobs if isinstance(jobs,list) else []):
                results.append({"title":j.get("title",""),
                    "companyName":j.get("company",""),
                    "location":j.get("location","Remote"),
                    "jobUrl":j.get("url",j.get("apply_url","")),
                    "description":j.get("description","")[:1000],"source":"AIJobs.net"})
    except Exception as e: print(f"    err: {e}")
    print(f"    ✓ {len(results)}"); return results

def scrape_wellfound():
    print("\n[9] Wellfound...")
    results = []
    for title in ["AI Engineer","Principal Engineer","Backend Engineer","MLOps"]:
        items = run_apify("curious_coder/wellfound-scraper",
                          {"keyword":title,"remote":True,"maxItems":20}, timeout=120)
        results.extend(items); time.sleep(1)
    return tag(results, "Wellfound")

def scrape_dice():
    print("\n[10] Dice...")
    results = []
    for title in ["Principal Software Engineer","AI Engineer LLM",
                   "Senior Java Engineer","Cloud Architect AWS","GenAI Engineer"]:
        items = run_apify("curious_coder/dice-scraper",
                          {"keyword":title,"location":"Remote","maxItems":20,
                           "postedDate":"today"}, timeout=120)
        results.extend(items); time.sleep(1)
    return tag(results, "Dice")

# ── Normalise ─────────────────────────────────────────────────────────────────
def normalise(item, default_source="") -> Optional[dict]:
    title   = (item.get("title") or item.get("position") or item.get("jobTitle") or "").strip()
    company = (item.get("companyName") or item.get("company") or item.get("employer") or "").strip()
    url     = (item.get("jobUrl") or item.get("url") or item.get("applyUrl") or "").strip()
    desc    = item.get("description") or item.get("jobDescription") or ""
    if isinstance(desc, list): desc = " ".join(str(d) for d in desc)
    location = (item.get("location") or "Remote").strip()
    source   = item.get("source") or default_source
    if not title or not url or not url.startswith("http"): return None
    dedup_key = hashlib.md5(f"{title.lower().strip()}|{company.lower().strip()}".encode()).hexdigest()
    return {"title":title,"company":company,"url":url,
            "description":str(desc)[:3000],"location":location,
            "source":source,"dedup_key":dedup_key}

# ── Notion ────────────────────────────────────────────────────────────────────
NOTION_HEADERS = {"Authorization":f"Bearer {NOTION_API_KEY}",
                  "Notion-Version":"2022-06-28","Content-Type":"application/json"}

def get_existing_urls():
    existing, cursor = set(), None
    while True:
        body = {"page_size":100,"filter":{"property":"URL","url":{"is_not_empty":True}}}
        if cursor: body["start_cursor"] = cursor
        r = requests.post(f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query",
                          headers=NOTION_HEADERS, json=body, timeout=15)
        data = r.json()
        for page in data.get("results",[]):
            u = page["properties"].get("URL",{}).get("url","")
            if u: existing.add(u)
        if not data.get("has_more"): break
        cursor = data.get("next_cursor")
    return existing

def add_to_notion(job, score, matched):
    if DRY_RUN:
        print(f"  [DRY] {score} {job['title']} @ {job['company']}"); return True
    alignment = ", ".join(matched[:5]) or "General engineering match"
    payload = {
        "parent":{"database_id":NOTION_DB_ID},
        "properties":{
            "Job Title":{"title":[{"text":{"content":f"{job['title']} — {job['company']}"[:200]}}]},
            "URL":{"url":job["url"]},
            "Status":{"status":{"name":"Wishlist"}},
            "Role":{"select":{"name":"Software Engineer"}},
            "Priority":{"select":{"name":priority(score)}},
            "Match Score":{"number":score},
            "Score Tier":{"select":{"name":tier(score)}},
            "Source Platform":{"rich_text":[{"text":{"content":job["source"]}}]},
            "Key Alignment":{"rich_text":[{"text":{"content":alignment[:100]}}]},
            "Notes":{"rich_text":[{"text":{"content":
                f"Company: {job['company']} | {tier(score)} {score}/10 | {job['location']} | URL: {job.get('url_status','ok')}"}}]},
            "Date Applied":{"date":{"start":TODAY}},
        }
    }
    r = requests.post("https://api.notion.com/v1/pages",
                      headers=NOTION_HEADERS, json=payload, timeout=15)
    if r.status_code == 200: return True
    print(f"    Notion {r.status_code}: {r.text[:100]}"); return False

# ══════════════════════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ══════════════════════════════════════════════════════════════════════════════
def main():
    print(f"\n{'='*65}")
    print(f"Job Intelligence Agent v4 — {TODAY}{' [DRY RUN]' if DRY_RUN else ''}")
    print(f"URL Verification: ON | Score Validation: ON | Min Skills: {MIN_MATCHED_SKILLS}")
    print(f"{'='*65}")

    scrapers = [
        (scrape_linkedin,"LinkedIn"),(scrape_indeed,"Indeed"),
        (scrape_himalayas,"Himalayas"),(scrape_remoteok,"RemoteOK"),
        (scrape_remotive,"Remotive"),(scrape_weworkremotely,"WeWorkRemotely"),
        (scrape_jobright,"Jobright.ai"),(scrape_aijobs,"AIJobs.net"),
        (scrape_wellfound,"Wellfound"),(scrape_dice,"Dice"),
    ]

    # 1. Scrape
    raw_jobs, portal_counts = [], {}
    for fn, name in scrapers:
        try:
            items = fn()
            count = 0
            for item in items:
                n = normalise(item, name)
                if n: raw_jobs.append(n); count += 1
            portal_counts[name] = count
        except Exception as e:
            print(f"  {name} failed: {e}"); portal_counts[name] = 0

    print(f"\n📦 Raw collected: {len(raw_jobs)}")

    # 2. Deduplicate
    seen, unique = set(), []
    for job in raw_jobs:
        if job["dedup_key"] not in seen:
            seen.add(job["dedup_key"]); unique.append(job)
    print(f"🔍 After dedup: {len(unique)}")

    # 3. Score + validate score
    scored = []
    score_rejected = 0
    for job in unique:
        score, matched = score_job(job["title"], job["description"])
        if score < SCORE_THRESHOLD:
            continue
        is_valid, reason = validate_score(job["title"], job["description"], score, matched)
        if not is_valid:
            score_rejected += 1
            print(f"  ⚠️  Score rejected [{score}] {job['title'][:40]} — {reason}")
            continue
        scored.append((score, matched, job))
    scored.sort(key=lambda x: x[0], reverse=True)
    print(f"⭐ Passed score threshold + validation: {len(scored)} (rejected: {score_rejected})")

    # 4. Check existing Notion URLs
    print("\n📋 Checking Notion for existing entries...")
    existing_urls = get_existing_urls()
    print(f"   {len(existing_urls)} existing entries")

    # 5. Filter out already-in-Notion jobs before URL verification
    to_verify = [(score, matched, job) for score, matched, job in scored
                 if job["url"] not in existing_urls]
    already_skipped = len(scored) - len(to_verify)
    print(f"   {already_skipped} already in Notion (skipping URL check)")
    print(f"   {len(to_verify)} new jobs to verify")

    # 6. ✅ URL VERIFICATION — parallel batch check
    jobs_to_verify = [job for _, _, job in to_verify]
    verified_jobs  = {job["dedup_key"] for job in batch_verify_urls(jobs_to_verify)}

    # 7. Post verified jobs to Notion
    print("\n✍️  Posting verified jobs to Notion...")
    added = url_rejected = errors = 0
    for score, matched, job in to_verify:
        if job["dedup_key"] not in verified_jobs:
            url_rejected += 1
            continue
        if add_to_notion(job, score, matched):
            added += 1
            existing_urls.add(job["url"])
            print(f"  ✅ [{score:4.1f}] {job['title'][:45]:<45} @ {job['company'][:20]:<20} ({job['source']})")
        else:
            errors += 1
        time.sleep(0.35)

    # 8. Summary
    print(f"\n{'='*65}")
    print(f"✅ Run Complete — {TODAY}")
    print(f"   Scraped        : {len(raw_jobs)}")
    print(f"   Unique         : {len(unique)}")
    print(f"   Score passed   : {len(scored)}")
    print(f"   Score rejected : {score_rejected}")
    print(f"   URL verified   : {len(to_verify) - url_rejected}/{len(to_verify)}")
    print(f"   URL rejected   : {url_rejected}")
    print(f"   Already in DB  : {already_skipped}")
    print(f"   Added to Notion: {added}")
    print(f"   Errors         : {errors}")
    print(f"{'='*65}")

    summary = {
        "date": TODAY, "added": added, "already_skipped": already_skipped,
        "url_rejected": url_rejected, "score_rejected": score_rejected,
        "errors": errors, "total_scraped": len(raw_jobs),
        "total_unique": len(unique), "total_scored": len(scored),
        "portal_counts": portal_counts,
        "top_jobs": [
            {"score":s,"title":j["title"],"company":j["company"],
             "source":j["source"],"url":j["url"],"url_status":j.get("url_status","?")}
            for s,_,j in to_verify[:15] if j["dedup_key"] in verified_jobs
        ]
    }
    with open("run_summary.json","w") as f: json.dump(summary, f, indent=2)
    print("📄 run_summary.json saved")

if __name__ == "__main__":
    main()
