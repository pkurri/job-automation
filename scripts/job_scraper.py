"""
Job Intelligence Agent v3
Portals: LinkedIn, Indeed, Glassdoor, ZipRecruiter, Dice,
         Himalayas, RemoteOK, Wellfound, Remotive, WeWorkRemotely,
         Jobright.ai, AIJobs.net, AIJobs.com, Techjobsforgood,
         Cord.co, EuroRemotely, Nodesk, NLPeople, MLJobs,
         Getwork, Pyjama Jobs, Lemon.io, Arc.dev, Otta/Greenhouse
40+ job titles from resume | Remote + Jacksonville FL | Last 24h
"""

import os, json, time, hashlib, requests
from datetime import datetime, timezone
from typing import Optional

APIFY_API_KEY   = os.environ["APIFY_API_KEY"]
NOTION_API_KEY  = os.environ["NOTION_API_KEY"]
NOTION_DB_ID    = os.environ["NOTION_DB_ID"]
SCORE_THRESHOLD = float(os.getenv("SCORE_THRESHOLD", "7.0"))
DRY_RUN         = os.getenv("DRY_RUN", "false").lower() == "true"
TODAY           = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# ── 40 job titles from resume ─────────────────────────────────────────────────
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
            kw  = requests.utils.quote(title)
            l   = requests.utils.quote(loc)
            wt  = "f_WT=2" if "remote" in loc.lower() else "f_WT=1,2,3"
            urls.append(
                f"https://www.linkedin.com/jobs/search/?keywords={kw}"
                f"&location={l}&f_TPR=r86400&{wt}&position=1&pageNum=0"
            )
    return list(dict.fromkeys(urls))

LINKEDIN_URLS = build_linkedin_urls()

# ── Skill weights ─────────────────────────────────────────────────────────────
SKILL_WEIGHTS = {
    "generative ai":3,"gen ai":3,"llm":3,"large language model":3,
    "rag":3,"retrieval augmented":3,"pgvector":3,"vector database":3,
    "hugging face":3,"langchain":3,"llamaindex":3,"langgraph":3,
    "prompt engineering":3,"bert":3,"openai":3,"agentic ai":3,
    "ai agent":3,"mlops":3,"ai platform":3,"foundation model":3,
    "fine tuning":3,"embeddings":3,"semantic search":3,
    "fastapi":2,"spring boot":2,"java 17":2,"java":2,"python":2,
    "pydantic":2,"hibernate":2,"jpa":2,"microservices":2,"rest api":2,
    "graphql":2,"grpc":2,"kafka":2,"spark":2,"scala":2,
    "node.js":2,"nodejs":2,"typescript":2,"express":2,
    "react":2,"redux":2,"vite":2,"next.js":2,
    "aws":2,"azure":2,"gcp":2,"kubernetes":2,"k8s":2,"docker":2,
    "lambda":2,"ec2":2,"s3":2,"api gateway":2,"github actions":2,
    "ci/cd":2,"openshift":2,"jenkins":2,"terraform":2,"helm":2,
    "elasticsearch":2,"postgresql":2,"mongodb":2,"redis":2,
    "hadoop":2,"hive":2,"data pipeline":2,"etl":2,
    "oauth2":1,"jwt":1,"sso":1,"opentelemetry":1,"prometheus":1,
    "grafana":1,"splunk":1,"datadog":1,"application insights":1,
    "distributed systems":1,"system design":1,"architecture":1,
    "full stack":1,"cloud native":1,"serverless":1,"event driven":1,
    "principal engineer":1,"staff engineer":1,"team lead":1,
    "mentoring":1,"playwright":1,"junit":1,"tdd":1,
}
SENIORITY = ["principal","staff","senior","lead","architect",
             "distinguished","sr.","sr ","head of"]

def score_job(title, description):
    text    = (title + " " + description).lower()
    total   = sum(w for s,w in SKILL_WEIGHTS.items() if s in text)
    matched = [s for s in SKILL_WEIGHTS if s in text]
    if any(k in title.lower() for k in SENIORITY): total += 3
    score = min(round((total / sum(SKILL_WEIGHTS.values())) * 10, 1), 10.0)
    return score, matched[:6]

def tier(s): return "🔥 Elite" if s>=9.5 else "✅ Strong" if s>=8.5 else "👍 Good"
def priority(s): return "High" if s>=8.5 else "Medium"

# ── Apify runner ──────────────────────────────────────────────────────────────
def run_apify(actor_id, input_data, timeout=240):
    hdrs = {"Authorization":f"Bearer {APIFY_API_KEY}","Content-Type":"application/json"}
    base = "https://api.apify.com/v2"
    r = requests.post(f"{base}/acts/{actor_id}/runs?timeout={timeout}&memory=1024",
                      headers=hdrs, json=input_data, timeout=30)
    if r.status_code not in (200,201): return []
    run_id = r.json()["data"]["id"]
    for _ in range(timeout//5):
        time.sleep(5)
        sr = requests.get(f"{base}/actor-runs/{run_id}", headers=hdrs, timeout=10)
        status = sr.json()["data"]["status"]
        if status in ("SUCCEEDED","FAILED","ABORTED","TIMED-OUT"): break
    if status != "SUCCEEDED": return []
    ds = sr.json()["data"]["defaultDatasetId"]
    items = requests.get(f"{base}/datasets/{ds}/items?limit=500",
                         headers=hdrs, timeout=30).json()
    print(f"    ✓ {len(items)} items")
    return items if isinstance(items, list) else []

# ── Scrapers ──────────────────────────────────────────────────────────────────
def tag(items, source):
    for i in items:
        i.setdefault("source", source)
    return items

def scrape_linkedin():
    print(f"\n[1] LinkedIn ({len(LINKEDIN_URLS)} URLs)...")
    all_items = []
    for i in range(0, len(LINKEDIN_URLS), 20):
        items = run_apify("curious_coder/linkedin-jobs-scraper",
                          {"urls": LINKEDIN_URLS[i:i+20], "count":100,
                           "scrapeCompany":False}, timeout=300)
        all_items.extend(items); time.sleep(3)
    return tag(all_items, "LinkedIn")

def scrape_indeed():
    print("\n[2] Indeed...")
    results = []
    queries = [
        ("Generative AI Engineer Python","remote"),
        ("LLM Engineer RAG pgvector","remote"),
        ("Principal Software Engineer Java","remote"),
        ("Staff Engineer Spring Boot AWS","remote"),
        ("AI Platform Engineer FastAPI","remote"),
        ("Cloud Architect AWS Kubernetes","remote"),
        ("Senior Full Stack React Java","remote"),
        ("Software Engineer Java","Jacksonville, FL"),
        ("AI ML Engineer Python","Jacksonville, FL"),
        ("Cloud Engineer AWS","Jacksonville, FL"),
    ]
    for kw, loc in queries:
        items = run_apify("curious_coder/indeed-scraper",
                          {"queries":[{"query":kw,"location":loc,"maxItems":20}],
                           "maxItems":20,"saveOnlyUniqueItems":True})
        results.extend(items); time.sleep(1)
    return tag(results, "Indeed")

def scrape_glassdoor():
    print("\n[3] Glassdoor...")
    results = []
    for title in TITLES[:12]:
        items = run_apify("bebity/glassdoor-jobs-scraper",
                          {"keyword":title,"location":"Remote","maxItems":10,
                           "timePosted":"last24Hours"}, timeout=120)
        results.extend(items); time.sleep(1)
    return tag(results, "Glassdoor")

def scrape_ziprecruiter():
    print("\n[4] ZipRecruiter...")
    results = []
    for title in TITLES[:10]:
        items = run_apify("vaclavrut/ziprecruiter-jobs-scraper",
                          {"searchTerm":title,"location":"Remote USA",
                           "datePosted":"today","maxItems":15}, timeout=120)
        results.extend(items); time.sleep(1)
    return tag(results, "ZipRecruiter")

def scrape_dice():
    print("\n[5] Dice...")
    results = []
    for title in ["Principal Software Engineer","AI Engineer LLM",
                   "Senior Java Engineer","Cloud Architect AWS",
                   "GenAI Engineer Python","MLOps Engineer"]:
        items = run_apify("curious_coder/dice-scraper",
                          {"keyword":title,"location":"Remote","maxItems":20,
                           "postedDate":"today"}, timeout=120)
        results.extend(items); time.sleep(1)
    return tag(results, "Dice")

def scrape_himalayas():
    print("\n[6] Himalayas...")
    results = []
    for q in ["software+engineer+ai","java+spring+boot","llm+rag+engineer",
              "cloud+engineer+aws","genai+engineer","mlops+engineer",
              "principal+engineer","data+engineer+kafka"]:
        try:
            r = requests.get(f"https://himalayas.app/jobs/api?q={q}&limit=30",
                             timeout=15, headers={"User-Agent":"JobBot/1.0"})
            if r.ok:
                jobs = r.json().get("jobs", r.json()) if isinstance(r.json(),dict) else r.json()
                for j in (jobs if isinstance(jobs,list) else []):
                    co = j.get("company",{})
                    results.append({"title":j.get("title",""),
                        "companyName":co.get("name","") if isinstance(co,dict) else str(co),
                        "location":j.get("location","Remote"),
                        "jobUrl":j.get("url",j.get("applyUrl","")),
                        "description":j.get("description",""),
                        "source":"Himalayas"})
        except Exception as e: print(f"    Himalayas err: {e}")
    print(f"    ✓ {len(results)}"); return results

def scrape_remoteok():
    print("\n[7] RemoteOK...")
    results = []
    for tag_name in ["software-engineer","python","java","react","devops",
                     "cloud","ai","machine-learning","backend","llm"]:
        try:
            r = requests.get(f"https://remoteok.com/api?tag={tag_name}",
                             timeout=15, headers={"User-Agent":"JobBot/1.0"})
            if r.ok:
                for j in (r.json()[1:] if isinstance(r.json(),list) else []):
                    results.append({"title":j.get("position",""),
                        "companyName":j.get("company",""),"location":"Remote",
                        "jobUrl":j.get("url",""),
                        "description":" ".join(j.get("tags",[])),
                        "source":"RemoteOK"})
            time.sleep(1)
        except Exception as e: print(f"    RemoteOK err: {e}")
    print(f"    ✓ {len(results)}"); return results

def scrape_wellfound():
    print("\n[8] Wellfound...")
    results = []
    for title in ["AI Engineer","Principal Engineer","Full Stack","Backend Engineer","MLOps"]:
        items = run_apify("curious_coder/wellfound-scraper",
                          {"keyword":title,"remote":True,"maxItems":20}, timeout=120)
        results.extend(items); time.sleep(1)
    return tag(results, "Wellfound")

def scrape_remotive():
    print("\n[9] Remotive...")
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
                        "description":j.get("description","")[:1000],
                        "source":"Remotive"})
        except Exception as e: print(f"    Remotive err: {e}")
    print(f"    ✓ {len(results)}"); return results

def scrape_weworkremotely():
    print("\n[10] WeWorkRemotely...")
    results = []
    for cat in ["remote-jobs/programming","remote-jobs/devops-sysadmin","remote-jobs/data-science"]:
        try:
            r = requests.get(f"https://weworkremotely.com/{cat}.json",
                             timeout=15, headers={"User-Agent":"JobBot/1.0"})
            if r.ok:
                for j in (r.json() if isinstance(r.json(),list) else []):
                    results.append({"title":j.get("title",""),
                        "companyName":j.get("company",""),
                        "location":j.get("region","Remote"),
                        "jobUrl":f"https://weworkremotely.com{j.get('url','')}",
                        "description":j.get("listing_type",""),
                        "source":"WeWorkRemotely"})
        except Exception as e: print(f"    WWR err: {e}")
    print(f"    ✓ {len(results)}"); return results

def scrape_jobright():
    """Jobright.ai — AI-powered job aggregator (400k+ jobs/day)."""
    print("\n[11] Jobright.ai...")
    results = []
    try:
        # Jobright GitHub repos contain daily-updated job JSON lists
        gh_repos = [
            "jobright-ai/2026-Software-Engineer-Job",
            "jobright-ai/2026-Tech-Internship",
        ]
        for repo in gh_repos:
            r = requests.get(
                f"https://api.github.com/repos/{repo}/contents/",
                timeout=10, headers={"User-Agent":"JobBot/1.0"}
            )
            if r.ok:
                files = [f for f in r.json() if f["name"].endswith(".md")]
                for f in files[:3]:  # last 3 days
                    content_r = requests.get(f["download_url"], timeout=10)
                    if content_r.ok:
                        lines = content_r.text.split("\n")
                        for line in lines:
                            if "| [" in line and "](http" in line:
                                try:
                                    parts = line.split("|")
                                    title   = parts[1].strip().strip("**").replace("[","").split("]")[0] if len(parts)>1 else ""
                                    company = parts[2].strip() if len(parts)>2 else ""
                                    loc     = parts[3].strip() if len(parts)>3 else "Remote"
                                    url_part = parts[4] if len(parts)>4 else ""
                                    import re
                                    url_match = re.search(r'\(https?://[^)]+\)', url_part)
                                    url = url_match.group(0)[1:-1] if url_match else ""
                                    if title and url:
                                        results.append({"title":title,"companyName":company,
                                            "location":loc,"jobUrl":url,
                                            "description":f"{title} {company}",
                                            "source":"Jobright.ai"})
                                except: pass
    except Exception as e:
        print(f"    Jobright.ai err: {e}")
    print(f"    ✓ {len(results)}"); return results

def scrape_aijobs():
    """AIJobs.net — dedicated AI/ML job board with free API."""
    print("\n[12] AIJobs.net...")
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
                    "description":j.get("description","")[:1000],
                    "source":"AIJobs.net"})
    except Exception as e: print(f"    AIJobs.net err: {e}")
    print(f"    ✓ {len(results)}"); return results

def scrape_aijobs_com():
    """AIJobs.com — official AI talent job board."""
    print("\n[13] AIJobs.com...")
    results = []
    try:
        for title in ["AI Engineer","Machine Learning Engineer","LLM Engineer",
                      "Principal Engineer","Data Engineer"]:
            r = requests.get(
                f"https://www.aijobs.com/api/jobs?q={requests.utils.quote(title)}&limit=20",
                timeout=15, headers={"User-Agent":"JobBot/1.0"}
            )
            if r.ok:
                jobs = r.json() if isinstance(r.json(),list) else r.json().get("jobs",[])
                for j in jobs:
                    results.append({"title":j.get("title",""),
                        "companyName":j.get("company",""),
                        "location":j.get("location","Remote"),
                        "jobUrl":j.get("url",""),
                        "description":j.get("description","")[:1000],
                        "source":"AIJobs.com"})
    except Exception as e: print(f"    AIJobs.com err: {e}")
    print(f"    ✓ {len(results)}"); return results

def scrape_nlpeople():
    """NLPeople — NLP/AI specialist jobs."""
    print("\n[14] NLPeople...")
    results = []
    try:
        r = requests.get("https://nlppeople.com/api/jobs/?limit=50",
                         timeout=15, headers={"User-Agent":"JobBot/1.0"})
        if r.ok:
            jobs = r.json() if isinstance(r.json(),list) else r.json().get("results",[])
            for j in jobs:
                results.append({"title":j.get("title",""),
                    "companyName":j.get("company",""),
                    "location":j.get("location","Remote"),
                    "jobUrl":j.get("url",""),
                    "description":j.get("description","")[:500],
                    "source":"NLPeople"})
    except Exception as e: print(f"    NLPeople err: {e}")
    print(f"    ✓ {len(results)}"); return results

def scrape_getwork():
    """Getwork — real-time job aggregator from company career pages."""
    print("\n[15] Getwork...")
    results = []
    for kw in ["principal software engineer","ai engineer","llm engineer","cloud engineer"]:
        try:
            r = requests.get(
                f"https://getwork.com/api/jobs?q={requests.utils.quote(kw)}&remote=true&limit=20",
                timeout=15, headers={"User-Agent":"JobBot/1.0"}
            )
            if r.ok:
                jobs = r.json().get("jobs", r.json()) if isinstance(r.json(),dict) else r.json()
                for j in (jobs if isinstance(jobs,list) else []):
                    results.append({"title":j.get("title",""),
                        "companyName":j.get("company",""),
                        "location":j.get("location","Remote"),
                        "jobUrl":j.get("url",j.get("job_url","")),
                        "description":j.get("description","")[:500],
                        "source":"Getwork"})
        except Exception as e: print(f"    Getwork err ({kw}): {e}")
    print(f"    ✓ {len(results)}"); return results

def scrape_arc():
    """Arc.dev — remote developer job board."""
    print("\n[16] Arc.dev...")
    results = []
    try:
        for kw in ["principal engineer","senior ai engineer","full stack remote","java spring boot"]:
            r = requests.get(
                f"https://arc.dev/api/jobs?q={requests.utils.quote(kw)}&limit=20",
                timeout=15, headers={"User-Agent":"JobBot/1.0"}
            )
            if r.ok:
                jobs = r.json().get("jobs", []) if isinstance(r.json(),dict) else r.json()
                for j in (jobs if isinstance(jobs,list) else []):
                    results.append({"title":j.get("title",""),
                        "companyName":j.get("company",""),
                        "location":"Remote",
                        "jobUrl":j.get("url",""),
                        "description":j.get("description","")[:500],
                        "source":"Arc.dev"})
    except Exception as e: print(f"    Arc.dev err: {e}")
    print(f"    ✓ {len(results)}"); return results

def scrape_pyjama_jobs():
    """Pyjama Jobs — remote-first job board."""
    print("\n[17] Pyjama Jobs...")
    results = []
    try:
        r = requests.get("https://pajamajobs.com/api/jobs?limit=50&category=engineering",
                         timeout=15, headers={"User-Agent":"JobBot/1.0"})
        if r.ok:
            jobs = r.json() if isinstance(r.json(),list) else r.json().get("jobs",[])
            for j in (jobs if isinstance(jobs,list) else []):
                results.append({"title":j.get("title",""),
                    "companyName":j.get("company",""),
                    "location":"Remote","jobUrl":j.get("url",""),
                    "description":j.get("description","")[:500],
                    "source":"Pyjama Jobs"})
    except Exception as e: print(f"    Pyjama Jobs err: {e}")
    print(f"    ✓ {len(results)}"); return results

# ── Normalise ─────────────────────────────────────────────────────────────────
def normalise(item, default_source=""):
    title   = (item.get("title") or item.get("position") or item.get("jobTitle") or "").strip()
    company = (item.get("companyName") or item.get("company") or item.get("employer") or "").strip()
    url     = (item.get("jobUrl") or item.get("url") or item.get("applyUrl") or "").strip()
    desc    = item.get("description") or item.get("jobDescription") or ""
    if isinstance(desc, list): desc = " ".join(str(d) for d in desc)
    location = (item.get("location") or item.get("formattedLocation") or "Remote").strip()
    source   = item.get("source") or default_source
    if not title or not url: return None
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
                f"Company: {job['company']} | {tier(score)} {score}/10 | {job['location']}"}}]},
            "Date Applied":{"date":{"start":TODAY}},
        }
    }
    r = requests.post("https://api.notion.com/v1/pages",
                      headers=NOTION_HEADERS, json=payload, timeout=15)
    if r.status_code == 200: return True
    print(f"    Notion {r.status_code}: {r.text[:100]}"); return False

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*65}")
    print(f"Job Intelligence Agent v3 — {TODAY}{' [DRY RUN]' if DRY_RUN else ''}")
    print(f"Portals: 17 | Titles: {len(TITLES)} | Locations: {len(LOCATIONS)}")
    print(f"{'='*65}")

    scrapers = [
        (scrape_linkedin,"LinkedIn"),(scrape_indeed,"Indeed"),
        (scrape_glassdoor,"Glassdoor"),(scrape_ziprecruiter,"ZipRecruiter"),
        (scrape_dice,"Dice"),(scrape_himalayas,"Himalayas"),
        (scrape_remoteok,"RemoteOK"),(scrape_wellfound,"Wellfound"),
        (scrape_remotive,"Remotive"),(scrape_weworkremotely,"WeWorkRemotely"),
        (scrape_jobright,"Jobright.ai"),(scrape_aijobs,"AIJobs.net"),
        (scrape_aijobs_com,"AIJobs.com"),(scrape_nlpeople,"NLPeople"),
        (scrape_getwork,"Getwork"),(scrape_arc,"Arc.dev"),
        (scrape_pyjama_jobs,"Pyjama Jobs"),
    ]

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

    print(f"\nRaw: {len(raw_jobs)}")
    seen, unique = set(), []
    for job in raw_jobs:
        if job["dedup_key"] not in seen:
            seen.add(job["dedup_key"]); unique.append(job)
    print(f"Unique: {len(unique)}")

    scored = [(score_job(j["title"],j["description"]),j) for j in unique]
    scored = [(s,m,j) for (s,m),j in scored if s >= SCORE_THRESHOLD]
    scored.sort(key=lambda x: x[0], reverse=True)
    print(f"Scored >= {SCORE_THRESHOLD}: {len(scored)}")

    print("\nChecking Notion...")
    existing = get_existing_urls()
    print(f"Existing: {len(existing)}")

    added = skipped = errors = 0
    for score, matched, job in scored:
        if job["url"] in existing: skipped += 1; continue
        if add_to_notion(job, score, matched):
            added += 1; existing.add(job["url"])
            print(f"  [{score:4.1f}] {job['title'][:45]:<45} @ {job['company'][:20]:<20} ({job['source']})")
        else: errors += 1
        time.sleep(0.35)

    print(f"\nDone — Scraped:{len(raw_jobs)} Unique:{len(unique)} Scored:{len(scored)} Added:{added} Skipped:{skipped}")

    summary = {"date":TODAY,"added":added,"skipped":skipped,"errors":errors,
               "total_scraped":len(raw_jobs),"total_unique":len(unique),
               "total_scored":len(scored),"portal_counts":portal_counts,
               "top_jobs":[{"score":s,"title":j["title"],"company":j["company"],
                             "source":j["source"],"url":j["url"]}
                            for s,_,j in scored[:15]]}
    with open("run_summary.json","w") as f: json.dump(summary,f,indent=2)
    print("run_summary.json saved")

if __name__ == "__main__":
    main()
