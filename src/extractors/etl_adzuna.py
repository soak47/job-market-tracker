import os, argparse, requests
from datetime import datetime
from dotenv import load_dotenv
from typing import List
from src.common.models import Job
from src.common.db import upsert_jobs, insert_skills, init_db, get_conn  # add insert_skills in db if missing
from src.common.skills import extract_skills

# --- if your db.py doesn't have insert_skills yet, use this fallback ---
try:
    insert_skills
except NameError:
    def insert_skills(hits):
        conn = get_conn()
        cur = conn.cursor()
        cur.executemany('INSERT INTO skills (job_id, skill) VALUES (?, ?)',
                        [(h["job_id"], h["skill"]) for h in hits])
        conn.commit(); conn.close()
# ----------------------------------------------------------------------

load_dotenv()
APP_ID = os.getenv("ADZUNA_APP_ID")
APP_KEY = os.getenv("ADZUNA_APP_KEY")
BASE = "https://api.adzuna.com/v1/api/jobs/{country}/search/{page}"

def fetch(country: str, query: str, where: str, page: int) -> dict:
    params = {
        "app_id": APP_ID, "app_key": APP_KEY,
        "what": query, "where": where,
        "results_per_page": 50, "content-type": "application/json"
    }
    r = requests.get(BASE.format(country=country.lower(), page=page), params=params, timeout=20)
    r.raise_for_status()
    return r.json()

def parse(payload: dict) -> List[Job]:
    jobs = []
    for it in payload.get("results", []):
        jid = str(it.get("id"))
        posted = it.get("created")
        if posted:
            posted = datetime.fromisoformat(posted.replace("Z","+00:00")).date().isoformat()
        smin, smax = it.get("salary_min"), it.get("salary_max")
        savg = (smin + smax)/2 if smin and smax else None
        jobs.append(Job(
            id=jid,
            title=it.get("title") or "",
            company=(it.get("company") or {}).get("display_name"),
            location=(it.get("location") or {}).get("display_name"),
            source="adzuna",
            posted_date=posted,
            description=it.get("description"),
            url=it.get("redirect_url"),
            salary_min=smin, salary_max=smax, salary_avg=savg,
            currency=it.get("salary_currency"),
            raw=it
        ))
    return jobs

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--country", default="AU")
    ap.add_argument("--query", required=True)
    ap.add_argument("--where", required=True)
    ap.add_argument("--pages", type=int, default=1)
    args = ap.parse_args()

    if not (APP_ID and APP_KEY):
        raise SystemExit("Missing ADZUNA_APP_ID / ADZUNA_APP_KEY in .env")

    init_db()
    all_jobs = []
    for p in range(1, args.pages+1):
        all_jobs += parse(fetch(args.country, args.query, args.where, p))

    if not all_jobs:
        print("No jobs found."); return

    upsert_jobs(all_jobs)
    hits = []
    for j in all_jobs:
        for s in set(extract_skills(f"{j.title} {j.description}")):
            hits.append({"job_id": j.id, "skill": s})
    if hits: insert_skills(hits)

    print(f"Ingested {len(all_jobs)} jobs • Extracted {len(hits)} skill hits")

if __name__ == "__main__":
    main()
