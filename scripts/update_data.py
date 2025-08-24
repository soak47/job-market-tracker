#!/usr/bin/env python3
import os, sys, csv, requests, datetime as dt
from urllib.parse import urlencode

APP_ID  = os.getenv("ADZUNA_APP_ID")
APP_KEY = os.getenv("ADZUNA_APP_KEY")
if not APP_ID or not APP_KEY:
    sys.exit("Missing ADZUNA_APP_ID/ADZUNA_APP_KEY")

COUNTRY = (os.getenv("ADZUNA_COUNTRY", "au") or "au").lower()

# Support either ADZUNA_QUERIES (comma-separated) or ADZUNA_QUERY (single)
raw = os.getenv("ADZUNA_QUERIES") or os.getenv("ADZUNA_QUERY", "data analyst")
QUERIES = [q.strip() for q in raw.split(",") if q.strip()]

MAX_PER = int(os.getenv("ADZUNA_MAX_RESULTS", "200"))
PAGE_SZ = 50

def fetch(term: str, page: int):
    params = {
        "app_id": APP_ID,
        "app_key": APP_KEY,
        "what": term,
        "results_per_page": PAGE_SZ,
        "content-type": "application/json",
    }
    url = f"https://api.adzuna.com/v1/api/jobs/{COUNTRY}/search/{page}?{urlencode(params)}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json().get("results", [])

now = dt.datetime.utcnow().strftime("%Y%m%d")
os.makedirs("data", exist_ok=True)
path = f"data/adzuna_{COUNTRY}_{now}.csv"

fields = [
    "id","title","company","location","created","category","contract_time",
    "salary_is_predicted","salary_min","salary_max","redirect_url","search_term"
]

seen, total = set(), 0
with open(path, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=fields)
    w.writeheader()

    for term in QUERIES:
        got, page = 0, 1
        while got < MAX_PER:
            results = fetch(term, page)
            if not results:
                break
            for j in results:
                jid = j.get("id")
                if not jid or jid in seen:
                    continue
                seen.add(jid)
                w.writerow({
                    "id": jid,
                    "title": j.get("title"),
                    "company": (j.get("company") or {}).get("display_name"),
                    "location": (j.get("location") or {}).get("display_name"),
                    "created": j.get("created"),
                    "category": (j.get("category") or {}).get("label"),
                    "contract_time": j.get("contract_time"),
                    "salary_is_predicted": j.get("salary_is_predicted"),
                    "salary_min": j.get("salary_min"),
                    "salary_max": j.get("salary_max"),
                    "redirect_url": j.get("redirect_url"),
                    "search_term": term,
                })
                got += 1
                total += 1
                if got >= MAX_PER:
                    break
            page += 1

print(f"Saved {total} rows from {len(QUERIES)} term(s) -> {path}")
