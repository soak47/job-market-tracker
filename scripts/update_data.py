#!/usr/bin/env python3
import os, sys, csv, requests, datetime as dt
from urllib.parse import urlencode

APP_ID  = os.getenv("ADZUNA_APP_ID")
APP_KEY = os.getenv("ADZUNA_APP_KEY")
if not APP_ID or not APP_KEY:
    sys.exit("Missing ADZUNA_APP_ID/ADZUNA_APP_KEY")

COUNTRY = os.getenv("ADZUNA_COUNTRY", "au")
QUERIES = [q.strip() for q in os.getenv("ADZUNA_QUERY", "data analyst").split(",") if q.strip()]
MAX_PER = int(os.getenv("ADZUNA_MAX_RESULTS", "500"))
PAGE_SZ = 50

def fetch(page, term):
    params = {"app_id": APP_ID, "app_key": APP_KEY, "what": term,
              "results_per_page": PAGE_SZ, "content-type":"application/json"}
    url = f"https://api.adzuna.com/v1/api/jobs/{COUNTRY}/search/{page}?{urlencode(params)}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json().get("results", [])

now = dt.datetime.utcnow().strftime("%Y%m%d")
os.makedirs("data", exist_ok=True)
path = f"data/adzuna_{COUNTRY}_{now}.csv"

fields = ["id","title","company","location","created","category","contract_time",
          "salary_is_predicted","salary_min","salary_max","redirect_url", "redirect_url", "search_term"]
seen = set()

with open(path, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=fields); w.writeheader()
    for term in QUERIES:
        rows = 0; page = 1
        while rows < MAX_PER:
            res = fetch(page, term)
            if not res: break
            for j in res:
                jid = j.get("id")
                if jid in seen: 
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
                rows += 1
                if rows >= MAX_PER: break
            page += 1

rows = 0; page = 1
with open(path, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=fields); w.writeheader()
    while rows < MAX:
        res = fetch(page)
        if not res: break
        for j in res:
            w.writerow({
                "id": j.get("id"),
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
            })
            rows += 1
            if rows >= MAX: break
        page += 1

print(f"Saved {rows} rows → {path}")
