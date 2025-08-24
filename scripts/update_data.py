#!/usr/bin/env python3
import os, sys, csv, requests, datetime as dt
from urllib.parse import urlencode

APP_ID  = os.getenv("ADZUNA_APP_ID")
APP_KEY = os.getenv("ADZUNA_APP_KEY")
if not APP_ID or not APP_KEY:
    sys.exit("Missing ADZUNA_APP_ID/ADZUNA_APP_KEY")

COUNTRY = os.getenv("ADZUNA_COUNTRY", "au")
QUERY   = os.getenv("ADZUNA_QUERY", "data analyst")
MAX     = int(os.getenv("ADZUNA_MAX_RESULTS", "200"))
PAGE_SZ = 50

def fetch(page: int):
    params = {
        "app_id": APP_ID, "app_key": APP_KEY, "what": QUERY,
        "results_per_page": PAGE_SZ, "content-type": "application/json",
    }
    url = f"https://api.adzuna.com/v1/api/jobs/{COUNTRY}/search/{page}?{urlencode(params)}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json().get("results", [])

now = dt.datetime.utcnow().strftime("%Y%m%d")
os.makedirs("data", exist_ok=True)
path = f"data/adzuna_{COUNTRY}_{now}.csv"

fields = ["id","title","company","location","created","category","contract_time",
          "salary_is_predicted","salary_min","salary_max","redirect_url"]

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
