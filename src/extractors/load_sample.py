import json
from pathlib import Path
from src.common.db import get_conn, init_db
from src.common.skills import extract_skills

SAMPLE = Path(__file__).resolve().parents[2] / "data" / "sample_adzuna.json"

def main():
    init_db()
    payload = json.loads(SAMPLE.read_text())
    conn = get_conn()
    cur = conn.cursor()

    jobs = payload.get("results", [])
    for item in jobs:
        jid = str(item["id"])
        title = item.get("title","")
        company = (item.get("company") or {}).get("display_name")
        location = (item.get("location") or {}).get("display_name")
        posted = (item.get("created") or "")[:10]
        desc = item.get("description")
        url = item.get("redirect_url")
        smin = item.get("salary_min")
        smax = item.get("salary_max")
        savg = (smin + smax)/2 if smin and smax else None
        cur.execute('''INSERT OR REPLACE INTO jobs
            (id,title,company,location,source,posted_date,description,url,salary_min,salary_max,salary_avg,currency)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''',
            (jid,title,company,location,"sample",posted,desc,url,smin,smax,savg,item.get("salary_currency"))
        )
        for skill in extract_skills(f"{title} {desc}"):
            cur.execute("INSERT INTO skills (job_id, skill) VALUES (?,?)", (jid, skill))

    conn.commit()
    conn.close()
    print(f"Loaded {len(jobs)} sample jobs.")

if __name__ == "__main__":
    main()
