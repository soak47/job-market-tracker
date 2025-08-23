import sqlite3
from pathlib import Path
from typing import Iterable
from src.common.models import Job

DB_PATH = Path(__file__).resolve().parents[2] / "data" / "jobs.db"

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        '''CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            title TEXT,
            company TEXT,
            location TEXT,
            source TEXT,
            posted_date TEXT,
            description TEXT,
            url TEXT,
            salary_min REAL,
            salary_max REAL,
            salary_avg REAL,
            currency TEXT
        );'''
    )
    cur.execute(
        '''CREATE TABLE IF NOT EXISTS skills (
            job_id TEXT,
            skill TEXT
        );'''
    )
    conn.commit()
    conn.close()

def upsert_jobs(jobs: Iterable[Job]):
    conn = get_conn()
    cur = conn.cursor()
    for j in jobs:
        cur.execute(
            '''INSERT OR REPLACE INTO jobs
               (id,title,company,location,source,posted_date,description,url,
                salary_min,salary_max,salary_avg,currency)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''',
            (j.id, j.title, j.company, j.location, j.source, j.posted_date, j.description, j.url,
             j.salary_min, j.salary_max, j.salary_avg, j.currency)
        )
    conn.commit()
    conn.close()

def insert_skills(hits: Iterable[dict]):
    conn = get_conn()
    cur = conn.cursor()
    cur.executemany('INSERT INTO skills (job_id, skill) VALUES (?,?)',
                    [(h["job_id"], h["skill"]) for h in hits])
    conn.commit()
    conn.close()
