📊 A lightweight dashboard that tracks job listings in Australia, showing skills frequency, salary distributions, and listing trends by state. Built with Streamlit, SQLite, and Python.

Features

ETL pipeline using the Adzuna API (trial/free tier).

Data stored in SQLite (data/jobs.db).

Cleaning steps:

Normalize messy location names to cities/states.

Handle duplicate postings.

Fix shorthand salaries (e.g. 175 → 175,000).

Interactive dashboard:

Filter by city, role keyword, and source.

View top skills (from a configurable keyword list).

Explore salary distributions and median salaries by city.

State-level bar charts + weekly trends.


QUICKSTART

# create virtual environment
python -m venv .venv
.venv\Scripts\activate   # on Windows
source .venv/bin/activate # on Mac/Linux

# install requirements
pip install -r requirements.txt

# set up DB
python -m src.setup_db

# run ETL pulls (example)
python -m src.extractors.etl_adzuna --country AU --query "data analyst" --where "Perth" --pages 2
python -m src.extractors.etl_adzuna --country AU --query "data analyst" --where "Melbourne" --pages 2

# launch dashboard
streamlit run app/Dashboard.py


Configuration

Add your Adzuna API keys in .env:

ADZUNA_APP_ID=your_id_here
ADZUNA_APP_KEY=your_key_here



Update skills in config/skills.yml.

Roadmap

Expand skill detection with regex/NLP.

Add scheduled ETL job for daily refresh.

Deploy on DigitalOcean and embed in wfdnelson.com.

Polish visuals and insights (top skills this week, salary by role, etc.).