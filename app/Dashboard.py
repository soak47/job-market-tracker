import sqlite3
from pathlib import Path
import pandas as pd
import altair as alt
import streamlit as st
import re

STATE_MAP = {
    "WA":"Western Australia","NSW":"New South Wales","VIC":"Victoria","QLD":"Queensland",
    "SA":"South Australia","TAS":"Tasmania","NT":"Northern Territory","ACT":"ACT"
}

def extract_state(loc: str) -> str:
    s = str(loc)
    m = re.search(r",\s*([A-Z]{2,3})\b", s)  # e.g. ", WA"
    if m:
        return STATE_MAP.get(m.group(1), m.group(1))
    # heuristics if no code present
    ls = s.lower()
    if "perth" in ls: return "Western Australia"
    if "sydney" in ls: return "New South Wales"
    if "melbourne" in ls: return "Victoria"
    if "brisbane" in ls: return "Queensland"
    return ""


DB_PATH = Path(__file__).resolve().parents[1] / "data" / "jobs.db"

st.set_page_config(page_title="Job Market Tracker — AU", page_icon="📈", layout="wide")
st.title("📈 Job Market Tracker — AU (MVP)")
st.caption("Skills frequency and salary distribution from ingested job ads")

@st.cache_data
def load_tables():
    if not DB_PATH.exists():
        st.error("Database not found. Run `python src/setup_db.py` and `python src/extractors/load_sample.py`.")
        st.stop()
    conn = sqlite3.connect(DB_PATH)
    jobs = pd.read_sql_query("SELECT * FROM jobs", conn)
    skills = pd.read_sql_query("SELECT * FROM skills", conn)
    conn.close()
    return jobs, skills

jobs, skills = load_tables()
# 1) LOAD → WORKING COPY
df = jobs.copy()

# 2) CLEAN (make the raw data reliable)
# 2a. dates
df["posted_date"] = pd.to_datetime(df["posted_date"], errors="coerce")
# 2b. numbers (and fix values like 175 -> 175000)
for col in ["salary_min", "salary_max", "salary_avg"]:
    df[col] = pd.to_numeric(df[col], errors="coerce")
    df.loc[df[col] < 1000, col] = df[col] * 1000

# 2c. normalize text (locations come messy)
def canonical_city(loc: str) -> str:
    s = str(loc).lower()
    # Sydney
    if any(k in s for k in ["sydney","nsw","chippendale","parramatta","north sydney","macquarie park","surry hills","ultimo","the rocks"]):
        return "Sydney"
    # Melbourne
    if any(k in s for k in ["melbourne","vic","st kilda","docklands","richmond","hawthorn","cbd vic","west melbourne"]):
        return "Melbourne"
    # Perth
    if any(k in s for k in ["perth","wa","west perth","osborne park","joondalup","welshpool"]):
        return "Perth"
    # Brisbane
    if any(k in s for k in ["brisbane","qld","fortitude valley","south brisbane","milton","toowong"]):
        return "Brisbane"
    # Fallback: first token before comma
    return s.split(",")[0].strip().title()

df["city_clean"] = df["location"].apply(canonical_city)





# 2d. de-duplicate likely repeats
df = df.sort_values(["posted_date","company","title"], na_position="last").drop_duplicates(
    subset=["title","company","city_clean","posted_date"], keep="first"
)

# 3) DERIVE (columns that make filtering/charts easier)
role_hint = df["title"].str.extract(r"(Analyst|Scientist|Engineer)", expand=False)
df["role_bucket"] = role_hint.fillna("Other")

# 4) FILTER UI (build options from *clean* columns)
col1, col2, col3 = st.columns(3)
with col1:
    cities = ["All"] + sorted([c for c in df["city_clean"].dropna().unique() if str(c).lower() != "australia"])
    city = st.selectbox("City", cities, index=0)
with col2:
    sources = ["All"] + sorted([s for s in df["source"].dropna().unique().tolist() if s.lower() != "sample"])
    source = st.selectbox("Source", sources, index=0)
with col3:
    roles = ["All"] + sorted(df["role_bucket"].dropna().unique().tolist())
    role = st.selectbox("Role", roles, index=0)
    kw = st.text_input("Keyword (optional)", "")





# 4b) APPLY filters
view = df.copy()
if city != "All":
    view = view[view["city_clean"] == city]   # exact match
if source != "All":
    view = view[view["source"] == source]
# Role filter
if role != "All":
    view = view[view["role_bucket"] == role]

# Keyword filter (title/company/location; case-insensitive)
if kw:
    mask = (
        view["title"].str.contains(kw, case=False, na=False)
        | view["company"].str.contains(kw, case=False, na=False)
        | view.get("location", pd.Series("", index=view.index)).astype(str).str.contains(kw, case=False, na=False)
    )
    view = view[mask]


# 5) CHARTS/TABLES (use `view`, not `df`)
st.subheader("Skills frequency")
skills_view = skills.merge(view[["id"]], left_on="job_id", right_on="id", how="inner")
top = skills_view["skill"].value_counts().rename_axis("skill").reset_index(name="count")
if top.empty:
    st.info("No skills extracted for current filters.")
else:
    st.altair_chart(
        alt.Chart(top).mark_bar().encode(
            x=alt.X("count:Q", title="Mentions"),
            y=alt.Y("skill:N", sort="-x", title="Skill")
        ).properties(height=400),
        use_container_width=True
    )
## SALARY DISTRIBUTION $$
st.subheader("Salary distribution (AUD)")
sal = view[["salary_avg","salary_min","salary_max"]].melt(value_name="salary", var_name="kind").dropna()
if sal.empty:
    st.info("No salary data available.")
else:
    st.altair_chart(
        alt.Chart(sal).mark_bar().encode(
            x=alt.X("salary:Q", bin=alt.Bin(maxbins=30), title="Salary (AUD)"),
            y=alt.Y("count()", title="Jobs")
        ).properties(height=300),
        use_container_width=True
    )

## Listing By State ##
st.subheader("Listings by state")
state_counts = (
    view.assign(state=view["location"].apply(extract_state))
        .loc[lambda x: x["state"].ne("")]
        .groupby("state", as_index=False)["id"].count()
        .rename(columns={"id":"listings"})
        .sort_values("listings", ascending=False)
)
if state_counts.empty:
    st.info("No state info available.")
else:
    st.altair_chart(
        alt.Chart(state_counts).mark_bar().encode(
            x=alt.X("listings:Q", title="Listings"),
            y=alt.Y("state:N", sort="-x", title="State")
        ).properties(height=320),
        use_container_width=True
    )

## TIME TREND BY STATE ##
st.subheader("Weekly listings by state")
if "posted_date" in view.columns:
    trend = (
        view.assign(
            state=view["location"].apply(extract_state),
            week=view["posted_date"].dt.to_period("W").dt.start_time
        ).loc[lambda x: x["state"].ne("")]
         .groupby(["week","state"], as_index=False)["id"].count()
         .rename(columns={"id":"listings"})
    )
    if trend.empty:
        st.info("No dated listings available.")
    else:
        st.altair_chart(
            alt.Chart(trend).mark_line().encode(
                x=alt.X("week:T", title="Week"),
                y=alt.Y("listings:Q", title="Listings"),
                color="state:N"
            ).properties(height=320),
            use_container_width=True
        )



st.divider()
st.write("Rows after filters:", len(view))
st.write("Median salary by city", view.groupby("city_clean")["salary_avg"].median().dropna().sort_values(ascending=False))

