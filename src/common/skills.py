from pathlib import Path
import yaml
from typing import List

SKILLS_YML = Path(__file__).resolve().parents[2] / "config" / "skills.yml"

def extract_skills(text: str) -> List[str]:
    text = (text or "").lower()
    skills = yaml.safe_load(SKILLS_YML.read_text())["skills"]
    hits = []
    for s in skills:
        if s.lower() in text:
            hits.append(s.lower())
    return hits
