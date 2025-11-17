# surveys_catalog.py
from typing import Dict, List
import csv

SURVEY_ID_FORMAT_DOCS: Dict[str, str] = {
    "CU": "https://www.bls.gov/cpi/factsheets/cpi-series-ids.htm",
    "LA": "https://www.bls.gov/help/hlpforma.htm#LA",
    "CE": "https://www.bls.gov/ces/naics/home.htm#2.3",
    "SM": "https://www.bls.gov/sae/additional-resources/state-and-area-ces-series-code-structure-under-naics.htm",
    "JT": "https://www.bls.gov/jlt/jlt_series_changes.htm",
    "OE": "https://www.bls.gov/oes/",
    "WP": "https://www.bls.gov/ppi/ppiseries.htm",
    # add more as you encounter surveys you’ll use
}

def normalize_surveys_payload(payload: dict) -> List[dict]:
    results = payload.get("Results") or payload.get("results") or {}
    surveys = results.get("survey", [])
    out = []
    for s in surveys:
        abbr = s.get("survey_abbreviation")
        name = s.get("survey_name")
        out.append({
            "survey_abbreviation": abbr,
            "survey_name": name,
            "id_format_doc_url": SURVEY_ID_FORMAT_DOCS.get(abbr),
        })
    return out

def write_surveys_csv(rows: List[dict], path="bls_surveys_catalog.csv"):
    if not rows:
        return
    cols = ["survey_abbreviation", "survey_name", "id_format_doc_url"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in cols})
    return path

if __name__ == "__main__":
    # paste your JSON response into `payload` or call client.surveys()
    payload = {
        "status":"REQUEST_SUCCEEDED","responseTime":13,"message":[],
        "Results": { "survey": [ {"survey_abbreviation":"AP","survey_name":"Consumer Price Index - Average Price Data"}, ... ] }
    }
    rows = normalize_surveys_payload(payload)
    path = write_surveys_csv(rows)
    print("Wrote:", path)
