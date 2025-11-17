# discover_and_pull.py
from __future__ import annotations
import csv, io, sys, requests
from typing import List
from bls_client import BLSClient  # from earlier

TIME_SERIES_BASE = "https://download.bls.gov/pub/time.series"

def fetch_series_universe(abbr: str) -> list[dict]:
    """
    Download <abbr>.series and return rows like:
      {"series_id": "CUSR0000SA0", "title": "All items in U.S. city average, SA", ...}
    The .series files are typically tab-delimited; we parse robustly.
    """
    url = f"{TIME_SERIES_BASE}/{abbr}/{abbr}.series"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    text = r.text

    rows: List[dict] = []
    # Try tab-delimited first; fall back to splitting on multiple spaces
    sample_has_tabs = "\t" in text.splitlines()[0] if text else False
    if sample_has_tabs:
        reader = csv.reader(io.StringIO(text), delimiter="\t")
        for parts in reader:
            if not parts:
                continue
            sid = (parts[0] or "").strip()
            if not sid or sid.lower().startswith("series_id"):
                # skip header if present
                continue
            title = parts[1].strip() if len(parts) > 1 else None
            rows.append({"series_id": sid, "title": title})
    else:
        for line in text.splitlines():
            line = line.strip()
            if not line or line.lower().startswith("series_id"):
                continue
            # split once: first token is series_id, rest is title
            first, *rest = line.split()
            rows.append({"series_id": first, "title": " ".join(rest) if rest else None})

    return rows

def chunk(seq, n):
    buf = []
    for x in seq:
        buf.append(x)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf

if __name__ == "__main__":
    # Example: enumerate all CPI-U series, then pull a safe recent window
    import os
    abbr = sys.argv[1] if len(sys.argv) > 1 else "cu"
    start_year, end_year = 2010, 2025

    universe = fetch_series_universe(abbr)
    print(f"{abbr}: discovered {len(universe):,} series")

    api_key = os.getenv('BLS_API_KEY')
    client = BLSClient(api_key=api_key)

    # Extract IDs and pull in batches of 50 (≤20-year window per request)
    ids = [r["series_id"] for r in universe]
    total_rows = 0
    for batch in chunk(ids, BLSClient.MAX_SERIES_PER_REQUEST):
        rows = client.get_many(
            batch,
            start_year=start_year,
            end_year=end_year,
            calculations=False,
            catalog=False,     # turn on if you want API-provided titles/metadata
            aspects=False,
            as_dataframe=False
        )
        # TODO: upsert 'rows' to Postgres here
        total_rows += len(rows)
    print(f"Pulled {total_rows:,} observations for {len(ids):,} series in {abbr} ({start_year}-{end_year})")
