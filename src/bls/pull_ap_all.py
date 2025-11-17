# pull_ap_all.py
from __future__ import annotations
import requests, csv, io, math, sys
from typing import List
from bls_client import BLSClient  # the client I drafted earlier

TIME_SERIES_BASE = "https://download.bls.gov/pub/time.series"

def fetch_series_universe(abbr: str) -> list[str]:
    url = f"{TIME_SERIES_BASE}/{abbr}/{abbr}.series"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    txt = r.text
    ids: List[str] = []
    # .series is typically tab-delimited: series_id <tab> series_title ...
    reader = csv.reader(io.StringIO(txt), delimiter="\t")
    for parts in reader:
        if not parts:
            continue
        sid = (parts[0] or "").strip()
        if not sid or sid.lower().startswith("series_id"):
            continue
        ids.append(sid)
    return ids

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
    import os
    api_key = sys.argv[1] if len(sys.argv) > 1 else os.getenv('BLS_API_KEY')
    start_year = int(sys.argv[2]) if len(sys.argv) > 2 else 2000
    end_year   = int(sys.argv[3]) if len(sys.argv) > 3 else 2025

    # 1) Discover all AP series
    ap_ids = fetch_series_universe("ap")
    print(f"AP series discovered: {len(ap_ids):,}")  # e.g., 1,482

    # 2) Create client (built-in throttle: ≤50 req / 10s, retries on 429/5xx)
    client = BLSClient(api_key=api_key)

    # 3) Pull full period (≤20-year windows handled by backfill_many)
    total_rows = 0
    # backfill_many already handles chunking across windows; we still chunk IDs to ≤50
    for i, batch in enumerate(chunk(ap_ids, BLSClient.MAX_SERIES_PER_REQUEST), 1):
        rows = client.backfill_many(
            batch,
            start_year=start_year,
            end_year=end_year,
            calculations=False,      # keep payloads smaller
            catalog=False,           # set True if you need API titles/metadata
            aspects=False,
            as_dataframe=False,
            sleep_between_windows=0  # small sleep is fine; client throttles bursts
        )
        total_rows += len(rows)
        # TODO: upsert 'rows' into your Postgres table here
        print(f"[{i}] batch size={len(batch)}  rows so far={total_rows:,}")

    print(f"DONE. Total observations pulled: {total_rows:,}")
