# bls_client.py
from __future__ import annotations
import time
import math
import logging
from typing import Iterable, List, Dict, Any, Optional, Generator, Tuple
import requests

log = logging.getLogger("BLSClient")
logging.basicConfig(level=logging.INFO)

class BLSClient:
    """
    Simple, safe client for BLS Public Data API v2.
    Docs: https://www.bls.gov/developers/api_signature_v2.htm

    Key points handled:
      - Batch series in chunks of <= 50.
      - Backfill windows of <= 20 years each.
      - Throttle bursts to <= 50 requests / 10 seconds.
      - Retry 429/5xx with exponential backoff + jitter.
      - Optional 'calculations', 'catalog', 'annualaverage', 'aspects'.

    Usage:
      client = BLSClient(api_key="YOUR_KEY")
      df = client.get_many(
          ["CUSR0000SA0", "LNS14000000"], start_year=2000, end_year=2025,
          calculations=False, as_dataframe=True
      )
    """

    BASE_URL = "https://api.bls.gov/publicAPI/v2"
    TIMESERIES_ENDPOINT = "/timeseries/data/"
    POPULAR_ENDPOINT = "/timeseries/popular"
    SURVEYS_ENDPOINT = "/surveys"

    # API constraints
    MAX_SERIES_PER_REQUEST = 50
    MAX_YEARS_PER_REQUEST = 20

    # Default rate limit (documented burst guidance)
    MAX_REQUESTS_PER_WINDOW = 50
    WINDOW_SECONDS = 10

    def __init__(
        self,
        api_key: Optional[str] = None,
        session: Optional[requests.Session] = None,
        max_requests_per_window: int = MAX_REQUESTS_PER_WINDOW,
        window_seconds: int = WINDOW_SECONDS,
        timeout: int = 60,
        user_agent: str = "Finexus-BLSClient/1.0 (+contact: wanfaliang88@gmail.com)",
    ):
        self.api_key = api_key
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": user_agent})
        self.timeout = timeout

        # naive sliding-window throttle
        self._max_req = max_requests_per_window
        self._window_sec = window_seconds
        self._req_timestamps: List[float] = []

    # ---------------------- Public methods ---------------------- #
    def get_latest(
        self, series_id: str, calculations: bool = False, as_dataframe: bool = False
    ):
        """Fetch the most recent observation for a single series."""
        url = f"{self.BASE_URL}{self.TIMESERIES_ENDPOINT}{series_id}?latest=true"
        params = {"latest": "true"}
        if self.api_key:
            params["registrationkey"] = self.api_key


        data = self._request_json("GET", url, params=params)
        rows = self._parse_timeseries_payload(data)
        return self._maybe_dataframe(rows) if as_dataframe else rows

    def get_one(
        self,
        series_id: str,
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        *,
        catalog: bool = False,
        calculations: bool = False,
        annualaverage: bool = False,
        aspects: bool = False,
        as_dataframe: bool = False,
    ):
        """Fetch one series over an optional year range (<= 20 years)."""
        return self.get_many(
            [series_id],
            start_year=start_year,
            end_year=end_year,
            catalog=catalog,
            calculations=calculations,
            annualaverage=annualaverage,
            aspects=aspects,
            as_dataframe=as_dataframe,
        )

    def get_many(
        self,
        series_ids: Iterable[str],
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        *,
        catalog: bool = False,
        calculations: bool = False,
        annualaverage: bool = False,
        aspects: bool = False,
        as_dataframe: bool = False,
    ):
        """
        Fetch up to many series (chunked in groups of 50) for up to 20 years per request.
        If the requested span > 20 years, call `backfill_many` instead.
        """
        _validate_range(start_year, end_year, self.MAX_YEARS_PER_REQUEST)
        series_ids = list(series_ids)
        all_rows: List[Dict[str, Any]] = []

        for chunk in _chunks(series_ids, self.MAX_SERIES_PER_REQUEST):
            body = {
                "seriesid": list(chunk),
                "startyear": str(start_year) if start_year else None,
                "endyear": str(end_year) if end_year else None,
                "catalog": catalog,
                "calculations": calculations,
                "annualaverage": annualaverage,
                "aspects": aspects,
            }
            if self.api_key:
                body["registrationkey"] = self.api_key

            url = f"{self.BASE_URL}{self.TIMESERIES_ENDPOINT}"
            data = self._request_json("POST", url, json=_drop_nones(body))
            rows = self._parse_timeseries_payload(data)
            all_rows.extend(rows)

        return self._maybe_dataframe(all_rows) if as_dataframe else all_rows

    def backfill_many(
        self,
        series_ids: Iterable[str],
        start_year: int,
        end_year: int,
        *,
        window_years: int = MAX_YEARS_PER_REQUEST,
        catalog: bool = False,
        calculations: bool = False,
        annualaverage: bool = False,
        aspects: bool = False,
        as_dataframe: bool = False,
        sleep_between_windows: float = 0.0,
    ):
        """
        Fetch long histories by splitting into <=20-year windows.
        """
        windows = list(_year_windows(start_year, end_year, window_years))
        all_rows: List[Dict[str, Any]] = []
        for i, (sy, ey) in enumerate(windows, 1):
            log.info(f"Window {i}/{len(windows)}: {sy}-{ey}")
            rows = self.get_many(
                series_ids,
                start_year=sy,
                end_year=ey,
                catalog=catalog,
                calculations=calculations,
                annualaverage=annualaverage,
                aspects=aspects,
                as_dataframe=False,
            )
            all_rows.extend(rows)
            if sleep_between_windows and i < len(windows):
                time.sleep(sleep_between_windows)
        return self._maybe_dataframe(all_rows) if as_dataframe else all_rows

    # ---- Discovery helpers ---- #
    def popular(self, survey: Optional[str] = None) -> Dict[str, Any]:
        """
        Top 25 popular series overall, or for a given survey (e.g., 'cu', 'ce', 'la', etc.).
        """
        url = f"{self.BASE_URL}{self.POPULAR_ENDPOINT}"
        if survey:
            url += f"/{survey}"
        return self._request_json("GET", url)

    def surveys(self) -> Dict[str, Any]:
        """List all BLS surveys and metadata."""
        url = f"{self.BASE_URL}{self.SURVEYS_ENDPOINT}"
        return self._request_json("GET", url)

    def survey(self, abbr: str) -> Dict[str, Any]:
        """Metadata for a single survey by abbreviation (e.g., 'cu', 'ce', 'la')."""
        url = f"{self.BASE_URL}{self.SURVEYS_ENDPOINT}/{abbr}"
        return self._request_json("GET", url)

    # ---------------------- Internals ---------------------- #
    def _request_json(self, method: str, url: str, **kwargs) -> Dict[str, Any]:
        """
        Throttled request with retries on 429/5xx, exponential backoff + jitter.
        """
        # throttle
        self._throttle()

        backoff = 1.0  # seconds
        max_backoff = 32.0
        max_tries = 7

        for attempt in range(1, max_tries + 1):
            try:
                resp = self.session.request(method, url, timeout=self.timeout, **kwargs)
                if resp.status_code == 429 or 500 <= resp.status_code < 600:
                    # Retry with backoff
                    raise _RetryableHTTPError(resp)
                resp.raise_for_status()
                data = resp.json()
                # BLS returns a status wrapper; check for errors inside payload too
                if isinstance(data, dict) and str(data.get("status")) != "REQUEST_SUCCEEDED":
                    # Some errors still come 200 OK with status field
                    msg = data.get("message") or data
                    raise _BLSAPIError(f"BLS error: {msg}")
                return data
            except _RetryableHTTPError as e:
                if attempt == max_tries:
                    raise
                sleep_for = backoff + _jitter(0.2, 0.8)
                log.warning(f"{e}; retrying in {sleep_for:.2f}s (attempt {attempt}/{max_tries})")
                time.sleep(sleep_for)
                backoff = min(max_backoff, backoff * 2)
            except requests.HTTPError as e:
                # Non-retryable 4xx (other than 429) should surface
                raise _BLSAPIError(f"HTTP error {e.response.status_code}: {e.response.text}") from e
            except requests.RequestException as e:
                # Network hiccup: retry
                if attempt == max_tries:
                    raise _BLSAPIError(f"Network error: {e}") from e
                sleep_for = backoff + _jitter(0.2, 0.8)
                log.warning(f"Network error {e}; retrying in {sleep_for:.2f}s (attempt {attempt}/{max_tries})")
                time.sleep(sleep_for)
                backoff = min(max_backoff, backoff * 2)

        raise _BLSAPIError("Unreachable")  # defensive

    def _throttle(self):
        """
        Ensure no more than MAX_REQUESTS_PER_WINDOW in any WINDOW_SECONDS window.
        """
        now = time.time()
        window_start = now - self._window_sec
        self._req_timestamps = [t for t in self._req_timestamps if t >= window_start]

        if len(self._req_timestamps) >= self._max_req:
            # Sleep until we fall below the threshold
            sleep_for = self._req_timestamps[0] + self._window_sec - now
            if sleep_for > 0:
                time.sleep(sleep_for + 0.01)
        self._req_timestamps.append(time.time())

    def _parse_timeseries_payload(self, payload: dict) -> list[dict]:
        """
        Normalize BLS timeseries payload into tidy rows:
        [
        {
            "series_id": str,
            "year": int,
            "period": str,                 # 'M01'..'M13', 'Q01', 'S01', 'A01'
            "periodName": str | None,
            "value": float | None,
            "footnotes": str | None,
            "latest": bool,
            "catalog": dict | None,        # only if requested/returned
            "aspects": dict | None,
            "calculations": dict | None,   # per-datapoint if requested/returned
        },
        ...
        ]
        """
        def as_bool(x):
            if isinstance(x, bool):
                return x
            if x is None:
                return False
            return str(x).strip().lower() in ("true", "t", "1", "yes")

        results = (payload or {}).get("Results") or payload.get("results") or {}
        series_list = results.get("series", []) or []

        out: list[dict] = []
        for s in series_list:
            sid = s.get("seriesID") or s.get("seriesId") or s.get("seriesid")
            catalog = s.get("catalog")
            aspects = s.get("aspects")

            data_points = s.get("data") or []
            for d in data_points:
                row = {
                    "series_id": sid,
                    "year": int(d["year"]) if d.get("year") and str(d["year"]).isdigit() else None,
                    "period": d.get("period"),
                    "periodName": d.get("periodName"),
                    "value": _to_float(d.get("value")),
                    "footnotes": _join_footnotes(d.get("footnotes")),
                    "latest": as_bool(d.get("latest")),
                }
                if catalog is not None:
                    row["catalog"] = catalog
                if aspects is not None:
                    row["aspects"] = aspects
                if isinstance(d.get("calculations"), dict):
                    row["calculations"] = d["calculations"]
                out.append(row)
        return out

    def _maybe_dataframe(self, rows: List[Dict[str, Any]]):
        try:
            import pandas as pd  # optional
            return pd.DataFrame(rows)
        except Exception:
            return rows


# ---------------------- Helpers & utilities ---------------------- #
def _chunks(seq: Iterable[Any], size: int) -> Generator[List[Any], None, None]:
    buf: List[Any] = []
    for x in seq:
        buf.append(x)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf

def _drop_nones(d: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in d.items() if v is not None}

def _year_windows(start: int, end: int, window: int) -> Generator[Tuple[int, int], None, None]:
    if start > end:
        start, end = end, start
    span = end - start + 1
    if span <= window:
        yield (start, end)
        return
    # inclusive windows, e.g., 2000-2019, 2020-2039, ...
    for i in range(0, span, window):
        sy = start + i
        ey = min(end, sy + window - 1)
        yield (sy, ey)

def _validate_range(start: Optional[int], end: Optional[int], max_years: int):
    if start is None or end is None:
        return
    if end < start:
        raise ValueError("end_year must be >= start_year")
    if (end - start + 1) > max_years:
        raise ValueError(f"Year span exceeds {max_years}. Use backfill_many().")

def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        return None

def _join_footnotes(notes: Any) -> Optional[str]:
    if not notes:
        return None
    if isinstance(notes, list):
        texts = [n.get("text") for n in notes if isinstance(n, dict) and n.get("text")]
        return "; ".join(texts) if texts else None
    return str(notes)

def _jitter(a: float, b: float) -> float:
    import random
    return random.uniform(a, b)

class _RetryableHTTPError(Exception):
    def __init__(self, response: requests.Response):
        self.response = response
        super().__init__(f"Retryable HTTP {response.status_code}")

class _BLSAPIError(Exception):
    pass


# ---------------------- Example script ---------------------- #
if __name__ == "__main__":
    # Quick smoke test
    import os
    api_key = os.getenv('BLS_API_KEY')
    client = BLSClient(api_key=api_key)

    # Latest CPI-U headline
    latest = client.get_latest("CUSR0000SA0", as_dataframe=False)
    print("Latest CPI-U:", latest[:3], "...")

    # Multiple series within a single 20-year window
    rows = client.get_many(
        ["CUSR0000SA0", "LNS14000000"],  # CPI-U SA; Unemployment rate
        start_year=2010,
        end_year=2025,
        calculations=False,
        as_dataframe=False,
    )
    print("Fetched rows:", len(rows))

    # Long backfill example (splits into windows automatically)
    long_rows = client.backfill_many(
        ["CES0000000001"],  # Total nonfarm payroll employment (thousands)
        start_year=1990,
        end_year=2025,
        calculations=False,
        sleep_between_windows=0.25,  # optional pacing between windows
        as_dataframe=False,
    )
    print("Long backfill rows:", len(long_rows))
