"""
BEA (Bureau of Economic Analysis) API Client

A robust client for the BEA Data Retrieval API.
Docs: https://apps.bea.gov/api/_pdf/bea_web_service_api_user_guide.pdf

Key features:
  - Rate limiting (100 requests/min, 100MB data/min, 30 errors/min)
  - Automatic retry with exponential backoff
  - Support for all BEA API methods
  - NIPA and Regional dataset helpers

Author: FinExus Data Collector
Created: 2025-11-26
"""
from __future__ import annotations
import time
import logging
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, UTC
import requests

log = logging.getLogger("BEAClient")


class BEAClient:
    """
    Client for BEA Public Data API.

    Usage:
        client = BEAClient(api_key="YOUR_36_CHAR_KEY")

        # Get list of datasets
        datasets = client.get_dataset_list()

        # Get NIPA data
        data = client.get_nipa_data(
            table_name="T10101",
            frequency="Q",
            year="2020,2021,2022,2023"
        )

        # Get Regional data
        data = client.get_regional_data(
            table_name="CAINC1",
            line_code=1,
            geo_fips="STATE",
            year="2020,2021,2022"
        )
    """

    BASE_URL = "https://apps.bea.gov/api/data"

    # Rate limits per BEA documentation
    MAX_REQUESTS_PER_MINUTE = 100
    MAX_DATA_MB_PER_MINUTE = 100
    MAX_ERRORS_PER_MINUTE = 30
    LOCKOUT_MINUTES = 60

    def __init__(
        self,
        api_key: str,
        session: Optional[requests.Session] = None,
        timeout: int = 60,
        max_retries: int = 5,
        user_agent: str = "Finexus-BEAClient/1.0",
    ):
        """
        Initialize BEA API client.

        Args:
            api_key: 36-character BEA API key (UserID)
            session: Optional requests session for connection pooling
            timeout: Request timeout in seconds
            max_retries: Maximum retry attempts for failed requests
            user_agent: User agent string for requests
        """
        if not api_key or len(api_key) != 36:
            raise ValueError("BEA API key must be 36 characters")

        self.api_key = api_key
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": user_agent})
        self.timeout = timeout
        self.max_retries = max_retries

        # Rate limiting tracking
        self._request_times: List[float] = []
        self._error_times: List[float] = []
        self._data_bytes: List[tuple[float, int]] = []  # (timestamp, bytes)

    # ===================== Core API Methods ===================== #

    def get_dataset_list(self) -> Dict[str, Any]:
        """
        Get list of all available BEA datasets.

        Returns:
            Dict with 'Dataset' key containing list of available datasets
        """
        return self._request("GetDataSetList")

    def get_parameter_list(self, dataset_name: str) -> Dict[str, Any]:
        """
        Get list of parameters for a specific dataset.

        Args:
            dataset_name: Name of dataset (e.g., 'NIPA', 'Regional')

        Returns:
            Dict with 'Parameter' key containing list of parameters
        """
        return self._request("GetParameterList", DatasetName=dataset_name)

    def get_parameter_values(
        self, dataset_name: str, parameter_name: str
    ) -> Dict[str, Any]:
        """
        Get valid values for a specific parameter.

        Args:
            dataset_name: Name of dataset
            parameter_name: Name of parameter

        Returns:
            Dict with 'ParamValue' key containing list of valid values
        """
        return self._request(
            "GetParameterValues",
            DatasetName=dataset_name,
            ParameterName=parameter_name,
        )

    def get_parameter_values_filtered(
        self,
        dataset_name: str,
        target_parameter: str,
        **filter_params,
    ) -> Dict[str, Any]:
        """
        Get filtered parameter values based on other parameter selections.

        Args:
            dataset_name: Name of dataset
            target_parameter: Parameter to get values for
            **filter_params: Filter parameters (e.g., TableName="CAINC1")

        Returns:
            Dict with filtered parameter values
        """
        return self._request(
            "GetParameterValuesFiltered",
            DatasetName=dataset_name,
            TargetParameter=target_parameter,
            **filter_params,
        )

    def get_data(self, dataset_name: str, **params) -> Dict[str, Any]:
        """
        Get data from a BEA dataset.

        Args:
            dataset_name: Name of dataset
            **params: Dataset-specific parameters

        Returns:
            Dict with 'Data' key containing the requested data
        """
        return self._request("GetData", DatasetName=dataset_name, **params)

    # ===================== NIPA Helpers ===================== #

    def get_nipa_tables(self) -> List[Dict[str, Any]]:
        """Get list of available NIPA tables."""
        result = self.get_parameter_values("NIPA", "TableName")
        return result.get("BEAAPI", {}).get("Results", {}).get("ParamValue", [])

    def get_nipa_data(
        self,
        table_name: str,
        frequency: str = "A",
        year: str = "ALL",
        show_millons: str = "N",
    ) -> Dict[str, Any]:
        """
        Get NIPA (National Income and Product Accounts) data.

        Args:
            table_name: NIPA table name (e.g., 'T10101' for GDP)
            frequency: 'A' (annual), 'Q' (quarterly), or 'M' (monthly)
            year: Year(s) - 'ALL', 'LAST5', 'LAST10', or comma-separated years
            show_millons: 'Y' to show values in millions, 'N' for actual units

        Returns:
            Dict containing NIPA data
        """
        return self.get_data(
            "NIPA",
            TableName=table_name,
            Frequency=frequency,
            Year=year,
            ShowMillions=show_millons,
        )

    def get_nipa_data_years(
        self,
        table_name: str,
        frequency: str,
        start_year: int,
        end_year: int,
    ) -> List[Dict[str, Any]]:
        """
        Get NIPA data for a specific year range.

        BEA limits year ranges, so this fetches data in batches if needed.

        Args:
            table_name: NIPA table name
            frequency: 'A', 'Q', or 'M'
            start_year: Starting year
            end_year: Ending year (inclusive)

        Returns:
            List of data records
        """
        # Build year string
        years = ",".join(str(y) for y in range(start_year, end_year + 1))

        result = self.get_nipa_data(
            table_name=table_name,
            frequency=frequency,
            year=years,
        )

        return self._extract_data(result)

    # ===================== Regional Helpers ===================== #

    def get_regional_tables(self) -> List[Dict[str, Any]]:
        """Get list of available Regional tables."""
        result = self.get_parameter_values("Regional", "TableName")
        return result.get("BEAAPI", {}).get("Results", {}).get("ParamValue", [])

    def get_regional_line_codes(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get available line codes for a Regional table.

        Args:
            table_name: Regional table name (e.g., 'CAINC1')

        Returns:
            List of line code definitions
        """
        result = self.get_parameter_values_filtered(
            "Regional",
            target_parameter="LineCode",
            TableName=table_name,
        )
        return result.get("BEAAPI", {}).get("Results", {}).get("ParamValue", [])

    def get_regional_geo_fips(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get available geographic FIPS codes for a Regional table.

        Args:
            table_name: Regional table name

        Returns:
            List of geographic areas with FIPS codes
        """
        result = self.get_parameter_values_filtered(
            "Regional",
            target_parameter="GeoFips",
            TableName=table_name,
        )
        return result.get("BEAAPI", {}).get("Results", {}).get("ParamValue", [])

    def get_regional_data(
        self,
        table_name: str,
        line_code: Union[int, str],
        geo_fips: str = "STATE",
        year: str = "ALL",
    ) -> Dict[str, Any]:
        """
        Get Regional economic data.

        Args:
            table_name: Regional table name (e.g., 'CAINC1', 'SAGDP1')
            line_code: Line code for specific statistic
            geo_fips: Geographic area - 'STATE', 'COUNTY', 'MSA', or specific FIPS
            year: Year(s) - 'ALL', 'LAST5', 'LAST10', or comma-separated years

        Returns:
            Dict containing Regional data
        """
        return self.get_data(
            "Regional",
            TableName=table_name,
            LineCode=str(line_code),
            GeoFips=geo_fips,
            Year=year,
        )

    def get_regional_data_years(
        self,
        table_name: str,
        line_code: Union[int, str],
        geo_fips: str,
        start_year: int,
        end_year: int,
    ) -> List[Dict[str, Any]]:
        """
        Get Regional data for a specific year range.

        Args:
            table_name: Regional table name
            line_code: Line code for specific statistic
            geo_fips: Geographic area code
            start_year: Starting year
            end_year: Ending year (inclusive)

        Returns:
            List of data records
        """
        years = ",".join(str(y) for y in range(start_year, end_year + 1))

        result = self.get_regional_data(
            table_name=table_name,
            line_code=line_code,
            geo_fips=geo_fips,
            year=years,
        )

        return self._extract_data(result)

    # ===================== GDP by Industry Helpers ===================== #

    def get_gdpbyindustry_tables(self) -> List[Dict[str, Any]]:
        """Get list of available GDP by Industry tables."""
        result = self.get_parameter_values("GDPbyIndustry", "TableID")
        return result.get("BEAAPI", {}).get("Results", {}).get("ParamValue", [])

    def get_gdpbyindustry_industries(self) -> List[Dict[str, Any]]:
        """Get list of available industry codes."""
        result = self.get_parameter_values("GDPbyIndustry", "Industry")
        return result.get("BEAAPI", {}).get("Results", {}).get("ParamValue", [])

    def get_gdpbyindustry_data(
        self,
        table_id: Union[int, str],
        frequency: str = "A",
        year: str = "ALL",
        industry: str = "ALL",
    ) -> Dict[str, Any]:
        """
        Get GDP by Industry data.

        Args:
            table_id: Table ID (integer or 'ALL')
            frequency: 'A' (annual) or 'Q' (quarterly)
            year: Year(s) - 'ALL' or comma-separated years (annual from 1997, quarterly from 2005)
            industry: Industry code(s) - 'ALL' or comma-separated codes

        Returns:
            Dict containing GDP by Industry data
        """
        return self.get_data(
            "GDPbyIndustry",
            TableID=str(table_id),
            Frequency=frequency,
            Year=year,
            Industry=industry,
        )

    def get_gdpbyindustry_data_years(
        self,
        table_id: Union[int, str],
        frequency: str,
        start_year: int,
        end_year: int,
        industry: str = "ALL",
    ) -> List[Dict[str, Any]]:
        """
        Get GDP by Industry data for a specific year range.

        Args:
            table_id: Table ID
            frequency: 'A' or 'Q'
            start_year: Starting year
            end_year: Ending year (inclusive)
            industry: Industry code(s) - 'ALL' or comma-separated codes

        Returns:
            List of data records
        """
        years = ",".join(str(y) for y in range(start_year, end_year + 1))

        result = self.get_gdpbyindustry_data(
            table_id=table_id,
            frequency=frequency,
            year=years,
            industry=industry,
        )

        return self._extract_data(result)

    # ===================== ITA (International Transactions) Helpers ===================== #

    def get_ita_indicators(self) -> List[Dict[str, Any]]:
        """Get list of available ITA indicators."""
        result = self.get_parameter_values("ITA", "Indicator")
        return result.get("BEAAPI", {}).get("Results", {}).get("ParamValue", [])

    def get_ita_areas(self) -> List[Dict[str, Any]]:
        """Get list of available ITA areas/countries."""
        result = self.get_parameter_values("ITA", "AreaOrCountry")
        return result.get("BEAAPI", {}).get("Results", {}).get("ParamValue", [])

    def get_ita_data(
        self,
        indicator: str = "All",
        area_or_country: str = "AllCountries",
        frequency: str = "A",
        year: str = "ALL",
    ) -> Dict[str, Any]:
        """
        Get ITA (International Transactions) data.

        Args:
            indicator: Indicator code(s) - 'All' or specific code (e.g., 'BalGds')
            area_or_country: Area/Country - 'AllCountries', 'All', or specific (e.g., 'China')
            frequency: 'A' (annual), 'QSA' (quarterly seasonally adjusted), 'QNSA' (quarterly not seasonally adjusted)
            year: Year(s) - 'ALL' or comma-separated years

        Note: Either exactly one indicator OR exactly one area_or_country must be specified
              (unless one is 'All'/'AllCountries')

        Returns:
            Dict containing ITA data
        """
        return self.get_data(
            "ITA",
            Indicator=indicator,
            AreaOrCountry=area_or_country,
            Frequency=frequency,
            Year=year,
        )

    def get_ita_data_by_indicator(
        self,
        indicator: str,
        frequency: str = "A",
        year: str = "ALL",
    ) -> List[Dict[str, Any]]:
        """
        Get ITA data for a specific indicator across all countries.

        Args:
            indicator: Indicator code (e.g., 'BalGds', 'PfInvAssets')
            frequency: 'A', 'QSA', or 'QNSA'
            year: Year(s) - 'ALL' or comma-separated years

        Returns:
            List of data records
        """
        result = self.get_ita_data(
            indicator=indicator,
            area_or_country="All",
            frequency=frequency,
            year=year,
        )
        return self._extract_data(result)

    def get_ita_data_by_area(
        self,
        area_or_country: str,
        frequency: str = "A",
        year: str = "ALL",
    ) -> List[Dict[str, Any]]:
        """
        Get ITA data for a specific area/country across all indicators.

        Args:
            area_or_country: Area/Country code (e.g., 'China', 'EU')
            frequency: 'A', 'QSA', or 'QNSA'
            year: Year(s) - 'ALL' or comma-separated years

        Returns:
            List of data records
        """
        result = self.get_ita_data(
            indicator="All",
            area_or_country=area_or_country,
            frequency=frequency,
            year=year,
        )
        return self._extract_data(result)

    # ===================== Fixed Assets Methods ===================== #

    def get_fixedassets_tables(self) -> List[Dict[str, Any]]:
        """
        Get list of available Fixed Assets tables.

        Returns:
            List of table dicts with 'TableName' and 'Description' keys
        """
        result = self.get_parameter_values("FixedAssets", "TableName")
        return result.get("BEAAPI", {}).get("Results", {}).get("ParamValue", [])

    def get_fixedassets_data(
        self,
        table_name: str,
        year: str = "ALL",
    ) -> Dict[str, Any]:
        """
        Get Fixed Assets data for a table.

        Args:
            table_name: Table name (e.g., 'FAAt201', 'FAAt405')
            year: Year(s) - 'ALL', 'X', or comma-separated years

        Returns:
            Raw API response dict

        Note:
            Fixed Assets only supports annual data.
        """
        return self.get_data(
            "FixedAssets",
            TableName=table_name,
            Year=year,
        )

    def get_fixedassets_table_data(
        self,
        table_name: str,
        year: str = "ALL",
    ) -> List[Dict[str, Any]]:
        """
        Get Fixed Assets data with extracted records.

        Args:
            table_name: Table name (e.g., 'FAAt201')
            year: Year specification

        Returns:
            List of data records
        """
        result = self.get_fixedassets_data(table_name=table_name, year=year)
        return self._extract_data(result)

    # ===================== Data Extraction Helpers ===================== #

    def _extract_data(self, result: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract data records from BEA API response."""
        beaapi = result.get("BEAAPI", {})
        results = beaapi.get("Results", {})

        # Handle different response structures
        if isinstance(results, dict):
            # Standard format: Results.Data
            if "Data" in results:
                return results["Data"]
            elif "data" in results:
                return results["data"]
        elif isinstance(results, list) and len(results) > 0:
            # GDPbyIndustry format: Results is a list, Data is inside first element
            first_result = results[0]
            if isinstance(first_result, dict):
                if "Data" in first_result:
                    return first_result["Data"]
                elif "data" in first_result:
                    return first_result["data"]
            # If Results is a list of data records directly
            return results

        return []

    def _extract_notes(self, result: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract notes/footnotes from BEA API response."""
        beaapi = result.get("BEAAPI", {})
        results = beaapi.get("Results", {})
        return results.get("Notes", [])

    def _extract_dimensions(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """Extract dimension metadata from BEA API response."""
        beaapi = result.get("BEAAPI", {})
        results = beaapi.get("Results", {})
        return results.get("Dimensions", {})

    # ===================== Request Handling ===================== #

    def _request(self, method: str, **params) -> Dict[str, Any]:
        """
        Make a request to the BEA API with rate limiting and retries.

        Args:
            method: API method name
            **params: Additional parameters

        Returns:
            JSON response from API
        """
        # Check rate limits before making request
        self._check_rate_limits()

        # Build request parameters
        request_params = {
            "UserID": self.api_key,
            "method": method,
            "ResultFormat": "JSON",
            **params,
        }

        # Remove None values
        request_params = {k: v for k, v in request_params.items() if v is not None}

        backoff = 1.0
        last_error = None

        for attempt in range(1, self.max_retries + 1):
            try:
                self._record_request()

                response = self.session.get(
                    self.BASE_URL,
                    params=request_params,
                    timeout=self.timeout,
                )

                # Track data volume
                content_length = len(response.content)
                self._record_data_bytes(content_length)

                # Handle HTTP 429 (rate limit)
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    self._record_error()
                    log.warning(f"Rate limited. Waiting {retry_after}s before retry.")
                    time.sleep(retry_after)
                    continue

                # Handle server errors
                if response.status_code >= 500:
                    self._record_error()
                    raise _RetryableError(f"Server error {response.status_code}")

                response.raise_for_status()

                data = response.json()

                # Check for API-level errors
                beaapi = data.get("BEAAPI", {})
                if "Error" in beaapi:
                    error = beaapi["Error"]
                    error_msg = error.get("ErrorMessage", str(error))
                    self._record_error()
                    raise BEAAPIError(f"BEA API Error: {error_msg}")

                return data

            except _RetryableError as e:
                last_error = e
                if attempt == self.max_retries:
                    raise BEAAPIError(f"Max retries exceeded: {e}") from e

                sleep_time = backoff + _jitter(0.1, 0.5)
                log.warning(f"{e}; retry {attempt}/{self.max_retries} in {sleep_time:.1f}s")
                time.sleep(sleep_time)
                backoff = min(60, backoff * 2)

            except requests.RequestException as e:
                last_error = e
                if attempt == self.max_retries:
                    raise BEAAPIError(f"Request failed: {e}") from e

                sleep_time = backoff + _jitter(0.1, 0.5)
                log.warning(f"Network error: {e}; retry {attempt}/{self.max_retries}")
                time.sleep(sleep_time)
                backoff = min(60, backoff * 2)

        raise BEAAPIError(f"Request failed after {self.max_retries} attempts: {last_error}")

    # ===================== Rate Limiting ===================== #

    def _check_rate_limits(self):
        """Check and enforce rate limits before making a request."""
        now = time.time()
        minute_ago = now - 60

        # Clean old entries
        self._request_times = [t for t in self._request_times if t > minute_ago]
        self._error_times = [t for t in self._error_times if t > minute_ago]
        self._data_bytes = [(t, b) for t, b in self._data_bytes if t > minute_ago]

        # Check request rate
        if len(self._request_times) >= self.MAX_REQUESTS_PER_MINUTE:
            wait_time = self._request_times[0] + 60 - now
            if wait_time > 0:
                log.info(f"Rate limit: waiting {wait_time:.1f}s (requests)")
                time.sleep(wait_time + 0.1)

        # Check error rate
        if len(self._error_times) >= self.MAX_ERRORS_PER_MINUTE:
            wait_time = self._error_times[0] + 60 - now
            if wait_time > 0:
                log.warning(f"Error rate limit: waiting {wait_time:.1f}s")
                time.sleep(wait_time + 0.1)

        # Check data volume
        total_bytes = sum(b for _, b in self._data_bytes)
        max_bytes = self.MAX_DATA_MB_PER_MINUTE * 1024 * 1024
        if total_bytes >= max_bytes:
            oldest_time = min(t for t, _ in self._data_bytes)
            wait_time = oldest_time + 60 - now
            if wait_time > 0:
                log.info(f"Data rate limit: waiting {wait_time:.1f}s")
                time.sleep(wait_time + 0.1)

    def _record_request(self):
        """Record a request timestamp for rate limiting."""
        self._request_times.append(time.time())

    def _record_error(self):
        """Record an error timestamp for rate limiting."""
        self._error_times.append(time.time())

    def _record_data_bytes(self, byte_count: int):
        """Record data volume for rate limiting."""
        self._data_bytes.append((time.time(), byte_count))

    # ===================== Utility Methods ===================== #

    def get_request_stats(self) -> Dict[str, Any]:
        """Get current rate limiting statistics."""
        now = time.time()
        minute_ago = now - 60

        recent_requests = [t for t in self._request_times if t > minute_ago]
        recent_errors = [t for t in self._error_times if t > minute_ago]
        recent_bytes = sum(b for t, b in self._data_bytes if t > minute_ago)

        return {
            "requests_last_minute": len(recent_requests),
            "errors_last_minute": len(recent_errors),
            "data_mb_last_minute": recent_bytes / (1024 * 1024),
            "requests_remaining": self.MAX_REQUESTS_PER_MINUTE - len(recent_requests),
            "errors_remaining": self.MAX_ERRORS_PER_MINUTE - len(recent_errors),
            "data_mb_remaining": self.MAX_DATA_MB_PER_MINUTE - (recent_bytes / (1024 * 1024)),
        }


# ===================== Exceptions ===================== #

class BEAAPIError(Exception):
    """Exception for BEA API errors."""
    pass


class _RetryableError(Exception):
    """Internal exception for retryable errors."""
    pass


# ===================== Helpers ===================== #

def _jitter(min_val: float, max_val: float) -> float:
    """Add random jitter to prevent thundering herd."""
    import random
    return random.uniform(min_val, max_val)


# ===================== Example Usage ===================== #

if __name__ == "__main__":
    import os

    logging.basicConfig(level=logging.INFO)

    api_key = os.getenv("BEA_API_KEY")
    if not api_key:
        print("Set BEA_API_KEY environment variable")
        exit(1)

    client = BEAClient(api_key=api_key)

    # Test: Get dataset list
    print("\n=== Dataset List ===")
    datasets = client.get_dataset_list()
    for ds in datasets.get("BEAAPI", {}).get("Results", {}).get("Dataset", []):
        print(f"  {ds.get('DatasetName')}: {ds.get('DatasetDescription')}")

    # Test: Get NIPA tables
    print("\n=== NIPA Tables (first 5) ===")
    tables = client.get_nipa_tables()
    for t in tables[:5]:
        print(f"  {t.get('TableName')}: {t.get('Description', '')[:60]}...")

    # Test: Get GDP data
    print("\n=== GDP Data (T10101, 2023) ===")
    gdp = client.get_nipa_data("T10101", frequency="A", year="2023")
    data = client._extract_data(gdp)
    for row in data[:3]:
        print(f"  {row.get('SeriesCode')}: {row.get('DataValue')} ({row.get('TimePeriod')})")

    # Test: Get Regional data
    print("\n=== State Personal Income (CAINC1, 2023) ===")
    pi = client.get_regional_data("CAINC1", line_code=1, geo_fips="STATE", year="2023")
    data = client._extract_data(pi)
    for row in data[:5]:
        print(f"  {row.get('GeoName')}: {row.get('DataValue')}")

    print("\n=== Rate Limit Stats ===")
    stats = client.get_request_stats()
    print(f"  Requests: {stats['requests_last_minute']}/{client.MAX_REQUESTS_PER_MINUTE}")
    print(f"  Data: {stats['data_mb_last_minute']:.2f}/{client.MAX_DATA_MB_PER_MINUTE} MB")
