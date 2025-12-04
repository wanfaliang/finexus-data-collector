"""
Treasury Fiscal Data API Client

A robust client for the U.S. Treasury Fiscal Data API.
Docs: https://fiscaldata.treasury.gov/api-documentation/

Key endpoints:
  - upcoming_auctions: Upcoming Treasury securities auctions
  - auctions_query: Historical auction results
  - avg_interest_rates: Average interest rates on Treasury securities
  - record_setting_auction: Record-setting auction data

Author: FinExus Data Collector
Created: 2025-12-03
"""
from __future__ import annotations
import time
import logging
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, date, UTC
import requests

log = logging.getLogger("TreasuryClient")


class TreasuryClient:
    """
    Client for U.S. Treasury Fiscal Data API.

    Usage:
        client = TreasuryClient()

        # Get upcoming auctions
        auctions = client.get_upcoming_auctions()

        # Get historical auction results
        results = client.get_auction_results(
            security_type='Note',
            security_term='10-Year',
            start_date='2020-01-01'
        )

        # Get average interest rates
        rates = client.get_average_interest_rates()
    """

    BASE_URL = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"

    # Endpoints
    ENDPOINTS = {
        'upcoming_auctions': 'v1/accounting/od/upcoming_auctions',
        'auctions_query': 'v1/accounting/od/auctions_query',
        'avg_interest_rates': 'v2/accounting/od/avg_interest_rates',
        'record_setting_auction': 'v2/accounting/od/record_setting_auction',
        'pdo2_offerings': 'v1/accounting/tb/pdo2_offerings_marketable_securities_other_regular_weekly_treasury_bills',
        'debt_to_penny': 'v2/accounting/od/debt_to_penny',
        'treasury_offset': 'v1/debt/top/top_state',
    }

    # Security terms we care about (Notes and Bonds)
    TARGET_TERMS = ['2-Year', '5-Year', '7-Year', '10-Year', '20-Year', '30-Year']

    # Mapping for reopenings to base terms (e.g., '9-Year 10-Month' -> '10-Year')
    # Reopenings have terms like 'X-Year Y-Month' where X+Y/12 ≈ base term years
    @staticmethod
    def normalize_term(term: str) -> Optional[str]:
        """
        Normalize security term to base term.

        Handles reopenings like '9-Year 10-Month' -> '10-Year'
        """
        if not term:
            return None

        # Direct match
        if term in TreasuryClient.TARGET_TERMS:
            return term

        # Handle reopenings (e.g., '9-Year 10-Month', '29-Year 11-Month')
        # These are existing securities being reopened, map to closest base term
        term_lower = term.lower()

        # Map based on approximate years
        if '29-year' in term_lower or '30-year' in term_lower:
            return '30-Year'
        elif '19-year' in term_lower or '20-year' in term_lower:
            return '20-Year'
        elif '9-year' in term_lower or '10-year' in term_lower:
            return '10-Year'
        elif '6-year' in term_lower or '7-year' in term_lower:
            return '7-Year'
        elif '4-year' in term_lower or '5-year' in term_lower:
            return '5-Year'
        elif '1-year' in term_lower or '2-year' in term_lower:
            return '2-Year'
        elif '3-year' in term_lower:
            return '3-Year'  # Not in TARGET_TERMS but we can track it

        return term  # Return original if no mapping found

    def __init__(
        self,
        session: Optional[requests.Session] = None,
        timeout: int = 60,
        max_retries: int = 3,
        user_agent: str = "Finexus-TreasuryClient/1.0",
    ):
        """
        Initialize Treasury Fiscal Data API client.

        Args:
            session: Optional requests session for connection pooling
            timeout: Request timeout in seconds
            max_retries: Maximum retry attempts for failed requests
            user_agent: User agent string for requests
        """
        self.session = session or requests.Session()
        self.session.headers.update({
            "User-Agent": user_agent,
            "Accept": "application/json",
        })
        self.timeout = timeout
        self.max_retries = max_retries

        # Request tracking
        self._request_count = 0
        self._last_request_time: Optional[float] = None

    # ===================== Core Request Method ===================== #

    def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Make a request to the Treasury Fiscal Data API.

        Args:
            endpoint: API endpoint path
            params: Query parameters

        Returns:
            API response as dict

        Raises:
            requests.RequestException: On request failure
            ValueError: On API error response
        """
        url = f"{self.BASE_URL}/{endpoint}"
        params = params or {}

        for attempt in range(self.max_retries):
            try:
                # Rate limiting - be gentle with the API
                if self._last_request_time:
                    elapsed = time.time() - self._last_request_time
                    if elapsed < 0.2:  # 200ms minimum between requests
                        time.sleep(0.2 - elapsed)

                log.debug(f"Treasury API request: {endpoint}, params={params}")

                response = self.session.get(url, params=params, timeout=self.timeout)
                self._last_request_time = time.time()
                self._request_count += 1

                response.raise_for_status()
                data = response.json()

                # Check for API-level errors
                if 'error' in data:
                    raise ValueError(f"API error: {data['error']}")

                log.debug(f"Treasury API response: {len(data.get('data', []))} records")
                return data

            except requests.exceptions.HTTPError as e:
                if response.status_code == 429:  # Rate limited
                    wait_time = 2 ** attempt * 5  # Exponential backoff
                    log.warning(f"Rate limited, waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                elif response.status_code >= 500:
                    wait_time = 2 ** attempt
                    log.warning(f"Server error {response.status_code}, retry {attempt + 1}/{self.max_retries}")
                    time.sleep(wait_time)
                    continue
                else:
                    raise

            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries - 1:
                    wait_time = 2 ** attempt
                    log.warning(f"Request failed: {e}, retry {attempt + 1}/{self.max_retries}")
                    time.sleep(wait_time)
                    continue
                raise

        raise requests.exceptions.RequestException(f"Failed after {self.max_retries} attempts")

    def _paginate_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        page_size: int = 1000,
        max_pages: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Make paginated requests to retrieve all data.

        Args:
            endpoint: API endpoint
            params: Base query parameters
            page_size: Records per page
            max_pages: Maximum pages to fetch (None = all)

        Returns:
            Combined list of all records
        """
        params = params or {}
        params['page[size]'] = page_size
        params['page[number]'] = 1

        all_data = []
        page = 1

        while True:
            params['page[number]'] = page
            response = self._make_request(endpoint, params)

            data = response.get('data', [])
            if not data:
                break

            all_data.extend(data)

            # Check pagination metadata
            meta = response.get('meta', {})
            total_pages = meta.get('total-pages', 1)

            log.info(f"Fetched page {page}/{total_pages}, {len(data)} records")

            if page >= total_pages:
                break

            if max_pages and page >= max_pages:
                log.info(f"Reached max_pages limit ({max_pages})")
                break

            page += 1

        return all_data

    # ===================== Auction Methods ===================== #

    def get_upcoming_auctions(
        self,
        security_types: Optional[List[str]] = None,
        target_terms_only: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Get upcoming Treasury securities auctions.

        Args:
            security_types: Filter by security type ('Note', 'Bond', 'Bill')
            target_terms_only: Only return 2Y, 5Y, 7Y, 10Y, 20Y, 30Y

        Returns:
            List of upcoming auction records
        """
        # Use correct field names from API (note: offering_amt not offering_amount, announcemt_date not announcement_date)
        params = {
            'sort': 'auction_date',
        }

        # Note: The upcoming_auctions endpoint may not support filtering well
        # We'll filter client-side for dates and security types
        filter_types = security_types or ['Note', 'Bond']
        today = date.today()

        response = self._make_request(self.ENDPOINTS['upcoming_auctions'], params)
        auctions = response.get('data', [])

        # Client-side filtering for security types
        auctions = [a for a in auctions if a.get('security_type') in filter_types]

        # Client-side filtering for future dates
        auctions = [
            a for a in auctions
            if a.get('auction_date') and datetime.strptime(a['auction_date'], '%Y-%m-%d').date() >= today
        ]

        if target_terms_only:
            # Filter by normalized term (handles reopenings like '9-Year 10-Month' -> '10-Year')
            auctions = [
                a for a in auctions
                if self.normalize_term(a.get('security_term')) in self.TARGET_TERMS
            ]

        return auctions

    def get_auction_results(
        self,
        security_type: Optional[str] = None,
        security_term: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        cusip: Optional[str] = None,
        target_terms_only: bool = True,
        max_records: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get historical auction results from auctions_query endpoint.

        Args:
            security_type: 'Note', 'Bond', 'Bill', 'TIPS', 'FRN'
            security_term: '2-Year', '5-Year', '10-Year', etc.
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            cusip: Specific CUSIP
            target_terms_only: Only return target terms (2Y-30Y)
            max_records: Maximum records to return

        Returns:
            List of auction result records
        """
        params = {
            'sort': '-auction_date',  # Most recent first
        }

        # Note: Some endpoints don't support filtering - we'll do client-side filtering
        # Just request all data and filter in Python

        # Paginate to get all results
        max_pages = (max_records // 1000 + 1) if max_records else 50  # Limit to 50 pages max (~50k records)
        auctions = self._paginate_request(
            self.ENDPOINTS['auctions_query'],
            params,
            page_size=1000,
            max_pages=max_pages,
        )

        # Client-side filtering
        if security_type:
            auctions = [a for a in auctions if a.get('security_type') == security_type]
        else:
            # Filter for Notes and Bonds only
            auctions = [a for a in auctions if a.get('security_type') in ['Note', 'Bond']]

        if security_term:
            # Match both exact term and normalized term
            auctions = [
                a for a in auctions
                if a.get('security_term') == security_term or self.normalize_term(a.get('security_term')) == security_term
            ]
        elif target_terms_only:
            # Filter by normalized term (handles reopenings)
            auctions = [a for a in auctions if self.normalize_term(a.get('security_term')) in self.TARGET_TERMS]

        if start_date:
            auctions = [a for a in auctions if a.get('auction_date', '') >= start_date]

        if end_date:
            auctions = [a for a in auctions if a.get('auction_date', '') <= end_date]

        if cusip:
            auctions = [a for a in auctions if a.get('cusip') == cusip]

        if max_records:
            auctions = auctions[:max_records]

        return auctions

    def get_auction_by_cusip(self, cusip: str) -> Optional[Dict[str, Any]]:
        """
        Get auction result for a specific CUSIP.

        Args:
            cusip: 9-character CUSIP

        Returns:
            Auction record or None if not found
        """
        results = self.get_auction_results(cusip=cusip, max_records=1)
        return results[0] if results else None

    def get_recent_auctions(
        self,
        days: int = 30,
        security_term: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get auctions from the last N days.

        Args:
            days: Number of days to look back
            security_term: Optional term filter

        Returns:
            List of recent auction records
        """
        from datetime import timedelta
        start_date = (date.today() - timedelta(days=days)).strftime('%Y-%m-%d')

        return self.get_auction_results(
            security_term=security_term,
            start_date=start_date,
        )

    # ===================== PDO2 Offerings (Alternative Data Source) ===================== #

    def get_pdo2_offerings(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get offerings of marketable securities (other than weekly bills).

        This is from the Treasury Bulletin and provides additional auction details.

        Args:
            start_date: Start date filter
            end_date: End date filter

        Returns:
            List of offering records
        """
        params = {
            'sort': '-record_date',
        }

        filters = []
        if start_date:
            filters.append(f'record_date:gte:{start_date}')
        if end_date:
            filters.append(f'record_date:lte:{end_date}')

        if filters:
            params['filter'] = ','.join(filters)

        return self._paginate_request(self.ENDPOINTS['pdo2_offerings'], params)

    # ===================== Interest Rates ===================== #

    def get_average_interest_rates(
        self,
        security_type_desc: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get average interest rates on Treasury securities.

        Args:
            security_type_desc: Filter by security type description
            start_date: Start date filter
            end_date: End date filter

        Returns:
            List of interest rate records
        """
        params = {
            'sort': '-record_date',
        }

        filters = []
        if security_type_desc:
            filters.append(f'security_type_desc:eq:{security_type_desc}')
        if start_date:
            filters.append(f'record_date:gte:{start_date}')
        if end_date:
            filters.append(f'record_date:lte:{end_date}')

        if filters:
            params['filter'] = ','.join(filters)

        return self._paginate_request(self.ENDPOINTS['avg_interest_rates'], params)

    # ===================== Record-Setting Auctions ===================== #

    def get_record_setting_auctions(
        self,
        security_class: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get record-setting auction data (highest/lowest yields, etc.).

        Args:
            security_class: Filter by security class

        Returns:
            List of record-setting auction records
        """
        params = {}

        if security_class:
            params['filter'] = f'security_class:eq:{security_class}'

        return self._paginate_request(self.ENDPOINTS['record_setting_auction'], params)

    # ===================== Debt Statistics ===================== #

    def get_debt_to_penny(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get daily national debt figures.

        Args:
            start_date: Start date filter
            end_date: End date filter

        Returns:
            List of debt records
        """
        params = {
            'sort': '-record_date',
        }

        filters = []
        if start_date:
            filters.append(f'record_date:gte:{start_date}')
        if end_date:
            filters.append(f'record_date:lte:{end_date}')

        if filters:
            params['filter'] = ','.join(filters)

        return self._paginate_request(self.ENDPOINTS['debt_to_penny'], params)

    # ===================== Utility Methods ===================== #

    def get_available_fields(self, endpoint_name: str) -> List[str]:
        """
        Get available fields for an endpoint by making a small request.

        Args:
            endpoint_name: Name of endpoint (from ENDPOINTS dict)

        Returns:
            List of field names
        """
        if endpoint_name not in self.ENDPOINTS:
            raise ValueError(f"Unknown endpoint: {endpoint_name}")

        params = {'page[size]': 1}
        response = self._make_request(self.ENDPOINTS[endpoint_name], params)

        data = response.get('data', [])
        if data:
            return list(data[0].keys())
        return []

    @property
    def request_count(self) -> int:
        """Number of requests made by this client instance."""
        return self._request_count


# Convenience function
def create_treasury_client(**kwargs) -> TreasuryClient:
    """Create a TreasuryClient instance with default settings."""
    return TreasuryClient(**kwargs)
