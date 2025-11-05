"""
Base Collector Class
Provides common functionality for all data collectors including:
- API request handling with retry logic
- Incremental update logic
- Error handling and logging
- Update tracking
"""
import time
from datetime import datetime, date, timedelta
from typing import Optional, Dict, List, Any
import logging

import requests
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import desc

from src.config import settings, FMP_ENDPOINTS
from src.database.models import (
    Company, TableUpdateTracking, DataCollectionLog
)


logger = logging.getLogger(__name__)


class BaseCollector:
    """Base class for all data collectors"""
    
    def __init__(self, session: Session):
        """
        Initialize base collector
        
        Args:
            session: SQLAlchemy database session
        """
        self.session = session
        self.api_key = settings.api.fmp_api_key
        self.sleep_sec = settings.api.sleep_sec
        self.timeout = settings.api.timeout
        self.retries = settings.api.retries
        self.backoff = settings.api.backoff
        
        self.http_session = requests.Session()
        self.http_session.headers.update({"User-Agent": "FinExusCollector/1.0"})

        # Tracking
        self.run_id: Optional[int] = None
        self.records_inserted = 0
        self.records_updated = 0
        self.records_failed = 0
        self.companies_processed = 0
        self.companies_failed = 0
        self.errors: List[Dict[str, Any]] = []

        # Force refill mode (bypass incremental update logic)
        self.force_refill = False
    
    def get_table_name(self) -> str:
        """
        Return the primary table name this collector updates

        Subclasses should override this to return their specific table name.
        This default implementation returns a generic name based on the class name.

        Returns:
            Table name string
        """
        # Default: convert class name to snake_case table name
        # e.g., FinancialCollector -> financial_collector
        class_name = self.__class__.__name__
        # Remove 'Collector' suffix if present
        if class_name.endswith('Collector'):
            class_name = class_name[:-9]  # Remove 'Collector'

        # Convert to snake_case
        import re
        snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', class_name).lower()
        return snake_case

    def is_index_symbol(self, symbol: str) -> bool:
        """
        Check if a symbol is a market index

        Args:
            symbol: The symbol to check

        Returns:
            True if the symbol is an index, False otherwise
        """
        # Check if symbol starts with ^ (common index notation)
        if symbol.startswith('^'):
            return True

        # Check if the symbol has exchange='INDEX' in companies table
        try:
            company = self.session.query(Company).filter(Company.symbol == symbol).first()
            if company and company.exchange == 'INDEX':
                return True
        except Exception:
            pass

        return False
    
    def collect_for_symbol(self, symbol: str) -> bool:
        """
        Collect data for a specific symbol
        
        Subclasses should override this method to implement symbol-specific collection.
        This default implementation logs a warning and returns True.
        
        Args:
            symbol: Stock ticker symbol
        
        Returns:
            True if successful, False otherwise
        """
        logger.warning(f"{self.__class__.__name__} does not implement collect_for_symbol for {symbol}")
        return True
    
    def start_collection_run(self, job_name: str, companies: List[str]) -> int:
        """
        Start a new collection run and create log entry
        
        Args:
            job_name: Name of the job
            companies: List of company symbols to process
        
        Returns:
            run_id for tracking
        """
        log_entry = DataCollectionLog(
            job_name=job_name,
            start_time=datetime.now(),
            status='running',
            companies_requested=len(companies),
            companies_processed=0,
            companies_failed=0,
            records_inserted=0,
            records_updated=0,
            records_failed=0
        )
        self.session.add(log_entry)
        self.session.commit()
        
        self.run_id = log_entry.run_id
        logger.info(f"Started collection run {self.run_id} for {job_name}")
        return self.run_id
    
    def end_collection_run(self, status: str = 'success'):
        """
        End collection run and update log entry
        
        Args:
            status: 'success', 'failed', or 'partial'
        """
        if not self.run_id:
            return
        
        log_entry = self.session.query(DataCollectionLog)\
            .filter(DataCollectionLog.run_id == self.run_id)\
            .first()
        
        if log_entry:
            log_entry.end_time = datetime.now()
            log_entry.status = status
            log_entry.companies_processed = self.companies_processed
            log_entry.companies_failed = self.companies_failed
            log_entry.records_inserted = self.records_inserted
            log_entry.records_updated = self.records_updated
            log_entry.records_failed = self.records_failed
            
            if self.errors:
                import json
                log_entry.error_details = json.dumps(self.errors)
                log_entry.error_message = f"{len(self.errors)} errors occurred"
            
            self.session.commit()
            logger.info(f"Ended collection run {self.run_id} with status: {status}")
    
    def _get(self, url: str, params: Dict = None) -> Optional[requests.Response]:
        """
        Make HTTP GET request with retry logic
        
        Args:
            url: API endpoint URL
            params: Query parameters
        
        Returns:
            Response object or None if failed
        """
        params = dict(params or {})
        params['apikey'] = self.api_key
        
        for attempt in range(self.retries):
            try:
                response = self.http_session.get(
                    url, 
                    params=params, 
                    timeout=self.timeout
                )
                
                if response.status_code == 200:
                    if self.sleep_sec:
                        time.sleep(self.sleep_sec)
                    return response
                
                # Retry on specific error codes
                if response.status_code in (429, 500, 502, 503, 504):
                    wait_time = self.backoff * (2 ** attempt)
                    logger.warning(
                        f"API returned {response.status_code}, "
                        f"retrying in {wait_time}s (attempt {attempt + 1}/{self.retries})"
                    )
                    time.sleep(wait_time)
                    continue
                
                # Other errors - don't retry
                logger.error(f"API request failed with status {response.status_code}")
                break
                
            except requests.exceptions.Timeout:
                logger.warning(f"Request timeout (attempt {attempt + 1}/{self.retries})")
                time.sleep(self.backoff * (2 ** attempt))
            except Exception as e:
                logger.error(f"Request error: {e}")
                time.sleep(self.backoff * (2 ** attempt))
        
        return None
    
    def _json_safe(self, response: Optional[requests.Response]) -> Optional[Any]:
        """
        Safely extract JSON from response
        
        Args:
            response: HTTP response object
        
        Returns:
            Parsed JSON or None
        """
        if response is None:
            return None
        
        try:
            return response.json()
        except Exception as e:
            logger.error(f"Failed to parse JSON response: {e}")
            return None
    
    def _to_dataframe(self, data: Any) -> pd.DataFrame:
        """
        Convert various data types to DataFrame
        
        Args:
            data: List, dict, or DataFrame
        
        Returns:
            DataFrame
        """
        if data is None:
            return pd.DataFrame()
        
        if isinstance(data, pd.DataFrame):
            return data
        
        if isinstance(data, list):
            return pd.DataFrame(data)
        
        if isinstance(data, dict):
            return pd.DataFrame([data])
        
        return pd.DataFrame()
    
    def should_update_symbol(
        self,
        table_name: str,
        symbol: str,
        max_age_days: Optional[int] = None
    ) -> bool:
        """
        Check if symbol needs updating based on tracking table

        Args:
            table_name: Name of table to check
            symbol: Stock symbol
            max_age_days: Maximum age in days before requiring update

        Returns:
            True if update needed, False otherwise
        """
        # Force refill mode: always update
        if self.force_refill:
            return True

        tracking = self.session.query(TableUpdateTracking)\
            .filter(TableUpdateTracking.table_name == table_name)\
            .filter(TableUpdateTracking.symbol == symbol)\
            .first()

        if not tracking:
            # Never updated before
            return True

        if max_age_days:
            age = (datetime.now() - tracking.last_update_timestamp).days
            if age >= max_age_days:
                return True

        # Check if next update is due
        if tracking.next_update_due and datetime.now() >= tracking.next_update_due:
            return True

        return False
    
    def get_last_date_from_db(
        self, 
        table_name: str, 
        symbol: str
    ) -> Optional[date]:
        """
        Get the last date available in database for a symbol
        
        Args:
            table_name: Name of table
            symbol: Stock symbol
        
        Returns:
            Last date or None
        """
        tracking = self.session.query(TableUpdateTracking)\
            .filter(TableUpdateTracking.table_name == table_name)\
            .filter(TableUpdateTracking.symbol == symbol)\
            .first()
        
        return tracking.last_api_date if tracking else None
    
    def update_tracking(
        self,
        table_name: str,
        symbol: str,
        last_api_date: Optional[date] = None,
        record_count: Optional[int] = None,
        next_update_frequency: Optional[str] = None
    ):
        """
        Update tracking table after successful collection
        
        Args:
            table_name: Name of table updated
            symbol: Stock symbol
            last_api_date: Latest date from API
            record_count: Number of records in table for this symbol
            next_update_frequency: 'daily', 'weekly', 'monthly', 'quarterly'
        """
        tracking = self.session.query(TableUpdateTracking)\
            .filter(TableUpdateTracking.table_name == table_name)\
            .filter(TableUpdateTracking.symbol == symbol)\
            .first()
        
        if not tracking:
            tracking = TableUpdateTracking(
                table_name=table_name,
                symbol=symbol,
                last_update_timestamp=datetime.now(),
                last_api_date=last_api_date,
                record_count=record_count or 0,
                consecutive_errors=0
            )
            self.session.add(tracking)
        else:
            tracking.last_update_timestamp = datetime.now()
            if last_api_date:
                tracking.last_api_date = last_api_date
            if record_count is not None:
                tracking.record_count = record_count
            tracking.consecutive_errors = 0
            tracking.last_error = None
        
        # Calculate next update due date
        if next_update_frequency:
            tracking.update_frequency = next_update_frequency
            tracking.next_update_due = self._calculate_next_update(next_update_frequency)
        
        self.session.commit()
    
    def record_error(
        self,
        table_name: str,
        symbol: str,
        error_message: str
    ):
        """
        Record an error in tracking table
        
        Args:
            table_name: Name of table
            symbol: Stock symbol
            error_message: Error description
        """
        tracking = self.session.query(TableUpdateTracking)\
            .filter(TableUpdateTracking.table_name == table_name)\
            .filter(TableUpdateTracking.symbol == symbol)\
            .first()
        
        if tracking:
            tracking.last_error = error_message
            tracking.consecutive_errors = (tracking.consecutive_errors or 0) + 1
            self.session.commit()
        
        # Also add to run errors list
        self.errors.append({
            'table': table_name,
            'symbol': symbol,
            'error': error_message,
            'timestamp': datetime.now().isoformat()
        })
    
    def _calculate_next_update(self, frequency: str) -> datetime:
        """
        Calculate next update due date based on frequency
        
        Args:
            frequency: 'daily', 'weekly', 'monthly', 'quarterly'
        
        Returns:
            Next update datetime
        """
        now = datetime.now()
        
        if frequency == 'daily':
            return now + timedelta(days=1)
        elif frequency == 'weekly':
            return now + timedelta(weeks=1)
        elif frequency == 'monthly':
            return now + timedelta(days=30)
        elif frequency == 'quarterly':
            return now + timedelta(days=90)
        else:
            return now + timedelta(days=1)
    
    def get_all_symbols(self) -> List[str]:
        """
        Get all symbols from companies table
        
        Returns:
            List of symbols
        """
        companies = self.session.query(Company.symbol).all()
        return [c[0] for c in companies]
    
    def collect_for_all_symbols(self) -> Dict[str, Any]:
        """
        Collect data for all symbols in database
        
        Returns:
            Summary dict with results
        """
        symbols = self.get_all_symbols()
        job_name = f"{self.__class__.__name__}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        self.start_collection_run(job_name, symbols)
        
        logger.info(f"Starting collection for {len(symbols)} symbols")
        
        for symbol in symbols:
            try:
                logger.info(f"Collecting data for {symbol}...")
                success = self.collect_for_symbol(symbol)
                
                if success:
                    self.companies_processed += 1
                    logger.info(f"✓ Successfully collected data for {symbol}")
                else:
                    self.companies_failed += 1
                    logger.warning(f"✗ Failed to collect data for {symbol}")
                    
            except Exception as e:
                self.companies_failed += 1
                error_msg = f"Unexpected error: {str(e)}"
                logger.error(f"✗ Error collecting {symbol}: {error_msg}")
                self.record_error(self.get_table_name(), symbol, error_msg)
        
        status = 'success' if self.companies_failed == 0 else 'partial'
        if self.companies_processed == 0:
            status = 'failed'
        
        self.end_collection_run(status)
        
        return {
            'run_id': self.run_id,
            'status': status,
            'total_symbols': len(symbols),
            'processed': self.companies_processed,
            'failed': self.companies_failed,
            'records_inserted': self.records_inserted,
            'records_updated': self.records_updated,
            'errors': len(self.errors)
        }

    def sanitize_record(self, record: Dict[str, Any], model: Any, symbol: str = None) -> Dict[str, Any]:
        """
        Sanitize a record to prevent database constraint violations.
        Only modifies values that would cause actual database errors.

        Args:
            record: Dictionary of field values
            model: SQLAlchemy model class
            symbol: Symbol for logging purposes

        Returns:
            Sanitized record dictionary
        """
        from sqlalchemy import Numeric, String, BigInteger
        from decimal import Decimal

        sanitized = record.copy()

        for column in model.__table__.columns:
            field_name = column.name
            if field_name not in sanitized:
                continue

            value = sanitized[field_name]
            if value is None:
                continue

            # Handle Numeric fields with precision/scale constraints
            if isinstance(column.type, Numeric):
                if isinstance(value, (int, float, Decimal)):
                    precision = column.type.precision
                    scale = column.type.scale

                    if precision and scale:
                        # Calculate max value: 10^(precision - scale) - 1
                        # E.g., Numeric(10, 4): 10^(10-4) = 10^6 = 1,000,000 max
                        #       Numeric(10, 6): 10^(10-6) = 10^4 = 10,000 max
                        max_value = 10 ** (precision - scale)

                        if abs(value) >= max_value * 0.9:  # 90% threshold for safety
                            logger.warning(f"Sanitizing {symbol or 'record'}.{field_name}: {value} -> NULL (exceeds Numeric({precision},{scale}) limit)")
                            sanitized[field_name] = None

            # Handle String fields - truncate to max length
            elif isinstance(column.type, String):
                if isinstance(value, str) and column.type.length:
                    max_len = column.type.length
                    if len(value) > max_len:
                        logger.warning(f"Sanitizing {symbol or 'record'}.{field_name}: truncating string from {len(value)} to {max_len} chars")
                        sanitized[field_name] = value[:max_len]

            # Handle BigInteger overflow (PostgreSQL max: 2^63 - 1)
            elif isinstance(column.type, BigInteger):
                if isinstance(value, (int, float)):
                    max_bigint = 9223372036854775807  # 2^63 - 1
                    if abs(value) > max_bigint:
                        logger.warning(f"Sanitizing {symbol or 'record'}.{field_name}: {value} -> NULL (exceeds BigInteger limit)")
                        sanitized[field_name] = None

        return sanitized


if __name__ == "__main__":
    # This is an abstract base class and cannot be instantiated directly
    print("BaseCollector is an abstract class. Use specific collector implementations.")
