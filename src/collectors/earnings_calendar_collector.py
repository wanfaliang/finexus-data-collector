"""
Earnings Calendar Collector
Collects earnings announcements calendar from FMP API
Tracks upcoming earnings dates with estimated and actual EPS/revenue
"""
import logging
from datetime import datetime, date, timedelta
from typing import Optional

import pandas as pd
from sqlalchemy.dialects.postgresql import insert

from src.collectors.base_collector import BaseCollector
from src.database.models import EarningsCalendar
from src.config import FMP_ENDPOINTS
from src.utils.data_transform import transform_keys

logger = logging.getLogger(__name__)


class EarningsCalendarCollector(BaseCollector):
    """Collector for earnings calendar events"""

    def get_table_name(self) -> str:
        return "earnings_calendar"

    def collect_upcoming(self, days: int = 90) -> dict:
        """
        Collect upcoming earnings announcements for the next N days

        Args:
            days: Number of days to fetch (max 90 per API limit)

        Returns:
            Dictionary with collection results
        """
        if days > 90:
            logger.warning(f"API limit is 90 days, reducing from {days} to 90")
            days = 90

        from_date = date.today()
        to_date = from_date + timedelta(days=days)

        logger.info(f"Collecting upcoming earnings: {from_date} to {to_date}")
        return self.collect_range(from_date, to_date)

    def collect_range(self, from_date: date, to_date: date) -> dict:
        """
        Collect earnings announcements for a specific date range

        Args:
            from_date: Start date (inclusive)
            to_date: End date (inclusive)

        Returns:
            Dictionary with collection results
        """
        # Validate date range (API limit is 90 days)
        delta = (to_date - from_date).days
        if delta > 90:
            raise ValueError(f"Date range exceeds 90-day API limit: {delta} days")

        logger.info(f"Fetching earnings calendar: {from_date} to {to_date}")

        try:
            # Fetch from API
            url = FMP_ENDPOINTS['earnings_calendar']
            params = {
                'from': from_date.strftime('%Y-%m-%d'),
                'to': to_date.strftime('%Y-%m-%d')
            }

            response = self._get(url, params)
            data = self._json_safe(response)

            if not data:
                logger.warning(f"No earnings data returned for {from_date} to {to_date}")
                return {
                    'from_date': from_date,
                    'to_date': to_date,
                    'announcements_received': 0,
                    'announcements_upserted': 0,
                    'success': False
                }

            # Convert to DataFrame
            df = pd.DataFrame(data)
            announcements_received = len(df)
            logger.info(f"Received {announcements_received:,} earnings announcements")

            if df.empty:
                return {
                    'from_date': from_date,
                    'to_date': to_date,
                    'announcements_received': 0,
                    'announcements_upserted': 0,
                    'success': True
                }

            # Parse dates
            df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date
            if 'lastUpdated' in df.columns:
                df['last_updated'] = pd.to_datetime(df['lastUpdated'], errors='coerce').dt.date

            # Drop duplicates on primary key (API sometimes returns duplicates)
            # Keep the last occurrence (most recent data)
            before_dedup = len(df)
            df = df.drop_duplicates(subset=['symbol', 'date'], keep='last')
            if len(df) < before_dedup:
                logger.warning(f"Removed {before_dedup - len(df)} duplicate (symbol, date) pairs")

            # Clean NaN/inf values BEFORE to_dict() - critical for PostgreSQL
            import numpy as np
            df = df.replace({np.nan: None, np.inf: None, -np.inf: None})

            # Convert to records
            records = df.to_dict('records')

            # Transform camelCase keys to snake_case (API returns camelCase)
            records = [transform_keys(record) for record in records]

            # Upsert all records
            announcements_upserted = self._upsert_announcements(records)

            logger.info(f"Successfully upserted {announcements_upserted:,} announcements")

            return {
                'from_date': from_date,
                'to_date': to_date,
                'announcements_received': announcements_received,
                'announcements_upserted': announcements_upserted,
                'success': True
            }

        except Exception as e:
            logger.error(f"Error collecting earnings calendar: {e}")
            return {
                'from_date': from_date,
                'to_date': to_date,
                'announcements_received': 0,
                'announcements_upserted': 0,
                'success': False,
                'error': str(e)
            }

    def collect_historical(self, start_date: date, end_date: Optional[date] = None) -> dict:
        """
        Backfill historical earnings calendar data

        Fetches data in 90-day chunks from start_date to end_date

        Args:
            start_date: Earliest date to fetch
            end_date: Latest date to fetch (defaults to today)

        Returns:
            Dictionary with summary results
        """
        if end_date is None:
            end_date = date.today()

        logger.info(f"="*80)
        logger.info(f"HISTORICAL BACKFILL: Earnings Calendar")
        logger.info(f"From {start_date} to {end_date}")
        logger.info(f"="*80)

        total_days = (end_date - start_date).days
        logger.info(f"Total date range: {total_days} days")

        results = {
            'start_date': start_date,
            'end_date': end_date,
            'chunks_processed': 0,
            'chunks_successful': 0,
            'chunks_failed': 0,
            'total_announcements': 0
        }

        # Fetch in 90-day chunks
        current_from = start_date
        chunk_size = 90

        while current_from <= end_date:
            current_to = min(current_from + timedelta(days=chunk_size - 1), end_date)

            logger.info(f"\nProcessing chunk: {current_from} to {current_to}")

            result = self.collect_range(current_from, current_to)

            results['chunks_processed'] += 1
            if result['success']:
                results['chunks_successful'] += 1
                results['total_announcements'] += result['announcements_upserted']
            else:
                results['chunks_failed'] += 1

            # Move to next chunk
            current_from = current_to + timedelta(days=1)

        logger.info(f"\n{'='*80}")
        logger.info(f"HISTORICAL BACKFILL COMPLETE")
        logger.info(f"  Chunks processed: {results['chunks_processed']}")
        logger.info(f"  Successful: {results['chunks_successful']}")
        logger.info(f"  Failed: {results['chunks_failed']}")
        logger.info(f"  Total announcements: {results['total_announcements']:,}")
        logger.info(f"{'='*80}")

        return results

    def _upsert_announcements(self, records: list) -> int:
        """
        Upsert earnings calendar announcements

        Uses ON CONFLICT DO UPDATE to:
        - Insert new announcements
        - Update existing announcements (when actuals are released)

        Args:
            records: List of announcement dictionaries

        Returns:
            Number of records upserted
        """
        if not records:
            return 0

        try:
            stmt = insert(EarningsCalendar).values(records)

            # On conflict (same symbol+date), update all fields
            stmt = stmt.on_conflict_do_update(
                index_elements=['symbol', 'date'],
                set_={
                    'eps_actual': stmt.excluded.eps_actual,
                    'eps_estimated': stmt.excluded.eps_estimated,
                    'revenue_actual': stmt.excluded.revenue_actual,
                    'revenue_estimated': stmt.excluded.revenue_estimated,
                    'last_updated': stmt.excluded.last_updated,
                    'updated_at': stmt.excluded.updated_at
                }
            )

            self.session.execute(stmt)
            self.session.commit()

            return len(records)

        except Exception as e:
            logger.error(f"Error upserting announcements: {e}")
            self.session.rollback()
            raise

    def collect_for_symbol(self, symbol: str) -> bool:
        """Not applicable - use collect_upcoming() or collect_range()"""
        logger.warning("Earnings calendar does not use symbol-based collection")
        return True


if __name__ == "__main__":
    from src.database.connection import get_session

    logging.basicConfig(level=logging.INFO)

    with get_session() as session:
        collector = EarningsCalendarCollector(session)

        # Test: Collect next 30 days
        print("\nCollecting upcoming earnings (next 30 days)...")
        result = collector.collect_upcoming(days=30)
        print(f"Success: {result['success']}")
        print(f"Announcements: {result['announcements_upserted']:,}")
