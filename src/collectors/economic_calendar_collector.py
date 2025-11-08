"""
Economic Calendar Collector
Collects economic data releases calendar from FMP API
Tracks upcoming events and historical releases with estimates and actual values
"""
import logging
from datetime import datetime, date, timedelta
from typing import Optional

import pandas as pd
from sqlalchemy.dialects.postgresql import insert

from src.collectors.base_collector import BaseCollector
from src.database.models import EconomicCalendar
from src.config import FMP_ENDPOINTS
from src.utils.data_transform import transform_keys

logger = logging.getLogger(__name__)


class EconomicCalendarCollector(BaseCollector):
    """Collector for economic calendar events"""

    def get_table_name(self) -> str:
        return "economic_calendar"

    def collect_upcoming(self, days: int = 90) -> dict:
        """
        Collect upcoming economic events for the next N days

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

        logger.info(f"Collecting upcoming economic events: {from_date} to {to_date}")
        return self.collect_range(from_date, to_date)

    def collect_range(self, from_date: date, to_date: date) -> dict:
        """
        Collect economic events for a specific date range

        Args:
            from_date: Start date (inclusive)
            to_date: End date (inclusive)

        Returns:
            Dictionary with collection results:
            {
                'from_date': date,
                'to_date': date,
                'events_received': int,
                'events_upserted': int,
                'success': bool
            }
        """
        # Validate date range (API limit is 90 days)
        delta = (to_date - from_date).days
        if delta > 90:
            raise ValueError(f"Date range exceeds 90-day API limit: {delta} days")

        logger.info(f"Fetching economic calendar: {from_date} to {to_date}")

        try:
            # Fetch from API
            url = FMP_ENDPOINTS['economic_calendar']
            params = {
                'from': from_date.strftime('%Y-%m-%d'),
                'to': to_date.strftime('%Y-%m-%d')
            }

            response = self._get(url, params)
            data = self._json_safe(response)

            if not data:
                logger.warning(f"No economic calendar data returned for {from_date} to {to_date}")
                return {
                    'from_date': from_date,
                    'to_date': to_date,
                    'events_received': 0,
                    'events_upserted': 0,
                    'success': False
                }

            # Convert to DataFrame
            df = pd.DataFrame(data)
            events_received = len(df)
            logger.info(f"Received {events_received:,} economic events")

            if df.empty:
                return {
                    'from_date': from_date,
                    'to_date': to_date,
                    'events_received': 0,
                    'events_upserted': 0,
                    'success': True
                }

            # Parse date as datetime (includes time component)
            df['date'] = pd.to_datetime(df['date'], errors='coerce')

            # Clean NaN/inf values BEFORE to_dict() - critical for PostgreSQL
            import numpy as np
            df = df.replace({np.nan: None, np.inf: None, -np.inf: None})

            # Convert to records
            records = df.to_dict('records')

            # Transform camelCase keys to snake_case (API returns camelCase)
            records = [transform_keys(record) for record in records]

            # Upsert all records
            events_upserted = self._upsert_events(records)

            logger.info(f"Successfully upserted {events_upserted:,} events")

            return {
                'from_date': from_date,
                'to_date': to_date,
                'events_received': events_received,
                'events_upserted': events_upserted,
                'success': True
            }

        except Exception as e:
            logger.error(f"Error collecting economic calendar: {e}")
            return {
                'from_date': from_date,
                'to_date': to_date,
                'events_received': 0,
                'events_upserted': 0,
                'success': False,
                'error': str(e)
            }

    def collect_historical(self, start_date: date, end_date: Optional[date] = None) -> dict:
        """
        Backfill historical economic calendar data

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
        logger.info(f"HISTORICAL BACKFILL: Economic Calendar")
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
            'total_events': 0
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
                results['total_events'] += result['events_upserted']
            else:
                results['chunks_failed'] += 1

            # Move to next chunk
            current_from = current_to + timedelta(days=1)

        logger.info(f"\n{'='*80}")
        logger.info(f"HISTORICAL BACKFILL COMPLETE")
        logger.info(f"  Chunks processed: {results['chunks_processed']}")
        logger.info(f"  Successful: {results['chunks_successful']}")
        logger.info(f"  Failed: {results['chunks_failed']}")
        logger.info(f"  Total events: {results['total_events']:,}")
        logger.info(f"{'='*80}")

        return results

    def _upsert_events(self, records: list) -> int:
        """
        Upsert economic calendar events

        Uses ON CONFLICT DO UPDATE to:
        - Insert new events
        - Update existing events (when estimates or actuals change)

        Args:
            records: List of event dictionaries

        Returns:
            Number of records upserted
        """
        if not records:
            return 0

        try:
            stmt = insert(EconomicCalendar).values(records)

            # On conflict (same date+country+event), update all fields
            stmt = stmt.on_conflict_do_update(
                index_elements=['date', 'country', 'event'],
                set_={
                    'currency': stmt.excluded.currency,
                    'previous': stmt.excluded.previous,
                    'estimate': stmt.excluded.estimate,
                    'actual': stmt.excluded.actual,
                    'change': stmt.excluded.change,
                    'change_percentage': stmt.excluded.change_percentage,
                    'impact': stmt.excluded.impact,
                    'unit': stmt.excluded.unit,
                    'updated_at': stmt.excluded.updated_at
                }
            )

            self.session.execute(stmt)
            self.session.commit()

            return len(records)

        except Exception as e:
            logger.error(f"Error upserting events: {e}")
            self.session.rollback()
            raise

    def collect_for_symbol(self, symbol: str) -> bool:
        """Not applicable for economic calendar - use collect_upcoming() or collect_range()"""
        logger.warning("Economic calendar does not use symbol-based collection")
        return True


if __name__ == "__main__":
    from src.database.connection import get_session

    logging.basicConfig(level=logging.INFO)

    with get_session() as session:
        collector = EconomicCalendarCollector(session)

        # Test: Collect next 90 days
        print("\nCollecting upcoming events (next 90 days)...")
        result = collector.collect_upcoming(days=90)
        print(f"Success: {result['success']}")
        print(f"Events: {result['events_upserted']:,}")
