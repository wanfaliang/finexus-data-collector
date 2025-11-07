"""
Bulk EOD Price Collector
Fetches end-of-day prices for ALL global symbols from FMP bulk API
Stores in prices_daily_bulk table (no validation, no foreign keys)
"""
import logging
from datetime import datetime, date, timedelta
from io import StringIO
from typing import Optional

import pandas as pd
from sqlalchemy.dialects.postgresql import insert

from src.collectors.base_collector import BaseCollector
from src.database.models import PriceDailyBulk

logger = logging.getLogger(__name__)


class BulkPriceCollector(BaseCollector):
    """Collector for bulk EOD prices - unvalidated data lake"""

    def get_table_name(self) -> str:
        return "prices_daily_bulk"

    def collect_for_symbol(self, symbol: str) -> bool:
        """Not applicable - use collect_bulk_eod() instead"""
        raise NotImplementedError(
            "BulkPriceCollector works on entire market data. "
            "Use collect_bulk_eod(date) instead of collect_for_symbol()."
        )

    def collect_bulk_eod(self, target_date: Optional[date] = None) -> dict:
        """
        Fetch and store bulk EOD data for a specific date

        Args:
            target_date: Date to fetch (defaults to yesterday)

        Returns:
            Dictionary with results:
            {
                'date': date object,
                'symbols_received': int,
                'symbols_inserted': int,
                'success': bool
            }
        """
        # Default to yesterday (most recent complete trading day)
        if target_date is None:
            target_date = datetime.now().date() - timedelta(days=1)

        # Convert to string for API
        date_str = target_date.strftime('%Y-%m-%d')

        logger.info(f"="*80)
        logger.info(f"BULK EOD COLLECTION - {date_str}")
        logger.info(f"="*80)

        try:
            # Fetch bulk data (CSV format)
            logger.info(f"Fetching bulk EOD data for {date_str}...")
            url = "https://financialmodelingprep.com/stable/eod-bulk"
            params = {'date': date_str}

            response = self._get(url, params)

            if not response:
                logger.warning(f"Failed to fetch bulk data for {date_str}")
                return {
                    'date': target_date,
                    'symbols_received': 0,
                    'symbols_inserted': 0,
                    'success': False
                }

            # Parse CSV response
            csv_text = response.text

            if not csv_text or len(csv_text) < 100:  # Sanity check
                logger.warning(f"No bulk data returned for {date_str}")
                return {
                    'date': target_date,
                    'symbols_received': 0,
                    'symbols_inserted': 0,
                    'success': False
                }

            # Read CSV into DataFrame
            df = pd.read_csv(StringIO(csv_text))

            if df.empty:
                logger.warning(f"Empty CSV data returned for {date_str}")
                return {
                    'date': target_date,
                    'symbols_received': 0,
                    'symbols_inserted': 0,
                    'success': False
                }

            symbols_received = len(df)
            logger.info(f"[OK] Received {symbols_received:,} symbols from bulk API")

            # Transform to our schema (no validation - accept all data)
            records = []
            skipped = 0

            for _, item in df.iterrows():
                try:
                    # Basic sanity check - must have symbol and date
                    if not item.get('symbol') or not item.get('date'):
                        skipped += 1
                        continue

                    record = {
                        'symbol': str(item['symbol']).strip(),
                        'date': pd.to_datetime(item['date']).date(),
                        'open': self._safe_float(item.get('open')),
                        'high': self._safe_float(item.get('high')),
                        'low': self._safe_float(item.get('low')),
                        'close': self._safe_float(item.get('close')),
                        'adj_close': self._safe_float(item.get('adjClose')),
                        'volume': self._safe_int(item.get('volume')),
                        'collected_at': datetime.now()
                    }

                    records.append(record)

                except (ValueError, KeyError, TypeError) as e:
                    skipped += 1
                    continue

            if skipped > 0:
                logger.warning(f"Skipped {skipped} records with invalid data")

            if not records:
                logger.warning("No valid records to insert")
                return {
                    'date': target_date,
                    'symbols_received': symbols_received,
                    'symbols_inserted': 0,
                    'success': False
                }

            # Batch upsert all records
            logger.info(f"Inserting {len(records):,} records into database...")
            inserted = self._batch_upsert(records)

            logger.info(f"="*80)
            logger.info(f"[SUCCESS] BULK EOD COLLECTION COMPLETE")
            logger.info(f"  Date: {date_str}")
            logger.info(f"  Symbols received: {symbols_received:,}")
            logger.info(f"  Symbols inserted: {inserted:,}")
            logger.info(f"="*80)

            return {
                'date': target_date,
                'symbols_received': symbols_received,
                'symbols_inserted': inserted,
                'success': True
            }

        except Exception as e:
            logger.error(f"Error collecting bulk EOD for {date_str}: {e}")
            self.session.rollback()  # Must rollback before any other DB operation
            self.record_error('prices_daily_bulk', date_str, str(e))
            return {
                'date': target_date,
                'symbols_received': 0,
                'symbols_inserted': 0,
                'success': False,
                'error': str(e)
            }

    def collect_bulk_date_range(self, start_date: date, end_date: date) -> dict:
        """
        Collect bulk EOD data for a date range

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            Dictionary with summary results
        """
        logger.info(f"Collecting bulk EOD data from {start_date} to {end_date}")

        results = {
            'start_date': start_date,
            'end_date': end_date,
            'dates_processed': 0,
            'dates_successful': 0,
            'dates_failed': 0,
            'total_symbols': 0
        }

        current_date = start_date
        while current_date <= end_date:
            result = self.collect_bulk_eod(current_date)

            results['dates_processed'] += 1
            if result['success']:
                results['dates_successful'] += 1
                results['total_symbols'] += result['symbols_inserted']
            else:
                results['dates_failed'] += 1

            # Move to next day
            current_date += timedelta(days=1)

        logger.info(f"="*80)
        logger.info(f"BULK DATE RANGE COLLECTION COMPLETE")
        logger.info(f"  Dates processed: {results['dates_processed']}")
        logger.info(f"  Successful: {results['dates_successful']}")
        logger.info(f"  Failed: {results['dates_failed']}")
        logger.info(f"  Total symbols: {results['total_symbols']:,}")
        logger.info(f"="*80)

        return results

    def _batch_upsert(self, records: list) -> int:
        """
        Batch upsert records into prices_daily_bulk

        Args:
            records: List of price record dictionaries

        Returns:
            Number of records inserted/updated
        """
        if not records:
            return 0

        # Sanitize records to prevent overflow
        sanitized_records = []
        for record in records:
            sanitized = self.sanitize_record(record, PriceDailyBulk)
            sanitized_records.append(sanitized)

        # Insert in batches to avoid memory issues
        # Using 1000 instead of 10000 to keep error messages manageable
        batch_size = 1000
        total_inserted = 0

        for i in range(0, len(sanitized_records), batch_size):
            batch = sanitized_records[i:i+batch_size]

            try:
                stmt = insert(PriceDailyBulk).values(batch)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['symbol', 'date'],
                    set_={
                        'open': stmt.excluded.open,
                        'high': stmt.excluded.high,
                        'low': stmt.excluded.low,
                        'close': stmt.excluded.close,
                        'adj_close': stmt.excluded.adj_close,
                        'volume': stmt.excluded.volume,
                        'collected_at': stmt.excluded.collected_at
                    }
                )

                self.session.execute(stmt)
                self.session.commit()  # Commit successful batch immediately
                total_inserted += len(batch)

                # Log progress for large batches
                if len(sanitized_records) > batch_size:
                    logger.info(f"  Progress: {total_inserted:,} / {len(sanitized_records):,}")

            except Exception as batch_error:
                logger.error(f"Error inserting batch {i}-{i+len(batch)}: {batch_error}")
                logger.error(f"First symbol in failed batch: {batch[0].get('symbol') if batch else 'unknown'}")
                self.session.rollback()

                # Try inserting records one by one to identify problem records
                logger.warning(f"Attempting individual inserts for batch {i}-{i+len(batch)}...")
                for idx, record in enumerate(batch):
                    try:
                        single_stmt = insert(PriceDailyBulk).values([record])
                        single_stmt = single_stmt.on_conflict_do_update(
                            index_elements=['symbol', 'date'],
                            set_={
                                'open': single_stmt.excluded.open,
                                'high': single_stmt.excluded.high,
                                'low': single_stmt.excluded.low,
                                'close': single_stmt.excluded.close,
                                'adj_close': single_stmt.excluded.adj_close,
                                'volume': single_stmt.excluded.volume,
                                'collected_at': single_stmt.excluded.collected_at
                            }
                        )
                        self.session.execute(single_stmt)
                        self.session.commit()
                        total_inserted += 1
                    except Exception as single_error:
                        logger.error(f"Failed to insert record: {record.get('symbol')} - {single_error}")
                        self.session.rollback()
                        # Skip this record and continue

        # All commits happen in the loop above
        return total_inserted

    def _safe_float(self, value) -> Optional[float]:
        """Safely convert value to float"""
        if value is None or value == '':
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _safe_int(self, value) -> Optional[int]:
        """Safely convert value to integer"""
        if value is None or value == '':
            return None
        try:
            return int(float(value))  # Handle "123.0" strings
        except (ValueError, TypeError):
            return None


if __name__ == "__main__":
    from src.database.connection import get_session

    logging.basicConfig(level=logging.INFO)

    with get_session() as session:
        collector = BulkPriceCollector(session)

        # Test collecting yesterday's data
        result = collector.collect_bulk_eod()

        if result['success']:
            print(f"\n[OK] Successfully collected {result['symbols_inserted']:,} symbols")
        else:
            print(f"\n[FAILED] Collection failed")
            if 'error' in result:
                print(f"Error: {result['error']}")
