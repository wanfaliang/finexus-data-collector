"""
Price Target Summary Bulk Collector
Fetches price target summary data for all companies from FMP bulk API (CSV)
"""
import logging
import requests
import pandas as pd
from io import StringIO
from typing import Dict

from sqlalchemy.dialects.postgresql import insert

from src.collectors.base_collector import BaseCollector
from src.database.models import PriceTargetSummaryBulk
from src.config import settings, FMP_ENDPOINTS

logger = logging.getLogger(__name__)


class PriceTargetSummaryBulkCollector(BaseCollector):
    """Collector for Price Target Summary bulk data"""

    def __init__(self, session):
        super().__init__(session)
        self.endpoint = FMP_ENDPOINTS['price_target_summary_bulk']

    def get_table_name(self) -> str:
        return "price_target_summary_bulk"

    def collect_bulk_price_target_summary(self) -> Dict:
        """
        Collect Price Target Summary data for all companies from bulk CSV API

        Returns:
            Dictionary with collection results
        """
        logger.info("="*80)
        logger.info("COLLECTING PRICE TARGET SUMMARY BULK DATA")
        logger.info("="*80)

        try:
            # Make API request for CSV
            logger.info(f"Downloading CSV from FMP bulk API...")
            logger.info(f"Endpoint: {self.endpoint}")

            params = {'apikey': settings.api.fmp_api_key}
            response = requests.get(
                self.endpoint,
                params=params,
                timeout=settings.api.timeout
            )

            if response.status_code != 200:
                error_msg = f"API request failed with status {response.status_code}"
                logger.error(error_msg)
                return {
                    'success': False,
                    'symbols_received': 0,
                    'symbols_inserted': 0,
                    'error': error_msg
                }

            # Parse CSV directly from response text
            csv_data = StringIO(response.text)
            df = pd.read_csv(csv_data)

            if df.empty:
                logger.warning("No data returned from API")
                return {
                    'success': True,
                    'symbols_received': 0,
                    'symbols_inserted': 0
                }

            symbols_received = len(df)
            logger.info(f"Received data for {symbols_received:,} symbols")
            logger.info(f"CSV columns: {len(df.columns)}")

            # Clean and transform data
            df_clean = self._clean_data(df)
            logger.info(f"Cleaned data: {len(df_clean):,} valid records")

            # Insert into database
            inserted = self._upsert_records(df_clean)
            self.records_inserted += inserted

            # Update tracking
            self.update_tracking(
                'price_target_summary_bulk',
                symbol=None,  # Global bulk data
                record_count=self.session.query(PriceTargetSummaryBulk).count(),
                next_update_frequency='daily'
            )

            logger.info("="*80)
            logger.info(f"✓ BULK PRICE TARGET SUMMARY COLLECTION COMPLETE")
            logger.info(f"  Symbols received: {symbols_received:,}")
            logger.info(f"  Symbols inserted/updated: {inserted:,}")
            logger.info("="*80)

            return {
                'success': True,
                'symbols_received': symbols_received,
                'symbols_inserted': inserted
            }

        except Exception as e:
            logger.error(f"Error collecting bulk price target summary: {e}")
            self.session.rollback()
            self.record_error('price_target_summary_bulk', 'ALL', str(e))
            return {
                'success': False,
                'symbols_received': 0,
                'symbols_inserted': 0,
                'error': str(e)
            }

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and transform bulk CSV data

        Args:
            df: Raw DataFrame from CSV

        Returns:
            Cleaned DataFrame
        """
        df = df.copy()

        # Column mapping - convert camelCase to snake_case
        column_mapping = {
            'symbol': 'symbol',
            'lastMonthCount': 'last_month_count',
            'lastMonthAvgPriceTarget': 'last_month_avg_price_target',
            'lastQuarterCount': 'last_quarter_count',
            'lastQuarterAvgPriceTarget': 'last_quarter_avg_price_target',
            'lastYearCount': 'last_year_count',
            'lastYearAvgPriceTarget': 'last_year_avg_price_target',
            'allTimeCount': 'all_time_count',
            'allTimeAvgPriceTarget': 'all_time_avg_price_target',
            'publishers': 'publishers',
        }

        # Rename columns
        df = df.rename(columns=column_mapping)

        # Convert numeric columns
        count_cols = ['last_month_count', 'last_quarter_count', 'last_year_count', 'all_time_count']
        price_cols = ['last_month_avg_price_target', 'last_quarter_avg_price_target',
                      'last_year_avg_price_target', 'all_time_avg_price_target']

        # Convert counts to integers
        for col in count_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].round().astype('Int64')

        # Convert price targets to numeric
        for col in price_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                # Replace inf and -inf with None
                df[col] = df[col].replace([float('inf'), float('-inf')], None)

        # Keep publishers as-is (string/JSON)
        if 'publishers' in df.columns:
            df['publishers'] = df['publishers'].astype(str)
            df['publishers'] = df['publishers'].replace('nan', None)

        # Drop rows with missing symbol
        df = df.dropna(subset=['symbol'])

        # Filter symbols longer than 20 chars
        df = df[df['symbol'].astype(str).str.len() <= 20]

        # Select only columns that exist in the model
        model_columns = [col for col in column_mapping.values() if col in df.columns]
        df = df[model_columns]

        return df

    def _upsert_records(self, df: pd.DataFrame) -> int:
        """
        Upsert records into price_target_summary_bulk table in batches

        Args:
            df: Cleaned DataFrame

        Returns:
            Number of records inserted/updated
        """
        if df.empty:
            return 0

        # Replace NaN with None for PostgreSQL
        df = df.where(pd.notnull(df), None)

        records = df.to_dict('records')

        # Sanitize records to prevent overflow
        sanitized_records = []
        for record in records:
            sanitized = self.sanitize_record(record, PriceTargetSummaryBulk, record.get('symbol'))
            sanitized_records.append(sanitized)

        total_records = len(sanitized_records)
        batch_size = 1000  # Process 1000 records at a time
        total_inserted = 0
        error_logged = False  # Only log first error

        logger.info(f"Inserting {total_records:,} records...")

        # Process in batches
        for i in range(0, total_records, batch_size):
            batch = sanitized_records[i:i + batch_size]

            try:
                # UPSERT: insert or update on conflict
                stmt = insert(PriceTargetSummaryBulk).values(batch)

                # Get all columns except primary key and metadata
                update_columns = {
                    col: getattr(stmt.excluded, col)
                    for col in df.columns
                    if col not in ['symbol', 'created_at']
                }
                update_columns['updated_at'] = stmt.excluded.updated_at

                stmt = stmt.on_conflict_do_update(
                    index_elements=['symbol'],
                    set_=update_columns
                )

                self.session.execute(stmt)
                self.session.commit()

                total_inserted += len(batch)

            except Exception as e:
                self.session.rollback()
                if not error_logged:
                    logger.error(f"Batch failed. First error: {str(e)[:500]}")
                    error_logged = True
                continue

        logger.info(f"✓ Inserted: {total_inserted:,}/{total_records:,}")
        return total_inserted
