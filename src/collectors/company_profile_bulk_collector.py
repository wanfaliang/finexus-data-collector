"""
Company Profile Bulk Collector
Fetches company profile data for all companies from FMP bulk API (CSV)
Downloads data in 4 parts (part=0,1,2,3) and combines them
"""
import logging
import requests
import pandas as pd
from io import StringIO
from typing import Dict, List

from sqlalchemy.dialects.postgresql import insert

from src.collectors.base_collector import BaseCollector
from src.database.models import CompanyProfileBulk
from src.config import settings, FMP_ENDPOINTS

logger = logging.getLogger(__name__)


class CompanyProfileBulkCollector(BaseCollector):
    """Collector for Company Profile bulk data"""

    def __init__(self, session):
        super().__init__(session)
        self.endpoint = FMP_ENDPOINTS['company_profile_bulk']

    def get_table_name(self) -> str:
        return "company_profile_bulk"

    def collect_bulk_company_profiles(self) -> Dict:
        """
        Collect Company Profile data for all companies from bulk CSV API
        Downloads data in 4 parts (part=0,1,2,3) and combines them

        Returns:
            Dictionary with collection results
        """
        logger.info("="*80)
        logger.info("COLLECTING COMPANY PROFILE BULK DATA")
        logger.info("="*80)

        try:
            # Download all 4 parts
            all_dataframes = []
            parts = [0, 1, 2, 3]

            for part in parts:
                logger.info(f"Downloading part {part}/3...")
                df_part = self._download_part(part)

                if df_part is not None and not df_part.empty:
                    all_dataframes.append(df_part)
                    logger.info(f"  Part {part}: {len(df_part):,} symbols")
                else:
                    logger.warning(f"  Part {part}: No data returned")

            if not all_dataframes:
                logger.warning("No data returned from any part")
                return {
                    'success': False,
                    'symbols_received': 0,
                    'symbols_inserted': 0,
                    'error': 'No data from API'
                }

            # Combine all parts
            logger.info("Combining all parts...")
            df = pd.concat(all_dataframes, ignore_index=True)
            symbols_received = len(df)
            logger.info(f"Total symbols received: {symbols_received:,}")
            logger.info(f"CSV columns: {len(df.columns)}")

            # Clean and transform data
            df_clean = self._clean_data(df)
            logger.info(f"Cleaned data: {len(df_clean):,} valid records")

            # Insert into database
            inserted = self._upsert_records(df_clean)
            self.records_inserted += inserted

            # Update tracking
            self.update_tracking(
                'company_profile_bulk',
                symbol=None,  # Global bulk data
                record_count=self.session.query(CompanyProfileBulk).count(),
                next_update_frequency='daily'
            )

            logger.info("="*80)
            logger.info(f"✓ BULK COMPANY PROFILE COLLECTION COMPLETE")
            logger.info(f"  Symbols received: {symbols_received:,}")
            logger.info(f"  Symbols inserted/updated: {inserted:,}")
            logger.info("="*80)

            return {
                'success': True,
                'symbols_received': symbols_received,
                'symbols_inserted': inserted
            }

        except Exception as e:
            logger.error(f"Error collecting bulk company profiles: {e}")
            self.session.rollback()
            self.record_error('company_profile_bulk', 'ALL', str(e))
            return {
                'success': False,
                'symbols_received': 0,
                'symbols_inserted': 0,
                'error': str(e)
            }

    def _download_part(self, part: int) -> pd.DataFrame:
        """
        Download a single part of the company profile bulk data

        Args:
            part: Part number (0, 1, 2, or 3)

        Returns:
            DataFrame with company profiles
        """
        try:
            params = {
                'apikey': settings.api.fmp_api_key,
                'part': str(part)
            }
            response = requests.get(
                self.endpoint,
                params=params,
                timeout=settings.api.timeout
            )

            if response.status_code != 200:
                logger.error(f"Part {part}: API request failed with status {response.status_code}")
                return None

            # Parse CSV directly from response text
            csv_data = StringIO(response.text)
            df = pd.read_csv(csv_data)

            return df

        except Exception as e:
            logger.error(f"Error downloading part {part}: {e}")
            return None

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
            'price': 'price',
            'marketCap': 'market_cap',
            'beta': 'beta',
            'lastDividend': 'last_dividend',
            'range': 'range',
            'change': 'change',
            'changePercentage': 'change_percentage',
            'volume': 'volume',
            'averageVolume': 'average_volume',
            'companyName': 'company_name',
            'currency': 'currency',
            'cik': 'cik',
            'isin': 'isin',
            'cusip': 'cusip',
            'exchange': 'exchange',
            'exchangeFullName': 'exchange_full_name',
            'industry': 'industry',
            'sector': 'sector',
            'website': 'website',
            'description': 'description',
            'ceo': 'ceo',
            'country': 'country',
            'fullTimeEmployees': 'full_time_employees',
            'phone': 'phone',
            'address': 'address',
            'city': 'city',
            'state': 'state',
            'zip': 'zip',
            'image': 'image',
            'ipoDate': 'ipo_date',
            'defaultImage': 'default_image',
            'isEtf': 'is_etf',
            'isActivelyTrading': 'is_actively_trading',
            'isAdr': 'is_adr',
            'isFund': 'is_fund',
        }

        # Rename columns
        df = df.rename(columns=column_mapping)

        # Convert numeric columns
        numeric_cols = ['price', 'beta', 'last_dividend', 'change', 'change_percentage']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].replace([float('inf'), float('-inf')], None)

        # Convert BigInteger columns
        bigint_cols = ['market_cap', 'volume', 'average_volume']
        for col in bigint_cols:
            if col in df.columns:
                BIGINT_MAX = 9223372036854775807
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].clip(lower=-BIGINT_MAX, upper=BIGINT_MAX)
                df[col] = df[col].round().astype('Int64')

        # Convert Integer column
        if 'full_time_employees' in df.columns:
            df['full_time_employees'] = pd.to_numeric(df['full_time_employees'], errors='coerce')
            df['full_time_employees'] = df['full_time_employees'].round().astype('Int64')

        # Convert Boolean columns
        bool_cols = ['default_image', 'is_etf', 'is_actively_trading', 'is_adr', 'is_fund']
        for col in bool_cols:
            if col in df.columns:
                # Handle TRUE/FALSE strings
                df[col] = df[col].astype(str).str.upper()
                df[col] = df[col].map({'TRUE': True, 'FALSE': False, '1': True, '0': False})

        # Handle string columns - truncate if needed
        string_truncate = {
            'company_name': 255,
            'exchange_full_name': 100,
            'industry': 100,
            'sector': 100,
            'website': 255,
            'ceo': 100,
            'phone': 50,
            'address': 255,
            'city': 100,
            'state': 50,
            'range': 50,
            'cik': 20,
            'isin': 20,
            'cusip': 20,
            'exchange': 20,
            'currency': 10,
            'country': 10,
            'zip': 20,
            'image': 255,
            'ipo_date': 20,
        }

        for col, max_len in string_truncate.items():
            if col in df.columns:
                df[col] = df[col].astype(str).str[:max_len]
                df[col] = df[col].replace('nan', None)

        # description can be very long - keep as Text (no truncation)
        if 'description' in df.columns:
            df['description'] = df['description'].astype(str)
            df['description'] = df['description'].replace('nan', None)

        # Drop rows with missing symbol
        df = df.dropna(subset=['symbol'])

        # Filter symbols longer than 20 chars
        df = df[df['symbol'].astype(str).str.len() <= 20]

        # Remove duplicates (keep last occurrence)
        df = df.drop_duplicates(subset=['symbol'], keep='last')

        # Select only columns that exist in the model
        model_columns = [col for col in column_mapping.values() if col in df.columns]
        df = df[model_columns]

        return df

    def _upsert_records(self, df: pd.DataFrame) -> int:
        """
        Upsert records into company_profile_bulk table in batches

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
            sanitized = self.sanitize_record(record, CompanyProfileBulk, record.get('symbol'))
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
                stmt = insert(CompanyProfileBulk).values(batch)

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
