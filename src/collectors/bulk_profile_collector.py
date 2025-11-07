"""
Bulk Profile Collector
Loads company profiles from bulk CSV files
"""
import logging
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy.dialects.postgresql import insert

from src.collectors.base_collector import BaseCollector
from src.database.models import Company
from src.utils.data_transform import transform_batch, transform_keys
from src.utils.bulk_utils import get_bulk_data_path, list_bulk_files

logger = logging.getLogger(__name__)

# Define US exchanges to filter for
US_EXCHANGES = [
    'NYSE',      # New York Stock Exchange
    'NASDAQ',    # NASDAQ
    'AMEX',      # American Stock Exchange
    'OTC',       # Over-the-Counter
    'CBOE',      # Chicago Board Options Exchange
    'PNK',       # Pink Sheets
    'BATS',      # BATS Exchange
    'NYSEARCA',  # NYSE Arca
    'INDEX',     # Market Indices
]


class BulkProfileCollector(BaseCollector):
    """Collector for bulk company profile data"""

    def __init__(self, session, us_only: bool = True):
        """
        Initialize bulk profile collector

        Args:
            session: SQLAlchemy session
            us_only: If True, filter for US exchanges only. If False, load all global companies.
        """
        super().__init__(session)
        self.us_only = us_only

    def get_table_name(self) -> str:
        return "companies"

    def process_bulk_file(self, file_path: Path) -> bool:
        """
        Process a bulk profile CSV file

        Args:
            file_path: Path to the CSV file

        Returns:
            True if successful
        """
        try:
            logger.info(f"Processing bulk profile file: {file_path.name}")

            # Read CSV
            df = pd.read_csv(file_path)
            logger.info(f"Loaded {len(df):,} records from CSV")

            # Filter for US exchanges only (if us_only is True)
            if self.us_only:
                initial_count = len(df)
                df = df[df['exchange'].isin(US_EXCHANGES)]
                if len(df) < initial_count:
                    logger.info(f"Filtered to {len(df):,} US companies (removed {initial_count - len(df):,} non-US)")
            else:
                logger.info(f"Loading ALL global companies (no exchange filter)")

            # Skip rows with null symbols
            initial_count = len(df)
            df = df[df['symbol'].notna()]
            if len(df) < initial_count:
                logger.warning(f"Skipped {initial_count - len(df)} rows with null symbols")

            # Remove duplicate symbols (keep first occurrence)
            before_dedup = len(df)
            df = df.drop_duplicates(subset=['symbol'], keep='first')
            if len(df) < before_dedup:
                logger.warning(f"Removed {before_dedup - len(df)} duplicate symbols from CSV")

            if df.empty:
                logger.warning("No valid records to process")
                return False

            # Transform column names from camelCase to snake_case
            records = df.to_dict('records')
            transformed_records = transform_batch(records, transform_keys)

            # Create dataframe with transformed columns
            df = pd.DataFrame(transformed_records)

            # Data type conversions
            df = self._convert_data_types(df)

            # Prepare records for insertion
            records = df.to_dict('records')

            # Insert in batches
            batch_size = 1000
            total_inserted = 0
            total_updated = 0

            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                inserted, updated = self._upsert_batch(batch)
                total_inserted += inserted
                total_updated += updated

                if (i + batch_size) % 5000 == 0:
                    logger.info(f"Processed {i + batch_size:,} / {len(records):,} records...")

            logger.info(f"✓ Completed: {total_inserted:,} inserted, {total_updated:,} updated")
            self.records_inserted += total_inserted
            self.records_updated += total_updated

            return True

        except Exception as e:
            logger.error(f"Error processing bulk profile file: {e}")
            raise

    def _convert_data_types(self, df: pd.DataFrame):
        """Convert data types to match database schema"""
        import numpy as np

        # Count NaN values before cleaning (for diagnostics)
        nan_count = df.isna().sum().sum()
        if nan_count > 0:
            logger.info(f"Cleaning {nan_count:,} NaN values from dataframe")

        # Convert date columns (handle NaT by converting to None)
        if 'ipo_date' in df.columns:
            df['ipo_date'] = pd.to_datetime(df['ipo_date'], errors='coerce')
            df['ipo_date'] = df['ipo_date'].apply(lambda x: x.date() if pd.notna(x) else None)

        # Cap numeric values to avoid overflow (NUMERIC(10, 4) max is 999,999.9999)
        numeric_10_4_cols = ['beta', 'last_dividend', 'change_percentage']
        for col in numeric_10_4_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].apply(lambda x: None if pd.isna(x) or abs(x) > 999999 else x)

        # Cap NUMERIC(20, 4) columns to avoid overflow (max is ~9.9e15)
        numeric_20_4_cols = ['price', 'change']
        for col in numeric_20_4_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].apply(lambda x: None if pd.isna(x) or abs(x) > 9999999999999999 else x)

        # Cap BigInteger columns to avoid overflow (PostgreSQL BIGINT max: 9223372036854775807)
        bigint_cols = ['market_cap', 'volume', 'average_volume']
        for col in bigint_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].apply(lambda x: None if pd.isna(x) or abs(x) > 9223372036854775807 else int(x) if pd.notna(x) else None)

        # Convert numeric columns that may have been read as floats but should be integers
        if 'full_time_employees' in df.columns:
            # Keep as string as model expects it
            df['full_time_employees'] = df['full_time_employees'].fillna('').astype(str)
            df['full_time_employees'] = df['full_time_employees'].replace('nan', '')

        # Convert cik to string and handle NaN
        if 'cik' in df.columns:
            df['cik'] = df['cik'].fillna('').astype(str)
            df['cik'] = df['cik'].replace('nan', '')
            df['cik'] = df['cik'].str.replace('.0', '', regex=False)  # Remove .0 from float conversion

        # Ensure boolean columns
        boolean_cols = ['default_image', 'is_etf', 'is_actively_trading', 'is_adr', 'is_fund']
        for col in boolean_cols:
            if col in df.columns:
                df[col] = df[col].fillna(False).astype(bool)

        # Truncate string columns to database limits and replace NaN with None
        string_limits = {
            'symbol': 20,
            'company_name': 200,
            'currency': 10,
            'cik': 20,
            'isin': 20,
            'cusip': 20,
            'exchange': 20,
            'exchange_full_name': 100,
            'industry': 100,
            'sector': 100,
            'country': 100,
            'website': 200,
            'ceo': 100,
            'phone': 50,
            'address': 200,
            'city': 100,
            'state': 50,
            'zip': 20,
            'image': 200,
            'full_time_employees': 20
        }

        for col, max_len in string_limits.items():
            if col in df.columns:
                df[col] = df[col].astype(str)
                df[col] = df[col].str[:max_len]  # Truncate to max length
                df[col] = df[col].where(pd.notna(df[col]) & (df[col] != 'nan') & (df[col] != ''), None)

        # FINAL PASS: Replace ALL NaN/inf with None using the CORRECT method
        # df.where() and df.apply() don't work - we MUST use replace with numpy.nan
        import numpy as np
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})

        return df

    def _upsert_batch(self, records: list) -> tuple[int, int]:
        """
        Insert or update a batch of records

        Returns:
            Tuple of (inserted_count, updated_count)
        """
        if not records:
            return 0, 0

        try:
            stmt = insert(Company).values(records)

            # Define update dictionary for conflict resolution
            # Update all fields except primary key and timestamps
            update_dict = {
                col.name: col
                for col in stmt.excluded
                if col.name not in ['company_id', 'symbol', 'created_at']
            }

            stmt = stmt.on_conflict_do_update(
                index_elements=['symbol'],
                set_=update_dict
            )

            result = self.session.execute(stmt)
            self.session.commit()

            # Approximate counts (PostgreSQL doesn't return exact insert/update counts easily)
            return len(records), 0

        except Exception as e:
            logger.error(f"Error upserting batch: {e}")
            self.session.rollback()

            # Try inserting records one by one to identify problematic records
            logger.info("Attempting to insert records individually...")
            inserted = 0
            failed = 0

            for record in records:
                try:
                    stmt = insert(Company).values([record])
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['symbol'],
                        set_={col.name: col for col in stmt.excluded
                              if col.name not in ['company_id', 'symbol', 'created_at']}
                    )
                    self.session.execute(stmt)
                    self.session.commit()
                    inserted += 1
                except Exception as e2:
                    symbol = record.get('symbol', 'UNKNOWN')
                    # Check for NaN values in the record
                    nan_fields = []
                    for k, v in record.items():
                        try:
                            if pd.isna(v):
                                nan_fields.append(f"{k}={v}")
                        except (TypeError, ValueError):
                            pass  # Not a value that can be NaN

                    if nan_fields:
                        logger.warning(f"Failed to insert {symbol}: NaN in [{', '.join(nan_fields[:5])}]")
                    else:
                        logger.warning(f"Failed to insert {symbol}: {str(e2)[:200]}")
                    self.session.rollback()
                    failed += 1

            logger.info(f"Individual insert complete: {inserted} succeeded, {failed} failed")
            return inserted, 0

    def process_latest_bulk_file(self) -> bool:
        """Find and process the most recent bulk profile file"""
        files = list_bulk_files('profile', subfolder='profiles')

        # Also check main folder
        main_files = list_bulk_files('profile')
        files.extend(main_files)

        if not files:
            logger.warning("No bulk profile files found")
            return False

        # Get most recent file
        latest_file = files[0]
        logger.info(f"Found latest bulk file: {latest_file}")

        return self.process_bulk_file(latest_file)

    def process_all_profile_files(self, subfolder: str = 'profiles') -> dict:
        """
        Process all profile bulk files in the folder

        Args:
            subfolder: Subfolder to search for files

        Returns:
            Dictionary with results per file
        """
        results = {}
        bulk_path = get_bulk_data_path() / subfolder

        if not bulk_path.exists():
            logger.warning(f"Folder not found: {bulk_path}")
            # Also check main folder
            bulk_path = get_bulk_data_path()
            if not bulk_path.exists():
                return results

        # Find all profile bulk files
        files = sorted(bulk_path.glob('profile_bulk_*.csv'))

        if not files:
            logger.warning(f"No profile bulk files found in {bulk_path}")
            return results

        logger.info(f"Found {len(files)} profile bulk files to process")

        for file_path in files:
            logger.info(f"\n{'='*80}")
            try:
                success = self.process_bulk_file(file_path)
                results[file_path.name] = success
            except Exception as e:
                logger.error(f"Failed to process {file_path.name}: {e}")
                results[file_path.name] = False

        return results

    def collect_for_all_symbols(self) -> dict:
        """Process all bulk profile data"""
        return self.process_all_profile_files()
