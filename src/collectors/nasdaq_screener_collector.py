"""
Nasdaq Screener Collector - Process downloaded CSV and load into database
"""
import logging
import pandas as pd
from pathlib import Path
from datetime import date, datetime
from typing import Optional, List

from sqlalchemy.dialects.postgresql import insert

from src.collectors.base_collector import BaseCollector
from src.database.models import NasdaqScreenerProfile
from src.config import settings

logger = logging.getLogger(__name__)

# Try Selenium first, fallback to Playwright
try:
    from src.utils.nasdaq_screener_selenium import download_nasdaq_screener_csv_selenium
    USE_SELENIUM = True
    logger.info("Using Selenium for Nasdaq screener downloads")
except ImportError:
    from src.utils.nasdaq_screener_downloader import download_nasdaq_screener_csv
    USE_SELENIUM = False
    logger.info("Using Playwright for Nasdaq screener downloads")


class NasdaqScreenerCollector(BaseCollector):
    """Collector for Nasdaq stock screener data"""

    def __init__(self, session):
        super().__init__(session)
        self.screener_path = Path(settings.data_collection.nasdaq_screener_path)

    def get_table_name(self) -> str:
        return "nasdaq_screener_profiles"

    def collect(self, csv_path: Optional[str] = None, snapshot_date: Optional[date] = None) -> bool:
        """
        Collect Nasdaq screener data from CSV

        Args:
            csv_path: Path to CSV file. If None, downloads fresh data.
            snapshot_date: Date for this snapshot. If None, uses today.

        Returns:
            True if successful
        """
        try:
            # Download CSV if not provided
            if csv_path is None:
                logger.info("Downloading fresh Nasdaq screener data...")
                if USE_SELENIUM:
                    csv_path = download_nasdaq_screener_csv_selenium(headless=False)  # Use visible browser for better compatibility
                else:
                    csv_path = download_nasdaq_screener_csv(headless=True)
                logger.info(f"Downloaded to: {csv_path}")
            else:
                csv_path = Path(csv_path)
                if not csv_path.exists():
                    raise FileNotFoundError(f"CSV file not found: {csv_path}")

            # Use today's date if not specified
            if snapshot_date is None:
                snapshot_date = datetime.now().date()

            logger.info(f"Processing Nasdaq screener CSV: {csv_path}")
            logger.info(f"Snapshot date: {snapshot_date}")

            # Read CSV
            df = pd.read_csv(csv_path)
            logger.info(f"Loaded {len(df):,} stocks from CSV")

            # Clean and transform data
            df_clean = self._clean_data(df, snapshot_date)
            logger.info(f"Cleaned data: {len(df_clean):,} valid records")

            # Insert into database
            inserted = self._insert_records(df_clean)
            self.records_inserted += inserted

            # Update tracking
            self.update_tracking(
                'nasdaq_screener_profiles',
                symbol=None,  # Global data, no specific symbol
                last_api_date=snapshot_date,
                record_count=self.session.query(NasdaqScreenerProfile)
                    .filter(NasdaqScreenerProfile.snapshot_date == snapshot_date).count(),
                next_update_frequency='daily'
            )

            logger.info(f"Successfully processed {inserted:,} Nasdaq screener records")
            return True

        except Exception as e:
            logger.error(f"Error collecting Nasdaq screener data: {e}")
            self.record_error('nasdaq_screener_profiles', 'ALL', str(e))
            return False

    def _clean_data(self, df: pd.DataFrame, snapshot_date: date) -> pd.DataFrame:
        """
        Clean and transform CSV data

        Args:
            df: Raw DataFrame from CSV
            snapshot_date: Snapshot date for all records

        Returns:
            Cleaned DataFrame
        """
        df = df.copy()

        # Add snapshot_date
        df['snapshot_date'] = snapshot_date

        # Rename columns to match database schema
        column_mapping = {
            'Symbol': 'symbol',
            'Name': 'name',
            'Last Sale': 'last_sale',
            'Net Change': 'net_change',
            '% Change': 'percent_change',
            'Market Cap': 'market_cap',
            'Country': 'country',
            'IPO Year': 'ipo_year',
            'Volume': 'volume',
            'Sector': 'sector',
            'Industry': 'industry'
        }
        df = df.rename(columns=column_mapping)

        # Clean price fields (remove $ sign)
        if 'last_sale' in df.columns:
            df['last_sale'] = df['last_sale'].astype(str).str.replace('$', '', regex=False).str.strip()
            df['last_sale'] = pd.to_numeric(df['last_sale'], errors='coerce')

        # Clean percentage fields (remove % sign)
        if 'percent_change' in df.columns:
            df['percent_change'] = df['percent_change'].astype(str).str.replace('%', '', regex=False).str.strip()
            df['percent_change'] = pd.to_numeric(df['percent_change'], errors='coerce')
            # Cap at Numeric(10,6) limits: -9999.99 to 9999.99
            df['percent_change'] = df['percent_change'].clip(-9999.99, 9999.99)

        # Convert net_change to numeric
        if 'net_change' in df.columns:
            df['net_change'] = pd.to_numeric(df['net_change'], errors='coerce')

        # PostgreSQL BigInteger max value
        BIGINT_MAX = 9223372036854775807

        # Convert market_cap to numeric (already in scientific notation)
        if 'market_cap' in df.columns:
            df['market_cap'] = pd.to_numeric(df['market_cap'], errors='coerce')
            # Convert to integer (handle NaN and overflow)
            df['market_cap'] = df['market_cap'].apply(
                lambda x: int(min(x, BIGINT_MAX)) if pd.notna(x) and x > 0 else None
            )

        # Convert volume to integer
        if 'volume' in df.columns:
            df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
            df['volume'] = df['volume'].apply(
                lambda x: int(min(x, BIGINT_MAX)) if pd.notna(x) and x > 0 else None
            )

        # Convert ipo_year to integer
        if 'ipo_year' in df.columns:
            df['ipo_year'] = pd.to_numeric(df['ipo_year'], errors='coerce')
            df['ipo_year'] = df['ipo_year'].apply(
                lambda x: int(x) if pd.notna(x) else None
            )

        # Handle NaN values for string columns
        string_cols = ['name', 'country', 'sector', 'industry']
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].replace({pd.NA: None, 'nan': None, '': None})

        # Drop rows with missing symbol
        df = df.dropna(subset=['symbol'])

        # Select only columns that exist in the model
        model_columns = [
            'symbol', 'snapshot_date', 'name', 'last_sale', 'net_change',
            'percent_change', 'market_cap', 'volume', 'country',
            'ipo_year', 'sector', 'industry'
        ]
        df = df[[col for col in model_columns if col in df.columns]]

        # CRITICAL: Convert integer columns to proper dtype to avoid float in to_dict()
        if 'market_cap' in df.columns:
            df['market_cap'] = df['market_cap'].astype('Int64')  # Nullable integer
        if 'volume' in df.columns:
            df['volume'] = df['volume'].astype('Int64')  # Nullable integer
        if 'ipo_year' in df.columns:
            df['ipo_year'] = df['ipo_year'].astype('Int64')  # Nullable integer

        return df

    def _insert_records(self, df: pd.DataFrame) -> int:
        """
        Insert records using UPSERT logic

        Args:
            df: Cleaned DataFrame

        Returns:
            Number of records inserted/updated
        """
        if df.empty:
            return 0

        records = df.to_dict('records')

        # Manual sanitization for BigInteger fields
        BIGINT_MAX = 9223372036854775807
        sanitized_records = []
        for r in records:
            # Cap market_cap
            if r.get('market_cap') is not None and r['market_cap'] > BIGINT_MAX:
                r['market_cap'] = BIGINT_MAX
            # Cap volume
            if r.get('volume') is not None and r['volume'] > BIGINT_MAX:
                r['volume'] = BIGINT_MAX
            sanitized_records.append(r)

        # UPSERT: insert or update on conflict
        stmt = insert(NasdaqScreenerProfile).values(sanitized_records)
        stmt = stmt.on_conflict_do_update(
            index_elements=['symbol', 'snapshot_date'],
            set_={
                'name': stmt.excluded.name,
                'last_sale': stmt.excluded.last_sale,
                'net_change': stmt.excluded.net_change,
                'percent_change': stmt.excluded.percent_change,
                'market_cap': stmt.excluded.market_cap,
                'volume': stmt.excluded.volume,
                'country': stmt.excluded.country,
                'ipo_year': stmt.excluded.ipo_year,
                'sector': stmt.excluded.sector,
                'industry': stmt.excluded.industry,
                'updated_at': stmt.excluded.updated_at
            }
        )

        self.session.execute(stmt)
        self.session.commit()

        return len(sanitized_records)

    def get_latest_snapshot_date(self) -> Optional[date]:
        """Get the most recent snapshot date in the database"""
        from sqlalchemy import func
        result = self.session.query(func.max(NasdaqScreenerProfile.snapshot_date)).scalar()
        return result

    def get_symbols_for_date(self, snapshot_date: date) -> List[str]:
        """Get all symbols for a specific snapshot date"""
        results = self.session.query(NasdaqScreenerProfile.symbol)\
            .filter(NasdaqScreenerProfile.snapshot_date == snapshot_date)\
            .all()
        return [r[0] for r in results]

    def collect_if_needed(self, max_age_days: int = 1) -> bool:
        """
        Collect data only if latest snapshot is older than max_age_days

        Args:
            max_age_days: Maximum age in days before collecting new data

        Returns:
            True if data was collected
        """
        latest_date = self.get_latest_snapshot_date()
        today = datetime.now().date()

        if latest_date is None:
            logger.info("No existing Nasdaq screener data found. Collecting...")
            return self.collect()

        days_old = (today - latest_date).days
        if days_old >= max_age_days:
            logger.info(f"Latest snapshot is {days_old} days old. Collecting fresh data...")
            return self.collect()
        else:
            logger.info(f"Latest snapshot is {days_old} days old. Skipping collection.")
            return True
