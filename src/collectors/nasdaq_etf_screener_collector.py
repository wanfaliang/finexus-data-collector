"""
Nasdaq ETF Screener Collector - Process downloaded CSV and load into database
"""
import logging
import pandas as pd
from pathlib import Path
from datetime import date, datetime
from typing import Optional, List

from sqlalchemy.dialects.postgresql import insert

from src.collectors.base_collector import BaseCollector
from src.database.models import NasdaqETFScreenerProfile
from src.config import settings

logger = logging.getLogger(__name__)

# Use Selenium for downloads
from src.utils.nasdaq_etf_screener_selenium import download_nasdaq_etf_screener_csv_selenium


class NasdaqETFScreenerCollector(BaseCollector):
    """Collector for Nasdaq ETF screener data"""

    def __init__(self, session):
        super().__init__(session)
        self.screener_path = Path(settings.data_collection.nasdaq_etf_screener_path)

    def get_table_name(self) -> str:
        return "nasdaq_etf_screener_profiles"

    def collect(self, csv_path: Optional[str] = None, snapshot_date: Optional[date] = None) -> bool:
        """
        Collect Nasdaq ETF screener data from CSV

        Args:
            csv_path: Path to CSV file. If None, downloads fresh data.
            snapshot_date: Date for this snapshot. If None, uses today.

        Returns:
            True if successful
        """
        try:
            # Download CSV if not provided
            if csv_path is None:
                logger.info("Downloading fresh Nasdaq ETF screener data...")
                csv_path = download_nasdaq_etf_screener_csv_selenium(headless=False)
                logger.info(f"Downloaded to: {csv_path}")
            else:
                csv_path = Path(csv_path)
                if not csv_path.exists():
                    raise FileNotFoundError(f"CSV file not found: {csv_path}")

            # Use today's date if not specified
            if snapshot_date is None:
                snapshot_date = datetime.now().date()

            logger.info(f"Processing Nasdaq ETF screener CSV: {csv_path}")
            logger.info(f"Snapshot date: {snapshot_date}")

            # Read CSV
            df = pd.read_csv(csv_path)
            logger.info(f"Loaded {len(df):,} ETFs from CSV")

            # Clean and transform data
            df_clean = self._clean_data(df, snapshot_date)
            logger.info(f"Cleaned data: {len(df_clean):,} valid records")

            # Insert into database
            inserted = self._insert_records(df_clean)
            self.records_inserted += inserted

            # Update tracking
            self.update_tracking(
                'nasdaq_etf_screener_profiles',
                symbol=None,  # Global data, no specific symbol
                last_api_date=snapshot_date,
                record_count=self.session.query(NasdaqETFScreenerProfile)
                    .filter(NasdaqETFScreenerProfile.snapshot_date == snapshot_date).count(),
                next_update_frequency='daily'
            )

            logger.info(f"Successfully processed {inserted:,} Nasdaq ETF screener records")
            return True

        except Exception as e:
            logger.error(f"Error collecting Nasdaq ETF screener data: {e}")
            self.record_error('nasdaq_etf_screener_profiles', 'ALL', str(e))
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
            'SYMBOL': 'symbol',
            'NAME': 'name',
            'LAST PRICE': 'last_price',
            'NET CHANGE': 'net_change',
            '% CHANGE': 'percent_change',
            'DELTA': 'delta',
            '1 yr % CHANGE': 'one_year_percent_change'
        }
        df = df.rename(columns=column_mapping)

        # Clean price fields (remove $ sign)
        if 'last_price' in df.columns:
            df['last_price'] = df['last_price'].astype(str).str.replace('$', '', regex=False).str.strip()
            df['last_price'] = pd.to_numeric(df['last_price'], errors='coerce')

        # Clean percentage fields (remove % sign)
        if 'percent_change' in df.columns:
            df['percent_change'] = df['percent_change'].astype(str).str.replace('%', '', regex=False).str.strip()
            df['percent_change'] = pd.to_numeric(df['percent_change'], errors='coerce')

        if 'one_year_percent_change' in df.columns:
            df['one_year_percent_change'] = df['one_year_percent_change'].astype(str).str.replace('%', '', regex=False).str.strip()
            df['one_year_percent_change'] = pd.to_numeric(df['one_year_percent_change'], errors='coerce')

        # Convert net_change to numeric
        if 'net_change' in df.columns:
            df['net_change'] = pd.to_numeric(df['net_change'], errors='coerce')

        # Clean delta field (ensure lowercase)
        if 'delta' in df.columns:
            df['delta'] = df['delta'].astype(str).str.lower().str.strip()
            df['delta'] = df['delta'].replace({'nan': None, '': None})

        # Handle NaN values for string columns
        string_cols = ['name', 'delta']
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].replace({pd.NA: None, 'nan': None, '': None})

        # Drop rows with missing symbol
        df = df.dropna(subset=['symbol'])

        # Drop footer/metadata rows (symbol contains timestamp info)
        df = df[~df['symbol'].astype(str).str.contains('Data as of', case=False, na=False)]

        # Drop rows where symbol is too long (>20 chars) - these are invalid
        df = df[df['symbol'].astype(str).str.len() <= 20]

        # Select only columns that exist in the model
        model_columns = [
            'symbol', 'snapshot_date', 'name', 'last_price', 'net_change',
            'percent_change', 'delta', 'one_year_percent_change'
        ]
        df = df[[col for col in model_columns if col in df.columns]]

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

        # UPSERT: insert or update on conflict
        stmt = insert(NasdaqETFScreenerProfile).values(records)
        stmt = stmt.on_conflict_do_update(
            index_elements=['symbol', 'snapshot_date'],
            set_={
                'name': stmt.excluded.name,
                'last_price': stmt.excluded.last_price,
                'net_change': stmt.excluded.net_change,
                'percent_change': stmt.excluded.percent_change,
                'delta': stmt.excluded.delta,
                'one_year_percent_change': stmt.excluded.one_year_percent_change,
                'updated_at': stmt.excluded.updated_at
            }
        )

        self.session.execute(stmt)
        self.session.commit()

        return len(records)

    def get_latest_snapshot_date(self) -> Optional[date]:
        """Get the most recent snapshot date in the database"""
        from sqlalchemy import func
        result = self.session.query(func.max(NasdaqETFScreenerProfile.snapshot_date)).scalar()
        return result

    def get_symbols_for_date(self, snapshot_date: date) -> List[str]:
        """Get all symbols for a specific snapshot date"""
        results = self.session.query(NasdaqETFScreenerProfile.symbol)\
            .filter(NasdaqETFScreenerProfile.snapshot_date == snapshot_date)\
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
            logger.info("No existing Nasdaq ETF screener data found. Collecting...")
            return self.collect()

        days_old = (today - latest_date).days
        if days_old >= max_age_days:
            logger.info(f"Latest snapshot is {days_old} days old. Collecting fresh data...")
            return self.collect()
        else:
            logger.info(f"Latest snapshot is {days_old} days old. Skipping collection.")
            return True
