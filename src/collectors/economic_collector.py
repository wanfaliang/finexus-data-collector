"""
Economic Data Collector - FRED and FMP economic indicators
Integrates with FREDCollector to fetch data and saves to database
"""
import logging
from datetime import datetime, date
from typing import Optional, Dict, List

import pandas as pd
from sqlalchemy.dialects.postgresql import insert

from src.collectors.base_collector import BaseCollector
from src.collectors.fred_collector import FREDCollector
from src.database.models import (
    EconomicIndicator, EconomicDataRaw,
    EconomicDataMonthly, EconomicDataQuarterly
)
from src.config import settings

logger = logging.getLogger(__name__)


class EconomicCollector(BaseCollector):
    """Collector for economic indicators from FRED and FMP"""

    def __init__(self, session, fmp_api_key: Optional[str] = None):
        super().__init__(session)
        self.fmp_api_key = settings.api.fmp_api_key
        self.fred_collector = None

    def get_table_name(self) -> str:
        return "economic_data"

    def collect_for_symbol(self, symbol: str) -> bool:
        """Not applicable for economic data (no symbols)"""
        return False

    def collect_all(self) -> bool:
        """
        Collect all economic indicators and save to database

        Returns:
            True if successful
        """
        try:
            logger.info("Starting economic data collection...")

            # Initialize FRED collector
            self.fred_collector = FREDCollector(fmp_api_key=self.fmp_api_key)

            # Fetch all data from FRED and FMP
            logger.info("Fetching data from FRED and FMP APIs...")
            self.fred_collector.fetch_all()

            if not self.fred_collector.raw_frames:
                logger.warning("No economic data fetched")
                return False

            # Save indicator metadata
            self._save_indicator_metadata()

            # Save raw data
            self._save_raw_data()

            # Generate and save monthly aggregations
            self._save_monthly_aggregations()

            # Generate and save quarterly aggregations
            self._save_quarterly_aggregations()

            # Update tracking
            self.update_tracking(
                'economic_data',
                symbol=None,  # Economic data is global, no symbol
                last_api_date=datetime.now().date(),
                record_count=self._count_total_records(),
                next_update_frequency='daily'
            )

            logger.info(f"✓ Economic data collection complete: {len(self.fred_collector.raw_frames)} indicators")
            return True

        except Exception as e:
            logger.error(f"Error collecting economic data: {e}")
            self.record_error('economic_data', 'ALL', str(e))
            self.session.rollback()
            return False

    def _save_indicator_metadata(self) -> None:
        """Save indicator metadata to economic_indicators table"""
        try:
            # Use dict to deduplicate by indicator_code
            indicators_dict = {}

            # Process all indicators from raw_frames
            for indicator_code, df in self.fred_collector.raw_frames.items():
                if df.empty:
                    continue

                frequency = self._infer_frequency(df)

                # Determine source and source_series_id
                if indicator_code in self.fred_collector.DEFAULT_INDICATORS:
                    source = 'FRED'
                    source_series_id = self.fred_collector.DEFAULT_INDICATORS[indicator_code]
                    description = f'FRED indicator: {source_series_id}'
                elif indicator_code in self.fred_collector.FMP_ECON_SERIES:
                    source = 'FMP'
                    source_series_id = self.fred_collector.FMP_ECON_SERIES[indicator_code]
                    description = f'FMP indicator: {source_series_id}'
                elif indicator_code.startswith('Treasury_'):
                    source = 'FMP'
                    source_series_id = f'treasury_curve_{indicator_code.lower()}'
                    description = f'Treasury yield: {indicator_code}'
                elif 'Yield_Curve' in indicator_code:
                    source = 'COMPUTED'
                    source_series_id = indicator_code
                    description = f'Computed spread: {indicator_code}'
                else:
                    # Unknown source, mark as FMP by default
                    source = 'FMP'
                    source_series_id = indicator_code
                    description = f'Economic indicator: {indicator_code}'

                # Add to dict (will overwrite duplicates, keeping last)
                indicators_dict[indicator_code] = {
                    'indicator_code': indicator_code,
                    'indicator_name': indicator_code.replace('_', ' '),
                    'source': source,
                    'source_series_id': source_series_id,
                    'native_frequency': frequency,
                    'units': self._infer_units(indicator_code),
                    'description': description,
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                }

            if not indicators_dict:
                logger.warning("No indicator metadata to save")
                return

            # Convert dict to list
            indicators = list(indicators_dict.values())

            # Upsert indicators
            stmt = insert(EconomicIndicator).values(indicators)
            stmt = stmt.on_conflict_do_update(
                index_elements=['indicator_code'],
                set_={
                    'indicator_name': stmt.excluded.indicator_name,
                    'source': stmt.excluded.source,
                    'source_series_id': stmt.excluded.source_series_id,
                    'native_frequency': stmt.excluded.native_frequency,
                    'units': stmt.excluded.units,
                    'description': stmt.excluded.description,
                    'updated_at': stmt.excluded.updated_at
                }
            )

            self.session.execute(stmt)
            self.session.commit()

            logger.info(f"✓ Saved metadata for {len(indicators)} indicators")

        except Exception as e:
            logger.error(f"Error saving indicator metadata: {e}")
            self.session.rollback()
            raise

    def _save_raw_data(self) -> None:
        """Save raw time series data to economic_data_raw table"""
        try:
            all_records = []

            for indicator_code, df in self.fred_collector.raw_frames.items():
                if df.empty or 'Date' not in df.columns:
                    continue

                # Ensure Date column is datetime
                df = df.copy()
                df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
                df = df.dropna(subset=['Date'])

                # Convert to date (not datetime)
                df['date'] = df['Date'].dt.date

                # Get value column (should be the indicator_code)
                value_col = indicator_code
                if value_col not in df.columns:
                    logger.warning(f"Value column {value_col} not found for {indicator_code}")
                    continue

                # Drop duplicates on date (keep last occurrence)
                df = df.drop_duplicates(subset=['date'], keep='last')

                # Create records
                for _, row in df.iterrows():
                    all_records.append({
                        'indicator_code': indicator_code,
                        'date': row['date'],
                        'value': float(row[value_col]) if pd.notna(row[value_col]) else None,
                        'created_at': datetime.now()
                    })

            if not all_records:
                logger.warning("No raw data records to save")
                return

            # Insert in batches to avoid memory issues
            batch_size = 10000
            total_inserted = 0

            for i in range(0, len(all_records), batch_size):
                batch = all_records[i:i+batch_size]

                stmt = insert(EconomicDataRaw).values(batch)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['indicator_code', 'date'],
                    set_={'value': stmt.excluded.value}
                )

                self.session.execute(stmt)
                total_inserted += len(batch)

            self.session.commit()
            self.records_inserted += total_inserted

            logger.info(f"✓ Saved {total_inserted} raw data points")

        except Exception as e:
            logger.error(f"Error saving raw data: {e}")
            self.session.rollback()
            raise

    def _save_monthly_aggregations(self) -> None:
        """Generate and save monthly aggregations"""
        try:
            monthly_panel = self.fred_collector.build_monthly_panel()

            if monthly_panel.empty:
                logger.warning("No monthly panel data to save")
                return

            # Convert wide to long format
            all_records = []

            for col in monthly_panel.columns:
                if col == 'Date':
                    continue

                df_long = monthly_panel[['Date', col]].copy()
                df_long = df_long.dropna(subset=[col])
                df_long['indicator_code'] = col
                df_long['value'] = df_long[col]
                df_long['date'] = pd.to_datetime(df_long['Date']).dt.date

                # Drop duplicates on date
                df_long = df_long.drop_duplicates(subset=['date'], keep='last')

                for _, row in df_long.iterrows():
                    all_records.append({
                        'indicator_code': row['indicator_code'],
                        'date': row['date'],
                        'value': float(row['value']) if pd.notna(row['value']) else None,
                        'created_at': datetime.now()
                    })

            if not all_records:
                logger.warning("No monthly records to save")
                return

            # Insert in batches
            batch_size = 10000
            total_inserted = 0

            for i in range(0, len(all_records), batch_size):
                batch = all_records[i:i+batch_size]

                stmt = insert(EconomicDataMonthly).values(batch)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['indicator_code', 'date'],
                    set_={'value': stmt.excluded.value}
                )

                self.session.execute(stmt)
                total_inserted += len(batch)

            self.session.commit()
            self.records_inserted += total_inserted

            logger.info(f"✓ Saved {total_inserted} monthly aggregations")

        except Exception as e:
            logger.error(f"Error saving monthly aggregations: {e}")
            self.session.rollback()
            raise

    def _save_quarterly_aggregations(self) -> None:
        """Generate and save quarterly aggregations"""
        try:
            quarterly_panel = self.fred_collector.build_quarterly_panel()

            if quarterly_panel.empty:
                logger.warning("No quarterly panel data to save")
                return

            # Convert wide to long format
            all_records = []

            for col in quarterly_panel.columns:
                if col == 'Date':
                    continue

                df_long = quarterly_panel[['Date', col]].copy()
                df_long = df_long.dropna(subset=[col])
                df_long['indicator_code'] = col
                df_long['value'] = df_long[col]
                df_long['date'] = pd.to_datetime(df_long['Date']).dt.date

                # Drop duplicates on date
                df_long = df_long.drop_duplicates(subset=['date'], keep='last')

                for _, row in df_long.iterrows():
                    all_records.append({
                        'indicator_code': row['indicator_code'],
                        'date': row['date'],
                        'value': float(row['value']) if pd.notna(row['value']) else None,
                        'created_at': datetime.now()
                    })

            if not all_records:
                logger.warning("No quarterly records to save")
                return

            # Insert in batches
            batch_size = 10000
            total_inserted = 0

            for i in range(0, len(all_records), batch_size):
                batch = all_records[i:i+batch_size]

                stmt = insert(EconomicDataQuarterly).values(batch)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['indicator_code', 'date'],
                    set_={'value': stmt.excluded.value}
                )

                self.session.execute(stmt)
                total_inserted += len(batch)

            self.session.commit()
            self.records_inserted += total_inserted

            logger.info(f"✓ Saved {total_inserted} quarterly aggregations")

        except Exception as e:
            logger.error(f"Error saving quarterly aggregations: {e}")
            self.session.rollback()
            raise

    def _infer_frequency(self, df: pd.DataFrame) -> str:
        """Infer data frequency from DataFrame"""
        if df.empty or 'Date' not in df.columns:
            return 'UNKNOWN'

        # Get median days between observations
        dates = pd.to_datetime(df['Date']).sort_values()
        if len(dates) < 2:
            return 'UNKNOWN'

        diffs = dates.diff().dt.days.median()

        if diffs <= 1:
            return 'DAILY'
        elif diffs <= 7:
            return 'WEEKLY'
        elif diffs <= 31:
            return 'MONTHLY'
        elif diffs <= 100:
            return 'QUARTERLY'
        else:
            return 'ANNUAL'

    def _infer_units(self, indicator_name: str) -> str:
        """Infer units from indicator name"""
        name_lower = indicator_name.lower()

        if 'rate' in name_lower or 'yield' in name_lower or 'curve' in name_lower:
            return 'Percent'
        elif 'price' in name_lower or 'gdp' in name_lower or 'sales' in name_lower:
            return 'Billions USD'
        elif 'index' in name_lower or 'sentiment' in name_lower:
            return 'Index'
        elif 'unemployment' in name_lower or 'utilization' in name_lower or 'participation' in name_lower:
            return 'Percent'
        elif 'payrolls' in name_lower or 'jobs' in name_lower or 'employment' in name_lower:
            return 'Thousands'
        elif 'starts' in name_lower or 'permits' in name_lower:
            return 'Thousands of Units'
        elif 'supply' in name_lower:
            return 'Months'
        else:
            return 'Units'

    def _count_total_records(self) -> int:
        """Count total records across all economic data tables"""
        try:
            raw_count = self.session.query(EconomicDataRaw).count()
            monthly_count = self.session.query(EconomicDataMonthly).count()
            quarterly_count = self.session.query(EconomicDataQuarterly).count()
            return raw_count + monthly_count + quarterly_count
        except Exception:
            return 0


if __name__ == "__main__":
    from src.database.connection import get_session
    import os

    logging.basicConfig(level=logging.INFO)

    # Get FMP API key from environment
    fmp_key = os.getenv('FMP_API_KEY')

    with get_session() as session:
        collector = EconomicCollector(session, fmp_api_key=fmp_key)
        success = collector.collect_all()
        print(f"Collection {'successful' if success else 'failed'}")
        if success:
            print(f"Records inserted: {collector.records_inserted}")
