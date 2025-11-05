"""
Employee History Collector
Handles: Historical employee count data
"""
import logging
from datetime import date
from typing import Optional

import pandas as pd
from sqlalchemy.dialects.postgresql import insert

from src.collectors.base_collector import BaseCollector
from src.config import FMP_ENDPOINTS, settings
from src.database.models import EmployeeHistory
from src.utils.data_transform import transform_batch, transform_keys

logger = logging.getLogger(__name__)


class EmployeeCollector(BaseCollector):
    """Collector for employee history data"""

    def __init__(self, session):
        super().__init__(session)
        self.years = settings.data_collection.default_years_history

    def get_table_name(self) -> str:
        return "employee_history"

    def collect_for_symbol(self, symbol: str) -> bool:
        """
        Collect employee history for a symbol

        Args:
            symbol: Stock ticker

        Returns:
            True if successful
        """
        # Skip indices - they don't have employee data
        if self.is_index_symbol(symbol):
            logger.info(f"Skipping employee data for index {symbol}")
            return True

        try:
            return self._collect_employee_history(symbol)
        except Exception as e:
            logger.error(f"Error collecting employee data for {symbol}: {e}")
            self.record_error('employee_history', symbol, str(e))
            return False

    def _collect_employee_history(self, symbol: str) -> bool:
        """Collect historical employee count data"""
        # Check if update needed (update quarterly)
        if not self.should_update_symbol('employee_history', symbol, max_age_days=90):
            logger.info(f"Employee history for {symbol} is up to date")
            return True

        # Fetch from API
        url = FMP_ENDPOINTS['employee_history']
        limit = self.years + 1
        params = {
            'symbol': symbol,
            'limit': limit
        }

        response = self._get(url, params)
        data = self._json_safe(response)

        if not data:
            logger.warning(f"No employee history data returned for {symbol}")
            return False

        # Transform API data from camelCase to snake_case
        transformed_data = transform_batch(data, transform_keys)

        df = self._to_dataframe(transformed_data)
        if df.empty:
            logger.warning(f"Empty employee history dataframe for {symbol}")
            return False

        # Add symbol
        df['symbol'] = symbol

        # Convert date columns
        if 'period_of_report' in df.columns:
            df['period_of_report'] = pd.to_datetime(df['period_of_report'], errors='coerce').dt.date
        if 'filing_date' in df.columns:
            df['filing_date'] = pd.to_datetime(df['filing_date'], errors='coerce').dt.date
        if 'acceptance_time' in df.columns:
            df['acceptance_time'] = pd.to_datetime(df['acceptance_time'], errors='coerce')

        # Truncate string fields to match database limits
        if 'source' in df.columns:
            df['source'] = df['source'].astype(str).str[:50]
        if 'form_type' in df.columns:
            df['form_type'] = df['form_type'].astype(str).str[:20]
        if 'cik' in df.columns:
            df['cik'] = df['cik'].astype(str).str[:20]
        if 'company_name' in df.columns:
            df['company_name'] = df['company_name'].astype(str).str[:200]

        # Sort by period of report
        if 'period_of_report' in df.columns:
            df = df.sort_values('period_of_report', ascending=False)

        # Drop duplicates on primary key (symbol, period_of_report) to avoid batch insert conflicts
        df = df.drop_duplicates(subset=['symbol', 'period_of_report'], keep='last')

        records = df.to_dict('records')
        if not records:
            return True

        # Upsert records
        stmt = insert(EmployeeHistory).values(records)
        pk_columns = ['symbol', 'period_of_report']

        update_dict = {
            col.name: col
            for col in stmt.excluded
            if col.name not in pk_columns
        }

        stmt = stmt.on_conflict_do_update(
            index_elements=pk_columns,
            set_=update_dict
        )

        self.session.execute(stmt)
        self.session.commit()

        self.records_inserted += len(records)

        # Update tracking
        latest_date = df['period_of_report'].max() if 'period_of_report' in df.columns else None
        record_count = self.session.query(EmployeeHistory)\
            .filter(EmployeeHistory.symbol == symbol)\
            .count()

        self.update_tracking(
            'employee_history',
            symbol,
            last_api_date=latest_date,
            record_count=record_count,
            next_update_frequency='quarterly'
        )

        logger.info(f"✓ {symbol} employee_history: {len(records)} upserted")
        return True
