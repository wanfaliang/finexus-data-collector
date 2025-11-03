"""
Enterprise Value Collector
Handles: Enterprise value calculations and metrics
"""
import logging
from datetime import date
from typing import Optional

import pandas as pd
from sqlalchemy.dialects.postgresql import insert

from src.collectors.base_collector import BaseCollector
from src.config import FMP_ENDPOINTS, settings
from src.database.models import EnterpriseValue
from src.utils.data_transform import transform_batch, transform_keys

logger = logging.getLogger(__name__)


class EnterpriseCollector(BaseCollector):
    """Collector for enterprise value data"""

    def __init__(self, session):
        super().__init__(session)
        self.years = settings.data_collection.default_years_history

    def get_table_name(self) -> str:
        return "enterprise_values"

    def collect_for_symbol(self, symbol: str) -> bool:
        """
        Collect enterprise value data for a symbol

        Args:
            symbol: Stock ticker

        Returns:
            True if successful
        """
        # Skip indices - they don't have enterprise value data
        if self.is_index_symbol(symbol):
            logger.info(f"Skipping enterprise value data for index {symbol}")
            return True

        try:
            return self._collect_enterprise_values(symbol)
        except Exception as e:
            logger.error(f"Error collecting enterprise value data for {symbol}: {e}")
            self.record_error('enterprise_values', symbol, str(e))
            return False

    def _collect_enterprise_values(self, symbol: str) -> bool:
        """Collect enterprise value data"""
        # Check if update needed (update quarterly)
        if not self.should_update_symbol('enterprise_values', symbol, max_age_days=90):
            logger.info(f"Enterprise values for {symbol} are up to date")
            return True

        # Fetch from API
        url = FMP_ENDPOINTS['enterprise_values']
        limit = self.years + 1
        params = {
            'symbol': symbol,
            'period': 'annual',
            'limit': limit
        }

        response = self._get(url, params)
        data = self._json_safe(response)

        if not data:
            logger.warning(f"No enterprise value data returned for {symbol}")
            return False

        # Transform API data from camelCase to snake_case
        transformed_data = transform_batch(data, transform_keys)

        df = self._to_dataframe(transformed_data)
        if df.empty:
            logger.warning(f"Empty enterprise value dataframe for {symbol}")
            return False

        # Add symbol
        df['symbol'] = symbol

        # Convert date column
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date

        # Sort by date
        if 'date' in df.columns:
            df = df.sort_values('date', ascending=False)

        records = df.to_dict('records')
        if not records:
            return True

        # Upsert records
        stmt = insert(EnterpriseValue).values(records)
        pk_columns = ['symbol', 'date']

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
        latest_date = df['date'].max() if 'date' in df.columns else None
        record_count = self.session.query(EnterpriseValue)\
            .filter(EnterpriseValue.symbol == symbol)\
            .count()

        self.update_tracking(
            'enterprise_values',
            symbol,
            last_api_date=latest_date,
            record_count=record_count,
            next_update_frequency='quarterly'
        )

        logger.info(f"✓ {symbol} enterprise_values: {len(records)} upserted")
        return True
