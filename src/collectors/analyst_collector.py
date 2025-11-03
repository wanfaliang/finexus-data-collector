"""
Analyst Data Collector
Handles: Analyst Estimates and Price Target Consensus
"""
import logging
from datetime import date, datetime
from typing import Optional

import pandas as pd
from sqlalchemy import desc
from sqlalchemy.dialects.postgresql import insert

from src.collectors.base_collector import BaseCollector
from src.config import FMP_ENDPOINTS
from src.database.models import AnalystEstimate, PriceTarget
from src.utils.data_transform import transform_batch, transform_keys

logger = logging.getLogger(__name__)


class AnalystCollector(BaseCollector):
    """Collector for analyst estimates and price targets"""
    
    def get_table_name(self) -> str:
        return "analyst_estimates"
    
    def collect_for_symbol(self, symbol: str) -> bool:
        """
        Collect analyst data for a symbol

        Args:
            symbol: Stock ticker

        Returns:
            True if successful
        """
        # Skip indices - they don't have analyst data
        if self.is_index_symbol(symbol):
            logger.info(f"Skipping analyst data for index {symbol}")
            return True

        try:
            # Collect both estimates and price targets
            estimates_success = self._collect_estimates(symbol)
            targets_success = self._collect_price_targets(symbol)
            
            return estimates_success or targets_success
            
        except Exception as e:
            logger.error(f"Error collecting analyst data for {symbol}: {e}")
            self.record_error('analyst_estimates', symbol, str(e))
            return False
    
    def _collect_estimates(self, symbol: str) -> bool:
        """Collect analyst estimates for revenue, EPS, etc."""
        
        # Check if update needed (update daily)
        if not self.should_update_symbol('analyst_estimates', symbol, max_age_days=1):
            logger.info(f"Analyst estimates for {symbol} are up to date")
            return True
        
        # Fetch from API
        url = FMP_ENDPOINTS['analyst_estimates']
        params = {
            'symbol': symbol,
            'period': 'annual',
            'limit': 10
        }
        
        response = self._get(url, params)
        data = self._json_safe(response)

        if not data:
            logger.warning(f"No analyst estimates returned for {symbol}")
            return False

        # Transform API data from camelCase to snake_case
        transformed_data = transform_batch(data, transform_keys)

        df = self._to_dataframe(transformed_data)
        if df.empty:
            logger.warning(f"Empty analyst estimates dataframe for {symbol}")
            return False
        
        # Add symbol
        df['symbol'] = symbol
        
        # Convert date
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date']).dt.date
        
        records = df.to_dict('records')
        if not records:
            return True
        
        # Upsert records
        stmt = insert(AnalystEstimate).values(records)
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
        record_count = self.session.query(AnalystEstimate)\
            .filter(AnalystEstimate.symbol == symbol)\
            .count()
        
        self.update_tracking(
            'analyst_estimates',
            symbol,
            last_api_date=latest_date,
            record_count=record_count,
            next_update_frequency='daily'
        )
        
        logger.info(f"✓ {symbol} analyst_estimates: {len(records)} upserted")
        return True
    
    def _collect_price_targets(self, symbol: str) -> bool:
        """Collect analyst price target consensus"""
        
        # Check if update needed (update daily)
        if not self.should_update_symbol('price_targets', symbol, max_age_days=1):
            logger.info(f"Price targets for {symbol} are up to date")
            return True
        
        # Fetch from API
        url = FMP_ENDPOINTS['price_target_consensus']
        params = {'symbol': symbol}
        
        response = self._get(url, params)
        data = self._json_safe(response)
        
        if not data:
            logger.warning(f"No price targets returned for {symbol}")
            return False
        
        # API returns a list, get the first item (most recent)
        if isinstance(data, list) and data:
            data = data[0]

        if not isinstance(data, dict):
            logger.warning(f"Unexpected price target data format for {symbol}")
            return False

        # Transform API data from camelCase to snake_case
        record = transform_keys(data)

        # Add required fields
        record['symbol'] = symbol
        record['published_date'] = datetime.now().date()
        
        # Upsert record
        stmt = insert(PriceTarget).values([record])
        stmt = stmt.on_conflict_do_update(
            index_elements=['symbol', 'published_date'],
            set_={
                'target_high': stmt.excluded.target_high,
                'target_low': stmt.excluded.target_low,
                'target_consensus': stmt.excluded.target_consensus,
                'target_median': stmt.excluded.target_median
            }
        )
        
        self.session.execute(stmt)
        self.session.commit()
        
        self.records_inserted += 1
        
        # Update tracking
        record_count = self.session.query(PriceTarget)\
            .filter(PriceTarget.symbol == symbol)\
            .count()
        
        self.update_tracking(
            'price_targets',
            symbol,
            last_api_date=record['published_date'],
            record_count=record_count,
            next_update_frequency='daily'
        )
        
        logger.info(f"✓ {symbol} price_targets: 1 upserted")
        return True


if __name__ == "__main__":
    from src.database.connection import get_session
    
    logging.basicConfig(level=logging.INFO)
    
    with get_session() as session:
        collector = AnalystCollector(session)
        success = collector.collect_for_symbol('AAPL')
        print(f"Collection {'successful' if success else 'failed'}")
