"""
Insider Trading and Institutional Ownership Collector
Handles: Insider trading transactions, institutional ownership, and insider statistics
"""
import logging
import time
from datetime import datetime, date
from typing import Optional, List, Dict

import pandas as pd
from sqlalchemy import desc
from sqlalchemy.dialects.postgresql import insert

from src.collectors.base_collector import BaseCollector
from src.config import FMP_ENDPOINTS
from src.database.models import (
    InsiderTrading, InstitutionalOwnership, InsiderStatistics
)
from src.utils.data_transform import transform_batch, transform_keys

logger = logging.getLogger(__name__)


class InsiderCollector(BaseCollector):
    """Collector for insider trading and institutional ownership data"""
    
    def get_table_name(self) -> str:
        return "insider_trading"
    
    def collect_for_symbol(self, symbol: str) -> bool:
        """
        Collect all insider/institutional data for a symbol

        Args:
            symbol: Stock ticker

        Returns:
            True if successful
        """
        # Skip indices - they don't have insider/institutional data
        if self.is_index_symbol(symbol):
            logger.info(f"Skipping insider/institutional data for index {symbol}")
            return True

        try:
            # Collect all three data types
            insider_success = self._collect_insider_trading(symbol)
            institutional_success = self._collect_institutional_ownership(symbol)
            stats_success = self._collect_insider_statistics(symbol)

            return insider_success or institutional_success or stats_success

        except Exception as e:
            logger.error(f"Error collecting insider data for {symbol}: {e}")
            self.record_error('insider_trading', symbol, str(e))
            # Rollback to prevent cascade failures
            self.session.rollback()
            return False
    
    def _collect_insider_trading(self, symbol: str) -> bool:
        """Collect insider trading transactions"""

        # Check if update needed (update weekly)
        if not self.should_update_symbol('insider_trading', symbol, max_age_days=7):
            logger.info(f"Insider trading for {symbol} is up to date")
            return True

        # In force refill mode, ignore last_filing to fetch all data
        last_filing = None if self.force_refill else self._get_last_insider_filing_date(symbol)
        
        # Fetch from API
        url = FMP_ENDPOINTS['insider_trading_search']
        params = {
            'symbol': symbol,
            'page': 1,
            'limit': 1000
        }
        
        response = self._get(url, params)
        data = self._json_safe(response)

        if not data:
            logger.warning(f"No insider trading data returned for {symbol}")
            return False

        # Transform API data from camelCase to snake_case
        transformed_data = transform_batch(data, transform_keys)

        df = self._to_dataframe(transformed_data)
        if df.empty:
            logger.warning(f"Empty insider trading dataframe for {symbol}")
            return False

        # Convert date columns
        if 'filing_date' in df.columns:
            df['filing_date'] = pd.to_datetime(df['filing_date']).dt.date
        if 'transaction_date' in df.columns:
            df['transaction_date'] = pd.to_datetime(df['transaction_date']).dt.date
        
        # Filter for only new records
        if last_filing:
            df = df[df['filing_date'] > last_filing]
        
        if df.empty:
            logger.info(f"No new insider trading records for {symbol}")
            return True

        # Handle special field name mappings
        if 'link' in df.columns:
            df['url'] = df['link']
            df = df.drop('link', axis=1)

        # Add symbol if not present
        df['symbol'] = symbol

        records = df.to_dict('records')

        if not records:
            return True
        
        # Insert records (no conflict handling - each transaction is unique)
        stmt = insert(InsiderTrading).values(records)
        
        self.session.execute(stmt)
        self.session.commit()
        
        self.records_inserted += len(records)
        
        # Update tracking
        latest_filing = df['filing_date'].max()
        record_count = self.session.query(InsiderTrading)\
            .filter(InsiderTrading.symbol == symbol)\
            .count()
        
        self.update_tracking(
            'insider_trading',
            symbol,
            last_api_date=latest_filing,
            record_count=record_count,
            next_update_frequency='daily'
        )
        
        logger.info(f"✓ {symbol} insider_trading: {len(records)} inserted")
        return True
    
    def _collect_institutional_ownership(self, symbol: str) -> bool:
        """Collect institutional ownership summary"""

        # Check if update needed (update quarterly)
        if not self.should_update_symbol('institutional_ownership', symbol, max_age_days=90):
            logger.info(f"Institutional ownership for {symbol} is up to date")
            return True

        current_year = datetime.now().year
        quarters = [1, 2, 3, 4]
        all_quarters = []

        # Collect data for current year and previous year, all quarters
        for year_offset in [0, 1]:
            year = current_year - year_offset
            for quarter in quarters:
                url = FMP_ENDPOINTS['institutional_ownership_summary']
                params = {
                    'symbol': symbol,
                    'year': year,
                    'quarter': quarter
                }

                response = self._get(url, params)
                data = self._json_safe(response)

                if not data:
                    continue

                # Transform API data from camelCase to snake_case
                transformed_data = transform_batch(data, transform_keys)

                df = self._to_dataframe(transformed_data)
                if df.empty:
                    continue

                # Add metadata
                df['symbol'] = symbol
                df['collected_year'] = year
                df['collected_quarter'] = quarter

                if 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date

                all_quarters.append(df)

                time.sleep(self.sleep_sec)

        if not all_quarters:
            logger.warning(f"No institutional ownership data returned for {symbol}")
            return False

        # Combine all quarters
        combined = pd.concat(all_quarters, ignore_index=True)

        records = combined.to_dict('records')
        if not records:
            return True

        # Upsert records
        stmt = insert(InstitutionalOwnership).values(records)
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
        latest_date = combined['date'].max()
        record_count = self.session.query(InstitutionalOwnership)\
            .filter(InstitutionalOwnership.symbol == symbol)\
            .count()

        self.update_tracking(
            'institutional_ownership',
            symbol,
            last_api_date=latest_date,
            record_count=record_count,
            next_update_frequency='quarterly'
        )

        logger.info(f"✓ {symbol} institutional_ownership: {len(records)} upserted")
        return True
    
    def _collect_insider_statistics(self, symbol: str) -> bool:
        """Collect aggregated insider trading statistics"""
        
        # Check if update needed (update quarterly)
        if not self.should_update_symbol('insider_statistics', symbol, max_age_days=90):
            logger.info(f"Insider statistics for {symbol} are up to date")
            return True
        
        # Fetch from API
        url = FMP_ENDPOINTS['insider_trading_statistics']
        params = {'symbol': symbol}
        
        response = self._get(url, params)
        data = self._json_safe(response)

        if not data:
            logger.warning(f"No insider statistics returned for {symbol}")
            return False

        # Transform API data from camelCase to snake_case
        transformed_data = transform_batch(data, transform_keys)

        df = self._to_dataframe(transformed_data)
        if df.empty:
            logger.warning(f"Empty insider statistics dataframe for {symbol}")
            return False
        
        # Add symbol
        df['symbol'] = symbol
        
        records = df.to_dict('records')
        if not records:
            return True
        
        # Upsert records
        stmt = insert(InsiderStatistics).values(records)
        pk_columns = ['symbol', 'year', 'quarter']
        
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
        record_count = self.session.query(InsiderStatistics)\
            .filter(InsiderStatistics.symbol == symbol)\
            .count()
        
        self.update_tracking(
            'insider_statistics',
            symbol,
            last_api_date=datetime.now().date(),
            record_count=record_count,
            next_update_frequency='quarterly'
        )
        
        logger.info(f"✓ {symbol} insider_statistics: {len(records)} upserted")
        return True
    
    def _get_last_insider_filing_date(self, symbol: str) -> Optional[date]:
        """Get most recent insider trading filing date for symbol"""
        result = self.session.query(InsiderTrading.filing_date)\
            .filter(InsiderTrading.symbol == symbol)\
            .order_by(desc(InsiderTrading.filing_date))\
            .first()
        
        return result[0] if result else None


if __name__ == "__main__":
    from src.database.connection import get_session
    
    logging.basicConfig(level=logging.INFO)
    
    with get_session() as session:
        collector = InsiderCollector(session)
        success = collector.collect_for_symbol('AAPL')
        print(f"Collection {'successful' if success else 'failed'}")
