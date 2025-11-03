"""Price Data Collector - Daily and Monthly prices"""
import logging
from datetime import date, datetime, timedelta
from typing import Optional, List

import pandas as pd
from sqlalchemy.dialects.postgresql import insert

from src.collectors.base_collector import BaseCollector
from src.config import FMP_ENDPOINTS, settings
from src.database.models import PriceDaily, PriceMonthly, Company
from src.utils.data_transform import transform_batch, transform_keys

logger = logging.getLogger(__name__)


class PriceCollector(BaseCollector):
    """Collector for price data"""

    def __init__(self, session):
        super().__init__(session)
        self.years = settings.data_collection.default_years_history

    def get_table_name(self) -> str:
        return "prices_daily"
    
    def collect_for_symbol(self, symbol: str) -> bool:
        """Collect price data for a symbol"""
        try:
            daily_success = self._collect_daily_prices(symbol)
            if daily_success:
                self._generate_monthly_prices(symbol)
            return daily_success
        except Exception as e:
            logger.error(f"Error collecting prices for {symbol}: {e}")
            self.record_error('prices_daily', symbol, str(e))
            return False
    
    def _collect_daily_prices(self, symbol: str) -> bool:
        """Collect daily price data"""
        if not self.should_update_symbol('prices_daily', symbol, max_age_days=1):
            logger.info(f"Daily prices for {symbol} are up to date")
            return True
        
        last_date = self._get_last_price_date(symbol)
        
        if last_date:
            from_date = last_date + timedelta(days=1)
            to_date = datetime.now().date()
            if from_date >= to_date:
                logger.info(f"Prices for {symbol} already current")
                return True
            start_date = from_date.strftime("%Y-%m-%d")
        else:
            start_date = (pd.Timestamp.today().normalize() - pd.DateOffset(years=self.years + 1)).strftime("%Y-%m-%d")

        url = FMP_ENDPOINTS['prices_full']
        params = {
            'symbol': symbol,
            'from': start_date
        }
        
        response = self._get(url, params)
        data = self._json_safe(response)

        if not data or not isinstance(data, list):
            logger.warning(f"No price data returned for {symbol}")
            return False

        # Transform API data from camelCase to snake_case
        transformed_data = transform_batch(data, transform_keys)

        df = pd.DataFrame(transformed_data)
        df['symbol'] = symbol
        df['date'] = pd.to_datetime(df['date']).dt.date

        records = df.to_dict('records')
        inserted = self._insert_daily_prices(records)
        self.records_inserted += inserted
        
        latest_date = df['date'].max()
        record_count = self.session.query(PriceDaily)\
            .filter(PriceDaily.symbol == symbol).count()
        
        self.update_tracking(
            'prices_daily', symbol,
            last_api_date=latest_date,
            record_count=record_count,
            next_update_frequency='daily'
        )
        
        logger.info(f"✓ {symbol} prices_daily: {inserted} inserted")
        return True
    
    def _get_last_price_date(self, symbol: str) -> Optional[date]:
        """Get most recent price date for symbol"""
        result = self.session.query(PriceDaily.date)\
            .filter(PriceDaily.symbol == symbol)\
            .order_by(PriceDaily.date.desc())\
            .first()
        return result[0] if result else None
    
    def _insert_daily_prices(self, records: List[dict]) -> int:
        """Insert price records with upsert logic"""
        if not records:
            return 0
        
        stmt = insert(PriceDaily).values(records)
        stmt = stmt.on_conflict_do_nothing(index_elements=['symbol', 'date'])
        
        result = self.session.execute(stmt)
        self.session.commit()
        return len(records)
    
    def _generate_monthly_prices(self, symbol: str):
        """Generate month-end prices from daily prices"""
        daily_prices = self.session.query(PriceDaily)\
            .filter(PriceDaily.symbol == symbol)\
            .order_by(PriceDaily.date).all()
        
        if not daily_prices:
            return
        
        df = pd.DataFrame([{
            'symbol': p.symbol, 'date': p.date, 'open': p.open,
            'high': p.high, 'low': p.low, 'close': p.close,
            'volume': p.volume
        } for p in daily_prices])
        
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        monthly = df.resample('ME').last().reset_index()
        monthly['date'] = monthly['date'].dt.date
        
        records = monthly.to_dict('records')
        if records:
            stmt = insert(PriceMonthly).values(records)
            stmt = stmt.on_conflict_do_update(
                index_elements=['symbol', 'date'],
                set_={'close': stmt.excluded.close, 'high': stmt.excluded.high,
                      'low': stmt.excluded.low, 'volume': stmt.excluded.volume}
            )
            self.session.execute(stmt)
            self.session.commit()
            logger.info(f"✓ Generated {len(records)} monthly prices for {symbol}")
    
    def _get_index_name(self, symbol: str) -> str:
        """Map index symbol to name"""
        index_map = {
            '^GSPC': 'S&P500', '^DJI': 'Dow Jones',
            '^IXIC': 'NASDAQ', '^RUT': 'Russell 2000'
        }
        return index_map.get(symbol, 'Unknown')

    def _ensure_index_exists(self, symbol: str) -> bool:
        """Ensure index exists in companies table before collecting prices"""
        try:
            # Check if index already exists
            existing = self.session.query(Company).filter(Company.symbol == symbol).first()
            if existing:
                return True

            # Create index entry
            index_name = self._get_index_name(symbol)
            index_company = Company(
                symbol=symbol,
                company_name=index_name,
                exchange='INDEX',
                currency='USD'
            )
            self.session.add(index_company)
            self.session.commit()
            logger.info(f"✓ Created index entry for {symbol} ({index_name})")
            return True
        except Exception as e:
            logger.error(f"Failed to create index entry for {symbol}: {e}")
            self.session.rollback()
            return False

    def collect_sp500(self) -> bool:
        """Collect S&P 500 index data"""
        if not self._ensure_index_exists('^GSPC'):
            return False
        return self.collect_for_symbol('^GSPC')

    def collect_dow_jones(self) -> bool:
        """Collect Dow Jones index data"""
        if not self._ensure_index_exists('^DJI'):
            return False
        return self.collect_for_symbol('^DJI')

    def collect_nasdaq(self) -> bool:
        """Collect NASDAQ index data"""
        if not self._ensure_index_exists('^IXIC'):
            return False
        return self.collect_for_symbol('^IXIC')

    def collect_russell_2000(self) -> bool:
        """Collect Russell 2000 index data"""
        if not self._ensure_index_exists('^RUT'):
            return False
        return self.collect_for_symbol('^RUT')

    def collect_all_indices(self) -> dict:
        """Collect data for all major indices"""
        results = {}
        indices = [
            ('^GSPC', 'S&P 500'),
            ('^DJI', 'Dow Jones'),
            ('^IXIC', 'NASDAQ'),
            ('^RUT', 'Russell 2000')
        ]

        for symbol, name in indices:
            logger.info(f"Collecting {name} index data...")
            if not self._ensure_index_exists(symbol):
                logger.error(f"Failed to ensure {name} exists")
                results[name] = False
                continue

            success = self.collect_for_symbol(symbol)
            results[name] = success
            if success:
                logger.info(f"✓ {name} data collected successfully")
            else:
                logger.error(f"✗ Failed to collect {name} data")

        return results
