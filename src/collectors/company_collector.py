"""
Company Profile Collector
Handles: Company profile information and metadata
"""
import logging
from datetime import datetime

import pandas as pd
from sqlalchemy.dialects.postgresql import insert

from src.collectors.base_collector import BaseCollector
from src.config import FMP_ENDPOINTS
from src.database.models import Company
from src.utils.data_transform import transform_keys

logger = logging.getLogger(__name__)


class CompanyCollector(BaseCollector):
    """Collector for company profile information"""
    
    def get_table_name(self) -> str:
        return "companies"
    
    def collect_for_symbol(self, symbol: str) -> bool:
        """
        Collect company profile for a symbol

        Args:
            symbol: Stock ticker

        Returns:
            True if successful
        """
        # Skip indices - they don't have company profiles
        if self.is_index_symbol(symbol):
            logger.info(f"Skipping company profile for index {symbol}")
            return True

        try:
            # Check if update needed (update every 15 days)
            if not self.should_update_symbol('companies', symbol, max_age_days=15):
                logger.info(f"Company profile for {symbol} is up to date")
                return True
            
            # Fetch from API
            url = FMP_ENDPOINTS['profile']
            params = {'symbol': symbol}
            
            response = self._get(url, params)
            data = self._json_safe(response)
            
            if not data:
                logger.warning(f"No company profile returned for {symbol}")
                return False
            
            # API returns a list, get the first item
            if isinstance(data, list) and data:
                profile = data[0]
            elif isinstance(data, dict):
                profile = data
            else:
                logger.warning(f"Unexpected profile data format for {symbol}")
                return False

            # Transform API data from camelCase to snake_case
            record = transform_keys(profile)

            # Handle date fields
            if 'ipo_date' in record and record['ipo_date']:
                try:
                    record['ipo_date'] = pd.to_datetime(record['ipo_date']).date()
                except:
                    record['ipo_date'] = None

            # Sanitize record
            record = self.sanitize_record(record, Company, symbol)

            # Upsert record
            stmt = insert(Company).values([record])
            
            # Update all fields on conflict
            update_dict = {
                col.name: col 
                for col in stmt.excluded 
                if col.name not in ['company_id', 'symbol', 'created_at']
            }
            
            stmt = stmt.on_conflict_do_update(
                index_elements=['symbol'],
                set_=update_dict
            )
            
            self.session.execute(stmt)
            self.session.commit()
            
            self.records_inserted += 1
            
            # Update tracking
            self.update_tracking(
                'companies',
                symbol,
                last_api_date=datetime.now().date(),
                record_count=1,
                next_update_frequency='weekly'
            )
            
            logger.info(f"✓ {symbol} company profile upserted")
            return True
            
        except Exception as e:
            logger.error(f"Error collecting company profile for {symbol}: {e}")
            self.record_error('companies', symbol, str(e))
            return False
    
    def _map_profile_fields(self, profile: dict, symbol: str) -> dict:
        """Map API fields to database model fields"""
        
        # Handle date fields
        ipo_date = None
        if 'ipoDate' in profile and profile['ipoDate']:
            try:
                ipo_date = pd.to_datetime(profile['ipoDate']).date()
            except:
                pass
        
        return {
            'symbol': symbol,
            'company_name': profile.get('companyName', ''),
            'price': profile.get('price'),
            'market_cap': profile.get('mktCap'),
            'beta': profile.get('beta'),
            'last_dividend': profile.get('lastDiv'),
            'range': profile.get('range'),
            'changes': profile.get('changes'),
            'change_percentage': profile.get('changesPercentage'),
            'volume': profile.get('volAvg'),
            'average_volume': profile.get('volAvg'),
            'currency': profile.get('currency'),
            'cik': profile.get('cik'),
            'isin': profile.get('isin'),
            'cusip': profile.get('cusip'),
            'exchange': profile.get('exchange'),
            'exchange_full_name': profile.get('exchangeShortName'),
            'industry': profile.get('industry'),
            'sector': profile.get('sector'),
            'country': profile.get('country'),
            'website': profile.get('website'),
            'description': profile.get('description'),
            'ceo': profile.get('ceo'),
            'full_time_employees': profile.get('fullTimeEmployees'),
            'phone': profile.get('phone'),
            'address': profile.get('address'),
            'city': profile.get('city'),
            'state': profile.get('state'),
            'zip_code': profile.get('zip'),
            'image_url': profile.get('image'),
            'ipo_date': ipo_date,
            'default_image': profile.get('defaultImage'),
            'is_etf': profile.get('isEtf'),
            'is_actively_trading': profile.get('isActivelyTrading'),
            'is_adr': profile.get('isAdr'),
            'is_fund': profile.get('isFund')
        }
    
    def collect_all_profiles(self):
        """
        Collect profiles for all symbols in database
        
        This is useful for refreshing all company information
        """
        return self.collect_for_all_symbols()
    
    def add_new_company(self, symbol: str, company_name: str = None) -> bool:
        """
        Add a new company to the database
        
        Args:
            symbol: Stock ticker
            company_name: Optional company name (will be fetched if not provided)
        
        Returns:
            True if successful
        """
        # Check if company already exists
        existing = self.session.query(Company)\
            .filter(Company.symbol == symbol)\
            .first()
        
        if existing:
            logger.info(f"Company {symbol} already exists, updating profile...")
            return self.collect_for_symbol(symbol)
        
        # If company name provided, create basic record first
        if company_name:
            basic_record = {
                'symbol': symbol,
                'company_name': company_name
            }
            
            stmt = insert(Company).values([basic_record])
            stmt = stmt.on_conflict_do_nothing(index_elements=['symbol'])
            
            self.session.execute(stmt)
            self.session.commit()
            
            logger.info(f"Created basic company record for {symbol}")
        
        # Now fetch full profile
        return self.collect_for_symbol(symbol)


if __name__ == "__main__":
    from src.database.connection import get_session
    
    logging.basicConfig(level=logging.INFO)
    
    with get_session() as session:
        collector = CompanyCollector(session)
        
        # Test adding a new company
        success = collector.add_new_company('AAPL', 'Apple Inc.')
        print(f"Add company: {'successful' if success else 'failed'}")
        
        # Test collecting profile
        success = collector.collect_for_symbol('AAPL')
        print(f"Collect profile: {'successful' if success else 'failed'}")
