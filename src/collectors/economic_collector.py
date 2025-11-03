"""Economic Indicators Collector - FRED data in vertical format"""
import logging
from datetime import datetime, date
from typing import List, Dict, Optional

import pandas as pd
from sqlalchemy.dialects.postgresql import insert

from src.collectors.base_collector import BaseCollector
from src.config import settings
from src.database.models import EconomicIndicator

logger = logging.getLogger(__name__)


class EconomicCollector(BaseCollector):
    """Collector for economic indicators from FRED"""
    
    INDICATORS = {
        'GDP': {'series_id': 'GDP', 'frequency': 'quarterly'},
        'Real_GDP': {'series_id': 'GDPC1', 'frequency': 'quarterly'},
        'CPI_All_Items': {'series_id': 'CPIAUCSL', 'frequency': 'monthly'},
        'Core_CPI': {'series_id': 'CPILFESL', 'frequency': 'monthly'},
        'Unemployment_Rate': {'series_id': 'UNRATE', 'frequency': 'monthly'},
        'Fed_Funds_Rate': {'series_id': 'FEDFUNDS', 'frequency': 'monthly'},
        'Treasury_10Y': {'series_id': 'DGS10', 'frequency': 'daily'},
        'Treasury_2Y': {'series_id': 'DGS2', 'frequency': 'daily'},
        'S&P_500_Index': {'series_id': 'SP500', 'frequency': 'daily'},
    }
    
    def __init__(self, session):
        super().__init__(session)
        self.fred_api_key = settings.api.fred_api_key
    
    def get_table_name(self) -> str:
        return "economic_indicators"
    
    def collect_for_symbol(self, symbol: str) -> bool:
        """Not applicable for economic data"""
        return False
    
    def collect_all_indicators(self) -> Dict:
        """Collect all economic indicators"""
        job_name = f"EconomicCollector_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.start_collection_run(job_name, list(self.INDICATORS.keys()))
        
        success_count = 0
        
        for indicator_name, config in self.INDICATORS.items():
            try:
                logger.info(f"Collecting {indicator_name}...")
                if self._collect_indicator(indicator_name, config):
                    success_count += 1
                    self.companies_processed += 1
                else:
                    self.companies_failed += 1
            except Exception as e:
                logger.error(f"Error collecting {indicator_name}: {e}")
                self.record_error('economic_indicators', indicator_name, str(e))
                self.companies_failed += 1
        
        status = 'success' if self.companies_failed == 0 else 'partial'
        self.end_collection_run(status)
        
        return {
            'run_id': self.run_id, 'status': status,
            'total_indicators': len(self.INDICATORS),
            'successful': success_count, 'failed': self.companies_failed,
            'records_inserted': self.records_inserted
        }
    
    def _collect_indicator(self, indicator_name: str, config: Dict) -> bool:
        """Collect data for a specific indicator"""
        series_id = config['series_id']
        frequency = config['frequency']
        last_date = self._get_last_indicator_date(indicator_name)
        
        url = f"https://api.stlouisfed.org/fred/series/observations"
        params = {
            'series_id': series_id, 'api_key': self.fred_api_key,
            'file_type': 'json', 'sort_order': 'asc'
        }
        
        if last_date:
            params['observation_start'] = (last_date + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
        else:
            params['observation_start'] = (datetime.now() - pd.Timedelta(days=365*10)).strftime('%Y-%m-%d')
        
        response = self._get(url, params)
        data = self._json_safe(response)
        
        if not data or 'observations' not in data:
            logger.warning(f"No data returned for {indicator_name}")
            return False
        
        observations = data['observations']
        if not observations:
            logger.info(f"No new data for {indicator_name}")
            self.update_tracking('economic_indicators', indicator_name)
            return True
        
        records = []
        for obs in observations:
            if obs['value'] != '.':
                try:
                    records.append({
                        'indicator_name': indicator_name,
                        'date': datetime.strptime(obs['date'], '%Y-%m-%d').date(),
                        'value': float(obs['value']),
                        'frequency': frequency,
                        'series_id': series_id
                    })
                except (ValueError, KeyError) as e:
                    logger.warning(f"Skipping invalid observation: {e}")
        
        if not records:
            return True
        
        inserted = self._insert_indicator_records(records)
        self.records_inserted += inserted
        
        latest_date = max(r['date'] for r in records)
        record_count = self.session.query(EconomicIndicator)\
            .filter(EconomicIndicator.indicator_name == indicator_name).count()
        
        self.update_tracking(
            'economic_indicators', indicator_name,
            last_api_date=latest_date,
            record_count=record_count,
            next_update_frequency=frequency
        )
        
        logger.info(f"✓ {indicator_name}: {inserted} records inserted")
        return True
    
    def _get_last_indicator_date(self, indicator_name: str) -> Optional[date]:
        """Get most recent date for an indicator"""
        result = self.session.query(EconomicIndicator.date)\
            .filter(EconomicIndicator.indicator_name == indicator_name)\
            .order_by(EconomicIndicator.date.desc()).first()
        return result[0] if result else None
    
    def _insert_indicator_records(self, records: List[Dict]) -> int:
        """Insert indicator records with upsert"""
        if not records:
            return 0
        
        stmt = insert(EconomicIndicator).values(records)
        stmt = stmt.on_conflict_do_update(
            index_elements=['indicator_name', 'date'],
            set_={'value': stmt.excluded.value}
        )
        
        result = self.session.execute(stmt)
        self.session.commit()
        return len(records)
