"""
Financial Statements Collector
Handles: Income Statements, Balance Sheets, Cash Flows, Ratios, Key Metrics
"""
import logging
from datetime import date
from typing import Optional, List, Dict, Any

import pandas as pd
from sqlalchemy import desc
from sqlalchemy.dialects.postgresql import insert

from src.collectors.base_collector import BaseCollector
from src.config import FMP_ENDPOINTS
from src.database.models import (
    IncomeStatement, BalanceSheet, CashFlow,
    FinancialRatio, KeyMetric
)
from src.utils.data_transform import transform_batch, transform_keys

logger = logging.getLogger(__name__)


class FinancialCollector(BaseCollector):
    """Collector for financial statements and metrics"""
    
    STATEMENT_MAP = {
        'income_statement': IncomeStatement,
        'balance_sheet': BalanceSheet,
        'cash_flow': CashFlow,
        'ratios': FinancialRatio,
        'key_metrics': KeyMetric
    }
    
    def get_table_name(self) -> str:
        return "financial_statements"
    
    def collect_for_symbol(self, symbol: str) -> bool:
        """Collect all financial data for a symbol"""
        # Skip indices - they don't have financial statements
        if self.is_index_symbol(symbol):
            logger.info(f"Skipping financial data for index {symbol}")
            return True

        success_count = 0

        for statement_type, model in self.STATEMENT_MAP.items():
            try:
                if self._collect_statement(symbol, statement_type, model):
                    success_count += 1
            except Exception as e:
                logger.error(f"Error collecting {statement_type} for {symbol}: {e}")
                self.record_error(model.__tablename__, symbol, str(e))
        
        return success_count > 0
    
    def _collect_statement(self, symbol: str, statement_type: str, model: Any) -> bool:
        """Collect specific statement type for a symbol"""
        table_name = model.__tablename__
        
        if not self.should_update_symbol(table_name, symbol, max_age_days=1):
            logger.info(f"{table_name} for {symbol} is up to date")
            return True
        
        last_date = self._get_last_date_for_table(model, symbol)
        
        url = FMP_ENDPOINTS[statement_type]
        params = {
            'symbol': symbol,
            'period': 'annual',
            'limit': 10 if last_date else 50
        }
        
        response = self._get(url, params)
        data = self._json_safe(response)

        if not data:
            logger.warning(f"No data returned for {symbol} {statement_type}")
            return False

        # Transform API data from camelCase to snake_case
        transformed_data = transform_batch(data, transform_keys)

        df = self._to_dataframe(transformed_data)
        if df.empty:
            logger.warning(f"Empty dataframe for {symbol} {statement_type}")
            return False
        
        if last_date:
            df['date'] = pd.to_datetime(df['date']).dt.date
            df = df[df['date'] > last_date]
        
        if df.empty:
            logger.info(f"No new records for {symbol} {statement_type}")
            self.update_tracking(table_name, symbol)
            return True
        
        records = df.to_dict('records')
        inserted, updated = self._upsert_records(model, records, symbol)
        
        self.records_inserted += inserted
        self.records_updated += updated
        
        latest_date = df['date'].max()
        record_count = self.session.query(model).filter(model.symbol == symbol).count()
        
        self.update_tracking(
            table_name, symbol, 
            last_api_date=latest_date,
            record_count=record_count,
            next_update_frequency='daily'
        )
        
        logger.info(f"✓ {symbol} {table_name}: {inserted} inserted, {updated} updated")
        return True
    
    def _get_last_date_for_table(self, model: Any, symbol: str) -> Optional[date]:
        """Get the most recent date for a symbol in a table"""
        result = self.session.query(model.date)\
            .filter(model.symbol == symbol)\
            .order_by(desc(model.date))\
            .first()
        
        return result[0] if result else None
    
    def _upsert_records(self, model: Any, records: List[Dict], symbol: str) -> tuple:
        """Upsert records using PostgreSQL ON CONFLICT"""
        if not records:
            return (0, 0)
        
        for record in records:
            record['symbol'] = symbol
            if 'date' in record and isinstance(record['date'], str):
                record['date'] = pd.to_datetime(record['date']).date()
        
        stmt = insert(model).values(records)
        pk_columns = [col.name for col in model.__table__.primary_key]
        update_dict = {
            col.name: col 
            for col in stmt.excluded 
            if col.name not in pk_columns
        }
        
        stmt = stmt.on_conflict_do_update(
            index_elements=pk_columns,
            set_=update_dict
        )
        
        result = self.session.execute(stmt)
        self.session.commit()
        
        return (len(records), 0)
