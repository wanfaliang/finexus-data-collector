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
        """Collect all financial data for a symbol (both annual and quarterly)"""
        # Skip indices - they don't have financial statements
        if self.is_index_symbol(symbol):
            logger.info(f"Skipping financial data for index {symbol}")
            return True

        success_count = 0

        # Collect both annual and quarterly data
        for period in ['annual', 'quarter']:
            for statement_type, model in self.STATEMENT_MAP.items():
                try:
                    if self._collect_statement(symbol, statement_type, model, period):
                        success_count += 1
                except Exception as e:
                    logger.error(f"Error collecting {statement_type} ({period}) for {symbol}: {e}")
                    self.record_error(model.__tablename__, symbol, str(e))
                    # Rollback to prevent cascade failures in subsequent statement types
                    self.session.rollback()

        return success_count > 0
    
    def _collect_statement(self, symbol: str, statement_type: str, model: Any, period: str = 'annual') -> bool:
        """Collect specific statement type for a symbol with given period (annual or quarter)"""
        table_name = model.__tablename__

        # Create a unique tracking key that includes period
        tracking_key = f"{table_name}_{period}"

        if not self.should_update_symbol(tracking_key, symbol, max_age_days=15):
            logger.info(f"{table_name} ({period}) for {symbol} is up to date")
            return True

        # In force refill mode, ignore last_date to fetch all data
        last_date = None if self.force_refill else self._get_last_date_for_table(model, symbol, period)

        # Set limit based on period type for consistent years of history
        if period == 'annual':
            limit = 10 if last_date else 50  # 50 years initially, 10 on update
        elif period == 'quarter':
            limit = 40 if last_date else 200  # 50 years initially (200 quarters), 40 on update (10 years)
        else:
            limit = 50  # fallback

        url = FMP_ENDPOINTS[statement_type]
        params = {
            'symbol': symbol,
            'period': period,
            'limit': limit
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
            logger.warning(f"Empty dataframe for {symbol} {statement_type} ({period})")
            return False

        if last_date:
            df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date
            df = df[df['date'] > last_date]

        if df.empty:
            logger.info(f"No new records for {symbol} {statement_type} ({period})")
            self.update_tracking(tracking_key, symbol)
            return True

        # The API already returns a 'period' field, no need to add it
        # Just ensure date conversion
        records = df.to_dict('records')
        inserted, updated = self._upsert_records(model, records, symbol)

        self.records_inserted += inserted
        self.records_updated += updated

        latest_date = df['date'].max()

        # Count records for the correct period type
        count_query = self.session.query(model).filter(model.symbol == symbol)
        if period == 'annual':
            count_query = count_query.filter(model.period == 'FY')
        elif period == 'quarter':
            count_query = count_query.filter(model.period.in_(['Q1', 'Q2', 'Q3', 'Q4']))
        record_count = count_query.count()

        self.update_tracking(
            tracking_key, symbol,
            last_api_date=latest_date,
            record_count=record_count,
            next_update_frequency='quarterly'
        )

        logger.info(f"✓ {symbol} {table_name} ({period}): {inserted} inserted, {updated} updated")
        return True
    
    def _get_last_date_for_table(self, model: Any, symbol: str, period: str = 'annual') -> Optional[date]:
        """Get the most recent date for a symbol in a table for a specific period"""
        # API uses 'annual' or 'quarter', but DB stores 'FY', 'Q1', 'Q2', 'Q3', 'Q4'
        query = self.session.query(model.date).filter(model.symbol == symbol)

        if period == 'annual':
            query = query.filter(model.period == 'FY')
        elif period == 'quarter':
            query = query.filter(model.period.in_(['Q1', 'Q2', 'Q3', 'Q4']))

        result = query.order_by(desc(model.date)).first()
        return result[0] if result else None
    
    def _upsert_records(self, model: Any, records: List[Dict], symbol: str) -> tuple:
        """Upsert records using PostgreSQL ON CONFLICT"""
        if not records:
            return (0, 0)

        # Sanitize and prepare records
        sanitized_records = []
        for record in records:
            record['symbol'] = symbol
            if 'date' in record and isinstance(record['date'], str):
                record['date'] = pd.to_datetime(record['date']).date()

            # Use base collector's smart sanitization
            sanitized = self.sanitize_record(record, model, symbol)
            sanitized_records.append(sanitized)

        records = sanitized_records

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
