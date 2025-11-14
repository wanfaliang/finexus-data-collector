"""
Key Metrics TTM Bulk Collector
Fetches trailing twelve months key metrics for all companies from FMP bulk API (CSV)
"""
import logging
import requests
import pandas as pd
from io import StringIO
from typing import Dict

from sqlalchemy.dialects.postgresql import insert

from src.collectors.base_collector import BaseCollector
from src.database.models import KeyMetricsTTMBulk
from src.config import settings, FMP_ENDPOINTS

logger = logging.getLogger(__name__)


class KeyMetricsTTMBulkCollector(BaseCollector):
    """Collector for Key Metrics TTM bulk data"""

    def __init__(self, session):
        super().__init__(session)
        self.endpoint = FMP_ENDPOINTS['key_metrics_ttm_bulk']

    def get_table_name(self) -> str:
        return "key_metrics_ttm_bulk"

    def collect_bulk_key_metrics_ttm(self) -> Dict:
        """
        Collect Key Metrics TTM data for all companies from bulk CSV API

        Returns:
            Dictionary with collection results
        """
        logger.info("="*80)
        logger.info("COLLECTING KEY METRICS TTM BULK DATA")
        logger.info("="*80)

        try:
            # Make API request for CSV
            logger.info(f"Downloading CSV from FMP bulk API...")
            logger.info(f"Endpoint: {self.endpoint}")

            params = {'apikey': settings.api.fmp_api_key}
            response = requests.get(
                self.endpoint,
                params=params,
                timeout=settings.api.timeout
            )

            if response.status_code != 200:
                error_msg = f"API request failed with status {response.status_code}"
                logger.error(error_msg)
                return {
                    'success': False,
                    'symbols_received': 0,
                    'symbols_inserted': 0,
                    'error': error_msg
                }

            # Parse CSV directly from response text
            csv_data = StringIO(response.text)
            df = pd.read_csv(csv_data)

            if df.empty:
                logger.warning("No data returned from API")
                return {
                    'success': True,
                    'symbols_received': 0,
                    'symbols_inserted': 0
                }

            symbols_received = len(df)
            logger.info(f"Received data for {symbols_received:,} symbols")
            logger.info(f"CSV columns: {len(df.columns)}")

            # Clean and transform data
            df_clean = self._clean_data(df)
            logger.info(f"Cleaned data: {len(df_clean):,} valid records")

            # Insert into database
            inserted = self._upsert_records(df_clean)
            self.records_inserted += inserted

            # Update tracking
            self.update_tracking(
                'key_metrics_ttm_bulk',
                symbol=None,  # Global bulk data
                record_count=self.session.query(KeyMetricsTTMBulk).count(),
                next_update_frequency='daily'
            )

            logger.info("="*80)
            logger.info(f"✓ BULK KEY METRICS TTM COLLECTION COMPLETE")
            logger.info(f"  Symbols received: {symbols_received:,}")
            logger.info(f"  Symbols inserted/updated: {inserted:,}")
            logger.info("="*80)

            return {
                'success': True,
                'symbols_received': symbols_received,
                'symbols_inserted': inserted
            }

        except Exception as e:
            logger.error(f"Error collecting bulk key metrics TTM: {e}")
            self.session.rollback()
            self.record_error('key_metrics_ttm_bulk', 'ALL', str(e))
            return {
                'success': False,
                'symbols_received': 0,
                'symbols_inserted': 0,
                'error': str(e)
            }

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and transform bulk CSV data

        Args:
            df: Raw DataFrame from CSV

        Returns:
            Cleaned DataFrame
        """
        df = df.copy()

        # Column mapping - convert camelCase to snake_case
        column_mapping = {
            'symbol': 'symbol',
            'marketCap': 'market_cap',
            'enterpriseValueTTM': 'enterprise_value_ttm',
            'evToSalesTTM': 'ev_to_sales_ttm',
            'evToOperatingCashFlowTTM': 'ev_to_operating_cash_flow_ttm',
            'evToFreeCashFlowTTM': 'ev_to_free_cash_flow_ttm',
            'evToEBITDATTM': 'ev_to_ebitda_ttm',
            'netDebtToEBITDATTM': 'net_debt_to_ebitda_ttm',
            'currentRatioTTM': 'current_ratio_ttm',
            'incomeQualityTTM': 'income_quality_ttm',
            'grahamNumberTTM': 'graham_number_ttm',
            'grahamNetNetTTM': 'graham_net_net_ttm',
            'taxBurdenTTM': 'tax_burden_ttm',
            'interestBurdenTTM': 'interest_burden_ttm',
            'workingCapitalTTM': 'working_capital_ttm',
            'investedCapitalTTM': 'invested_capital_ttm',
            'returnOnAssetsTTM': 'return_on_assets_ttm',
            'operatingReturnOnAssetsTTM': 'operating_return_on_assets_ttm',
            'returnOnTangibleAssetsTTM': 'return_on_tangible_assets_ttm',
            'returnOnEquityTTM': 'return_on_equity_ttm',
            'returnOnInvestedCapitalTTM': 'return_on_invested_capital_ttm',
            'returnOnCapitalEmployedTTM': 'return_on_capital_employed_ttm',
            'earningsYieldTTM': 'earnings_yield_ttm',
            'freeCashFlowYieldTTM': 'free_cash_flow_yield_ttm',
            'capexToOperatingCashFlowTTM': 'capex_to_operating_cash_flow_ttm',
            'capexToDepreciationTTM': 'capex_to_depreciation_ttm',
            'capexToRevenueTTM': 'capex_to_revenue_ttm',
            'salesGeneralAndAdministrativeToRevenueTTM': 'sales_general_and_administrative_to_revenue_ttm',
            'researchAndDevelopementToRevenueTTM': 'research_and_developement_to_revenue_ttm',
            'stockBasedCompensationToRevenueTTM': 'stock_based_compensation_to_revenue_ttm',
            'intangiblesToTotalAssetsTTM': 'intangibles_to_total_assets_ttm',
            'averageReceivablesTTM': 'average_receivables_ttm',
            'averagePayablesTTM': 'average_payables_ttm',
            'averageInventoryTTM': 'average_inventory_ttm',
            'daysOfSalesOutstandingTTM': 'days_of_sales_outstanding_ttm',
            'daysOfPayablesOutstandingTTM': 'days_of_payables_outstanding_ttm',
            'daysOfInventoryOutstandingTTM': 'days_of_inventory_outstanding_ttm',
            'operatingCycleTTM': 'operating_cycle_ttm',
            'cashConversionCycleTTM': 'cash_conversion_cycle_ttm',
            'freeCashFlowToEquityTTM': 'free_cash_flow_to_equity_ttm',
            'freeCashFlowToFirmTTM': 'free_cash_flow_to_firm_ttm',
            'tangibleAssetValueTTM': 'tangible_asset_value_ttm',
            'netCurrentAssetValueTTM': 'net_current_asset_value_ttm',
        }

        # Rename columns
        df = df.rename(columns=column_mapping)

        # Convert numeric columns
        numeric_cols = [
            'market_cap', 'enterprise_value_ttm', 'ev_to_sales_ttm',
            'ev_to_operating_cash_flow_ttm', 'ev_to_free_cash_flow_ttm',
            'ev_to_ebitda_ttm', 'net_debt_to_ebitda_ttm', 'current_ratio_ttm',
            'income_quality_ttm', 'graham_number_ttm', 'graham_net_net_ttm',
            'tax_burden_ttm', 'interest_burden_ttm', 'working_capital_ttm',
            'invested_capital_ttm', 'return_on_assets_ttm',
            'operating_return_on_assets_ttm', 'return_on_tangible_assets_ttm',
            'return_on_equity_ttm', 'return_on_invested_capital_ttm',
            'return_on_capital_employed_ttm', 'earnings_yield_ttm',
            'free_cash_flow_yield_ttm', 'capex_to_operating_cash_flow_ttm',
            'capex_to_depreciation_ttm', 'capex_to_revenue_ttm',
            'sales_general_and_administrative_to_revenue_ttm',
            'research_and_developement_to_revenue_ttm',
            'stock_based_compensation_to_revenue_ttm',
            'intangibles_to_total_assets_ttm', 'average_receivables_ttm',
            'average_payables_ttm', 'average_inventory_ttm',
            'days_of_sales_outstanding_ttm', 'days_of_payables_outstanding_ttm',
            'days_of_inventory_outstanding_ttm', 'operating_cycle_ttm',
            'cash_conversion_cycle_ttm', 'free_cash_flow_to_equity_ttm',
            'free_cash_flow_to_firm_ttm', 'tangible_asset_value_ttm',
            'net_current_asset_value_ttm'
        ]

        # Convert to numeric and replace inf/NaN with None
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                # Replace inf and -inf with None
                df[col] = df[col].replace([float('inf'), float('-inf')], None)

        # Handle BigInteger/Numeric overflow - use sanitize_record instead of manual clipping
        # Convert market_cap to Int64 to avoid float precision issues
        if 'market_cap' in df.columns:
            BIGINT_MAX = 9223372036854775807
            df['market_cap'] = df['market_cap'].clip(lower=-BIGINT_MAX, upper=BIGINT_MAX)
            df['market_cap'] = df['market_cap'].round().astype('Int64')

        # Drop rows with missing symbol
        df = df.dropna(subset=['symbol'])

        # Filter symbols longer than 20 chars
        df = df[df['symbol'].astype(str).str.len() <= 20]

        # Select only columns that exist in the model
        model_columns = [col for col in column_mapping.values() if col in df.columns]
        df = df[model_columns]

        return df

    def _upsert_records(self, df: pd.DataFrame) -> int:
        """
        Upsert records into key_metrics_ttm_bulk table in batches

        Args:
            df: Cleaned DataFrame

        Returns:
            Number of records inserted/updated
        """
        if df.empty:
            return 0

        # Replace NaN with None for PostgreSQL
        df = df.where(pd.notnull(df), None)

        records = df.to_dict('records')

        # Sanitize records to prevent overflow
        sanitized_records = []
        for record in records:
            sanitized = self.sanitize_record(record, KeyMetricsTTMBulk, record.get('symbol'))
            sanitized_records.append(sanitized)

        total_records = len(sanitized_records)
        batch_size = 1000  # Process 1000 records at a time
        total_inserted = 0
        error_logged = False  # Only log first error

        logger.info(f"Inserting {total_records:,} records...")

        # Process in batches
        for i in range(0, total_records, batch_size):
            batch = sanitized_records[i:i + batch_size]

            try:
                # UPSERT: insert or update on conflict
                stmt = insert(KeyMetricsTTMBulk).values(batch)

                # Get all columns except primary key and metadata
                update_columns = {
                    col: getattr(stmt.excluded, col)
                    for col in df.columns
                    if col not in ['symbol', 'created_at']
                }
                update_columns['updated_at'] = stmt.excluded.updated_at

                stmt = stmt.on_conflict_do_update(
                    index_elements=['symbol'],
                    set_=update_columns
                )

                self.session.execute(stmt)
                self.session.commit()

                total_inserted += len(batch)

            except Exception as e:
                self.session.rollback()
                if not error_logged:
                    logger.error(f"Batch failed. First error: {str(e)[:500]}")
                    error_logged = True
                continue

        logger.info(f"✓ Inserted: {total_inserted:,}/{total_records:,}")
        return total_inserted
