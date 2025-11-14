"""
Ratios TTM Bulk Collector
Fetches trailing twelve months financial ratios for all companies from FMP bulk API (CSV)
"""
import logging
import requests
import pandas as pd
from io import StringIO
from typing import Dict

from sqlalchemy.dialects.postgresql import insert

from src.collectors.base_collector import BaseCollector
from src.database.models import RatiosTTMBulk
from src.config import settings, FMP_ENDPOINTS

logger = logging.getLogger(__name__)


class RatiosTTMBulkCollector(BaseCollector):
    """Collector for Financial Ratios TTM bulk data"""

    def __init__(self, session):
        super().__init__(session)
        self.endpoint = FMP_ENDPOINTS['ratios_ttm_bulk']

    def get_table_name(self) -> str:
        return "ratios_ttm_bulk"

    def collect_bulk_ratios_ttm(self) -> Dict:
        """
        Collect Financial Ratios TTM data for all companies from bulk CSV API

        Returns:
            Dictionary with collection results
        """
        logger.info("="*80)
        logger.info("COLLECTING FINANCIAL RATIOS TTM BULK DATA")
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
                'ratios_ttm_bulk',
                symbol=None,  # Global bulk data
                record_count=self.session.query(RatiosTTMBulk).count(),
                next_update_frequency='daily'
            )

            logger.info("="*80)
            logger.info(f"✓ BULK FINANCIAL RATIOS TTM COLLECTION COMPLETE")
            logger.info(f"  Symbols received: {symbols_received:,}")
            logger.info(f"  Symbols inserted/updated: {inserted:,}")
            logger.info("="*80)

            return {
                'success': True,
                'symbols_received': symbols_received,
                'symbols_inserted': inserted
            }

        except Exception as e:
            logger.error(f"Error collecting bulk ratios TTM: {e}")
            self.session.rollback()
            self.record_error('ratios_ttm_bulk', 'ALL', str(e))
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
            'grossProfitMarginTTM': 'gross_profit_margin_ttm',
            'ebitMarginTTM': 'ebit_margin_ttm',
            'ebitdaMarginTTM': 'ebitda_margin_ttm',
            'operatingProfitMarginTTM': 'operating_profit_margin_ttm',
            'pretaxProfitMarginTTM': 'pretax_profit_margin_ttm',
            'continuousOperationsProfitMarginTTM': 'continuous_operations_profit_margin_ttm',
            'netProfitMarginTTM': 'net_profit_margin_ttm',
            'bottomLineProfitMarginTTM': 'bottom_line_profit_margin_ttm',
            'receivablesTurnoverTTM': 'receivables_turnover_ttm',
            'payablesTurnoverTTM': 'payables_turnover_ttm',
            'inventoryTurnoverTTM': 'inventory_turnover_ttm',
            'fixedAssetTurnoverTTM': 'fixed_asset_turnover_ttm',
            'assetTurnoverTTM': 'asset_turnover_ttm',
            'currentRatioTTM': 'current_ratio_ttm',
            'quickRatioTTM': 'quick_ratio_ttm',
            'solvencyRatioTTM': 'solvency_ratio_ttm',
            'cashRatioTTM': 'cash_ratio_ttm',
            'priceToEarningsRatioTTM': 'price_to_earnings_ratio_ttm',
            'priceToEarningsGrowthRatioTTM': 'price_to_earnings_growth_ratio_ttm',
            'forwardPriceToEarningsGrowthRatioTTM': 'forward_price_to_earnings_growth_ratio_ttm',
            'priceToBookRatioTTM': 'price_to_book_ratio_ttm',
            'priceToSalesRatioTTM': 'price_to_sales_ratio_ttm',
            'priceToFreeCashFlowRatioTTM': 'price_to_free_cash_flow_ratio_ttm',
            'priceToOperatingCashFlowRatioTTM': 'price_to_operating_cash_flow_ratio_ttm',
            'debtToAssetsRatioTTM': 'debt_to_assets_ratio_ttm',
            'debtToEquityRatioTTM': 'debt_to_equity_ratio_ttm',
            'debtToCapitalRatioTTM': 'debt_to_capital_ratio_ttm',
            'longTermDebtToCapitalRatioTTM': 'long_term_debt_to_capital_ratio_ttm',
            'financialLeverageRatioTTM': 'financial_leverage_ratio_ttm',
            'workingCapitalTurnoverRatioTTM': 'working_capital_turnover_ratio_ttm',
            'operatingCashFlowRatioTTM': 'operating_cash_flow_ratio_ttm',
            'operatingCashFlowSalesRatioTTM': 'operating_cash_flow_sales_ratio_ttm',
            'freeCashFlowOperatingCashFlowRatioTTM': 'free_cash_flow_operating_cash_flow_ratio_ttm',
            'debtServiceCoverageRatioTTM': 'debt_service_coverage_ratio_ttm',
            'interestCoverageRatioTTM': 'interest_coverage_ratio_ttm',
            'shortTermOperatingCashFlowCoverageRatioTTM': 'short_term_operating_cash_flow_coverage_ratio_ttm',
            'operatingCashFlowCoverageRatioTTM': 'operating_cash_flow_coverage_ratio_ttm',
            'capitalExpenditureCoverageRatioTTM': 'capital_expenditure_coverage_ratio_ttm',
            'dividendPaidAndCapexCoverageRatioTTM': 'dividend_paid_and_capex_coverage_ratio_ttm',
            'dividendPayoutRatioTTM': 'dividend_payout_ratio_ttm',
            'dividendYieldTTM': 'dividend_yield_ttm',
            'enterpriseValueTTM': 'enterprise_value_ttm',
            'revenuePerShareTTM': 'revenue_per_share_ttm',
            'netIncomePerShareTTM': 'net_income_per_share_ttm',
            'interestDebtPerShareTTM': 'interest_debt_per_share_ttm',
            'cashPerShareTTM': 'cash_per_share_ttm',
            'bookValuePerShareTTM': 'book_value_per_share_ttm',
            'tangibleBookValuePerShareTTM': 'tangible_book_value_per_share_ttm',
            'shareholdersEquityPerShareTTM': 'shareholders_equity_per_share_ttm',
            'operatingCashFlowPerShareTTM': 'operating_cash_flow_per_share_ttm',
            'capexPerShareTTM': 'capex_per_share_ttm',
            'freeCashFlowPerShareTTM': 'free_cash_flow_per_share_ttm',
            'netIncomePerEBTTTM': 'net_income_per_ebt_ttm',
            'ebtPerEbitTTM': 'ebt_per_ebit_ttm',
            'priceToFairValueTTM': 'price_to_fair_value_ttm',
            'debtToMarketCapTTM': 'debt_to_market_cap_ttm',
            'effectiveTaxRateTTM': 'effective_tax_rate_ttm',
            'enterpriseValueMultipleTTM': 'enterprise_value_multiple_ttm',
            'dividendPerShareTTM': 'dividend_per_share_ttm',
        }

        # Rename columns
        df = df.rename(columns=column_mapping)

        # Convert numeric columns
        numeric_cols = [col for col in column_mapping.values() if col != 'symbol']

        # Convert to numeric and replace inf/NaN with None
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                # Replace inf and -inf with None
                df[col] = df[col].replace([float('inf'), float('-inf')], None)

        # Convert enterprise_value_ttm to Int64 to avoid float precision issues
        if 'enterprise_value_ttm' in df.columns:
            BIGINT_MAX = 9223372036854775807
            df['enterprise_value_ttm'] = df['enterprise_value_ttm'].clip(lower=-BIGINT_MAX, upper=BIGINT_MAX)
            df['enterprise_value_ttm'] = df['enterprise_value_ttm'].round().astype('Int64')

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
        Upsert records into ratios_ttm_bulk table in batches

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
            sanitized = self.sanitize_record(record, RatiosTTMBulk, record.get('symbol'))
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
                stmt = insert(RatiosTTMBulk).values(batch)

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
