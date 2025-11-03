"""
Bulk Financial Statements Collector
Loads income statements, balance sheets, and cash flows from bulk CSV files
"""
import logging
import re
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
from sqlalchemy.dialects.postgresql import insert

from src.collectors.base_collector import BaseCollector
from src.database.models import IncomeStatement, BalanceSheet, CashFlow
from src.utils.data_transform import transform_batch, transform_keys
from src.utils.bulk_utils import get_bulk_data_path, list_bulk_files

logger = logging.getLogger(__name__)


class BulkFinancialCollector(BaseCollector):
    """Collector for bulk financial statement data"""

    STATEMENT_MAP = {
        'income_statement': IncomeStatement,
        'balance_sheet_statement': BalanceSheet,
        'cash_flow_statement': CashFlow
    }

    def get_table_name(self) -> str:
        return "financial_statements"

    def parse_filename(self, filename: str) -> Optional[Tuple[str, str, str]]:
        """
        Parse bulk financial statement filename

        Args:
            filename: e.g., '2024_income_statement_FY_bulk.csv'

        Returns:
            Tuple of (year, statement_type, period) or None if invalid
        """
        # Pattern: YYYY_{statement_type}_{PERIOD}_bulk.csv
        # statement_type can be: income_statement, balance_sheet_statement, cash_flow_statement
        pattern = r'(\d{4})_(income_statement|balance_sheet_statement|cash_flow_statement)_(FY|Q[1-4])_bulk\.csv'
        match = re.match(pattern, filename)

        if match:
            year, statement_type, period = match.groups()
            return year, statement_type, period

        return None

    def process_bulk_file(self, file_path: Path) -> bool:
        """
        Process a bulk financial statement CSV file

        Args:
            file_path: Path to the CSV file

        Returns:
            True if successful
        """
        try:
            # Parse filename
            parsed = self.parse_filename(file_path.name)
            if not parsed:
                logger.error(f"Invalid filename format: {file_path.name}")
                return False

            year, statement_type, period = parsed
            model = self.STATEMENT_MAP[statement_type]
            table_name = model.__tablename__

            logger.info(f"Processing {statement_type} for {year} {period}")
            logger.info(f"File: {file_path.name}")

            # Read CSV
            df = pd.read_csv(file_path)
            logger.info(f"Loaded {len(df):,} records from CSV")

            # Skip rows with null symbols
            initial_count = len(df)
            df = df[df['symbol'].notna()]
            if len(df) < initial_count:
                logger.warning(f"Skipped {initial_count - len(df)} rows with null symbols")

            if df.empty:
                logger.warning("No valid records to process")
                return False

            # Transform column names from camelCase to snake_case
            records = df.to_dict('records')
            transformed_records = transform_batch(records, transform_keys)

            # Create dataframe with transformed columns
            df = pd.DataFrame(transformed_records)

            # Data type conversions
            self._convert_data_types(df)

            # Prepare records for insertion
            records = df.to_dict('records')

            # Insert in batches
            batch_size = 1000
            total_inserted = 0

            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                inserted = self._upsert_batch(batch, model)
                total_inserted += inserted

                if (i + batch_size) % 5000 == 0:
                    logger.info(f"Processed {i + batch_size:,} / {len(records):,} records...")

            logger.info(f"✓ Completed {statement_type} {year} {period}: {total_inserted:,} records")
            self.records_inserted += total_inserted

            return True

        except Exception as e:
            logger.error(f"Error processing bulk financial file: {e}")
            raise

    def _convert_data_types(self, df: pd.DataFrame):
        """Convert data types to match database schema"""
        # Convert date columns
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df['date'] = df['date'].apply(lambda x: x.date() if pd.notna(x) else None)

        if 'filing_date' in df.columns:
            df['filing_date'] = pd.to_datetime(df['filing_date'], errors='coerce')
            df['filing_date'] = df['filing_date'].apply(lambda x: x.date() if pd.notna(x) else None)

        if 'accepted_date' in df.columns:
            df['accepted_date'] = pd.to_datetime(df['accepted_date'], errors='coerce')

        # Replace NaN with None for nullable columns
        df = df.where(pd.notna(df), None)

    def _upsert_batch(self, records: list, model) -> int:
        """
        Insert or update a batch of records

        Args:
            records: List of record dictionaries
            model: SQLAlchemy model class

        Returns:
            Number of records processed
        """
        if not records:
            return 0

        try:
            stmt = insert(model).values(records)

            # Define update dictionary for conflict resolution
            # Update all fields except primary key and timestamps
            pk_columns = ['symbol', 'date', 'period']
            update_dict = {
                col.name: col
                for col in stmt.excluded
                if col.name not in pk_columns + ['created_at']
            }

            stmt = stmt.on_conflict_do_update(
                index_elements=pk_columns,
                set_=update_dict
            )

            result = self.session.execute(stmt)
            self.session.commit()

            return len(records)

        except Exception as e:
            logger.error(f"Error upserting batch: {e}")
            self.session.rollback()

            # Try inserting records one by one to identify problematic records
            logger.info("Attempting to insert records individually...")
            inserted = 0
            failed = 0

            for record in records:
                try:
                    stmt = insert(model).values([record])
                    stmt = stmt.on_conflict_do_update(
                        index_elements=pk_columns,
                        set_={col.name: col for col in stmt.excluded
                              if col.name not in pk_columns + ['created_at']}
                    )
                    self.session.execute(stmt)
                    self.session.commit()
                    inserted += 1
                except Exception as e2:
                    logger.warning(f"Failed to insert {record.get('symbol', 'UNKNOWN')}: {str(e2)[:100]}")
                    self.session.rollback()
                    failed += 1

            logger.info(f"Individual insert complete: {inserted} succeeded, {failed} failed")
            return inserted

    def process_all_financial_files(self, subfolder: str = 'financials') -> dict:
        """
        Process all financial statement files in the bulk data folder

        Args:
            subfolder: Subfolder to search for files

        Returns:
            Dictionary with results per file
        """
        results = {}
        bulk_path = get_bulk_data_path() / subfolder

        if not bulk_path.exists():
            logger.warning(f"Folder not found: {bulk_path}")
            return results

        # Find all financial statement bulk files
        patterns = [
            '*_income_statement_*_bulk.csv',
            '*_balance_sheet_statement_*_bulk.csv',
            '*_cash_flow_statement_*_bulk.csv'
        ]

        files = []
        for pattern in patterns:
            files.extend(sorted(bulk_path.glob(pattern)))

        if not files:
            logger.warning(f"No financial statement files found in {bulk_path}")
            return results

        logger.info(f"Found {len(files)} financial statement files to process")

        for file_path in files:
            logger.info(f"\n{'='*80}")
            try:
                success = self.process_bulk_file(file_path)
                results[file_path.name] = success
            except Exception as e:
                logger.error(f"Failed to process {file_path.name}: {e}")
                results[file_path.name] = False

        return results

    def collect_for_all_symbols(self) -> dict:
        """Process all bulk financial statement data"""
        return self.process_all_financial_files()
