# pc_flat_file_parser.py
"""
Parser for BLS Producer Price Index - Industry (PC) flat files downloaded from:
https://download.bls.gov/pub/time.series/pc/

Parses tab-delimited files and loads into PostgreSQL database.
"""
import csv
import logging
from pathlib import Path
from typing import List, Dict, Iterator
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime, UTC

# Import BLS models
import sys
sys.path.append(str(Path(__file__).parent.parent))
from database.bls_models import (
    PCIndustry, PCProduct, PCSeries, PCData
)

log = logging.getLogger("PCFlatFileParser")
logging.basicConfig(level=logging.INFO)


class PCFlatFileParser:
    """Parser for PC (Producer Price Index - Industry) survey flat files"""

    def __init__(self, data_dir: str = "data/bls/pc"):
        self.data_dir = Path(data_dir)
        if not self.data_dir.exists():
            raise FileNotFoundError(f"Data directory not found: {self.data_dir}")

    # ==================== REFERENCE TABLE PARSERS ====================

    def parse_industries(self) -> Iterator[Dict]:
        """Parse pc.industry file"""
        file_path = self.data_dir / "pc.industry"
        log.info(f"Parsing industries from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'industry_code': row['industry_code'].strip(),
                    'industry_name': row['industry_name'].strip(),
                }

    def parse_products(self) -> Iterator[Dict]:
        """Parse pc.product file"""
        file_path = self.data_dir / "pc.product"
        log.info(f"Parsing products from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'industry_code': row['industry_code'].strip(),
                    'product_code': row['product_code'].strip(),
                    'product_name': row['product_name'].strip(),
                }

    def parse_series(self) -> Iterator[Dict]:
        """Parse pc.series file"""
        file_path = self.data_dir / "pc.series"
        log.info(f"Parsing series from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                series_id = row['series_id'].strip()
                industry_code = row['industry_code'].strip()
                product_code = row['product_code'].strip()
                seasonal = row.get('seasonal', '').strip() or None
                base_date = row.get('base_date', '').strip() or None
                series_title = row['series_title'].strip()
                footnote_codes = row.get('footnote_codes', '').strip() or None

                # Parse begin/end dates
                begin_year = row.get('begin_year', '').strip()
                begin_year = int(begin_year) if begin_year else None

                begin_period = row.get('begin_period', '').strip() or None

                end_year = row.get('end_year', '').strip()
                end_year = int(end_year) if end_year else None

                end_period = row.get('end_period', '').strip() or None

                # Determine if series is active (based on end date)
                current_year = datetime.now(UTC).year
                is_active = (end_year is None) or (end_year >= current_year - 1)

                yield {
                    'series_id': series_id,
                    'industry_code': industry_code,
                    'product_code': product_code,
                    'seasonal_code': seasonal,
                    'base_date': base_date,
                    'series_title': series_title,
                    'footnote_codes': footnote_codes,
                    'begin_year': begin_year,
                    'begin_period': begin_period,
                    'end_year': end_year,
                    'end_period': end_period,
                    'is_active': is_active,
                }

    # ==================== DATA PARSERS ====================

    def parse_data_file(self, filename: str) -> Iterator[Dict]:
        """
        Parse a PC data file (e.g., pc.data.0.Current, pc.data.14.Chemicals)

        Args:
            filename: Name of the data file (without directory path)

        Yields:
            Dictionary with series_id, year, period, value, footnote_codes
        """
        file_path = self.data_dir / filename
        log.info(f"Parsing data from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]

            for row in reader:
                series_id = row['series_id'].strip()
                year = int(row['year'])
                period = row['period'].strip()
                value_str = row['value'].strip()

                # Parse value (handle empty values)
                value = float(value_str) if value_str else None

                footnote_codes = row.get('footnote_codes', '').strip() or None

                yield {
                    'series_id': series_id,
                    'year': year,
                    'period': period,
                    'value': value,
                    'footnote_codes': footnote_codes,
                }

    # ==================== DATABASE LOADERS ====================

    def _upsert_batch(self, session: Session, model, data: List[Dict], key_columns: List[str]):
        """
        Upsert a batch of records using PostgreSQL's ON CONFLICT DO UPDATE

        Args:
            session: SQLAlchemy session
            model: The model class (e.g., PCIndustry)
            data: List of dictionaries to insert/update
            key_columns: List of column names that form the unique constraint
        """
        if not data:
            return

        stmt = insert(model).values(data)

        # Build update dict (exclude primary keys and created_at)
        update_dict = {
            col.name: stmt.excluded[col.name]
            for col in model.__table__.columns
            if col.name not in key_columns and col.name != 'created_at'
        }

        # Add updated_at if it exists
        if hasattr(model, 'updated_at'):
            update_dict['updated_at'] = datetime.now(UTC)

        stmt = stmt.on_conflict_do_update(
            index_elements=key_columns,
            set_=update_dict
        )

        session.execute(stmt)

    def load_reference_tables(self, session: Session):
        """Load all reference tables (industries, products, series)"""
        log.info("Loading PC reference tables...")

        # Load industries
        industries = list(self.parse_industries())
        log.info(f"  Upserting {len(industries)} industries...")
        self._upsert_batch(session, PCIndustry, industries, ['industry_code'])
        session.commit()

        # Load products
        products = list(self.parse_products())
        log.info(f"  Upserting {len(products)} products...")
        self._upsert_batch(session, PCProduct, products, ['industry_code', 'product_code'])
        session.commit()

        # Load series
        series = list(self.parse_series())
        log.info(f"  Upserting {len(series)} series...")
        self._upsert_batch(session, PCSeries, series, ['series_id'])
        session.commit()

        log.info("Reference tables loaded successfully!")

    def load_data(
        self,
        session: Session,
        data_files: List[str] = None,
        batch_size: int = 10000
    ):
        """
        Load time series data from specified files

        Args:
            session: SQLAlchemy session
            data_files: List of data filenames to load. If None, loads pc.data.0.Current
            batch_size: Number of records to batch before committing
        """
        if data_files is None:
            data_files = ['pc.data.0.Current']  # Default to main file

        log.info(f"Loading PC data from {len(data_files)} file(s)...")

        for data_file in data_files:
            log.info(f"  Processing {data_file}...")

            batch = []
            total_rows = 0

            for row in self.parse_data_file(data_file):
                batch.append(row)

                if len(batch) >= batch_size:
                    self._upsert_batch(
                        session,
                        PCData,
                        batch,
                        ['series_id', 'year', 'period']
                    )
                    session.commit()
                    total_rows += len(batch)
                    log.info(f"    Loaded {total_rows:,} rows...")
                    batch = []

            # Load remaining rows
            if batch:
                self._upsert_batch(
                    session,
                    PCData,
                    batch,
                    ['series_id', 'year', 'period']
                )
                session.commit()
                total_rows += len(batch)

            log.info(f"  ✓ Loaded {total_rows:,} observations from {data_file}")

        log.info("All data files loaded successfully!")


# ==================== HELPER FUNCTIONS ====================

def get_all_data_files(data_dir: str = "data/bls/pc") -> List[str]:
    """
    Get list of all PC data files in the directory

    Returns:
        List of data file names (e.g., ['pc.data.0.Current', 'pc.data.14.Chemicals', ...])
    """
    data_path = Path(data_dir)
    data_files = sorted([
        f.name for f in data_path.glob("pc.data.*")
        if f.is_file()
    ])
    return data_files
