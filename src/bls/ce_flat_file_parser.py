# ce_flat_file_parser.py
"""
Parser for BLS Current Employment Statistics (CE) flat files downloaded from:
https://download.bls.gov/pub/time.series/ce/

Parses tab-delimited files and loads into PostgreSQL database.
"""
import csv
import logging
from pathlib import Path
from typing import List, Dict, Iterator
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert

# Import BLS models
import sys
sys.path.append(str(Path(__file__).parent.parent))
from database.bls_models import (
    CEIndustry, CEDataType, CESupersector, CESeries, CEData
)

log = logging.getLogger("CEFlatFileParser")
logging.basicConfig(level=logging.INFO)


class CEFlatFileParser:
    """Parser for CE (Current Employment Statistics) survey flat files"""

    def __init__(self, data_dir: str = "data/bls/ce"):
        self.data_dir = Path(data_dir)
        if not self.data_dir.exists():
            raise FileNotFoundError(f"Data directory not found: {self.data_dir}")

    # ==================== REFERENCE TABLE PARSERS ====================

    def parse_industries(self) -> Iterator[Dict]:
        """Parse ce.industry file"""
        file_path = self.data_dir / "ce.industry"
        log.info(f"Parsing industries from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'industry_code': row['industry_code'].strip(),
                    'naics_code': row.get('naics_code', '').strip() or None,
                    'industry_name': row['industry_name'].strip(),
                    'publishing_status': row.get('publishing_status', '').strip() or None,
                    'display_level': int(row['display_level']) if row.get('display_level', '').strip() else None,
                    'selectable': row.get('selectable', '').strip() or None,
                    'sort_sequence': int(row['sort_sequence']) if row.get('sort_sequence', '').strip() else None,
                }

    def parse_data_types(self) -> Iterator[Dict]:
        """Parse ce.datatype file"""
        file_path = self.data_dir / "ce.datatype"
        log.info(f"Parsing data types from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'data_type_code': row['data_type_code'].strip(),
                    'data_type_text': row['data_type_text'].strip(),
                }

    def parse_supersectors(self) -> Iterator[Dict]:
        """Parse ce.supersector file"""
        file_path = self.data_dir / "ce.supersector"
        log.info(f"Parsing supersectors from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'supersector_code': row['supersector_code'].strip(),
                    'supersector_name': row['supersector_name'].strip(),
                }

    def parse_series(self) -> Iterator[Dict]:
        """Parse ce.series file"""
        file_path = self.data_dir / "ce.series"
        log.info(f"Parsing series from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                series_id = row['series_id'].strip()
                supersector_code = row.get('supersector_code', '').strip() or None
                industry_code = row['industry_code'].strip()
                data_type_code = row['data_type_code'].strip()
                seasonal = row.get('seasonal', '').strip() or None
                series_title = row['series_title'].strip()
                footnote_codes = row.get('footnote_codes', '').strip() or None

                # Parse begin/end dates
                begin_year = int(row['begin_year']) if row.get('begin_year', '').strip() else None
                begin_period = row.get('begin_period', '').strip() or None
                end_year = int(row['end_year']) if row.get('end_year', '').strip() else None
                end_period = row.get('end_period', '').strip() or None

                # Determine if series is active (has recent data)
                is_active = end_year and end_year >= 2024

                yield {
                    'series_id': series_id,
                    'supersector_code': supersector_code,
                    'industry_code': industry_code,
                    'data_type_code': data_type_code,
                    'seasonal_code': seasonal,
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
        Parse a CE data file

        Args:
            filename: Name of the data file to parse

        Yields:
            Dict with series_id, year, period, value, footnote_codes
        """
        file_path = self.data_dir / filename
        if not file_path.exists():
            log.warning(f"Data file not found: {file_path}")
            return

        log.info(f"Parsing data from {file_path}")

        count = 0
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                series_id = row['series_id'].strip()
                year = int(row['year'].strip())
                period = row['period'].strip()
                value_str = row['value'].strip()
                footnote_codes = row.get('footnote_codes', '').strip() or None

                # Parse value (can be empty/missing)
                value = None
                if value_str and value_str != '-':
                    try:
                        value = float(value_str)
                    except ValueError:
                        log.warning(f"Invalid value for {series_id} {year}-{period}: {value_str}")

                yield {
                    'series_id': series_id,
                    'year': year,
                    'period': period,
                    'value': value,
                    'footnote_codes': footnote_codes,
                }

                count += 1
                if count % 100000 == 0:
                    log.info(f"  Parsed {count:,} rows from {filename}...")

        log.info(f"Completed parsing {file_path}: {count:,} rows")

    # ==================== DATABASE LOADING ====================

    def load_reference_tables(self, session: Session):
        """Load all reference tables"""
        log.info("=" * 80)
        log.info("LOADING CE REFERENCE TABLES")
        log.info("=" * 80)

        # 1. Load industries
        log.info("Loading industries...")
        industries = list(self.parse_industries())
        self._upsert_batch(session, CEIndustry, industries, 'industry_code')
        log.info(f"  Loaded {len(industries)} industries")

        # 2. Load data types
        log.info("Loading data types...")
        data_types = list(self.parse_data_types())
        self._upsert_batch(session, CEDataType, data_types, 'data_type_code')
        log.info(f"  Loaded {len(data_types)} data types")

        # 3. Load supersectors
        log.info("Loading supersectors...")
        supersectors = list(self.parse_supersectors())
        self._upsert_batch(session, CESupersector, supersectors, 'supersector_code')
        log.info(f"  Loaded {len(supersectors)} supersectors")

        # 4. Load series
        log.info("Loading series...")
        series = list(self.parse_series())
        self._upsert_batch(session, CESeries, series, 'series_id')
        log.info(f"  Loaded {len(series)} series")

        session.commit()
        log.info("Reference tables loaded successfully!")

    def load_data(self, session: Session, data_files: List[str] = None, batch_size: int = 10000):
        """
        Load time series data

        Args:
            session: SQLAlchemy session
            data_files: List of data files to load (default: ce.data.0.AllCESSeries)
            batch_size: Batch size for database inserts
        """
        log.info("=" * 80)
        log.info("LOADING CE TIME SERIES DATA")
        log.info("=" * 80)

        # Default to all series file
        if data_files is None:
            data_files = ['ce.data.0.AllCESSeries']

        batch = []
        total_loaded = 0

        for filename in data_files:
            for row in self.parse_data_file(filename):
                batch.append(row)

                if len(batch) >= batch_size:
                    self._upsert_batch(session, CEData, batch, ['series_id', 'year', 'period'])
                    total_loaded += len(batch)
                    log.info(f"  Loaded {total_loaded:,} data points...")
                    batch = []

        # Load remaining batch
        if batch:
            self._upsert_batch(session, CEData, batch, ['series_id', 'year', 'period'])
            total_loaded += len(batch)

        session.commit()
        log.info(f"Time series data loaded successfully! Total: {total_loaded:,} observations")

    def _upsert_batch(self, session: Session, model_class, data: List[Dict], conflict_keys):
        """Upsert a batch of records using PostgreSQL's ON CONFLICT DO UPDATE"""
        if not data:
            return

        # Normalize conflict_keys to list
        if isinstance(conflict_keys, str):
            conflict_keys = [conflict_keys]

        # Build insert statement with ON CONFLICT DO UPDATE
        stmt = insert(model_class).values(data)

        # Update all columns except the conflict keys on conflict
        update_dict = {
            c.name: c
            for c in stmt.excluded
            if c.name not in conflict_keys and c.name != 'created_at'
        }

        stmt = stmt.on_conflict_do_update(
            index_elements=conflict_keys,
            set_=update_dict
        )

        session.execute(stmt)


if __name__ == "__main__":
    # Example usage
    import os
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    # Get database URL from environment
    DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://user:password@localhost:5432/finexus_db')

    # Create engine and session
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Create parser
        parser = CEFlatFileParser(data_dir="data/bls/ce")

        # Load reference tables
        parser.load_reference_tables(session)

        # Load all data from main file
        parser.load_data(session, data_files=['ce.data.0.AllCESSeries'], batch_size=10000)

        print("\n" + "=" * 80)
        print("SUCCESS! CE data loaded to database")
        print("=" * 80)

    except Exception as e:
        log.error(f"Error loading data: {e}")
        session.rollback()
        raise
    finally:
        session.close()
