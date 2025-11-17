# cw_flat_file_parser.py
"""
Parser for BLS Consumer Price Index - Urban Wage Earners and Clerical Workers (CW) flat files downloaded from:
https://download.bls.gov/pub/time.series/cw/

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
    BLSPeriod, BLSPeriodicity, CWArea, CWItem, CWSeries, CWData, CWAspect
)

log = logging.getLogger("CWFlatFileParser")
logging.basicConfig(level=logging.INFO)


class CWFlatFileParser:
    """Parser for CW (Consumer Price Index - Urban Wage Earners and Clerical Workers) survey flat files"""

    def __init__(self, data_dir: str = "data/bls/cw"):
        self.data_dir = Path(data_dir)
        if not self.data_dir.exists():
            raise FileNotFoundError(f"Data directory not found: {self.data_dir}")

    # ==================== REFERENCE TABLE PARSERS ====================

    def parse_areas(self) -> Iterator[Dict]:
        """Parse cw.area file"""
        file_path = self.data_dir / "cw.area"
        log.info(f"Parsing areas from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'area_code': row['area_code'].strip(),
                    'area_name': row['area_name'].strip(),
                    'display_level': int(row['display_level']) if row.get('display_level') else None,
                    'selectable': row.get('selectable', '').strip() or None,
                    'sort_sequence': int(row['sort_sequence']) if row.get('sort_sequence') else None,
                }

    def parse_periods(self) -> Iterator[Dict]:
        """Parse cw.period file"""
        file_path = self.data_dir / "cw.period"
        log.info(f"Parsing periods from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for i, row in enumerate(reader, 1):
                period_code = row['period'].strip()
                period_abbr = row['period_abbr'].strip()
                period_name = row['period_name'].strip()

                # Determine period type
                if period_code.startswith('M'):
                    period_type = 'MONTHLY'
                elif period_code.startswith('Q'):
                    period_type = 'QUARTERLY'
                elif period_code.startswith('A'):
                    period_type = 'ANNUAL'
                else:
                    period_type = 'OTHER'

                yield {
                    'period_code': period_code,
                    'period_abbr': period_abbr,
                    'period_name': period_name,
                    'period_type': period_type,
                    'sort_order': i,
                }

    def parse_periodicity(self) -> Iterator[Dict]:
        """Parse cw.periodicity file"""
        file_path = self.data_dir / "cw.periodicity"
        log.info(f"Parsing periodicity from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'periodicity_code': row['periodicity_code'].strip(),
                    'periodicity_name': row['periodicity_name'].strip(),
                    'description': row.get('description', '').strip() or None,
                }

    def parse_items(self) -> Iterator[Dict]:
        """Parse cw.item file"""
        file_path = self.data_dir / "cw.item"
        log.info(f"Parsing items from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                item_code = row['item_code'].strip()
                item_name = row['item_name'].strip()
                display_level = int(row['display_level']) if row.get('display_level') else None
                selectable = row.get('selectable', '').strip() or None
                sort_sequence = int(row['sort_sequence']) if row.get('sort_sequence') else None

                yield {
                    'item_code': item_code,
                    'item_name': item_name,
                    'display_level': display_level,
                    'selectable': selectable,
                    'sort_sequence': sort_sequence,
                }

    def parse_series(self) -> Iterator[Dict]:
        """Parse cw.series file"""
        file_path = self.data_dir / "cw.series"
        log.info(f"Parsing series from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                series_id = row['series_id'].strip()
                area_code = row['area_code'].strip()
                item_code = row['item_code'].strip()
                seasonal = row.get('seasonal', '').strip() or None
                periodicity_code = row.get('periodicity_code', '').strip() or None
                base_code = row.get('base_code', '').strip() or None
                base_period = row.get('base_period', '').strip() or None
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
                    'area_code': area_code,
                    'item_code': item_code,
                    'seasonal_code': seasonal,
                    'periodicity_code': periodicity_code,
                    'base_code': base_code,
                    'base_period': base_period,
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
        Parse a CW data file

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

    def parse_aspects(self) -> Iterator[Dict]:
        """Parse cw.aspect file"""
        file_path = self.data_dir / "cw.aspect"
        if not file_path.exists():
            log.warning(f"Aspect file not found: {file_path}")
            return

        log.info(f"Parsing aspects from {file_path}")

        count = 0
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                series_id = row['series_id'].strip()
                year = int(row['year'].strip())
                period = row['period'].strip()
                aspect_type = row['aspect_type'].strip()
                value = row.get('value', '').strip() or None
                footnote_codes = row.get('footnote_codes', '').strip() or None

                yield {
                    'series_id': series_id,
                    'year': year,
                    'period': period,
                    'aspect_type': aspect_type,
                    'value': value,
                    'footnote_codes': footnote_codes,
                }

                count += 1
                if count % 100000 == 0:
                    log.info(f"  Parsed {count:,} aspect rows...")

        log.info(f"Completed parsing aspects: {count:,} rows")

    # ==================== DATABASE LOADING ====================

    def load_reference_tables(self, session: Session):
        """Load all reference tables"""
        log.info("=" * 80)
        log.info("LOADING CW REFERENCE TABLES")
        log.info("=" * 80)

        # 1. Load areas (CW-specific table - must load first for foreign keys)
        log.info("Loading areas...")
        areas = list(self.parse_areas())
        self._upsert_batch(session, CWArea, areas, 'area_code')
        log.info(f"  Loaded {len(areas)} areas")

        # 2. Load periods (shared BLS table)
        log.info("Loading periods...")
        periods = list(self.parse_periods())
        self._upsert_batch(session, BLSPeriod, periods, 'period_code')
        log.info(f"  Loaded {len(periods)} periods")

        # 3. Load periodicity (shared BLS table)
        log.info("Loading periodicity...")
        periodicity = list(self.parse_periodicity())
        self._upsert_batch(session, BLSPeriodicity, periodicity, 'periodicity_code')
        log.info(f"  Loaded {len(periodicity)} periodicity codes")

        # 4. Load items (CW-specific)
        log.info("Loading items...")
        items = list(self.parse_items())
        self._upsert_batch(session, CWItem, items, 'item_code')
        log.info(f"  Loaded {len(items)} items")

        # 5. Load series (CW-specific)
        log.info("Loading series...")
        series = list(self.parse_series())
        self._upsert_batch(session, CWSeries, series, 'series_id')
        log.info(f"  Loaded {len(series)} series")

        session.commit()
        log.info("Reference tables loaded successfully!")

    def load_data(self, session: Session, data_files: List[str] = None, batch_size: int = 10000):
        """
        Load time series data

        Args:
            session: SQLAlchemy session
            data_files: List of data files to load (default: just cw.data.0.Current)
            batch_size: Batch size for database inserts
        """
        log.info("=" * 80)
        log.info("LOADING CW TIME SERIES DATA")
        log.info("=" * 80)

        # Default to just Current file for initial load
        if data_files is None:
            data_files = ['cw.data.0.Current']

        batch = []
        total_loaded = 0

        for filename in data_files:
            for row in self.parse_data_file(filename):
                batch.append(row)

                if len(batch) >= batch_size:
                    self._upsert_batch(session, CWData, batch, ['series_id', 'year', 'period'])
                    total_loaded += len(batch)
                    log.info(f"  Loaded {total_loaded:,} data points...")
                    batch = []

        # Load remaining batch
        if batch:
            self._upsert_batch(session, CWData, batch, ['series_id', 'year', 'period'])
            total_loaded += len(batch)

        session.commit()
        log.info(f"Time series data loaded successfully! Total: {total_loaded:,} observations")

    def load_aspects(self, session: Session, batch_size: int = 10000):
        """Load aspect data"""
        log.info("=" * 80)
        log.info("LOADING CW ASPECT DATA")
        log.info("=" * 80)

        batch = []
        total_loaded = 0

        for row in self.parse_aspects():
            batch.append(row)

            if len(batch) >= batch_size:
                self._upsert_batch(session, CWAspect, batch, ['series_id', 'year', 'period', 'aspect_type'])
                total_loaded += len(batch)
                log.info(f"  Loaded {total_loaded:,} aspect records...")
                batch = []

        # Load remaining batch
        if batch:
            self._upsert_batch(session, CWAspect, batch, ['series_id', 'year', 'period', 'aspect_type'])
            total_loaded += len(batch)

        session.commit()
        log.info(f"Aspect data loaded successfully! Total: {total_loaded:,} records")

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
        parser = CWFlatFileParser(data_dir="data/bls/cw")

        # Load reference tables
        parser.load_reference_tables(session)

        # Load time series data (just Current file for now)
        parser.load_data(session, data_files=['cw.data.0.Current'], batch_size=10000)

        # Optionally load aspects
        # parser.load_aspects(session, batch_size=10000)

        print("\n" + "=" * 80)
        print("SUCCESS! CW data loaded to database")
        print("=" * 80)

    except Exception as e:
        log.error(f"Error loading data: {e}")
        session.rollback()
        raise
    finally:
        session.close()
