# ap_flat_file_parser.py
"""
Parser for BLS Average Price (AP) flat files downloaded from:
https://download.bls.gov/pub/time.series/ap/

Parses tab-delimited files and loads into PostgreSQL database.
"""
import csv
import logging
from pathlib import Path
from typing import List, Dict, Iterator, Optional
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert

# Import BLS models
import sys
sys.path.append(str(Path(__file__).parent.parent))
from database.bls_models import (
    BLSArea, BLSPeriod, APItem, APSeries, APData, BLSSurvey
)

log = logging.getLogger("APFlatFileParser")
logging.basicConfig(level=logging.INFO)


class APFlatFileParser:
    """Parser for AP survey flat files"""

    def __init__(self, data_dir: str = "data/bls/ap"):
        self.data_dir = Path(data_dir)
        if not self.data_dir.exists():
            raise FileNotFoundError(f"Data directory not found: {self.data_dir}")

    # ==================== REFERENCE TABLE PARSERS ====================

    def parse_areas(self) -> Iterator[Dict]:
        """Parse ap.area file"""
        file_path = self.data_dir / "ap.area"
        log.info(f"Parsing areas from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                # Clean whitespace
                area_code = row['area_code'].strip()
                area_name = row['area_name'].strip()

                # Determine area type
                if area_code == '0000':
                    area_type = 'National'
                elif area_code.startswith('0'):
                    area_type = 'Region'
                elif area_code.startswith('A'):
                    area_type = 'City'
                else:
                    area_type = 'Other'

                yield {
                    'area_code': area_code,
                    'area_name': area_name,
                    'area_type': area_type,
                }

    def parse_items(self) -> Iterator[Dict]:
        """Parse ap.item file"""
        file_path = self.data_dir / "ap.item"
        log.info(f"Parsing items from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                item_code = row['item_code'].strip()
                item_name = row['item_name'].strip()

                # Infer category from item code
                code_prefix = item_code[:2]
                if code_prefix == '70':
                    category = 'Food'
                elif code_prefix == '72':
                    category = 'Household Fuels'
                elif code_prefix == '73':
                    category = 'Gasoline'
                else:
                    category = 'Other'

                # Extract unit if mentioned in name
                unit = None
                if 'per lb.' in item_name or 'per pound' in item_name:
                    unit = 'per lb'
                elif 'per gallon' in item_name:
                    unit = 'per gallon'
                elif 'per therm' in item_name:
                    unit = 'per therm'

                yield {
                    'item_code': item_code,
                    'item_name': item_name,
                    'category': category,
                    'unit': unit,
                }

    def parse_periods(self) -> Iterator[Dict]:
        """Parse ap.period file"""
        file_path = self.data_dir / "ap.period"
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

    def parse_series(self) -> Iterator[Dict]:
        """Parse ap.series file"""
        file_path = self.data_dir / "ap.series"
        log.info(f"Parsing series from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            # Read and strip fieldnames to handle trailing whitespace
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                series_id = row['series_id'].strip()
                area_code = row['area_code'].strip()
                item_code = row['item_code'].strip()
                series_title = row['series_title'].strip()

                # Extract seasonal code from series_id (3rd character)
                # APU... = Not seasonally adjusted
                # APS... = Seasonally adjusted
                seasonal_code = series_id[2] if len(series_id) > 2 else 'U'

                # Parse begin/end dates
                begin_year = int(row['begin_year']) if row['begin_year'].strip() else None
                begin_period = row['begin_period'].strip() or None
                end_year = int(row['end_year']) if row['end_year'].strip() else None
                end_period = row['end_period'].strip() or None

                # Determine if series is active (has recent data)
                is_active = end_year and end_year >= 2024

                yield {
                    'series_id': series_id,
                    'seasonal_code': seasonal_code,
                    'area_code': area_code,
                    'item_code': item_code,
                    'series_title': series_title,
                    'begin_year': begin_year,
                    'begin_period': begin_period,
                    'end_year': end_year,
                    'end_period': end_period,
                    'is_active': is_active,
                }

    # ==================== DATA PARSERS ====================

    def parse_data_file(self, filename: str) -> Iterator[Dict]:
        """
        Parse a data file (ap.data.0.Current, ap.data.3.Food, etc.)

        Args:
            filename: Name of the data file to parse

        Yields:
            Dict with series_id, year, period, value, footnote_codes
        """
        file_path = self.data_dir / filename
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
                if value_str:
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
                if count % 50000 == 0:
                    log.info(f"  Parsed {count:,} rows...")

        log.info(f"Completed parsing {file_path}: {count:,} rows")

    def parse_all_data_files(self) -> Iterator[Dict]:
        """Parse all AP data files and yield combined data"""
        data_files = [
            'ap.data.0.Current',
            'ap.data.1.HouseholdFuels',
            'ap.data.2.Gasoline',
            'ap.data.3.Food',
        ]

        for filename in data_files:
            file_path = self.data_dir / filename
            if file_path.exists():
                yield from self.parse_data_file(filename)
            else:
                log.warning(f"Data file not found: {file_path}")

    # ==================== DATABASE LOADING ====================

    def load_reference_tables(self, session: Session):
        """Load all reference tables (areas, items, periods, series)"""
        log.info("=" * 80)
        log.info("LOADING REFERENCE TABLES")
        log.info("=" * 80)

        # 1. Load areas
        log.info("Loading areas...")
        areas = list(self.parse_areas())
        self._upsert_batch(session, BLSArea, areas, 'area_code')
        log.info(f"  Loaded {len(areas)} areas")

        # 2. Load items
        log.info("Loading items...")
        items = list(self.parse_items())
        self._upsert_batch(session, APItem, items, 'item_code')
        log.info(f"  Loaded {len(items)} items")

        # 3. Load periods
        log.info("Loading periods...")
        periods = list(self.parse_periods())
        self._upsert_batch(session, BLSPeriod, periods, 'period_code')
        log.info(f"  Loaded {len(periods)} periods")

        # 4. Load series
        log.info("Loading series...")
        series = list(self.parse_series())
        self._upsert_batch(session, APSeries, series, 'series_id')
        log.info(f"  Loaded {len(series)} series")

        session.commit()
        log.info("Reference tables loaded successfully!")

    def load_data(self, session: Session, batch_size: int = 10000):
        """Load all time series data"""
        log.info("=" * 80)
        log.info("LOADING TIME SERIES DATA")
        log.info("=" * 80)

        batch = []
        total_loaded = 0

        for row in self.parse_all_data_files():
            batch.append(row)

            if len(batch) >= batch_size:
                self._upsert_batch(session, APData, batch, ['series_id', 'year', 'period'])
                total_loaded += len(batch)
                log.info(f"  Loaded {total_loaded:,} data points...")
                batch = []

        # Load remaining batch
        if batch:
            self._upsert_batch(session, APData, batch, ['series_id', 'year', 'period'])
            total_loaded += len(batch)

        session.commit()
        log.info(f"Time series data loaded successfully! Total: {total_loaded:,} observations")

    def _upsert_batch(self, session: Session, model_class, data: List[Dict], conflict_keys):
        """
        Upsert a batch of records using PostgreSQL's ON CONFLICT DO UPDATE

        Args:
            session: SQLAlchemy session
            model_class: Model class (e.g., APData)
            data: List of dicts to insert
            conflict_keys: Column(s) for conflict resolution (string or list)
        """
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
        parser = APFlatFileParser(data_dir="data/bls/ap")

        # Load reference tables
        parser.load_reference_tables(session)

        # Load time series data
        parser.load_data(session, batch_size=10000)

        print("\n" + "=" * 80)
        print("SUCCESS! All AP data loaded to database")
        print("=" * 80)

    except Exception as e:
        log.error(f"Error loading data: {e}")
        session.rollback()
        raise
    finally:
        session.close()
