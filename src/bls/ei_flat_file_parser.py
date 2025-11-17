# ei_flat_file_parser.py
"""
Parser for BLS Import/Export Price Indexes (EI) flat files downloaded from:
https://download.bls.gov/pub/time.series/ei/

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
    BLSPeriod, EIIndex, EISeries, EIData
)

log = logging.getLogger("EIFlatFileParser")
logging.basicConfig(level=logging.INFO)


class EIFlatFileParser:
    """Parser for EI (Import/Export Price Indexes) survey flat files"""

    def __init__(self, data_dir: str = "data/bls/ei"):
        self.data_dir = Path(data_dir)
        if not self.data_dir.exists():
            raise FileNotFoundError(f"Data directory not found: {self.data_dir}")

    # ==================== REFERENCE TABLE PARSERS ====================

    def parse_indexes(self) -> Iterator[Dict]:
        """Parse ei.index file"""
        file_path = self.data_dir / "ei.index"
        log.info(f"Parsing indexes from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'index_code': row['index_code'].strip(),
                    'index_name': row['index_name'].strip(),
                }

    def parse_series(self) -> Iterator[Dict]:
        """Parse ei.series file"""
        file_path = self.data_dir / "ei.series"
        log.info(f"Parsing series from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                series_id = row['series_id'].strip()

                # Parse series metadata
                yield {
                    'series_id': series_id,
                    'seasonal_code': row.get('seasonal', '').strip() or None,
                    'index_code': row.get('index_code', '').strip() or None,
                    'series_name': row.get('series_name', '').strip() or None,
                    'base_period': row.get('base_period', '').strip() or None,
                    'series_title': row.get('series_title', '').strip() or series_id,
                    'footnote_codes': row.get('footnote_codes', '').strip() or None,
                    'begin_year': int(row['begin_year']) if row.get('begin_year') else None,
                    'begin_period': row.get('begin_period', '').strip() or None,
                    'end_year': int(row['end_year']) if row.get('end_year') else None,
                    'end_period': row.get('end_period', '').strip() or None,
                    'is_active': True,  # Default to active
                }

    # ==================== DATA FILE PARSERS ====================

    def parse_data_file(self, filename: str) -> Iterator[Dict]:
        """
        Parse an EI data file

        Args:
            filename: Name of the data file (e.g., 'ei.data.0.Current')
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
                value = row.get('value', '').strip()

                # Convert value to float
                if value and value not in ('', '.', '-'):
                    try:
                        value = float(value)
                    except ValueError:
                        log.warning(f"Invalid value for {series_id} {year} {period}: {value}")
                        value = None
                else:
                    value = None

                yield {
                    'series_id': series_id,
                    'year': year,
                    'period': period,
                    'value': value,
                    'footnote_codes': row.get('footnote_codes', '').strip() or None,
                }

    # ==================== DATABASE LOADERS ====================

    def _upsert_batch(self, session: Session, model, batch: List[Dict], conflict_keys):
        """Helper to perform UPSERT (insert or update on conflict)"""
        if not batch:
            return

        stmt = insert(model).values(batch)

        # Determine which fields to update on conflict
        update_dict = {c.name: c for c in stmt.excluded if c.name not in (
            conflict_keys if isinstance(conflict_keys, list) else [conflict_keys]
        ) and c.name not in ('created_at',)}

        stmt = stmt.on_conflict_do_update(
            index_elements=conflict_keys if isinstance(conflict_keys, list) else [conflict_keys],
            set_=update_dict
        )

        session.execute(stmt)
        session.commit()

    def load_reference_tables(self, session: Session):
        """Load all reference tables (indexes, series)"""
        log.info("Loading EI reference tables...")

        # Load indexes
        indexes = list(self.parse_indexes())
        self._upsert_batch(session, EIIndex, indexes, 'index_code')
        log.info(f"Loaded {len(indexes)} index types")

        # Load series
        series = list(self.parse_series())
        self._upsert_batch(session, EISeries, series, 'series_id')
        log.info(f"Loaded {len(series)} series")

    def load_data(self, session: Session, data_files: List[str] = None, batch_size: int = 10000):
        """
        Load time series data from data files

        Args:
            session: SQLAlchemy session
            data_files: List of data file names to load. If None, loads only Current file.
            batch_size: Number of records to batch before committing
        """
        # Default to Current file only
        if data_files is None:
            data_files = ['ei.data.0.Current']

        log.info(f"Loading EI data from {len(data_files)} file(s)...")

        batch = []
        total_loaded = 0
        seen_keys = set()  # Track seen (series_id, year, period) to avoid duplicates

        for filename in data_files:
            for row in self.parse_data_file(filename):
                # Create unique key for deduplication
                key = (row['series_id'], row['year'], row['period'])

                # Skip if already seen in batch
                if key in seen_keys:
                    continue

                seen_keys.add(key)
                batch.append(row)

                if len(batch) >= batch_size:
                    self._upsert_batch(session, EIData, batch, ['series_id', 'year', 'period'])
                    total_loaded += len(batch)
                    log.info(f"Loaded {total_loaded:,} observations...")
                    batch = []
                    seen_keys.clear()  # Clear seen keys after batch insert

        # Load remaining batch
        if batch:
            self._upsert_batch(session, EIData, batch, ['series_id', 'year', 'period'])
            total_loaded += len(batch)

        log.info(f"Total loaded: {total_loaded:,} observations")

    def load_all(self, session: Session, data_files: List[str] = None):
        """Load everything - reference tables and data"""
        self.load_reference_tables(session)
        self.load_data(session, data_files=data_files)


if __name__ == "__main__":
    # Test parsing
    parser = EIFlatFileParser()

    print("=== Indexes ===")
    for idx in parser.parse_indexes():
        print(idx)

    print("\n=== Series (first 5) ===")
    for i, series in enumerate(parser.parse_series()):
        if i >= 5:
            break
        print(series)

    print("\n=== Data (first 5) ===")
    for i, data in enumerate(parser.parse_data_file('ei.data.0.Current')):
        if i >= 5:
            break
        print(data)
