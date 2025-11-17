# bd_flat_file_parser.py
"""
Parser for BLS Business Employment Dynamics (BD) flat files downloaded from:
https://download.bls.gov/pub/time.series/bd/

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
    BLSPeriod, BLSPeriodicity, BDState, BDIndustry, BDDataClass,
    BDDataElement, BDSizeClass, BDRateLevel, BDUnitAnalysis, BDOwnership,
    BDSeries, BDData
)

log = logging.getLogger("BDFlatFileParser")
logging.basicConfig(level=logging.INFO)


class BDFlatFileParser:
    """Parser for BD (Business Employment Dynamics) survey flat files"""

    def __init__(self, data_dir: str = "data/bls/bd"):
        self.data_dir = Path(data_dir)
        if not self.data_dir.exists():
            raise FileNotFoundError(f"Data directory not found: {self.data_dir}")

    # ==================== REFERENCE TABLE PARSERS ====================

    def parse_states(self) -> Iterator[Dict]:
        """Parse bd.state file"""
        file_path = self.data_dir / "bd.state"
        log.info(f"Parsing states from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'state_code': row['state_code'].strip(),
                    'state_name': row['state_name'].strip(),
                }

    def parse_industries(self) -> Iterator[Dict]:
        """Parse bd.industry file"""
        file_path = self.data_dir / "bd.industry"
        log.info(f"Parsing industries from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'industry_code': row['industry_code'].strip(),
                    'industry_name': row['industry_name'].strip(),
                    'display_level': int(row['display_level']) if row.get('display_level') else None,
                    'selectable': row.get('selectable', '').strip() or None,
                    'sort_sequence': int(row['sort_sequence']) if row.get('sort_sequence') else None,
                }

    def parse_dataclasses(self) -> Iterator[Dict]:
        """Parse bd.dataclass file"""
        file_path = self.data_dir / "bd.dataclass"
        log.info(f"Parsing data classes from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'dataclass_code': row['dataclass_code'].strip(),
                    'dataclass_name': row['dataclass_name'].strip(),
                    'display_level': int(row['display_level']) if row.get('display_level') else None,
                    'selectable': row.get('selectable', '').strip() or None,
                    'sort_sequence': int(row['sort_sequence']) if row.get('sort_sequence') else None,
                }

    def parse_dataelements(self) -> Iterator[Dict]:
        """Parse bd.dataelement file"""
        file_path = self.data_dir / "bd.dataelement"
        log.info(f"Parsing data elements from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'dataelement_code': row['dataelement_code'].strip(),
                    'dataelement_name': row['dataelement_name'].strip(),
                }

    def parse_sizeclasses(self) -> Iterator[Dict]:
        """Parse bd.sizeclass file"""
        file_path = self.data_dir / "bd.sizeclass"
        log.info(f"Parsing size classes from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'sizeclass_code': row['sizeclass_code'].strip(),
                    'sizeclass_name': row['sizeclass_name'].strip(),
                }

    def parse_ratelevels(self) -> Iterator[Dict]:
        """Parse bd.ratelevel file"""
        file_path = self.data_dir / "bd.ratelevel"
        log.info(f"Parsing rate levels from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'ratelevel_code': row['ratelevel_code'].strip(),
                    'ratelevel_name': row['ratelevel_name'].strip(),
                }

    def parse_unitanalysis(self) -> Iterator[Dict]:
        """Parse bd.unitanalysis file"""
        file_path = self.data_dir / "bd.unitanalysis"
        log.info(f"Parsing unit analysis from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'unitanalysis_code': row['unitanalysis_code'].strip(),
                    'unitanalysis_name': row['unitanalysis_name'].strip(),
                }

    def parse_ownership(self) -> Iterator[Dict]:
        """Parse bd.ownership file"""
        file_path = self.data_dir / "bd.ownership"
        log.info(f"Parsing ownership from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'ownership_code': row['ownership_code'].strip(),
                    'ownership_name': row['ownership_name'].strip(),
                }

    def parse_periodicity(self) -> Iterator[Dict]:
        """Parse bd.periodicity file"""
        file_path = self.data_dir / "bd.periodicity"
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

    def parse_series(self) -> Iterator[Dict]:
        """Parse bd.series file"""
        file_path = self.data_dir / "bd.series"
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
                    'msa_code': row.get('msa_code', '').strip() or None,
                    'state_code': row.get('state_code', '').strip() or None,
                    'county_code': row.get('county_code', '').strip() or None,
                    'industry_code': row.get('industry_code', '').strip() or None,
                    'unitanalysis_code': row.get('unitanalysis_code', '').strip() or None,
                    'dataelement_code': row.get('dataelement_code', '').strip() or None,
                    'sizeclass_code': row.get('sizeclass_code', '').strip() or None,
                    'dataclass_code': row.get('dataclass_code', '').strip() or None,
                    'ratelevel_code': row.get('ratelevel_code', '').strip() or None,
                    'periodicity_code': row.get('periodicity_code', '').strip() or None,
                    'ownership_code': row.get('ownership_code', '').strip() or None,
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
        Parse a BD data file

        Args:
            filename: Name of the data file (e.g., 'bd.data.0.Current')
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
        """Load all reference tables (states, industries, etc., series)"""
        log.info("Loading BD reference tables...")

        # Load states
        states = list(self.parse_states())
        self._upsert_batch(session, BDState, states, 'state_code')
        log.info(f"Loaded {len(states)} states")

        # Load industries
        industries = list(self.parse_industries())
        self._upsert_batch(session, BDIndustry, industries, 'industry_code')
        log.info(f"Loaded {len(industries)} industries")

        # Load data classes
        dataclasses = list(self.parse_dataclasses())
        self._upsert_batch(session, BDDataClass, dataclasses, 'dataclass_code')
        log.info(f"Loaded {len(dataclasses)} data classes")

        # Load data elements
        dataelements = list(self.parse_dataelements())
        self._upsert_batch(session, BDDataElement, dataelements, 'dataelement_code')
        log.info(f"Loaded {len(dataelements)} data elements")

        # Load size classes
        sizeclasses = list(self.parse_sizeclasses())
        self._upsert_batch(session, BDSizeClass, sizeclasses, 'sizeclass_code')
        log.info(f"Loaded {len(sizeclasses)} size classes")

        # Load rate levels
        ratelevels = list(self.parse_ratelevels())
        self._upsert_batch(session, BDRateLevel, ratelevels, 'ratelevel_code')
        log.info(f"Loaded {len(ratelevels)} rate levels")

        # Load unit analysis
        unitanalysis = list(self.parse_unitanalysis())
        self._upsert_batch(session, BDUnitAnalysis, unitanalysis, 'unitanalysis_code')
        log.info(f"Loaded {len(unitanalysis)} unit analysis codes")

        # Load ownership
        ownership = list(self.parse_ownership())
        self._upsert_batch(session, BDOwnership, ownership, 'ownership_code')
        log.info(f"Loaded {len(ownership)} ownership codes")

        # Load periodicity (shared table)
        periodicity = list(self.parse_periodicity())
        self._upsert_batch(session, BLSPeriodicity, periodicity, 'periodicity_code')
        log.info(f"Loaded {len(periodicity)} periodicity codes")

        # Load series
        series = list(self.parse_series())
        self._upsert_batch(session, BDSeries, series, 'series_id')
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
            data_files = ['bd.data.0.Current']

        log.info(f"Loading BD data from {len(data_files)} file(s)...")

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
                    self._upsert_batch(session, BDData, batch, ['series_id', 'year', 'period'])
                    total_loaded += len(batch)
                    log.info(f"Loaded {total_loaded:,} observations...")
                    batch = []
                    seen_keys.clear()  # Clear seen keys after batch insert

        # Load remaining batch
        if batch:
            self._upsert_batch(session, BDData, batch, ['series_id', 'year', 'period'])
            total_loaded += len(batch)

        log.info(f"Total loaded: {total_loaded:,} observations")

    def load_all(self, session: Session, data_files: List[str] = None):
        """Load everything - reference tables and data"""
        self.load_reference_tables(session)
        self.load_data(session, data_files=data_files)


if __name__ == "__main__":
    # Test parsing
    parser = BDFlatFileParser()

    print("=== States ===")
    for state in parser.parse_states():
        print(state)

    print("\n=== Data Classes (first 5) ===")
    for i, dc in enumerate(parser.parse_dataclasses()):
        if i >= 5:
            break
        print(dc)

    print("\n=== Series (first 5) ===")
    for i, series in enumerate(parser.parse_series()):
        if i >= 5:
            break
        print(series)

    print("\n=== Data (first 5) ===")
    for i, data in enumerate(parser.parse_data_file('bd.data.0.Current')):
        if i >= 5:
            break
        print(data)
