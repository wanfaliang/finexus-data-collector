# oe_flat_file_parser.py
"""
Parser for BLS Occupational Employment and Wage Statistics (OE/OEWS) flat files downloaded from:
https://download.bls.gov/pub/time.series/oe/

Parses tab-delimited files and loads into PostgreSQL database.
NOTE: OE has very large files (1.2GB series file, 12M+ data rows) - optimized for batch processing.
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
    OEAreaType, OEDataType, OEIndustry, OEOccupation,
    OESector, OEArea, OESeries, OEData
)

log = logging.getLogger("OEFlatFileParser")
logging.basicConfig(level=logging.INFO)


class OEFlatFileParser:
    """Parser for OE (Occupational Employment and Wage Statistics) survey flat files"""

    def __init__(self, data_dir: str = "data/bls/oe"):
        self.data_dir = Path(data_dir)
        if not self.data_dir.exists():
            raise FileNotFoundError(f"Data directory not found: {self.data_dir}")

    # ==================== REFERENCE TABLE PARSERS ====================

    def parse_areatypes(self) -> Iterator[Dict]:
        """Parse oe.areatype file"""
        file_path = self.data_dir / "oe.areatype"
        log.info(f"Parsing area types from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'areatype_code': row['areatype_code'].strip(),
                    'areatype_name': row['areatype_name'].strip(),
                }

    def parse_datatypes(self) -> Iterator[Dict]:
        """Parse oe.datatype file"""
        file_path = self.data_dir / "oe.datatype"
        log.info(f"Parsing data types from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'datatype_code': row['datatype_code'].strip(),
                    'datatype_name': row['datatype_name'].strip(),
                }

    def parse_industries(self) -> Iterator[Dict]:
        """Parse oe.industry file"""
        file_path = self.data_dir / "oe.industry"
        log.info(f"Parsing industries from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'industry_code': row['industry_code'].strip(),
                    'industry_name': row['industry_name'].strip(),
                }

    def parse_occupations(self) -> Iterator[Dict]:
        """Parse oe.occupation file"""
        file_path = self.data_dir / "oe.occupation"
        log.info(f"Parsing occupations from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'occupation_code': row['occupation_code'].strip(),
                    'occupation_name': row['occupation_name'].strip(),
                    'occupation_description': row.get('occupation_description', '').strip() or None,
                    'display_level': int(row['display_level']) if row.get('display_level', '').strip() else None,
                    'selectable': row.get('selectable', '').strip() or None,
                    'sort_sequence': int(row['sort_sequence']) if row.get('sort_sequence', '').strip() else None,
                }

    def parse_sectors(self) -> Iterator[Dict]:
        """Parse oe.sector file"""
        file_path = self.data_dir / "oe.sector"
        log.info(f"Parsing sectors from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'sector_code': row['sector_code'].strip(),
                    'sector_name': row['sector_name'].strip(),
                }

    def parse_areas(self) -> Iterator[Dict]:
        """Parse oe.area file"""
        file_path = self.data_dir / "oe.area"
        log.info(f"Parsing areas from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'area_code': row['area_code'].strip(),
                    'area_name': row['area_name'].strip(),
                }

    def parse_series(self, batch_size: int = 50000) -> Iterator[List[Dict]]:
        """Parse oe.series file in batches (file is 1.2GB - very large!)"""
        file_path = self.data_dir / "oe.series"
        log.info(f"Parsing series from {file_path} (large file - processing in batches)")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]

            batch = []
            for row in reader:
                series_id = row['series_id'].strip()
                seasonal = row.get('seasonal', '').strip() or None
                areatype_code = row.get('areatype_code', '').strip() or None
                industry_code = row.get('industry_code', '').strip() or None
                occupation_code = row.get('occupation_code', '').strip() or None
                datatype_code = row.get('datatype_code', '').strip() or None
                state_code = row.get('state_code', '').strip() or None
                area_code = row.get('area_code', '').strip() or None
                sector_code = row.get('sector_code', '').strip() or None
                series_title = row.get('series_title', '').strip() or None
                footnote_codes = row.get('footnote_codes', '').strip() or None

                # Parse begin/end dates
                begin_year = row.get('begin_year', '').strip()
                begin_year = int(begin_year) if begin_year else None

                begin_period = row.get('begin_period', '').strip() or None

                end_year = row.get('end_year', '').strip()
                end_year = int(end_year) if end_year else None

                end_period = row.get('end_period', '').strip() or None

                # Determine if series is active
                current_year = datetime.now(UTC).year
                is_active = (end_year is None) or (end_year >= current_year - 1)

                batch.append({
                    'series_id': series_id,
                    'seasonal': seasonal,
                    'areatype_code': areatype_code,
                    'industry_code': industry_code,
                    'occupation_code': occupation_code,
                    'datatype_code': datatype_code,
                    'state_code': state_code,
                    'area_code': area_code,
                    'sector_code': sector_code,
                    'series_title': series_title,
                    'footnote_codes': footnote_codes,
                    'begin_year': begin_year,
                    'begin_period': begin_period,
                    'end_year': end_year,
                    'end_period': end_period,
                    'is_active': is_active,
                })

                if len(batch) >= batch_size:
                    yield batch
                    batch = []

            if batch:
                yield batch

    # ==================== DATA PARSERS ====================

    def parse_data_file(self, filename: str) -> Iterator[Dict]:
        """Parse an OE data file"""
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
                # Handle '-' and other non-numeric values as missing
                try:
                    value = float(value_str) if value_str and value_str != '-' else None
                except ValueError:
                    value = None
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
        """Upsert a batch of records using PostgreSQL's ON CONFLICT DO UPDATE"""
        if not data:
            return

        stmt = insert(model).values(data)
        update_dict = {
            col.name: stmt.excluded[col.name]
            for col in model.__table__.columns
            if col.name not in key_columns and col.name != 'created_at'
        }

        if hasattr(model, 'updated_at'):
            update_dict['updated_at'] = datetime.now(UTC)

        stmt = stmt.on_conflict_do_update(
            index_elements=key_columns,
            set_=update_dict
        )

        session.execute(stmt)

    def load_reference_tables(self, session: Session):
        """Load all reference tables"""
        log.info("Loading OE reference tables...")

        areatypes = list(self.parse_areatypes())
        log.info(f"  Upserting {len(areatypes)} area types...")
        self._upsert_batch(session, OEAreaType, areatypes, ['areatype_code'])
        session.commit()

        datatypes = list(self.parse_datatypes())
        log.info(f"  Upserting {len(datatypes)} data types...")
        self._upsert_batch(session, OEDataType, datatypes, ['datatype_code'])
        session.commit()

        industries = list(self.parse_industries())
        log.info(f"  Upserting {len(industries)} industries...")
        self._upsert_batch(session, OEIndustry, industries, ['industry_code'])
        session.commit()

        occupations = list(self.parse_occupations())
        log.info(f"  Upserting {len(occupations)} occupations...")
        self._upsert_batch(session, OEOccupation, occupations, ['occupation_code'])
        session.commit()

        sectors = list(self.parse_sectors())
        log.info(f"  Upserting {len(sectors)} sectors...")
        self._upsert_batch(session, OESector, sectors, ['sector_code'])
        session.commit()

        areas = list(self.parse_areas())
        log.info(f"  Upserting {len(areas)} areas...")
        self._upsert_batch(session, OEArea, areas, ['area_code'])
        session.commit()

        # Series file is VERY large (1.2GB) - process in batches
        log.info("  Processing series file (1.2GB - this may take several minutes)...")
        total_series = 0
        for batch_num, series_batch in enumerate(self.parse_series(batch_size=50000), 1):
            self._upsert_batch(session, OESeries, series_batch, ['series_id'])
            session.commit()
            total_series += len(series_batch)
            log.info(f"    Batch {batch_num}: Loaded {total_series:,} series...")

        log.info(f"  ✓ Loaded {total_series:,} total series")
        log.info("Reference tables loaded successfully!")

    def load_data(self, session: Session, data_files: List[str] = None, batch_size: int = 10000):
        """Load time series data from specified files"""
        if data_files is None:
            data_files = ['oe.data.1.AllData']  # Default to all historical data

        log.info(f"Loading OE data from {len(data_files)} file(s)...")

        for data_file in data_files:
            log.info(f"  Processing {data_file}...")

            batch = []
            total_rows = 0

            for row in self.parse_data_file(data_file):
                batch.append(row)

                if len(batch) >= batch_size:
                    self._upsert_batch(session, OEData, batch, ['series_id', 'year', 'period'])
                    session.commit()
                    total_rows += len(batch)
                    log.info(f"    Loaded {total_rows:,} rows...")
                    batch = []

            if batch:
                self._upsert_batch(session, OEData, batch, ['series_id', 'year', 'period'])
                session.commit()
                total_rows += len(batch)

            log.info(f"  ✓ Loaded {total_rows:,} observations from {data_file}")

        log.info("All data files loaded successfully!")


def get_all_data_files(data_dir: str = "data/bls/oe") -> List[str]:
    """Get list of all OE data files"""
    data_path = Path(data_dir)
    data_files = sorted([f.name for f in data_path.glob("oe.data.*") if f.is_file()])
    return data_files
