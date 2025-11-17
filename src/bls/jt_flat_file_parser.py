# jt_flat_file_parser.py
"""
Parser for BLS Job Openings and Labor Turnover Survey (JOLTS - JT) flat files downloaded from:
https://download.bls.gov/pub/time.series/jt/

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
    JTDataElement, JTIndustry, JTState, JTArea,
    JTSizeClass, JTRateLevel, JTSeries, JTData
)

log = logging.getLogger("JTFlatFileParser")
logging.basicConfig(level=logging.INFO)


class JTFlatFileParser:
    """Parser for JT (JOLTS) survey flat files"""

    def __init__(self, data_dir: str = "data/bls/jt"):
        self.data_dir = Path(data_dir)
        if not self.data_dir.exists():
            raise FileNotFoundError(f"Data directory not found: {self.data_dir}")

    # ==================== REFERENCE TABLE PARSERS ====================

    def parse_dataelements(self) -> Iterator[Dict]:
        """Parse jt.dataelement file"""
        file_path = self.data_dir / "jt.dataelement"
        log.info(f"Parsing data elements from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'dataelement_code': row['dataelement_code'].strip(),
                    'dataelement_text': row['dataelement_text'].strip(),
                    'display_level': int(row['display_level']) if row.get('display_level', '').strip() else None,
                    'selectable': row.get('selectable', '').strip() or None,
                    'sort_sequence': int(row['sort_sequence']) if row.get('sort_sequence', '').strip() else None,
                }

    def parse_industries(self) -> Iterator[Dict]:
        """Parse jt.industry file"""
        file_path = self.data_dir / "jt.industry"
        log.info(f"Parsing industries from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'industry_code': row['industry_code'].strip(),
                    'industry_text': row['industry_text'].strip(),
                    'display_level': int(row['display_level']) if row.get('display_level', '').strip() else None,
                    'selectable': row.get('selectable', '').strip() or None,
                    'sort_sequence': int(row['sort_sequence']) if row.get('sort_sequence', '').strip() else None,
                }

    def parse_states(self) -> Iterator[Dict]:
        """Parse jt.state file"""
        file_path = self.data_dir / "jt.state"
        log.info(f"Parsing states from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'state_code': row['state_code'].strip(),
                    'state_text': row['state_text'].strip(),
                    'display_level': int(row['display_level']) if row.get('display_level', '').strip() else None,
                    'selectable': row.get('selectable', '').strip() or None,
                    'sort_sequence': int(row['sort_sequence']) if row.get('sort_sequence', '').strip() else None,
                }

    def parse_areas(self) -> Iterator[Dict]:
        """Parse jt.area file"""
        file_path = self.data_dir / "jt.area"
        log.info(f"Parsing areas from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'area_code': row['area_code'].strip(),
                    'area_text': row['area_text'].strip(),
                    'display_level': int(row['display_level']) if row.get('display_level', '').strip() else None,
                    'selectable': row.get('selectable', '').strip() or None,
                    'sort_sequence': int(row['sort_sequence']) if row.get('sort_sequence', '').strip() else None,
                }

    def parse_sizeclasses(self) -> Iterator[Dict]:
        """Parse jt.sizeclass file"""
        file_path = self.data_dir / "jt.sizeclass"
        log.info(f"Parsing size classes from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'sizeclass_code': row['sizeclass_code'].strip(),
                    'sizeclass_text': row['sizeclass_text'].strip(),
                    'display_level': int(row['display_level']) if row.get('display_level', '').strip() else None,
                    'selectable': row.get('selectable', '').strip() or None,
                    'sort_sequence': int(row['sort_sequence']) if row.get('sort_sequence', '').strip() else None,
                }

    def parse_ratelevels(self) -> Iterator[Dict]:
        """Parse jt.ratelevel file"""
        file_path = self.data_dir / "jt.ratelevel"
        log.info(f"Parsing rate/level codes from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'ratelevel_code': row['ratelevel_code'].strip(),
                    'ratelevel_text': row['ratelevel_text'].strip(),
                    'display_level': int(row['display_level']) if row.get('display_level', '').strip() else None,
                    'selectable': row.get('selectable', '').strip() or None,
                    'sort_sequence': int(row['sort_sequence']) if row.get('sort_sequence', '').strip() else None,
                }

    def parse_series(self) -> Iterator[Dict]:
        """Parse jt.series file"""
        file_path = self.data_dir / "jt.series"
        log.info(f"Parsing series from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                series_id = row['series_id'].strip()
                seasonal = row.get('seasonal', '').strip() or None
                industry_code = row.get('industry_code', '').strip() or None
                state_code = row.get('state_code', '').strip() or None
                area_code = row.get('area_code', '').strip() or None
                sizeclass_code = row.get('sizeclass_code', '').strip() or None
                dataelement_code = row.get('dataelement_code', '').strip() or None
                ratelevel_code = row.get('ratelevel_code', '').strip() or None
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

                yield {
                    'series_id': series_id,
                    'seasonal': seasonal,
                    'industry_code': industry_code,
                    'state_code': state_code,
                    'area_code': area_code,
                    'sizeclass_code': sizeclass_code,
                    'dataelement_code': dataelement_code,
                    'ratelevel_code': ratelevel_code,
                    'footnote_codes': footnote_codes,
                    'begin_year': begin_year,
                    'begin_period': begin_period,
                    'end_year': end_year,
                    'end_period': end_period,
                    'is_active': is_active,
                }

    # ==================== DATA PARSERS ====================

    def parse_data_file(self, filename: str) -> Iterator[Dict]:
        """Parse a JT data file"""
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
                # Handle '-' as missing value indicator
                value = float(value_str) if value_str and value_str != '-' else None
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
        log.info("Loading JT reference tables...")

        dataelements = list(self.parse_dataelements())
        log.info(f"  Upserting {len(dataelements)} data elements...")
        self._upsert_batch(session, JTDataElement, dataelements, ['dataelement_code'])
        session.commit()

        industries = list(self.parse_industries())
        log.info(f"  Upserting {len(industries)} industries...")
        self._upsert_batch(session, JTIndustry, industries, ['industry_code'])
        session.commit()

        states = list(self.parse_states())
        log.info(f"  Upserting {len(states)} states...")
        self._upsert_batch(session, JTState, states, ['state_code'])
        session.commit()

        areas = list(self.parse_areas())
        log.info(f"  Upserting {len(areas)} areas...")
        self._upsert_batch(session, JTArea, areas, ['area_code'])
        session.commit()

        sizeclasses = list(self.parse_sizeclasses())
        log.info(f"  Upserting {len(sizeclasses)} size classes...")
        self._upsert_batch(session, JTSizeClass, sizeclasses, ['sizeclass_code'])
        session.commit()

        ratelevels = list(self.parse_ratelevels())
        log.info(f"  Upserting {len(ratelevels)} rate/level codes...")
        self._upsert_batch(session, JTRateLevel, ratelevels, ['ratelevel_code'])
        session.commit()

        series = list(self.parse_series())
        log.info(f"  Upserting {len(series)} series...")
        self._upsert_batch(session, JTSeries, series, ['series_id'])
        session.commit()

        log.info("Reference tables loaded successfully!")

    def load_data(self, session: Session, data_files: List[str] = None, batch_size: int = 10000):
        """Load time series data from specified files"""
        if data_files is None:
            data_files = ['jt.data.1.AllItems']  # Default to all historical data

        log.info(f"Loading JT data from {len(data_files)} file(s)...")

        for data_file in data_files:
            log.info(f"  Processing {data_file}...")

            batch = []
            total_rows = 0

            for row in self.parse_data_file(data_file):
                batch.append(row)

                if len(batch) >= batch_size:
                    self._upsert_batch(session, JTData, batch, ['series_id', 'year', 'period'])
                    session.commit()
                    total_rows += len(batch)
                    log.info(f"    Loaded {total_rows:,} rows...")
                    batch = []

            if batch:
                self._upsert_batch(session, JTData, batch, ['series_id', 'year', 'period'])
                session.commit()
                total_rows += len(batch)

            log.info(f"  ✓ Loaded {total_rows:,} observations from {data_file}")

        log.info("All data files loaded successfully!")


def get_all_data_files(data_dir: str = "data/bls/jt") -> List[str]:
    """Get list of all JT data files"""
    data_path = Path(data_dir)
    data_files = sorted([f.name for f in data_path.glob("jt.data.*") if f.is_file()])
    return data_files
