# sm_flat_file_parser.py
"""
Parser for BLS State and Metro Area Employment (SM) flat files downloaded from:
https://download.bls.gov/pub/time.series/sm/

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
    SMState, SMArea, SMSupersector, SMIndustry, SMSeries, SMData
)

log = logging.getLogger("SMFlatFileParser")
logging.basicConfig(level=logging.INFO)


class SMFlatFileParser:
    """Parser for SM (State and Metro Area Employment) survey flat files"""

    def __init__(self, data_dir: str = "data/bls/sm"):
        self.data_dir = Path(data_dir)
        if not self.data_dir.exists():
            raise FileNotFoundError(f"Data directory not found: {self.data_dir}")

    # ==================== REFERENCE TABLE PARSERS ====================

    def parse_states(self) -> Iterator[Dict]:
        """Parse sm.state file"""
        file_path = self.data_dir / "sm.state"
        log.info(f"Parsing states from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'state_code': row['state_code'].strip(),
                    'state_name': row['state_name'].strip(),
                }

    def parse_areas(self) -> Iterator[Dict]:
        """Parse sm.area file"""
        file_path = self.data_dir / "sm.area"
        log.info(f"Parsing areas from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'area_code': row['area_code'].strip(),
                    'area_name': row['area_name'].strip(),
                }

    def parse_supersectors(self) -> Iterator[Dict]:
        """Parse sm.supersector file"""
        file_path = self.data_dir / "sm.supersector"
        log.info(f"Parsing supersectors from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'supersector_code': row['supersector_code'].strip(),
                    'supersector_name': row['supersector_name'].strip(),
                }

    def parse_industries(self) -> Iterator[Dict]:
        """Parse sm.industry file"""
        file_path = self.data_dir / "sm.industry"
        log.info(f"Parsing industries from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'industry_code': row['industry_code'].strip(),
                    'industry_name': row['industry_name'].strip(),
                }

    def parse_series(self) -> Iterator[Dict]:
        """Parse sm.series file"""
        file_path = self.data_dir / "sm.series"
        log.info(f"Parsing series from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                series_id = row['series_id'].strip()
                state_code = row['state_code'].strip()
                area_code = row['area_code'].strip()
                supersector_code = row['supersector_code'].strip()
                industry_code = row['industry_code'].strip()
                data_type_code = row['data_type_code'].strip()
                seasonal = row.get('seasonal', '').strip() or None
                benchmark_year_str = row.get('benchmark_year', '').strip()
                benchmark_year = int(benchmark_year_str) if benchmark_year_str else None
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
                    'state_code': state_code,
                    'area_code': area_code,
                    'supersector_code': supersector_code,
                    'industry_code': industry_code,
                    'data_type_code': data_type_code,
                    'seasonal_code': seasonal,
                    'benchmark_year': benchmark_year,
                    'footnote_codes': footnote_codes,
                    'begin_year': begin_year,
                    'begin_period': begin_period,
                    'end_year': end_year,
                    'end_period': end_period,
                    'is_active': is_active,
                }

    # ==================== DATA PARSERS ====================

    def parse_data_file(self, filename: str) -> Iterator[Dict]:
        """Parse an SM data file"""
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
        log.info("Loading SM reference tables...")

        states = list(self.parse_states())
        log.info(f"  Upserting {len(states)} states...")
        self._upsert_batch(session, SMState, states, ['state_code'])
        session.commit()

        areas = list(self.parse_areas())
        log.info(f"  Upserting {len(areas)} areas...")
        self._upsert_batch(session, SMArea, areas, ['area_code'])
        session.commit()

        supersectors = list(self.parse_supersectors())
        log.info(f"  Upserting {len(supersectors)} supersectors...")
        self._upsert_batch(session, SMSupersector, supersectors, ['supersector_code'])
        session.commit()

        industries = list(self.parse_industries())
        log.info(f"  Upserting {len(industries)} industries...")
        self._upsert_batch(session, SMIndustry, industries, ['industry_code'])
        session.commit()

        series = list(self.parse_series())
        log.info(f"  Upserting {len(series)} series...")
        self._upsert_batch(session, SMSeries, series, ['series_id'])
        session.commit()

        log.info("Reference tables loaded successfully!")

    def load_data(self, session: Session, data_files: List[str] = None, batch_size: int = 10000):
        """Load time series data from specified files"""
        if data_files is None:
            data_files = ['sm.data.1.AllData']  # Default to all historical data

        log.info(f"Loading SM data from {len(data_files)} file(s)...")

        for data_file in data_files:
            log.info(f"  Processing {data_file}...")

            batch = []
            total_rows = 0

            for row in self.parse_data_file(data_file):
                batch.append(row)

                if len(batch) >= batch_size:
                    self._upsert_batch(session, SMData, batch, ['series_id', 'year', 'period'])
                    session.commit()
                    total_rows += len(batch)
                    log.info(f"    Loaded {total_rows:,} rows...")
                    batch = []

            if batch:
                self._upsert_batch(session, SMData, batch, ['series_id', 'year', 'period'])
                session.commit()
                total_rows += len(batch)

            log.info(f"  ✓ Loaded {total_rows:,} observations from {data_file}")

        log.info("All data files loaded successfully!")


def get_all_data_files(data_dir: str = "data/bls/sm") -> List[str]:
    """Get list of all SM data files"""
    data_path = Path(data_dir)
    data_files = sorted([f.name for f in data_path.glob("sm.data.*") if f.is_file()])
    return data_files
