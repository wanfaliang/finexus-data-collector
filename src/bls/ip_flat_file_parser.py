# ip_flat_file_parser.py
"""
Parser for BLS Industry Productivity (IP) flat files downloaded from:
https://download.bls.gov/pub/time.series/ip/

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
    IPSector, IPIndustry, IPMeasure, IPDuration, IPType, IPArea,
    IPSeries, IPData
)

log = logging.getLogger("IPFlatFileParser")
logging.basicConfig(level=logging.INFO)


class IPFlatFileParser:
    """Parser for IP (Industry Productivity) flat files"""

    def __init__(self, data_dir: str = "data/bls/ip"):
        self.data_dir = Path(data_dir)
        if not self.data_dir.exists():
            raise FileNotFoundError(f"Data directory not found: {self.data_dir}")

    # ==================== REFERENCE TABLE PARSERS ====================

    def parse_sectors(self) -> Iterator[Dict]:
        """Parse ip.sector file"""
        file_path = self.data_dir / "ip.sector"
        log.info(f"Parsing sectors from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'sector_code': row['sector_code'].strip(),
                    'sector_text': row['sector_text'].strip(),
                }

    def parse_industries(self) -> Iterator[Dict]:
        """Parse ip.industry file"""
        file_path = self.data_dir / "ip.industry"
        log.info(f"Parsing industries from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'industry_code': row['industry_code'].strip(),
                    'naics_code': row.get('naics_code', '').strip() or None,
                    'industry_text': row['industry_text'].strip(),
                    'display_level': int(row['display_level']) if row.get('display_level', '').strip() else None,
                    'selectable': row.get('selectable', '').strip() or None,
                    'sort_sequence': int(row['sort_sequence']) if row.get('sort_sequence', '').strip() else None,
                }

    def parse_measures(self) -> Iterator[Dict]:
        """Parse ip.measure file"""
        file_path = self.data_dir / "ip.measure"
        log.info(f"Parsing measures from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'measure_code': row['measure_code'].strip(),
                    'measure_text': row['measure_text'].strip(),
                    'display_level': int(row['display_level']) if row.get('display_level', '').strip() else None,
                    'selectable': row.get('selectable', '').strip() or None,
                    'sort_sequence': int(row['sort_sequence']) if row.get('sort_sequence', '').strip() else None,
                }

    def parse_durations(self) -> Iterator[Dict]:
        """Parse ip.duration file"""
        file_path = self.data_dir / "ip.duration"
        log.info(f"Parsing durations from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'duration_code': row['duration_code'].strip(),
                    'duration_text': row['duration_text'].strip(),
                }

    def parse_types(self) -> Iterator[Dict]:
        """Parse ip.type file"""
        file_path = self.data_dir / "ip.type"
        log.info(f"Parsing types from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'type_code': row['type_code'].strip(),
                    'type_text': row['type_text'].strip(),
                }

    def parse_areas(self) -> Iterator[Dict]:
        """Parse ip.area file"""
        file_path = self.data_dir / "ip.area"
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

    def parse_series(self) -> Iterator[Dict]:
        """Parse ip.series file"""
        file_path = self.data_dir / "ip.series"
        log.info(f"Parsing series from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]

            for row in reader:
                series_id = row['series_id'].strip()
                seasonal = row.get('seasonal', '').strip() or None
                sector_code = row.get('sector_code', '').strip() or None
                industry_code = row.get('industry_code', '').strip() or None
                measure_code = row.get('measure_code', '').strip() or None
                duration_code = row.get('duration_code', '').strip() or None
                base_year = row.get('base_year', '').strip() or None
                type_code = row.get('type_code', '').strip() or None
                area_code = row.get('area_code', '').strip() or None
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

                yield {
                    'series_id': series_id,
                    'seasonal': seasonal,
                    'sector_code': sector_code,
                    'industry_code': industry_code,
                    'measure_code': measure_code,
                    'duration_code': duration_code,
                    'base_year': base_year,
                    'type_code': type_code,
                    'area_code': area_code,
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
        """Parse an IP data file"""
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
        log.info("Loading IP reference tables...")

        sectors = list(self.parse_sectors())
        log.info(f"  Upserting {len(sectors)} sectors...")
        self._upsert_batch(session, IPSector, sectors, ['sector_code'])
        session.commit()

        industries = list(self.parse_industries())
        log.info(f"  Upserting {len(industries)} industries...")
        self._upsert_batch(session, IPIndustry, industries, ['industry_code'])
        session.commit()

        measures = list(self.parse_measures())
        log.info(f"  Upserting {len(measures)} measures...")
        self._upsert_batch(session, IPMeasure, measures, ['measure_code'])
        session.commit()

        durations = list(self.parse_durations())
        log.info(f"  Upserting {len(durations)} duration types...")
        self._upsert_batch(session, IPDuration, durations, ['duration_code'])
        session.commit()

        types = list(self.parse_types())
        log.info(f"  Upserting {len(types)} data types...")
        self._upsert_batch(session, IPType, types, ['type_code'])
        session.commit()

        areas = list(self.parse_areas())
        log.info(f"  Upserting {len(areas)} areas...")
        self._upsert_batch(session, IPArea, areas, ['area_code'])
        session.commit()

        series = list(self.parse_series())
        log.info(f"  Upserting {len(series)} series...")
        self._upsert_batch(session, IPSeries, series, ['series_id'])
        session.commit()

        log.info("Reference tables loaded successfully!")

    def load_data(self, session: Session, data_files: List[str] = None, batch_size: int = 10000):
        """Load time series data from specified files"""
        if data_files is None:
            data_files = ['ip.data.1.AllData']  # Default to all historical data

        log.info(f"Loading IP data from {len(data_files)} file(s)...")

        for data_file in data_files:
            log.info(f"  Processing {data_file}...")

            batch = []
            total_rows = 0

            for row in self.parse_data_file(data_file):
                batch.append(row)

                if len(batch) >= batch_size:
                    self._upsert_batch(session, IPData, batch, ['series_id', 'year', 'period'])
                    session.commit()
                    total_rows += len(batch)
                    log.info(f"    Loaded {total_rows:,} rows...")
                    batch = []

            if batch:
                self._upsert_batch(session, IPData, batch, ['series_id', 'year', 'period'])
                session.commit()
                total_rows += len(batch)

            log.info(f"  ✓ Loaded {total_rows:,} observations from {data_file}")

        log.info("All data files loaded successfully!")


def get_all_data_files(data_dir: str = "data/bls/ip") -> List[str]:
    """Get list of all IP data files"""
    data_path = Path(data_dir)
    data_files = sorted([f.name for f in data_path.glob("ip.data.*") if f.is_file()])
    return data_files
