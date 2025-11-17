# ec_flat_file_parser.py
"""
Parser for BLS Employment Cost Index (EC) flat files downloaded from:
https://download.bls.gov/pub/time.series/ec/

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
    ECCompensation, ECGroup, ECOwnership, ECPeriodicity, ECSeries, ECData
)

log = logging.getLogger("ECFlatFileParser")
logging.basicConfig(level=logging.INFO)


class ECFlatFileParser:
    """Parser for EC (Employment Cost Index) survey flat files"""

    def __init__(self, data_dir: str = "data/bls/ec"):
        self.data_dir = Path(data_dir)
        if not self.data_dir.exists():
            raise FileNotFoundError(f"Data directory not found: {self.data_dir}")

    # ==================== REFERENCE TABLE PARSERS ====================

    def parse_compensations(self) -> Iterator[Dict]:
        """Parse ec.compensation file"""
        file_path = self.data_dir / "ec.compensation"
        log.info(f"Parsing compensation types from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'comp_code': row['comp_code'].strip(),
                    'comp_text': row['comp_text'].strip(),
                }

    def parse_groups(self) -> Iterator[Dict]:
        """Parse ec.group file (fixed-width format)"""
        file_path = self.data_dir / "ec.group"
        log.info(f"Parsing groups from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            # Skip header line
            f.readline()
            for line in f:
                line = line.strip()
                if not line:
                    continue
                # Split on whitespace - first token is code, rest is name
                parts = line.split(maxsplit=1)
                if len(parts) >= 2:
                    yield {
                        'group_code': parts[0].strip(),
                        'group_name': parts[1].strip(),
                    }

    def parse_ownerships(self) -> Iterator[Dict]:
        """Parse ec.ownership file"""
        file_path = self.data_dir / "ec.ownership"
        log.info(f"Parsing ownership types from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'ownership_code': row['ownership_code'].strip(),
                    'ownership_name': row['ownership_name'].strip(),
                }

    def parse_periodicities(self) -> Iterator[Dict]:
        """Parse ec.periodicity file"""
        file_path = self.data_dir / "ec.periodicity"
        log.info(f"Parsing periodicity types from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'periodicity_code': row['periodicity_code'].strip(),
                    'periodicity_text': row['periodicity_text'].strip(),
                }

    def parse_series(self) -> Iterator[Dict]:
        """Parse ec.series file (has extra hidden column)"""
        file_path = self.data_dir / "ec.series"
        log.info(f"Parsing series from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            # EC series file has a hidden empty column between seasonal and begin_year
            # Add placeholder for it
            reader.fieldnames.insert(6, '_hidden_')
            for row in reader:
                series_id = row['series_id'].strip()
                comp_code = row.get('comp_code', '').strip() or None
                group_code = row.get('group_code', '').strip() or None
                ownership_code = row.get('ownership_code', '').strip() or None
                periodicity_code = row.get('periodicity_code', '').strip() or None
                seasonal = row.get('seasonal', '').strip() or None

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
                    'comp_code': comp_code,
                    'group_code': group_code,
                    'ownership_code': ownership_code,
                    'periodicity_code': periodicity_code,
                    'seasonal': seasonal,
                    'begin_year': begin_year,
                    'begin_period': begin_period,
                    'end_year': end_year,
                    'end_period': end_period,
                    'is_active': is_active,
                }

    # ==================== DATA PARSERS ====================

    def parse_data_file(self, filename: str) -> Iterator[Dict]:
        """Parse an EC data file"""
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
        log.info("Loading EC reference tables...")

        compensations = list(self.parse_compensations())
        log.info(f"  Upserting {len(compensations)} compensation types...")
        self._upsert_batch(session, ECCompensation, compensations, ['comp_code'])
        session.commit()

        groups = list(self.parse_groups())
        log.info(f"  Upserting {len(groups)} groups...")
        self._upsert_batch(session, ECGroup, groups, ['group_code'])
        session.commit()

        ownerships = list(self.parse_ownerships())
        log.info(f"  Upserting {len(ownerships)} ownership types...")
        self._upsert_batch(session, ECOwnership, ownerships, ['ownership_code'])
        session.commit()

        periodicities = list(self.parse_periodicities())
        log.info(f"  Upserting {len(periodicities)} periodicity types...")
        self._upsert_batch(session, ECPeriodicity, periodicities, ['periodicity_code'])
        session.commit()

        series = list(self.parse_series())
        log.info(f"  Upserting {len(series)} series...")
        self._upsert_batch(session, ECSeries, series, ['series_id'])
        session.commit()

        log.info("Reference tables loaded successfully!")

    def load_data(self, session: Session, data_files: List[str] = None, batch_size: int = 10000):
        """Load time series data from specified files"""
        if data_files is None:
            data_files = ['ec.data.1.AllData']  # Default to all historical data

        log.info(f"Loading EC data from {len(data_files)} file(s)...")

        for data_file in data_files:
            log.info(f"  Processing {data_file}...")

            batch = []
            total_rows = 0

            for row in self.parse_data_file(data_file):
                batch.append(row)

                if len(batch) >= batch_size:
                    self._upsert_batch(session, ECData, batch, ['series_id', 'year', 'period'])
                    session.commit()
                    total_rows += len(batch)
                    log.info(f"    Loaded {total_rows:,} rows...")
                    batch = []

            if batch:
                self._upsert_batch(session, ECData, batch, ['series_id', 'year', 'period'])
                session.commit()
                total_rows += len(batch)

            log.info(f"  ✓ Loaded {total_rows:,} observations from {data_file}")

        log.info("All data files loaded successfully!")


def get_all_data_files(data_dir: str = "data/bls/ec") -> List[str]:
    """Get list of all EC data files"""
    data_path = Path(data_dir)
    data_files = sorted([f.name for f in data_path.glob("ec.data.*") if f.is_file()])
    return data_files
