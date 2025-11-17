# tu_flat_file_parser.py
"""
Parser for BLS American Time Use Survey (TU) flat files downloaded from:
https://download.bls.gov/pub/time.series/tu/

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
    TUStatType, TUActivityCode, TUSex, TUAge, TURace, TUEducation,
    TUMaritalStatus, TULaborForceStatus, TUOrigin, TURegion,
    TUWhere, TUWho, TUTimeOfDay, TUSeries, TUData, TUAspect
)

log = logging.getLogger("TUFlatFileParser")
logging.basicConfig(level=logging.INFO)


class TUFlatFileParser:
    """Parser for TU (American Time Use Survey) flat files"""

    def __init__(self, data_dir: str = "data/bls/tu"):
        self.data_dir = Path(data_dir)
        if not self.data_dir.exists():
            raise FileNotFoundError(f"Data directory not found: {self.data_dir}")

    # ==================== REFERENCE TABLE PARSERS ====================

    def parse_stattypes(self) -> Iterator[Dict]:
        """Parse tu.stattype file"""
        file_path = self.data_dir / "tu.stattype"
        log.info(f"Parsing statistic types from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'stattype_code': row['stattype_code'].strip(),
                    'stattype_text': row['stattype_text'].strip(),
                    'display_level': int(row['display_level']) if row.get('display_level', '').strip() else None,
                    'selectable': row.get('selectable', '').strip() or None,
                    'sort_sequence': int(row['sort_sequence']) if row.get('sort_sequence', '').strip() else None,
                }

    def parse_actcodes(self) -> Iterator[Dict]:
        """Parse tu.actcode file"""
        file_path = self.data_dir / "tu.actcode"
        log.info(f"Parsing activity codes from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'actcode_code': row['actcode_code'].strip(),
                    'actcode_text': row['actcode_text'].strip(),
                    'display_level': int(row['display_level']) if row.get('display_level', '').strip() else None,
                    'selectable': row.get('selectable', '').strip() or None,
                    'sort_sequence': int(row['sort_sequence']) if row.get('sort_sequence', '').strip() else None,
                }

    def parse_sex(self) -> Iterator[Dict]:
        """Parse tu.sex file"""
        file_path = self.data_dir / "tu.sex"
        log.info(f"Parsing sex categories from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'sex_code': row['sex_code'].strip(),
                    'sex_text': row['sex_text'].strip(),
                    'display_level': int(row['display_level']) if row.get('display_level', '').strip() else None,
                    'selectable': row.get('selectable', '').strip() or None,
                    'sort_sequence': int(row['sort_sequence']) if row.get('sort_sequence', '').strip() else None,
                }

    def parse_ages(self) -> Iterator[Dict]:
        """Parse tu.age file"""
        file_path = self.data_dir / "tu.age"
        log.info(f"Parsing age groups from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'age_code': row['age_code'].strip(),
                    'age_text': row['age_text'].strip(),
                    'display_level': int(row['display_level']) if row.get('display_level', '').strip() else None,
                    'selectable': row.get('selectable', '').strip() or None,
                    'sort_sequence': int(row['sort_sequence']) if row.get('sort_sequence', '').strip() else None,
                }

    def parse_races(self) -> Iterator[Dict]:
        """Parse tu.race file"""
        file_path = self.data_dir / "tu.race"
        log.info(f"Parsing race categories from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'race_code': row['race_code'].strip(),
                    'race_text': row['race_text'].strip(),
                    'display_level': int(row['display_level']) if row.get('display_level', '').strip() else None,
                    'selectable': row.get('selectable', '').strip() or None,
                    'sort_sequence': int(row['sort_sequence']) if row.get('sort_sequence', '').strip() else None,
                }

    def parse_education(self) -> Iterator[Dict]:
        """Parse tu.educ file"""
        file_path = self.data_dir / "tu.educ"
        log.info(f"Parsing education levels from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'educ_code': row['educ_code'].strip(),
                    'educ_text': row['educ_text'].strip(),
                    'display_level': int(row['display_level']) if row.get('display_level', '').strip() else None,
                    'selectable': row.get('selectable', '').strip() or None,
                    'sort_sequence': int(row['sort_sequence']) if row.get('sort_sequence', '').strip() else None,
                }

    def parse_marital_status(self) -> Iterator[Dict]:
        """Parse tu.maritlstat file"""
        file_path = self.data_dir / "tu.maritlstat"
        log.info(f"Parsing marital status from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'maritlstat_code': row['maritlstat_code'].strip(),
                    'maritlstat_text': row['maritlstat_text'].strip(),
                    'display_level': int(row['display_level']) if row.get('display_level', '').strip() else None,
                    'selectable': row.get('selectable', '').strip() or None,
                    'sort_sequence': int(row['sort_sequence']) if row.get('sort_sequence', '').strip() else None,
                }

    def parse_labor_force_status(self) -> Iterator[Dict]:
        """Parse tu.lfstat file"""
        file_path = self.data_dir / "tu.lfstat"
        log.info(f"Parsing labor force status from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'lfstat_code': row['lfstat_code'].strip(),
                    'lfstat_text': row['lfstat_text'].strip(),
                    'display_level': int(row['display_level']) if row.get('display_level', '').strip() else None,
                    'selectable': row.get('selectable', '').strip() or None,
                    'sort_sequence': int(row['sort_sequence']) if row.get('sort_sequence', '').strip() else None,
                }

    def parse_origin(self) -> Iterator[Dict]:
        """Parse tu.orig file"""
        file_path = self.data_dir / "tu.orig"
        log.info(f"Parsing Hispanic/Latino origin from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'orig_code': row['orig_code'].strip(),
                    'orig_text': row['orig_text'].strip(),
                    'display_level': int(row['display_level']) if row.get('display_level', '').strip() else None,
                    'selectable': row.get('selectable', '').strip() or None,
                    'sort_sequence': int(row['sort_sequence']) if row.get('sort_sequence', '').strip() else None,
                }

    def parse_regions(self) -> Iterator[Dict]:
        """Parse tu.region file"""
        file_path = self.data_dir / "tu.region"
        log.info(f"Parsing regions from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'region_code': row['region_code'].strip(),
                    'region_text': row['region_text'].strip(),
                    'display_level': int(row['display_level']) if row.get('display_level', '').strip() else None,
                    'selectable': row.get('selectable', '').strip() or None,
                    'sort_sequence': int(row['sort_sequence']) if row.get('sort_sequence', '').strip() else None,
                }

    def parse_where(self) -> Iterator[Dict]:
        """Parse tu.where file"""
        file_path = self.data_dir / "tu.where"
        log.info(f"Parsing where codes from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'where_code': row['where_code'].strip(),
                    'where_text': row['where_text'].strip(),
                    'display_level': int(row['display_level']) if row.get('display_level', '').strip() else None,
                    'selectable': row.get('selectable', '').strip() or None,
                    'sort_sequence': int(row['sort_sequence']) if row.get('sort_sequence', '').strip() else None,
                }

    def parse_who(self) -> Iterator[Dict]:
        """Parse tu.who file"""
        file_path = self.data_dir / "tu.who"
        log.info(f"Parsing who codes from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'who_code': row['who_code'].strip(),
                    'who_text': row['who_text'].strip(),
                    'display_level': int(row['display_level']) if row.get('display_level', '').strip() else None,
                    'selectable': row.get('selectable', '').strip() or None,
                    'sort_sequence': int(row['sort_sequence']) if row.get('sort_sequence', '').strip() else None,
                }

    def parse_timeofday(self) -> Iterator[Dict]:
        """Parse tu.timeday file"""
        file_path = self.data_dir / "tu.timeday"
        log.info(f"Parsing time of day codes from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            for row in reader:
                yield {
                    'timeday_code': row['timeday_code'].strip(),
                    'timeday_text': row['timeday_text'].strip(),
                    'display_level': int(row['display_level']) if row.get('display_level', '').strip() else None,
                    'selectable': row.get('selectable', '').strip() or None,
                    'sort_sequence': int(row['sort_sequence']) if row.get('sort_sequence', '').strip() else None,
                }

    def parse_series(self) -> Iterator[Dict]:
        """Parse tu.series file"""
        file_path = self.data_dir / "tu.series"
        log.info(f"Parsing series from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]

            for row in reader:
                series_id = row['series_id'].strip()
                seasonal = row.get('seasonal', '').strip() or None

                # Parse all dimension codes
                stattype_code = row.get('stattype_code', '').strip() or None
                datays_code = row.get('datays_code', '').strip() or None
                sex_code = row.get('sex_code', '').strip() or None
                region_code = row.get('region_code', '').strip() or None
                lfstat_code = row.get('lfstat_code', '').strip() or None
                educ_code = row.get('educ_code', '').strip() or None
                maritlstat_code = row.get('maritlstat_code', '').strip() or None
                age_code = row.get('age_code', '').strip() or None
                orig_code = row.get('orig_code', '').strip() or None
                race_code = row.get('race_code', '').strip() or None
                mjcow_code = row.get('mjcow_code', '').strip() or None
                nmet_code = row.get('nmet_code', '').strip() or None
                where_code = row.get('where_code', '').strip() or None
                sjmj_code = row.get('sjmj_code', '').strip() or None
                timeday_code = row.get('timeday_code', '').strip() or None
                actcode_code = row.get('actcode_code', '').strip() or None
                industry_code = row.get('industry_code', '').strip() or None
                occ_code = row.get('occ_code', '').strip() or None
                prhhchild_code = row.get('prhhchild_code', '').strip() or None
                earn_code = row.get('earn_code', '').strip() or None
                disability_code = row.get('disability_code', '').strip() or None
                who_code = row.get('who_code', '').strip() or None
                hhnscc03_code = row.get('hhnscc03_code', '').strip() or None
                schenr_code = row.get('schenr_code', '').strip() or None
                prownhhchild_code = row.get('prownhhchild_code', '').strip() or None
                work_code = row.get('work_code', '').strip() or None
                elnum_code = row.get('elnum_code', '').strip() or None
                ecage_code = row.get('ecage_code', '').strip() or None
                elfreq_code = row.get('elfreq_code', '').strip() or None
                eldur_code = row.get('eldur_code', '').strip() or None
                elwho_code = row.get('elwho_code', '').strip() or None
                ecytd_code = row.get('ecytd_code', '').strip() or None
                elder_code = row.get('elder_code', '').strip() or None
                lfstatw_code = row.get('lfstatw_code', '').strip() or None
                pertype_code = row.get('pertype_code', '').strip() or None

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
                    'stattype_code': stattype_code,
                    'datays_code': datays_code,
                    'sex_code': sex_code,
                    'region_code': region_code,
                    'lfstat_code': lfstat_code,
                    'educ_code': educ_code,
                    'maritlstat_code': maritlstat_code,
                    'age_code': age_code,
                    'orig_code': orig_code,
                    'race_code': race_code,
                    'mjcow_code': mjcow_code,
                    'nmet_code': nmet_code,
                    'where_code': where_code,
                    'sjmj_code': sjmj_code,
                    'timeday_code': timeday_code,
                    'actcode_code': actcode_code,
                    'industry_code': industry_code,
                    'occ_code': occ_code,
                    'prhhchild_code': prhhchild_code,
                    'earn_code': earn_code,
                    'disability_code': disability_code,
                    'who_code': who_code,
                    'hhnscc03_code': hhnscc03_code,
                    'schenr_code': schenr_code,
                    'prownhhchild_code': prownhhchild_code,
                    'work_code': work_code,
                    'elnum_code': elnum_code,
                    'ecage_code': ecage_code,
                    'elfreq_code': elfreq_code,
                    'eldur_code': eldur_code,
                    'elwho_code': elwho_code,
                    'ecytd_code': ecytd_code,
                    'elder_code': elder_code,
                    'lfstatw_code': lfstatw_code,
                    'pertype_code': pertype_code,
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
        """Parse a TU data file"""
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

    def parse_aspect_file(self) -> Iterator[Dict]:
        """Parse the tu.aspect file (standard errors)"""
        file_path = self.data_dir / "tu.aspect"
        log.info(f"Parsing aspect data from {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            reader.fieldnames = [name.strip() for name in reader.fieldnames]

            for row in reader:
                series_id = row['series_id'].strip()
                year = int(row['year'])
                period = row['period'].strip()
                aspect_type = row['aspect_type'].strip()
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
                    'aspect_type': aspect_type,
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
        log.info("Loading TU reference tables...")

        stattypes = list(self.parse_stattypes())
        log.info(f"  Upserting {len(stattypes)} statistic types...")
        self._upsert_batch(session, TUStatType, stattypes, ['stattype_code'])
        session.commit()

        actcodes = list(self.parse_actcodes())
        log.info(f"  Upserting {len(actcodes)} activity codes...")
        self._upsert_batch(session, TUActivityCode, actcodes, ['actcode_code'])
        session.commit()

        sex = list(self.parse_sex())
        log.info(f"  Upserting {len(sex)} sex categories...")
        self._upsert_batch(session, TUSex, sex, ['sex_code'])
        session.commit()

        ages = list(self.parse_ages())
        log.info(f"  Upserting {len(ages)} age groups...")
        self._upsert_batch(session, TUAge, ages, ['age_code'])
        session.commit()

        races = list(self.parse_races())
        log.info(f"  Upserting {len(races)} race categories...")
        self._upsert_batch(session, TURace, races, ['race_code'])
        session.commit()

        education = list(self.parse_education())
        log.info(f"  Upserting {len(education)} education levels...")
        self._upsert_batch(session, TUEducation, education, ['educ_code'])
        session.commit()

        marital_status = list(self.parse_marital_status())
        log.info(f"  Upserting {len(marital_status)} marital status categories...")
        self._upsert_batch(session, TUMaritalStatus, marital_status, ['maritlstat_code'])
        session.commit()

        lfstat = list(self.parse_labor_force_status())
        log.info(f"  Upserting {len(lfstat)} labor force status categories...")
        self._upsert_batch(session, TULaborForceStatus, lfstat, ['lfstat_code'])
        session.commit()

        origin = list(self.parse_origin())
        log.info(f"  Upserting {len(origin)} origin categories...")
        self._upsert_batch(session, TUOrigin, origin, ['orig_code'])
        session.commit()

        regions = list(self.parse_regions())
        log.info(f"  Upserting {len(regions)} regions...")
        self._upsert_batch(session, TURegion, regions, ['region_code'])
        session.commit()

        where = list(self.parse_where())
        log.info(f"  Upserting {len(where)} where codes...")
        self._upsert_batch(session, TUWhere, where, ['where_code'])
        session.commit()

        who = list(self.parse_who())
        log.info(f"  Upserting {len(who)} who codes...")
        self._upsert_batch(session, TUWho, who, ['who_code'])
        session.commit()

        timeofday = list(self.parse_timeofday())
        log.info(f"  Upserting {len(timeofday)} time of day codes...")
        self._upsert_batch(session, TUTimeOfDay, timeofday, ['timeday_code'])
        session.commit()

        series = list(self.parse_series())
        log.info(f"  Upserting {len(series)} series...")
        self._upsert_batch(session, TUSeries, series, ['series_id'])
        session.commit()

        log.info("Reference tables loaded successfully!")

    def load_data(self, session: Session, data_files: List[str] = None, batch_size: int = 10000):
        """Load time series data from specified files"""
        if data_files is None:
            data_files = ['tu.data.1.AllData']  # Default to all historical data

        log.info(f"Loading TU data from {len(data_files)} file(s)...")

        for data_file in data_files:
            log.info(f"  Processing {data_file}...")

            batch = []
            total_rows = 0

            for row in self.parse_data_file(data_file):
                batch.append(row)

                if len(batch) >= batch_size:
                    self._upsert_batch(session, TUData, batch, ['series_id', 'year', 'period'])
                    session.commit()
                    total_rows += len(batch)
                    log.info(f"    Loaded {total_rows:,} rows...")
                    batch = []

            if batch:
                self._upsert_batch(session, TUData, batch, ['series_id', 'year', 'period'])
                session.commit()
                total_rows += len(batch)

            log.info(f"  ✓ Loaded {total_rows:,} observations from {data_file}")

        log.info("All data files loaded successfully!")

    def load_aspect(self, session: Session, batch_size: int = 10000):
        """Load aspect data (standard errors)"""
        log.info("Loading TU aspect data (standard errors)...")

        batch = []
        total_rows = 0

        for row in self.parse_aspect_file():
            batch.append(row)

            if len(batch) >= batch_size:
                self._upsert_batch(session, TUAspect, batch, ['series_id', 'year', 'period', 'aspect_type'])
                session.commit()
                total_rows += len(batch)
                log.info(f"  Loaded {total_rows:,} aspect rows...")
                batch = []

        if batch:
            self._upsert_batch(session, TUAspect, batch, ['series_id', 'year', 'period', 'aspect_type'])
            session.commit()
            total_rows += len(batch)

        log.info(f"✓ Loaded {total_rows:,} aspect observations")


def get_all_data_files(data_dir: str = "data/bls/tu") -> List[str]:
    """Get list of all TU data files"""
    data_path = Path(data_dir)
    data_files = sorted([f.name for f in data_path.glob("tu.data.*") if f.is_file()])
    return data_files
