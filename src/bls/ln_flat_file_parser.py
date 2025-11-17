#!/usr/bin/env python3
"""
BLS LN (Labor Force Statistics from Current Population Survey) Flat File Parser

This module parses LN flat files from the BLS and loads them into PostgreSQL.
LN survey has 67K+ series tracking labor force participation, employment, unemployment,
and demographics across 33+ dimensions.
"""
import csv
from pathlib import Path
from typing import Iterator, Dict, List, Optional
from datetime import datetime, UTC

from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert

from database.bls_models import (
    LNLaborForceStatus, LNPeriodicity, LNAbsence, LNActivity, LNAge,
    LNCertification, LNClass, LNDuration, LNEducation, LNEntrance,
    LNExperience, LNHeadOfHousehold, LNHour, LNIndustry, LNJobDesire,
    LNLook, LNMaritalStatus, LNMultipleJobholder, LNOccupation, LNOrigin,
    LNPercentage, LNRace, LNAbsenceReason, LNJobSearch, LNPartTimeReason,
    LNSeek, LNSex, LNDataType, LNVeteran, LNWorkStatus, LNBorn, LNChild,
    LNDisability, LNTelework, LNSeries, LNData
)


class LNFlatFileParser:
    """Parser for LN (CPS Labor Force Statistics) flat files"""

    def __init__(self, data_dir: str = "data/bls/ln"):
        """Initialize the parser with the directory containing LN flat files"""
        self.data_dir = Path(data_dir)

    # ==================== REFERENCE TABLE PARSERS ====================

    def parse_lfst(self) -> Iterator[Dict]:
        """Parse ln.lfst - Labor force status codes"""
        filepath = self.data_dir / "ln.lfst"
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                yield {
                    'lfst_code': row['lfst_code'].strip(),
                    'lfst_text': row['lfst_text'].strip()
                }

    def parse_periodicity(self) -> Iterator[Dict]:
        """Parse ln.periodicity - Periodicity codes"""
        filepath = self.data_dir / "ln.periodicity"
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                yield {
                    'periodicity_code': row['periodicity_code'].strip(),
                    'periodicity_text': row['periodicity_text'].strip()
                }

    def parse_absn(self) -> Iterator[Dict]:
        """Parse ln.absn - Absence codes"""
        filepath = self.data_dir / "ln.absn"
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                yield {
                    'absn_code': row['absn_code'].strip(),
                    'absn_text': row['absn_text'].strip()
                }

    def parse_activity(self) -> Iterator[Dict]:
        """Parse ln.activity - Activity codes"""
        filepath = self.data_dir / "ln.activity"
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                yield {
                    'activity_code': row['activity_code'].strip(),
                    'activity_text': row['activity_text'].strip()
                }

    def parse_ages(self) -> Iterator[Dict]:
        """Parse ln.ages - Age group codes"""
        filepath = self.data_dir / "ln.ages"
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                yield {
                    'ages_code': row['ages_code'].strip(),
                    'ages_text': row['ages_text'].strip()
                }

    def parse_cert(self) -> Iterator[Dict]:
        """Parse ln.cert - Certification codes"""
        filepath = self.data_dir / "ln.cert"
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                yield {
                    'cert_code': row['cert_code'].strip(),
                    'cert_text': row['cert_text'].strip()
                }

    def parse_class(self) -> Iterator[Dict]:
        """Parse ln.class - Class of worker codes"""
        filepath = self.data_dir / "ln.class"
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                yield {
                    'class_code': row['class_code'].strip(),
                    'class_text': row['class_text'].strip()
                }

    def parse_duration(self) -> Iterator[Dict]:
        """Parse ln.duration - Duration codes"""
        filepath = self.data_dir / "ln.duration"
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                yield {
                    'duration_code': row['duration_code'].strip(),
                    'duration_text': row['duration_text'].strip()
                }

    def parse_education(self) -> Iterator[Dict]:
        """Parse ln.education - Education codes"""
        filepath = self.data_dir / "ln.education"
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                yield {
                    'education_code': row['education_code'].strip(),
                    'education_text': row['education_text'].strip()
                }

    def parse_entr(self) -> Iterator[Dict]:
        """Parse ln.entr - Entrance to labor force codes"""
        filepath = self.data_dir / "ln.entr"
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                yield {
                    'entr_code': row['entr_code'].strip(),
                    'entr_text': row['entr_text'].strip()
                }

    def parse_expr(self) -> Iterator[Dict]:
        """Parse ln.expr - Work experience codes"""
        filepath = self.data_dir / "ln.expr"
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                yield {
                    'expr_code': row['expr_code'].strip(),
                    'expr_text': row['expr_text'].strip()
                }

    def parse_hheader(self) -> Iterator[Dict]:
        """Parse ln.hheader - Head of household codes"""
        filepath = self.data_dir / "ln.hheader"
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                yield {
                    'hheader_code': row['hheader_code'].strip(),
                    'hheader_text': row['hheader_text'].strip()
                }

    def parse_hour(self) -> Iterator[Dict]:
        """Parse ln.hour - Hours worked codes"""
        filepath = self.data_dir / "ln.hour"
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                yield {
                    'hour_code': row['hour_code'].strip(),
                    'hour_text': row['hour_text'].strip()
                }

    def parse_indy(self) -> Iterator[Dict]:
        """Parse ln.indy - Industry codes"""
        filepath = self.data_dir / "ln.indy"
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                yield {
                    'indy_code': row['indy_code'].strip(),
                    'indy_text': row['indy_text'].strip()
                }

    def parse_jdes(self) -> Iterator[Dict]:
        """Parse ln.jdes - Want a job codes"""
        filepath = self.data_dir / "ln.jdes"
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                yield {
                    'jdes_code': row['jdes_code'].strip(),
                    'jdes_text': row['jdes_text'].strip()
                }

    def parse_look(self) -> Iterator[Dict]:
        """Parse ln.look - Job seeker codes"""
        filepath = self.data_dir / "ln.look"
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                yield {
                    'look_code': row['look_code'].strip(),
                    'look_text': row['look_text'].strip()
                }

    def parse_mari(self) -> Iterator[Dict]:
        """Parse ln.mari - Marital status codes"""
        filepath = self.data_dir / "ln.mari"
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                yield {
                    'mari_code': row['mari_code'].strip(),
                    'mari_text': row['mari_text'].strip()
                }

    def parse_mjhs(self) -> Iterator[Dict]:
        """Parse ln.mjhs - Multiple jobholder codes"""
        filepath = self.data_dir / "ln.mjhs"
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                yield {
                    'mjhs_code': row['mjhs_code'].strip(),
                    'mjhs_text': row['mjhs_text'].strip()
                }

    def parse_occupation(self) -> Iterator[Dict]:
        """Parse ln.occupation - Occupation codes"""
        filepath = self.data_dir / "ln.occupation"
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                yield {
                    'occupation_code': row['occupation_code'].strip(),
                    'occupation_text': row['occupation_text'].strip()
                }

    def parse_orig(self) -> Iterator[Dict]:
        """Parse ln.orig - Hispanic or Latino origin codes"""
        filepath = self.data_dir / "ln.orig"
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                yield {
                    'orig_code': row['orig_code'].strip(),
                    'orig_text': row['orig_text'].strip()
                }

    def parse_pcts(self) -> Iterator[Dict]:
        """Parse ln.pcts - Percentage codes"""
        filepath = self.data_dir / "ln.pcts"
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                yield {
                    'pcts_code': row['pcts_code'].strip(),
                    'pcts_text': row['pcts_text'].strip()
                }

    def parse_race(self) -> Iterator[Dict]:
        """Parse ln.race - Race codes"""
        filepath = self.data_dir / "ln.race"
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                yield {
                    'race_code': row['race_code'].strip(),
                    'race_text': row['race_text'].strip()
                }

    def parse_rjnw(self) -> Iterator[Dict]:
        """Parse ln.rjnw - Absence reason codes"""
        filepath = self.data_dir / "ln.rjnw"
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                yield {
                    'rjnw_code': row['rjnw_code'].strip(),
                    'rjnw_text': row['rjnw_text'].strip()
                }

    def parse_rnlf(self) -> Iterator[Dict]:
        """Parse ln.rnlf - Job search codes"""
        filepath = self.data_dir / "ln.rnlf"
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                yield {
                    'rnlf_code': row['rnlf_code'].strip(),
                    'rnlf_text': row['rnlf_text'].strip()
                }

    def parse_rwns(self) -> Iterator[Dict]:
        """Parse ln.rwns - Part time reason codes"""
        filepath = self.data_dir / "ln.rwns"
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                yield {
                    'rwns_code': row['rwns_code'].strip(),
                    'rwns_text': row['rwns_text'].strip()
                }

    def parse_seek(self) -> Iterator[Dict]:
        """Parse ln.seek - Job seeker codes"""
        filepath = self.data_dir / "ln.seek"
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                yield {
                    'seek_code': row['seek_code'].strip(),
                    'seek_text': row['seek_text'].strip()
                }

    def parse_sexs(self) -> Iterator[Dict]:
        """Parse ln.sexs - Sex codes"""
        filepath = self.data_dir / "ln.sexs"
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                yield {
                    'sexs_code': row['sexs_code'].strip(),
                    'sexs_text': row['sexs_text'].strip()
                }

    def parse_tdat(self) -> Iterator[Dict]:
        """Parse ln.tdat - Data type codes"""
        filepath = self.data_dir / "ln.tdat"
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                yield {
                    'tdat_code': row['tdat_code'].strip(),
                    'tdat_text': row['tdat_text'].strip()
                }

    def parse_vets(self) -> Iterator[Dict]:
        """Parse ln.vets - Veteran status codes"""
        filepath = self.data_dir / "ln.vets"
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                yield {
                    'vets_code': row['vets_code'].strip(),
                    'vets_text': row['vets_text'].strip()
                }

    def parse_wkst(self) -> Iterator[Dict]:
        """Parse ln.wkst - Work status codes"""
        filepath = self.data_dir / "ln.wkst"
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                yield {
                    'wkst_code': row['wkst_code'].strip(),
                    'wkst_text': row['wkst_text'].strip()
                }

    def parse_born(self) -> Iterator[Dict]:
        """Parse ln.born - Nativity/Citizenship codes"""
        filepath = self.data_dir / "ln.born"
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                yield {
                    'born_code': row['born_code'].strip(),
                    'born_text': row['born_text'].strip()
                }

    def parse_chld(self) -> Iterator[Dict]:
        """Parse ln.chld - Presence of children codes"""
        filepath = self.data_dir / "ln.chld"
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                yield {
                    'chld_code': row['chld_code'].strip(),
                    'chld_text': row['chld_text'].strip()
                }

    def parse_disa(self) -> Iterator[Dict]:
        """Parse ln.disa - Disability codes"""
        filepath = self.data_dir / "ln.disa"
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                yield {
                    'disa_code': row['disa_code'].strip(),
                    'disa_text': row['disa_text'].strip()
                }

    def parse_tlwk(self) -> Iterator[Dict]:
        """Parse ln.tlwk - Telework codes"""
        filepath = self.data_dir / "ln.tlwk"
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                yield {
                    'tlwk_code': row['tlwk_code'].strip(),
                    'tlwk_text': row['tlwk_text'].strip()
                }

    # ==================== SERIES PARSER ====================

    def parse_series(self) -> Iterator[Dict]:
        """Parse ln.series file - 67K+ series with 37 dimension codes"""
        filepath = self.data_dir / "ln.series"

        with open(filepath, 'r', encoding='utf-8', newline='') as f:
            # Read and clean headers (they might have extra spaces)
            headers = f.readline().strip().split('\t')
            headers = [h.strip() for h in headers]

            # Create reader with cleaned headers
            reader = csv.DictReader(f, fieldnames=headers, delimiter='\t')

            for row in reader:
                # Calculate is_active based on end_year
                end_year = int(row['end_year']) if row.get('end_year') else None
                current_year = datetime.now().year
                is_active = end_year is None or end_year >= (current_year - 1)

                yield {
                    'series_id': row['series_id'].strip(),
                    'lfst_code': row['lfst_code'].strip(),
                    'periodicity_code': row['periodicity_code'].strip(),
                    'series_title': row['series_title'].strip(),
                    'absn_code': row['absn_code'].strip(),
                    'activity_code': row['activity_code'].strip(),
                    'ages_code': row['ages_code'].strip(),
                    'cert_code': row['cert_code'].strip(),
                    'class_code': row['class_code'].strip(),
                    'duration_code': row['duration_code'].strip(),
                    'education_code': row['education_code'].strip(),
                    'entr_code': row['entr_code'].strip(),
                    'expr_code': row['expr_code'].strip(),
                    'hheader_code': row['hheader_code'].strip(),
                    'hour_code': row['hour_code'].strip(),
                    'indy_code': row['indy_code'].strip(),
                    'jdes_code': row['jdes_code'].strip(),
                    'look_code': row['look_code'].strip(),
                    'mari_code': row['mari_code'].strip(),
                    'mjhs_code': row['mjhs_code'].strip(),
                    'occupation_code': row['occupation_code'].strip(),
                    'orig_code': row['orig_code'].strip(),
                    'pcts_code': row['pcts_code'].strip(),
                    'race_code': row['race_code'].strip(),
                    'rjnw_code': row['rjnw_code'].strip(),
                    'rnlf_code': row['rnlf_code'].strip(),
                    'rwns_code': row['rwns_code'].strip(),
                    'seek_code': row['seek_code'].strip(),
                    'sexs_code': row['sexs_code'].strip(),
                    'tdat_code': row['tdat_code'].strip(),
                    'vets_code': row['vets_code'].strip(),
                    'wkst_code': row['wkst_code'].strip(),
                    'born_code': row['born_code'].strip(),
                    'chld_code': row['chld_code'].strip(),
                    'disa_code': row['disa_code'].strip(),
                    'seasonal': row['seasonal'].strip(),
                    'tlwk_code': row['tlwk_code'].strip(),
                    'footnote_codes': row.get('footnote_codes', '').strip() or None,
                    'begin_year': int(row['begin_year']) if row.get('begin_year') else None,
                    'begin_period': row.get('begin_period', '').strip() or None,
                    'end_year': end_year,
                    'end_period': row.get('end_period', '').strip() or None,
                    'is_active': is_active,
                }

    # ==================== DATA PARSER ====================

    def parse_data_file(self, filename: str) -> Iterator[Dict]:
        """Parse an LN data file (e.g., ln.data.1.AllData)"""
        filepath = self.data_dir / filename

        with open(filepath, 'r', encoding='utf-8', newline='') as f:
            # Read and clean headers (they might have extra spaces)
            headers = f.readline().strip().split('\t')
            headers = [h.strip() for h in headers]

            # Create reader with cleaned headers
            reader = csv.DictReader(f, fieldnames=headers, delimiter='\t')

            for row in reader:
                # Handle missing values represented as '-'
                value_str = row.get('value', '').strip()
                value = None if value_str in ('', '-') else float(value_str)

                yield {
                    'series_id': row['series_id'].strip(),
                    'year': int(row['year']),
                    'period': row['period'].strip(),
                    'value': value,
                    'footnote_codes': row.get('footnote_codes', '').strip() or None,
                }

    # ==================== LOADING FUNCTIONS ====================

    def load_reference_tables(self, session: Session):
        """Load all 33 LN reference tables"""

        # Define reference tables and their parsers
        ref_tables = [
            (LNLaborForceStatus, self.parse_lfst, 'lfst_code', 'Labor Force Status'),
            (LNPeriodicity, self.parse_periodicity, 'periodicity_code', 'Periodicity'),
            (LNAbsence, self.parse_absn, 'absn_code', 'Absence'),
            (LNActivity, self.parse_activity, 'activity_code', 'Activity'),
            (LNAge, self.parse_ages, 'ages_code', 'Age Groups'),
            (LNCertification, self.parse_cert, 'cert_code', 'Certification'),
            (LNClass, self.parse_class, 'class_code', 'Class of Worker'),
            (LNDuration, self.parse_duration, 'duration_code', 'Duration'),
            (LNEducation, self.parse_education, 'education_code', 'Education'),
            (LNEntrance, self.parse_entr, 'entr_code', 'Entrance to Labor Force'),
            (LNExperience, self.parse_expr, 'expr_code', 'Work Experience'),
            (LNHeadOfHousehold, self.parse_hheader, 'hheader_code', 'Head of Household'),
            (LNHour, self.parse_hour, 'hour_code', 'Hours Worked'),
            (LNIndustry, self.parse_indy, 'indy_code', 'Industry'),
            (LNJobDesire, self.parse_jdes, 'jdes_code', 'Want a Job'),
            (LNLook, self.parse_look, 'look_code', 'Job Seeker'),
            (LNMaritalStatus, self.parse_mari, 'mari_code', 'Marital Status'),
            (LNMultipleJobholder, self.parse_mjhs, 'mjhs_code', 'Multiple Jobholder'),
            (LNOccupation, self.parse_occupation, 'occupation_code', 'Occupation'),
            (LNOrigin, self.parse_orig, 'orig_code', 'Hispanic/Latino Origin'),
            (LNPercentage, self.parse_pcts, 'pcts_code', 'Percentage'),
            (LNRace, self.parse_race, 'race_code', 'Race'),
            (LNAbsenceReason, self.parse_rjnw, 'rjnw_code', 'Absence Reason'),
            (LNJobSearch, self.parse_rnlf, 'rnlf_code', 'Job Search'),
            (LNPartTimeReason, self.parse_rwns, 'rwns_code', 'Part Time Reason'),
            (LNSeek, self.parse_seek, 'seek_code', 'Job Seeker'),
            (LNSex, self.parse_sexs, 'sexs_code', 'Sex'),
            (LNDataType, self.parse_tdat, 'tdat_code', 'Data Type'),
            (LNVeteran, self.parse_vets, 'vets_code', 'Veteran Status'),
            (LNWorkStatus, self.parse_wkst, 'wkst_code', 'Work Status'),
            (LNBorn, self.parse_born, 'born_code', 'Nativity/Citizenship'),
            (LNChild, self.parse_chld, 'chld_code', 'Presence of Children'),
            (LNDisability, self.parse_disa, 'disa_code', 'Disability'),
            (LNTelework, self.parse_tlwk, 'tlwk_code', 'Telework'),
        ]

        for model, parser_func, pk_field, name in ref_tables:
            print(f"Loading {name}...")

            # Collect all data
            data = list(parser_func())

            if not data:
                print(f"  WARNING: No data found for {name}")
                continue

            # UPSERT: Insert or update on conflict
            stmt = insert(model).values(data)
            stmt = stmt.on_conflict_do_update(
                index_elements=[pk_field],
                set_={k: stmt.excluded[k] for k in data[0].keys() if k != pk_field}
            )
            session.execute(stmt)
            session.commit()

            print(f"  Loaded {len(data)} records")

        # Load series
        print("\nLoading series metadata (67K+ series)...")
        series_data = list(self.parse_series())
        print(f"  Parsed {len(series_data)} series")

        # Batch insert series
        batch_size = 5000
        for i in range(0, len(series_data), batch_size):
            batch = series_data[i:i + batch_size]
            stmt = insert(LNSeries).values(batch)
            stmt = stmt.on_conflict_do_update(
                index_elements=['series_id'],
                set_={k: stmt.excluded[k] for k in batch[0].keys() if k != 'series_id'}
            )
            session.execute(stmt)
            session.commit()
            print(f"  Loaded {min(i + batch_size, len(series_data))}/{len(series_data)} series")

        print(f"  Loaded {len(series_data)} series")

    def load_data(self, session: Session, data_files: Optional[List[str]] = None, batch_size: int = 10000):
        """Load LN time series data files"""

        if data_files is None:
            data_files = ['ln.data.1.AllData']

        for filename in data_files:
            print(f"\nLoading {filename}...")

            batch = []
            total_count = 0

            for row in self.parse_data_file(filename):
                batch.append(row)

                if len(batch) >= batch_size:
                    # UPSERT batch
                    stmt = insert(LNData).values(batch)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['series_id', 'year', 'period'],
                        set_={
                            'value': stmt.excluded.value,
                            'footnote_codes': stmt.excluded.footnote_codes,
                            'updated_at': datetime.now(UTC),
                        }
                    )
                    session.execute(stmt)
                    session.commit()

                    total_count += len(batch)
                    print(f"  Loaded {total_count:,} observations...")
                    batch = []

            # Load remaining batch
            if batch:
                stmt = insert(LNData).values(batch)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['series_id', 'year', 'period'],
                    set_={
                        'value': stmt.excluded.value,
                        'footnote_codes': stmt.excluded.footnote_codes,
                        'updated_at': datetime.now(UTC),
                    }
                )
                session.execute(stmt)
                session.commit()
                total_count += len(batch)

            print(f"  Loaded {total_count:,} observations from {filename}")


def get_all_data_files(data_dir: str) -> List[str]:
    """Get list of all LN data files"""
    import glob

    pattern = str(Path(data_dir) / "ln.data.*")
    files = glob.glob(pattern)

    return [Path(f).name for f in sorted(files)]
