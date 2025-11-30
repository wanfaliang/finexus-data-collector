"""
BEA Data Collector

Collectors for NIPA and Regional datasets from the Bureau of Economic Analysis.
Handles backfilling historical data and incremental updates.

Author: FinExus Data Collector
Created: 2025-11-26
"""
from datetime import datetime, date, UTC
from typing import Any, Dict, List, Optional, Callable
from decimal import Decimal
import logging

from sqlalchemy import func, update as sql_update, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from src.bea.bea_client import BEAClient, BEAAPIError
from src.database.bea_models import (
    BEADataset, NIPATable, NIPASeries, NIPAData,
    RegionalTable, RegionalLineCode, RegionalGeoFips, RegionalData,
    GDPSummary, PersonalIncomeSummary,
    GDPByIndustryTable, GDPByIndustryIndustry, GDPByIndustryData
)
from src.database.bea_tracking_models import (
    BEAAPIUsageLog, BEADatasetFreshness, BEATableUpdateStatus,
    BEASentinelSeries, BEACollectionRun
)

log = logging.getLogger("BEACollector")


# ===================== Year Specification Helper ===================== #

def convert_year_spec(year_spec: str) -> str:
    """
    Convert year specification to actual years.

    Args:
        year_spec: Year specification ('ALL', 'LAST5', 'LAST10', or comma-separated years)

    Returns:
        Converted year string suitable for BEA API
    """
    if year_spec == 'ALL':
        return 'ALL'

    current_year = datetime.now().year

    if year_spec == 'LAST5':
        years = list(range(current_year - 4, current_year + 1))
        return ','.join(str(y) for y in years)

    if year_spec == 'LAST10':
        years = list(range(current_year - 9, current_year + 1))
        return ','.join(str(y) for y in years)

    # Otherwise, assume it's already comma-separated years or a single year
    return year_spec


# ===================== Progress Tracking ===================== #

class CollectionProgress:
    """Progress tracking for BEA data collection"""

    def __init__(self, dataset_name: str, run_type: str, total_tables: int = 0):
        self.dataset_name = dataset_name
        self.run_type = run_type  # 'backfill', 'update', 'refresh'
        self.total_tables = total_tables
        self.tables_processed = 0
        self.series_processed = 0
        self.data_points_inserted = 0
        self.data_points_updated = 0
        self.api_requests = 0
        self.errors: List[str] = []
        self.start_time = datetime.now(UTC)
        self.end_time: Optional[datetime] = None
        self.run_id: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        duration = (self.end_time or datetime.now(UTC)) - self.start_time
        return {
            'dataset_name': self.dataset_name,
            'run_type': self.run_type,
            'run_id': self.run_id,
            'total_tables': self.total_tables,
            'tables_processed': self.tables_processed,
            'series_processed': self.series_processed,
            'data_points_inserted': self.data_points_inserted,
            'data_points_updated': self.data_points_updated,
            'api_requests': self.api_requests,
            'progress_pct': (self.tables_processed / self.total_tables * 100) if self.total_tables > 0 else 0,
            'errors': self.errors,
            'duration_seconds': duration.total_seconds(),
        }


# ===================== API Usage Tracking ===================== #

def record_api_usage(
    session: Session,
    dataset_name: str,
    method_name: str,
    http_status: int,
    response_time_ms: int,
    data_bytes: int,
    is_error: bool = False,
    script_name: str = 'bea_collector'
):
    """Record an API request in the usage log"""
    now = datetime.now(UTC)
    log_entry = BEAAPIUsageLog(
        usage_date=now.date(),
        usage_minute=now.replace(second=0, microsecond=0),
        requests_count=1,
        data_bytes=data_bytes,
        error_count=1 if is_error else 0,
        dataset_name=dataset_name,
        method_name=method_name,
        http_status=http_status,
        response_time_ms=response_time_ms,
        script_name=script_name,
        created_at=now,
    )
    session.add(log_entry)
    session.commit()


def start_collection_run(
    session: Session,
    dataset_name: str,
    run_type: str,
    frequency: Optional[str] = None,
    geo_scope: Optional[str] = None,
    year_spec: Optional[str] = None,
    tables_filter: Optional[List[str]] = None
) -> int:
    """Start a collection run and return the run ID"""
    import json
    run = BEACollectionRun(
        dataset_name=dataset_name,
        run_type=run_type,
        frequency=frequency,
        geo_scope=geo_scope,
        year_spec=year_spec,
        started_at=datetime.now(UTC),
        status='running',
        tables_filter=json.dumps(tables_filter) if tables_filter else None,
    )
    session.add(run)
    session.commit()
    return run.run_id


def complete_collection_run(
    session: Session,
    run_id: int,
    progress: CollectionProgress,
    status: str = 'completed'
):
    """Update a collection run with final statistics"""
    session.execute(
        sql_update(BEACollectionRun).where(
            BEACollectionRun.run_id == run_id
        ).values(
            completed_at=datetime.now(UTC),
            status=status,
            error_message='; '.join(progress.errors) if progress.errors else None,
            tables_processed=progress.tables_processed,
            series_processed=progress.series_processed,
            data_points_inserted=progress.data_points_inserted,
            data_points_updated=progress.data_points_updated,
            api_requests_made=progress.api_requests,
        )
    )
    session.commit()


# ===================== NIPA Collector ===================== #

class NIPACollector:
    """Collector for NIPA (National Income and Product Accounts) data"""

    def __init__(self, client: BEAClient, session: Session):
        self.client = client
        self.session = session

    def sync_tables_catalog(self) -> int:
        """
        Sync NIPA table catalog from BEA API.

        Returns:
            Number of tables synced
        """
        log.info("Syncing NIPA tables catalog...")
        tables = self.client.get_nipa_tables()

        now = datetime.now(UTC)
        count = 0

        for t in tables:
            table_name = t.get('TableName', '')
            if not table_name:
                continue

            # Parse frequency support from Description
            # Format: "Table 1.1.1. Percent Change... (A) (Q)" or "... (A) (Q) (M)"
            description = t.get('Description', '')
            has_annual = '(A)' in description
            has_quarterly = '(Q)' in description
            has_monthly = '(M)' in description

            # Default to annual if no frequency indicators found
            if not has_annual and not has_quarterly and not has_monthly:
                has_annual = True

            stmt = insert(NIPATable).values(
                table_name=table_name,
                table_description=description,
                has_annual=has_annual,
                has_quarterly=has_quarterly,
                has_monthly=has_monthly,
                is_active=True,
                created_at=now,
                updated_at=now,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=['table_name'],
                set_={
                    'table_description': stmt.excluded.table_description,
                    'has_annual': has_annual,
                    'has_quarterly': has_quarterly,
                    'has_monthly': has_monthly,
                    'updated_at': now,
                }
            )
            self.session.execute(stmt)
            count += 1

        self.session.commit()
        log.info(f"Synced {count} NIPA tables")
        return count

    def collect_table_data(
        self,
        table_name: str,
        frequency: str = 'A',
        year: str = 'ALL',
        progress: Optional[CollectionProgress] = None
    ) -> Dict[str, int]:
        """
        Collect data for a single NIPA table.

        Args:
            table_name: NIPA table name (e.g., 'T10101')
            frequency: 'A' (annual), 'Q' (quarterly), 'M' (monthly)
            year: Year specification ('ALL', 'LAST5', 'LAST10', or comma-separated)
            progress: Optional progress tracker

        Returns:
            Dict with 'series_count' and 'data_points' counts
        """
        # Convert year specification to actual years
        actual_year = convert_year_spec(year)
        log.info(f"Collecting NIPA table {table_name} ({frequency}, {actual_year})...")

        result = self.client.get_nipa_data(
            table_name=table_name,
            frequency=frequency,
            year=actual_year,
        )

        if progress:
            progress.api_requests += 1

        data = self.client._extract_data(result)

        if not data:
            log.warning(f"No data returned for NIPA table {table_name}")
            return {'series_count': 0, 'data_points': 0}

        now = datetime.now(UTC)
        series_seen = set()
        data_points = 0

        for row in data:
            series_code = row.get('SeriesCode', '')
            if not series_code:
                continue

            # Upsert series if not seen yet
            if series_code not in series_seen:
                series_seen.add(series_code)

                stmt = insert(NIPASeries).values(
                    series_code=series_code,
                    table_name=table_name,
                    line_number=int(row.get('LineNumber', 0)),
                    line_description=row.get('LineDescription', ''),
                    metric_name=row.get('METRIC_NAME', ''),
                    cl_unit=row.get('CL_UNIT', ''),
                    unit_mult=int(row.get('UNIT_MULT', 0)) if row.get('UNIT_MULT') else None,
                    is_active=True,
                    created_at=now,
                    updated_at=now,
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=['series_code'],
                    set_={
                        'line_description': stmt.excluded.line_description,
                        'metric_name': stmt.excluded.metric_name,
                        'cl_unit': stmt.excluded.cl_unit,
                        'unit_mult': stmt.excluded.unit_mult,
                        'updated_at': now,
                    }
                )
                self.session.execute(stmt)

            # Parse time period
            time_period = row.get('TimePeriod', '')

            # Parse value
            value_str = row.get('DataValue', '')
            try:
                # Handle values with commas
                value = Decimal(value_str.replace(',', '')) if value_str and value_str not in ('', 'ND', '(ND)') else None
            except:
                value = None

            # Upsert data point
            stmt = insert(NIPAData).values(
                series_code=series_code,
                time_period=time_period,
                value=value,
                note_ref=row.get('NoteRef', ''),
                created_at=now,
                updated_at=now,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=['series_code', 'time_period'],
                set_={
                    'value': stmt.excluded.value,
                    'note_ref': stmt.excluded.note_ref,
                    'updated_at': now,
                }
            )
            self.session.execute(stmt)
            data_points += 1

        self.session.commit()

        if progress:
            progress.series_processed += len(series_seen)
            progress.data_points_inserted += data_points

        log.info(f"Collected {len(series_seen)} series, {data_points} data points for {table_name}")
        return {'series_count': len(series_seen), 'data_points': data_points}

    def backfill_all_tables(
        self,
        frequency: str = 'A',
        year: str = 'ALL',
        tables: Optional[List[str]] = None,
        progress_callback: Optional[Callable[[CollectionProgress], None]] = None,
        run_id: Optional[int] = None
    ) -> CollectionProgress:
        """
        Backfill all NIPA tables (or specified subset).

        Args:
            frequency: 'A', 'Q', or 'M'
            year: Year specification
            tables: Optional list of specific tables to backfill
            progress_callback: Optional callback for progress updates
            run_id: Optional existing run_id (if called from task_runner)

        Returns:
            CollectionProgress with results
        """
        # Sync tables catalog first to ensure frequency flags are up to date
        self.sync_tables_catalog()

        # Get list of tables filtered by frequency support
        if tables:
            table_list = tables
        else:
            # Build query with frequency filter
            query = self.session.query(NIPATable.table_name).filter(
                NIPATable.is_active == True
            )
            # Filter by frequency support
            if frequency == 'A':
                query = query.filter(NIPATable.has_annual == True)
            elif frequency == 'Q':
                query = query.filter(NIPATable.has_quarterly == True)
            elif frequency == 'M':
                query = query.filter(NIPATable.has_monthly == True)

            result = query.all()
            table_list = [r[0] for r in result]

        progress = CollectionProgress('NIPA', 'backfill', len(table_list))

        # Use existing run_id or create a new one
        if run_id:
            progress.run_id = run_id
        else:
            progress.run_id = start_collection_run(
                self.session, 'NIPA', 'backfill',
                frequency=frequency,
                year_spec=year,
                tables_filter=tables
            )

        # Skip archival tables (ending with letters like A, B, C) when not fetching ALL years
        # These tables have historical data only and cause errors with LAST5/LAST10
        skip_archival = year != 'ALL'
        archival_suffixes = ('A', 'B', 'C', 'D', 'E')

        log.info(f"Starting NIPA backfill for {len(table_list)} tables ({frequency}, {year})...")

        for table_name in table_list:
            # Skip archival tables when using recent years
            if skip_archival and table_name and table_name[-1] in archival_suffixes:
                log.debug(f"Skipping archival table {table_name} (year={year})")
                continue

            try:
                stats = self.collect_table_data(
                    table_name=table_name,
                    frequency=frequency,
                    year=year,
                    progress=progress,
                )
                progress.tables_processed += 1

                # Update table status
                self._update_table_status(table_name, stats)

                if progress_callback:
                    progress_callback(progress)

            except BEAAPIError as e:
                error_msg = f"Table {table_name}: {str(e)}"
                log.error(error_msg)
                progress.errors.append(error_msg)

                # Check if rate limited
                if 'rate' in str(e).lower() or '429' in str(e):
                    log.warning("Rate limited, stopping collection")
                    break

            except Exception as e:
                error_msg = f"Table {table_name}: {str(e)}"
                log.error(error_msg)
                progress.errors.append(error_msg)
                self.session.rollback()

        progress.end_time = datetime.now(UTC)

        # Complete collection run
        status = 'completed' if not progress.errors else 'partial'
        complete_collection_run(self.session, progress.run_id, progress, status)

        # Update dataset freshness
        self._update_freshness(progress)

        return progress

    def _update_table_status(self, table_name: str, stats: Dict[str, int]):
        """Update table status after collection"""
        now = datetime.now(UTC)

        # Get latest data period
        latest = self.session.query(
            func.max(NIPAData.time_period)
        ).join(NIPASeries).filter(
            NIPASeries.table_name == table_name
        ).scalar()

        # Parse year from time period (e.g., '2023' or '2023Q4')
        last_year = None
        last_period = None
        if latest:
            if len(latest) == 4:
                last_year = int(latest)
                last_period = 'A'
            elif 'Q' in latest:
                last_year = int(latest[:4])
                last_period = latest[4:]
            elif 'M' in latest:
                last_year = int(latest[:4])
                last_period = latest[4:]

        stmt = insert(BEATableUpdateStatus).values(
            dataset_name='NIPA',
            table_name=table_name,
            last_data_year=last_year,
            last_data_period=last_period,
            last_checked_at=now,
            last_updated_at=now,
            is_current=True,
            rows_in_table=stats.get('data_points', 0),
            created_at=now,
            updated_at=now,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=['dataset_name', 'table_name'],
            set_={
                'last_data_year': stmt.excluded.last_data_year,
                'last_data_period': stmt.excluded.last_data_period,
                'last_checked_at': now,
                'last_updated_at': now,
                'is_current': True,
                'rows_in_table': stmt.excluded.rows_in_table,
                'updated_at': now,
            }
        )
        self.session.execute(stmt)
        self.session.commit()

    def _update_freshness(self, progress: CollectionProgress):
        """Update dataset freshness tracking"""
        now = datetime.now(UTC)

        # Get latest data year from database
        latest_year = self.session.query(func.max(NIPAData.time_period)).scalar()
        latest_data_year = int(latest_year[:4]) if latest_year else None

        # Count statistics
        tables_count = self.session.query(func.count(NIPATable.table_name)).filter(
            NIPATable.is_active == True
        ).scalar() or 0

        series_count = self.session.query(func.count(NIPASeries.series_code)).filter(
            NIPASeries.is_active == True
        ).scalar() or 0

        data_count = self.session.query(func.count()).select_from(NIPAData).scalar() or 0

        stmt = insert(BEADatasetFreshness).values(
            dataset_name='NIPA',
            latest_data_year=latest_data_year,
            last_checked_at=now,
            last_bea_update_detected=now if progress.data_points_inserted > 0 else None,
            needs_update=False,
            update_in_progress=False,
            last_update_completed=now,
            tables_count=tables_count,
            series_count=series_count,
            data_points_count=data_count,
            total_checks=1,
            total_updates_detected=1 if progress.data_points_inserted > 0 else 0,
            created_at=now,
            updated_at=now,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=['dataset_name'],
            set_={
                'latest_data_year': stmt.excluded.latest_data_year,
                'last_checked_at': now,
                'last_bea_update_detected': stmt.excluded.last_bea_update_detected,
                'needs_update': False,
                'update_in_progress': False,
                'last_update_completed': now,
                'tables_count': stmt.excluded.tables_count,
                'series_count': stmt.excluded.series_count,
                'data_points_count': stmt.excluded.data_points_count,
                'total_checks': BEADatasetFreshness.total_checks + 1,
                'total_updates_detected': BEADatasetFreshness.total_updates_detected + (1 if progress.data_points_inserted > 0 else 0),
                'updated_at': now,
            }
        )
        self.session.execute(stmt)
        self.session.commit()


# ===================== Regional Collector ===================== #

class RegionalCollector:
    """Collector for Regional economic data"""

    def __init__(self, client: BEAClient, session: Session):
        self.client = client
        self.session = session

    def sync_tables_catalog(self) -> int:
        """Sync Regional table catalog from BEA API"""
        log.info("Syncing Regional tables catalog...")
        tables = self.client.get_regional_tables()

        now = datetime.now(UTC)
        count = 0

        for t in tables:
            # Regional API returns 'Key' instead of 'TableName'
            table_name = t.get('Key') or t.get('TableName', '')
            if not table_name:
                continue

            stmt = insert(RegionalTable).values(
                table_name=table_name,
                table_description=t.get('Desc') or t.get('Description', ''),
                is_active=True,
                created_at=now,
                updated_at=now,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=['table_name'],
                set_={
                    'table_description': stmt.excluded.table_description,
                    'updated_at': now,
                }
            )
            self.session.execute(stmt)
            count += 1

        self.session.commit()
        log.info(f"Synced {count} Regional tables")
        return count

    def sync_line_codes(self, table_name: str) -> int:
        """Sync line codes for a Regional table"""
        log.info(f"Syncing line codes for {table_name}...")
        line_codes = self.client.get_regional_line_codes(table_name)

        now = datetime.now(UTC)
        count = 0

        for lc in line_codes:
            # Regional API returns 'Key' instead of 'LineCode'
            line_code = lc.get('Key') or lc.get('LineCode')
            if line_code is None:
                continue

            stmt = insert(RegionalLineCode).values(
                table_name=table_name,
                line_code=int(line_code),
                line_description=lc.get('Desc') or lc.get('Description', ''),
                created_at=now,
            )
            stmt = stmt.on_conflict_do_nothing()
            self.session.execute(stmt)
            count += 1

        self.session.commit()
        log.info(f"Synced {count} line codes for {table_name}")
        return count

    def sync_geo_fips(self, table_name: str) -> int:
        """Sync geographic FIPS codes for a Regional table"""
        log.info(f"Syncing geo FIPS codes for {table_name}...")
        geo_fips_list = self.client.get_regional_geo_fips(table_name)

        now = datetime.now(UTC)
        count = 0

        for geo in geo_fips_list:
            geo_fips = geo.get('GeoFips', '')
            if not geo_fips:
                continue

            # Determine geo type from FIPS code
            geo_type = self._classify_geo_fips(geo_fips)

            stmt = insert(RegionalGeoFips).values(
                geo_fips=geo_fips,
                geo_name=geo.get('GeoName', ''),
                geo_type=geo_type,
                created_at=now,
                updated_at=now,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=['geo_fips'],
                set_={
                    'geo_name': stmt.excluded.geo_name,
                    'geo_type': stmt.excluded.geo_type,
                    'updated_at': now,
                }
            )
            self.session.execute(stmt)
            count += 1

        self.session.commit()
        log.info(f"Synced {count} geo FIPS codes for {table_name}")
        return count

    def _classify_geo_fips(self, geo_fips: str) -> str:
        """Classify geographic FIPS code type"""
        if geo_fips == '00000':
            return 'Nation'
        elif len(geo_fips) == 5 and geo_fips.endswith('000'):
            return 'State'
        elif len(geo_fips) == 5:
            return 'County'
        elif geo_fips.startswith('M'):
            return 'MSA'
        elif geo_fips.startswith('R'):
            return 'Region'
        else:
            return 'Other'

    def collect_table_data(
        self,
        table_name: str,
        line_code: int,
        geo_fips: str = 'STATE',
        year: str = 'ALL',
        progress: Optional[CollectionProgress] = None
    ) -> Dict[str, int]:
        """
        Collect data for a Regional table/line_code combination.

        Args:
            table_name: Regional table name
            line_code: Line code for specific statistic
            geo_fips: Geographic scope ('STATE', 'COUNTY', 'MSA', or specific FIPS)
            year: Year specification
            progress: Optional progress tracker

        Returns:
            Dict with 'data_points' count
        """
        # Convert year specification to actual years
        actual_year = convert_year_spec(year)
        log.info(f"Collecting Regional {table_name}, line {line_code}, geo {geo_fips}, year {actual_year}...")

        result = self.client.get_regional_data(
            table_name=table_name,
            line_code=line_code,
            geo_fips=geo_fips,
            year=actual_year,
        )

        if progress:
            progress.api_requests += 1

        data = self.client._extract_data(result)

        if not data:
            log.warning(f"No data returned for {table_name}/{line_code}/{geo_fips}")
            return {'data_points': 0}

        now = datetime.now(UTC)
        data_points = 0
        geo_fips_seen = set()

        for row in data:
            row_geo_fips = row.get('GeoFips', '')
            if not row_geo_fips:
                continue

            # Ensure geo FIPS exists in reference table
            if row_geo_fips not in geo_fips_seen:
                geo_fips_seen.add(row_geo_fips)
                geo_type = self._classify_geo_fips(row_geo_fips)

                stmt = insert(RegionalGeoFips).values(
                    geo_fips=row_geo_fips,
                    geo_name=row.get('GeoName', ''),
                    geo_type=geo_type,
                    created_at=now,
                    updated_at=now,
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=['geo_fips'],
                    set_={
                        'geo_name': stmt.excluded.geo_name,
                        'updated_at': now,
                    }
                )
                self.session.execute(stmt)

            # Ensure line code exists
            stmt = insert(RegionalLineCode).values(
                table_name=table_name,
                line_code=line_code,
                line_description=row.get('Description', ''),
                cl_unit=row.get('CL_UNIT', ''),
                unit_mult=int(row.get('UNIT_MULT', 0)) if row.get('UNIT_MULT') else None,
                created_at=now,
            )
            stmt = stmt.on_conflict_do_nothing()
            self.session.execute(stmt)

            # Parse time period (year)
            time_period = row.get('TimePeriod', '')

            # Parse value
            value_str = row.get('DataValue', '')
            try:
                value = Decimal(value_str.replace(',', '')) if value_str and value_str not in ('', 'NA', '(NA)', '(D)') else None
            except:
                value = None

            # Upsert data point
            stmt = insert(RegionalData).values(
                table_name=table_name,
                line_code=line_code,
                geo_fips=row_geo_fips,
                time_period=time_period,
                value=value,
                cl_unit=row.get('CL_UNIT', ''),
                unit_mult=int(row.get('UNIT_MULT', 0)) if row.get('UNIT_MULT') else None,
                note_ref=row.get('NoteRef', ''),
                created_at=now,
                updated_at=now,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=['table_name', 'line_code', 'geo_fips', 'time_period'],
                set_={
                    'value': stmt.excluded.value,
                    'cl_unit': stmt.excluded.cl_unit,
                    'unit_mult': stmt.excluded.unit_mult,
                    'note_ref': stmt.excluded.note_ref,
                    'updated_at': now,
                }
            )
            self.session.execute(stmt)
            data_points += 1

        self.session.commit()

        if progress:
            progress.data_points_inserted += data_points

        log.info(f"Collected {data_points} data points for {table_name}/{line_code}")
        return {'data_points': data_points}

    def backfill_table(
        self,
        table_name: str,
        geo_fips: str = 'STATE',
        year: str = 'ALL',
        progress: Optional[CollectionProgress] = None
    ) -> Dict[str, int]:
        """
        Backfill all line codes for a Regional table.

        Args:
            table_name: Regional table name
            geo_fips: Geographic scope
            year: Year specification
            progress: Optional progress tracker

        Returns:
            Dict with 'line_codes' and 'data_points' counts
        """
        # Sync line codes first
        self.sync_line_codes(table_name)

        # Get all line codes for this table
        line_codes = self.session.query(RegionalLineCode.line_code).filter(
            RegionalLineCode.table_name == table_name
        ).all()
        line_codes = [lc[0] for lc in line_codes]

        total_points = 0

        for line_code in line_codes:
            try:
                stats = self.collect_table_data(
                    table_name=table_name,
                    line_code=line_code,
                    geo_fips=geo_fips,
                    year=year,
                    progress=progress,
                )
                total_points += stats['data_points']

            except BEAAPIError as e:
                log.error(f"Error collecting {table_name}/{line_code}: {e}")
                if progress:
                    progress.errors.append(f"{table_name}/{line_code}: {str(e)}")

                # Check if rate limited
                if 'rate' in str(e).lower() or '429' in str(e):
                    raise

        return {'line_codes': len(line_codes), 'data_points': total_points}

    def backfill_all_tables(
        self,
        geo_fips: str = 'STATE',
        year: str = 'ALL',
        tables: Optional[List[str]] = None,
        progress_callback: Optional[Callable[[CollectionProgress], None]] = None,
        run_id: Optional[int] = None
    ) -> CollectionProgress:
        """
        Backfill all Regional tables (or specified subset).

        Args:
            geo_fips: Geographic scope ('STATE', 'COUNTY', 'MSA')
            year: Year specification
            tables: Optional list of specific tables
            progress_callback: Optional callback for progress updates
            run_id: Optional existing run_id (if called from task_runner)

        Returns:
            CollectionProgress with results
        """
        # Get list of tables
        if tables:
            table_list = tables
        else:
            result = self.session.query(RegionalTable.table_name).filter(
                RegionalTable.is_active == True
            ).all()
            table_list = [r[0] for r in result]

            if not table_list:
                self.sync_tables_catalog()
                result = self.session.query(RegionalTable.table_name).filter(
                    RegionalTable.is_active == True
                ).all()
                table_list = [r[0] for r in result]

        progress = CollectionProgress('Regional', 'backfill', len(table_list))

        # Use existing run_id or create a new one
        if run_id:
            progress.run_id = run_id
        else:
            progress.run_id = start_collection_run(
                self.session, 'Regional', 'backfill',
                geo_scope=geo_fips,
                year_spec=year,
                tables_filter=tables
            )

        log.info(f"Starting Regional backfill for {len(table_list)} tables (geo={geo_fips}, {year})...")

        for table_name in table_list:
            try:
                stats = self.backfill_table(
                    table_name=table_name,
                    geo_fips=geo_fips,
                    year=year,
                    progress=progress,
                )
                progress.tables_processed += 1

                # Update table status
                self._update_table_status(table_name, stats)

                if progress_callback:
                    progress_callback(progress)

            except BEAAPIError as e:
                error_msg = f"Table {table_name}: {str(e)}"
                log.error(error_msg)
                progress.errors.append(error_msg)

                if 'rate' in str(e).lower() or '429' in str(e):
                    log.warning("Rate limited, stopping collection")
                    break

            except Exception as e:
                error_msg = f"Table {table_name}: {str(e)}"
                log.error(error_msg)
                progress.errors.append(error_msg)
                self.session.rollback()

        progress.end_time = datetime.now(UTC)

        # Complete collection run
        status = 'completed' if not progress.errors else 'partial'
        complete_collection_run(self.session, progress.run_id, progress, status)

        # Update dataset freshness
        self._update_freshness(progress)

        return progress

    def _update_table_status(self, table_name: str, stats: Dict[str, int]):
        """Update table status after collection"""
        now = datetime.now(UTC)

        latest = self.session.query(
            func.max(RegionalData.time_period)
        ).filter(
            RegionalData.table_name == table_name
        ).scalar()

        last_year = int(latest) if latest and latest.isdigit() else None

        stmt = insert(BEATableUpdateStatus).values(
            dataset_name='Regional',
            table_name=table_name,
            last_data_year=last_year,
            last_checked_at=now,
            last_updated_at=now,
            is_current=True,
            rows_in_table=stats.get('data_points', 0),
            created_at=now,
            updated_at=now,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=['dataset_name', 'table_name'],
            set_={
                'last_data_year': stmt.excluded.last_data_year,
                'last_checked_at': now,
                'last_updated_at': now,
                'is_current': True,
                'rows_in_table': stmt.excluded.rows_in_table,
                'updated_at': now,
            }
        )
        self.session.execute(stmt)
        self.session.commit()

    def _update_freshness(self, progress: CollectionProgress):
        """Update dataset freshness tracking"""
        now = datetime.now(UTC)

        latest_year = self.session.query(func.max(RegionalData.time_period)).scalar()
        latest_data_year = int(latest_year) if latest_year and latest_year.isdigit() else None

        tables_count = self.session.query(func.count(RegionalTable.table_name)).filter(
            RegionalTable.is_active == True
        ).scalar() or 0

        line_codes_count = self.session.query(func.count()).select_from(RegionalLineCode).scalar() or 0
        data_count = self.session.query(func.count()).select_from(RegionalData).scalar() or 0

        stmt = insert(BEADatasetFreshness).values(
            dataset_name='Regional',
            latest_data_year=latest_data_year,
            last_checked_at=now,
            last_bea_update_detected=now if progress.data_points_inserted > 0 else None,
            needs_update=False,
            update_in_progress=False,
            last_update_completed=now,
            tables_count=tables_count,
            series_count=line_codes_count,
            data_points_count=data_count,
            total_checks=1,
            total_updates_detected=1 if progress.data_points_inserted > 0 else 0,
            created_at=now,
            updated_at=now,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=['dataset_name'],
            set_={
                'latest_data_year': stmt.excluded.latest_data_year,
                'last_checked_at': now,
                'last_bea_update_detected': stmt.excluded.last_bea_update_detected,
                'needs_update': False,
                'update_in_progress': False,
                'last_update_completed': now,
                'tables_count': stmt.excluded.tables_count,
                'series_count': stmt.excluded.series_count,
                'data_points_count': stmt.excluded.data_points_count,
                'total_checks': BEADatasetFreshness.total_checks + 1,
                'total_updates_detected': BEADatasetFreshness.total_updates_detected + (1 if progress.data_points_inserted > 0 else 0),
                'updated_at': now,
            }
        )
        self.session.execute(stmt)
        self.session.commit()


# ===================== GDP by Industry Collector ===================== #

class GDPByIndustryCollector:
    """Collector for GDP by Industry data"""

    def __init__(self, client: BEAClient, session: Session):
        self.client = client
        self.session = session

    def sync_tables_catalog(self) -> int:
        """
        Sync GDP by Industry table catalog from BEA API.

        Returns:
            Number of tables synced
        """
        log.info("Syncing GDP by Industry tables catalog...")
        tables = self.client.get_gdpbyindustry_tables()

        now = datetime.now(UTC)
        count = 0

        for t in tables:
            # API returns 'Key' for TableID and 'Desc' for description
            table_id = t.get('Key') or t.get('TableID')
            if table_id is None:
                continue

            try:
                table_id = int(table_id)
            except (ValueError, TypeError):
                continue

            description = t.get('Desc') or t.get('Description', '')
            # Parse description to detect frequency support
            # Format is like "Value Added by Industry (A) (Q)" or "Components of Value Added by Industry (A)"
            has_annual = '(A)' in description
            has_quarterly = '(Q)' in description

            stmt = insert(GDPByIndustryTable).values(
                table_id=table_id,
                table_description=description,
                has_annual=has_annual if has_annual else True,  # Default to True if not specified
                has_quarterly=has_quarterly,  # Only True if (Q) in description
                is_active=True,
                created_at=now,
                updated_at=now,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=['table_id'],
                set_={
                    'table_description': stmt.excluded.table_description,
                    'has_annual': has_annual if has_annual else True,
                    'has_quarterly': has_quarterly,
                    'updated_at': now,
                }
            )
            self.session.execute(stmt)
            count += 1

        self.session.commit()
        log.info(f"Synced {count} GDP by Industry tables")
        return count

    def sync_industries_catalog(self) -> int:
        """
        Sync industry codes catalog from BEA API.

        Returns:
            Number of industries synced
        """
        log.info("Syncing GDP by Industry industries catalog...")
        industries = self.client.get_gdpbyindustry_industries()

        now = datetime.now(UTC)
        count = 0

        for ind in industries:
            industry_code = ind.get('Key') or ind.get('Industry')
            if not industry_code:
                continue

            stmt = insert(GDPByIndustryIndustry).values(
                industry_code=str(industry_code),
                industry_description=ind.get('Desc') or ind.get('Description', ''),
                is_active=True,
                created_at=now,
                updated_at=now,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=['industry_code'],
                set_={
                    'industry_description': stmt.excluded.industry_description,
                    'updated_at': now,
                }
            )
            self.session.execute(stmt)
            count += 1

        self.session.commit()
        log.info(f"Synced {count} industries")
        return count

    def collect_table_data(
        self,
        table_id: int,
        frequency: str = 'A',
        year: str = 'ALL',
        industry: str = 'ALL',
        progress: Optional[CollectionProgress] = None
    ) -> Dict[str, int]:
        """
        Collect data for a single GDP by Industry table.

        Args:
            table_id: Table ID
            frequency: 'A' (annual) or 'Q' (quarterly)
            year: Year specification ('ALL' or comma-separated years)
            industry: Industry code ('ALL' or comma-separated codes)
            progress: Optional progress tracker

        Returns:
            Dict with 'industries_count' and 'data_points' counts
        """
        # Convert year specification to actual years
        actual_year = convert_year_spec(year)
        log.info(f"Collecting GDP by Industry table {table_id} ({frequency}, {actual_year})...")

        result = self.client.get_gdpbyindustry_data(
            table_id=table_id,
            frequency=frequency,
            year=actual_year,
            industry=industry,
        )

        if progress:
            progress.api_requests += 1

        data = self.client._extract_data(result)

        if not data:
            log.warning(f"No data returned for GDP by Industry table {table_id}")
            return {'industries_count': 0, 'data_points': 0}

        now = datetime.now(UTC)
        industries_seen = set()
        data_points = 0

        for row in data:
            industry_code = row.get('Industry', '')
            if not industry_code:
                continue

            # Ensure industry exists in reference table
            if industry_code not in industries_seen:
                industries_seen.add(industry_code)

                stmt = insert(GDPByIndustryIndustry).values(
                    industry_code=industry_code,
                    industry_description=row.get('IndusrtyDescription') or row.get('IndustryDescription', ''),
                    is_active=True,
                    created_at=now,
                    updated_at=now,
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=['industry_code'],
                    set_={
                        'industry_description': stmt.excluded.industry_description,
                        'updated_at': now,
                    }
                )
                self.session.execute(stmt)

            # Parse time period from Year and Quarter fields
            # GDPbyIndustry API returns 'Year' and 'Quarter', not 'TimePeriod'
            # Quarter can be numeric ('1', '2') or Roman numeral ('I', 'II', 'III', 'IV')
            year_val = row.get('Year', '')
            quarter_val = row.get('Quarter', '')

            # Convert Roman numerals to Arabic numerals
            roman_to_arabic = {'I': '1', 'II': '2', 'III': '3', 'IV': '4'}
            if quarter_val in roman_to_arabic:
                quarter_val = roman_to_arabic[quarter_val]

            if frequency == 'Q' and quarter_val and quarter_val != year_val:
                # Quarterly data: format as "2024Q1"
                time_period = f"{year_val}Q{quarter_val}"
            else:
                # Annual data: just the year
                time_period = str(year_val)

            # Determine row_type from IndustrYDescription
            # Tables 6 & 7 have multiple rows per industry:
            # - 'total' for the industry total (description matches industry name)
            # - 'compensation' for "Compensation of employees"
            # - 'taxes' for "Taxes on production and imports less subsidies"
            # - 'surplus' for "Gross operating surplus"
            industry_desc = row.get('IndustrYDescription') or row.get('IndustryDescription', '')
            row_type = 'total'  # default
            if industry_desc == 'Compensation of employees':
                row_type = 'compensation'
            elif industry_desc == 'Taxes on production and imports less subsidies':
                row_type = 'taxes'
            elif industry_desc == 'Gross operating surplus':
                row_type = 'surplus'

            # Parse value
            value_str = row.get('DataValue', '')
            try:
                value = Decimal(value_str.replace(',', '')) if value_str and value_str not in ('', 'ND', '(ND)', 'NA', '(NA)') else None
            except:
                value = None

            # Upsert data point
            stmt = insert(GDPByIndustryData).values(
                table_id=table_id,
                industry_code=industry_code,
                frequency=frequency,
                time_period=time_period,
                row_type=row_type,
                value=value,
                table_description=row.get('TableName', ''),
                industry_description=industry_desc,
                cl_unit=row.get('CL_UNIT', ''),
                unit_mult=int(row.get('UNIT_MULT', 0)) if row.get('UNIT_MULT') else None,
                note_ref=row.get('NoteRef', ''),
                created_at=now,
                updated_at=now,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=['table_id', 'industry_code', 'frequency', 'time_period', 'row_type'],
                set_={
                    'value': stmt.excluded.value,
                    'table_description': stmt.excluded.table_description,
                    'industry_description': stmt.excluded.industry_description,
                    'cl_unit': stmt.excluded.cl_unit,
                    'unit_mult': stmt.excluded.unit_mult,
                    'note_ref': stmt.excluded.note_ref,
                    'updated_at': now,
                }
            )
            self.session.execute(stmt)
            data_points += 1

        self.session.commit()

        if progress:
            progress.series_processed += len(industries_seen)
            progress.data_points_inserted += data_points

        log.info(f"Collected {len(industries_seen)} industries, {data_points} data points for table {table_id}")
        return {'industries_count': len(industries_seen), 'data_points': data_points}

    def backfill_all_tables(
        self,
        frequency: str = 'A',
        year: str = 'ALL',
        tables: Optional[List[int]] = None,
        progress_callback: Optional[Callable[[CollectionProgress], None]] = None,
        run_id: Optional[int] = None
    ) -> CollectionProgress:
        """
        Backfill all GDP by Industry tables (or specified subset).

        Args:
            frequency: 'A' (annual) or 'Q' (quarterly)
            year: Year specification
            tables: Optional list of specific table IDs to backfill
            progress_callback: Optional callback for progress updates
            run_id: Optional existing run_id (if called from task_runner)

        Returns:
            CollectionProgress with results
        """
        # Sync catalogs first
        self.sync_tables_catalog()
        self.sync_industries_catalog()

        # Get list of tables
        if tables:
            table_list = tables
        else:
            # Filter tables based on frequency support
            query = self.session.query(GDPByIndustryTable.table_id).filter(
                GDPByIndustryTable.is_active == True
            )
            if frequency == 'Q':
                query = query.filter(GDPByIndustryTable.has_quarterly == True)
            result = query.all()
            table_list = [r[0] for r in result]

        progress = CollectionProgress('GDPbyIndustry', 'backfill', len(table_list))

        # Use existing run_id or create a new one
        if run_id:
            progress.run_id = run_id
        else:
            progress.run_id = start_collection_run(
                self.session, 'GDPbyIndustry', 'backfill',
                frequency=frequency,
                year_spec=year,
                tables_filter=[str(t) for t in tables] if tables else None
            )

        log.info(f"Starting GDP by Industry backfill for {len(table_list)} tables ({frequency}, {year})...")

        for table_id in table_list:
            try:
                # Check if table supports the requested frequency
                if frequency == 'Q':
                    table_info = self.session.query(GDPByIndustryTable).filter(
                        GDPByIndustryTable.table_id == table_id
                    ).first()
                    if table_info and not table_info.has_quarterly:
                        log.debug(f"Skipping table {table_id}: does not support quarterly data")
                        continue

                stats = self.collect_table_data(
                    table_id=table_id,
                    frequency=frequency,
                    year=year,
                    progress=progress,
                )
                progress.tables_processed += 1

                # Update table status
                self._update_table_status(table_id, frequency, stats)

                if progress_callback:
                    progress_callback(progress)

            except BEAAPIError as e:
                error_msg = f"Table {table_id}: {str(e)}"
                log.error(error_msg)
                progress.errors.append(error_msg)

                # Check if rate limited
                if 'rate' in str(e).lower() or '429' in str(e):
                    log.warning("Rate limited, stopping collection")
                    break

            except Exception as e:
                error_msg = f"Table {table_id}: {str(e)}"
                log.error(error_msg)
                progress.errors.append(error_msg)
                self.session.rollback()

        progress.end_time = datetime.now(UTC)

        # Complete collection run
        status = 'completed' if not progress.errors else 'partial'
        complete_collection_run(self.session, progress.run_id, progress, status)

        # Update dataset freshness
        self._update_freshness(progress)

        return progress

    def _update_table_status(self, table_id: int, frequency: str, stats: Dict[str, int]):
        """Update table status after collection"""
        now = datetime.now(UTC)

        # Get latest data period
        latest = self.session.query(
            func.max(GDPByIndustryData.time_period)
        ).filter(
            GDPByIndustryData.table_id == table_id,
            GDPByIndustryData.frequency == frequency
        ).scalar()

        # Parse year from time period
        last_year = None
        last_period = None
        if latest:
            if len(latest) == 4:
                last_year = int(latest)
                last_period = 'A'
            elif 'Q' in latest:
                last_year = int(latest[:4])
                last_period = latest[4:]

        stmt = insert(BEATableUpdateStatus).values(
            dataset_name='GDPbyIndustry',
            table_name=str(table_id),
            last_data_year=last_year,
            last_data_period=last_period,
            last_checked_at=now,
            last_updated_at=now,
            is_current=True,
            rows_in_table=stats.get('data_points', 0),
            created_at=now,
            updated_at=now,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=['dataset_name', 'table_name'],
            set_={
                'last_data_year': stmt.excluded.last_data_year,
                'last_data_period': stmt.excluded.last_data_period,
                'last_checked_at': now,
                'last_updated_at': now,
                'is_current': True,
                'rows_in_table': stmt.excluded.rows_in_table,
                'updated_at': now,
            }
        )
        self.session.execute(stmt)
        self.session.commit()

    def _update_freshness(self, progress: CollectionProgress):
        """Update dataset freshness tracking"""
        now = datetime.now(UTC)

        # Get latest data year from database (filter to valid time periods starting with year)
        latest_year = self.session.query(func.max(GDPByIndustryData.time_period)).filter(
            GDPByIndustryData.time_period.op('~')('^[0-9]{4}')  # Regex: starts with 4 digits
        ).scalar()
        try:
            latest_data_year = int(latest_year[:4]) if latest_year and len(latest_year) >= 4 else None
        except (ValueError, TypeError):
            latest_data_year = None

        # Count statistics
        tables_count = self.session.query(func.count(GDPByIndustryTable.table_id)).filter(
            GDPByIndustryTable.is_active == True
        ).scalar() or 0

        industries_count = self.session.query(func.count(GDPByIndustryIndustry.industry_code)).filter(
            GDPByIndustryIndustry.is_active == True
        ).scalar() or 0

        data_count = self.session.query(func.count()).select_from(GDPByIndustryData).scalar() or 0

        stmt = insert(BEADatasetFreshness).values(
            dataset_name='GDPbyIndustry',
            latest_data_year=latest_data_year,
            last_checked_at=now,
            last_bea_update_detected=now if progress.data_points_inserted > 0 else None,
            needs_update=False,
            update_in_progress=False,
            last_update_completed=now,
            tables_count=tables_count,
            series_count=industries_count,
            data_points_count=data_count,
            total_checks=1,
            total_updates_detected=1 if progress.data_points_inserted > 0 else 0,
            created_at=now,
            updated_at=now,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=['dataset_name'],
            set_={
                'latest_data_year': stmt.excluded.latest_data_year,
                'last_checked_at': now,
                'last_bea_update_detected': stmt.excluded.last_bea_update_detected,
                'needs_update': False,
                'update_in_progress': False,
                'last_update_completed': now,
                'tables_count': stmt.excluded.tables_count,
                'series_count': stmt.excluded.series_count,
                'data_points_count': stmt.excluded.data_points_count,
                'total_checks': BEADatasetFreshness.total_checks + 1,
                'total_updates_detected': BEADatasetFreshness.total_updates_detected + (1 if progress.data_points_inserted > 0 else 0),
                'updated_at': now,
            }
        )
        self.session.execute(stmt)
        self.session.commit()


# ===================== Unified Collector ===================== #

class BEACollector:
    """Unified collector for all BEA datasets"""

    def __init__(self, client: BEAClient, session: Session):
        self.client = client
        self.session = session
        self.nipa = NIPACollector(client, session)
        self.regional = RegionalCollector(client, session)
        self.gdpbyindustry = GDPByIndustryCollector(client, session)

    def sync_dataset_catalog(self):
        """Sync BEA dataset catalog"""
        log.info("Syncing BEA dataset catalog...")
        result = self.client.get_dataset_list()

        datasets = result.get('BEAAPI', {}).get('Results', {}).get('Dataset', [])

        now = datetime.now(UTC)
        for ds in datasets:
            stmt = insert(BEADataset).values(
                dataset_name=ds.get('DatasetName', ''),
                dataset_description=ds.get('DatasetDescription', ''),
                is_active=True,
                created_at=now,
                updated_at=now,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=['dataset_name'],
                set_={
                    'dataset_description': stmt.excluded.dataset_description,
                    'updated_at': now,
                }
            )
            self.session.execute(stmt)

        self.session.commit()
        log.info(f"Synced {len(datasets)} BEA datasets")


# ===================== Sentinel Manager ===================== #

class SentinelManager:
    """
    Manages sentinel series selection and checking for BEA datasets.

    Sentinels are a representative sample (5-10%) of series used to detect
    when BEA has released new data, without checking every series.
    """

    # Target sentinel percentage
    SENTINEL_PERCENTAGE = 0.07  # 7% of series
    MIN_SENTINELS = 5
    MAX_SENTINELS = 100  # Cap to avoid too many API calls

    def __init__(self, client: BEAClient, session: Session):
        self.client = client
        self.session = session

    def select_sentinels(self, dataset_name: str, frequency: str = 'A') -> Dict[str, Any]:
        """
        Automatically select sentinel series for a dataset.

        Selection strategy:
        - NIPA: Pick series from priority tables (GDP, Income) plus samples from each section
        - Regional: Pick line_code=1 (totals) from priority tables for national + sample states
        - GDPbyIndustry: Pick key industries from priority tables

        Args:
            dataset_name: 'NIPA', 'Regional', or 'GDPbyIndustry'
            frequency: 'A', 'Q', or 'M' (for NIPA/GDPbyIndustry)

        Returns:
            Dict with 'selected', 'total_series', 'percentage'
        """
        if dataset_name == 'NIPA':
            return self._select_nipa_sentinels(frequency)
        elif dataset_name == 'Regional':
            return self._select_regional_sentinels()
        elif dataset_name == 'GDPbyIndustry':
            return self._select_gdpbyindustry_sentinels(frequency)
        else:
            raise ValueError(f"Unknown dataset: {dataset_name}")

    def _select_nipa_sentinels(self, frequency: str = 'A') -> Dict[str, Any]:
        """Select sentinel series for NIPA dataset."""
        from src.database.bea_models import NIPASeries, NIPAData, NIPATable

        # Determine which frequency flag to check
        if frequency == 'A':
            freq_filter = NIPATable.has_annual == True
        elif frequency == 'Q':
            freq_filter = NIPATable.has_quarterly == True
        elif frequency == 'M':
            freq_filter = NIPATable.has_monthly == True
        else:
            freq_filter = NIPATable.has_annual == True

        # Get tables that have the requested frequency
        tables_with_freq = self.session.query(NIPATable.table_name).filter(
            NIPATable.is_active == True,
            freq_filter
        ).all()
        valid_tables = [t[0] for t in tables_with_freq]

        if not valid_tables:
            return {'selected': 0, 'total_series': 0, 'percentage': 0, 'message': f'No tables with {frequency} frequency found.'}

        # Get total series count from valid tables
        total_series = self.session.query(func.count(NIPASeries.series_code)).filter(
            NIPASeries.is_active == True,
            NIPASeries.table_name.in_(valid_tables)
        ).scalar() or 0

        if total_series == 0:
            return {'selected': 0, 'total_series': 0, 'percentage': 0, 'message': 'No series found. Run backfill first.'}

        # Calculate target count
        target_count = max(
            self.MIN_SENTINELS,
            min(self.MAX_SENTINELS, int(total_series * self.SENTINEL_PERCENTAGE))
        )

        # Priority tables - only include those that have the requested frequency
        priority_tables = ['T10101', 'T10105', 'T10106', 'T20100', 'T20301', 'T30100']
        priority_tables = [t for t in priority_tables if t in valid_tables]

        # Get series from priority tables (line 1-5 typically are key aggregates)
        priority_series = self.session.query(NIPASeries).filter(
            NIPASeries.table_name.in_(priority_tables),
            NIPASeries.is_active == True,
            NIPASeries.line_number <= 5
        ).all() if priority_tables else []

        # Sample additional series from other valid tables to reach target
        other_tables = [t for t in valid_tables if t not in priority_tables]
        other_series = self.session.query(NIPASeries).filter(
            NIPASeries.table_name.in_(other_tables),
            NIPASeries.is_active == True,
            NIPASeries.line_number == 1  # Line 1 is usually the table total
        ).all() if other_tables else []

        # Combine and limit
        selected_series = priority_series[:target_count]
        remaining = target_count - len(selected_series)
        if remaining > 0 and other_series:
            # Sample evenly across tables
            step = max(1, len(other_series) // remaining)
            selected_series.extend(other_series[::step][:remaining])

        # Clear existing sentinels for this dataset (including legacy ones with NULL frequency)
        self.session.query(BEASentinelSeries).filter(
            BEASentinelSeries.dataset_name == 'NIPA'
        ).delete()

        # Insert new sentinels - only for series that have recent data
        # Also skip tables ending in 'A' or 'B' (archival/alternative tables that may not be fetchable)
        now = datetime.now(UTC)
        current_year = datetime.now().year
        min_year = current_year - 4  # LAST5 years
        sentinel_order = 0

        # Tables ending with letters (like T31800A) are often archival and cause API errors
        skip_table_suffixes = ('A', 'B', 'C', 'D', 'E')

        for series in selected_series:
            # Skip archival/alternative tables
            if series.table_name and series.table_name[-1] in skip_table_suffixes:
                log.debug(f"Skipping sentinel {series.series_code}: archival table {series.table_name}")
                continue

            # Get latest value for this series - must be within LAST5 years
            latest = self.session.query(NIPAData).filter(
                NIPAData.series_code == series.series_code,
                NIPAData.time_period >= str(min_year)  # Only recent data
            ).order_by(NIPAData.time_period.desc()).first()

            # Skip if no recent data exists
            if not latest:
                log.debug(f"Skipping sentinel {series.series_code}: no recent data")
                continue

            sentinel_order += 1
            sentinel = BEASentinelSeries(
                dataset_name='NIPA',
                sentinel_id=f"{series.series_code}_{frequency}",
                sentinel_order=sentinel_order,
                table_name=series.table_name,
                series_code=series.series_code,
                frequency=frequency,
                selection_reason=f"Line {series.line_number}: {series.line_description[:50] if series.line_description else 'N/A'}",
                last_value=latest.value,
                last_year=int(latest.time_period[:4]) if latest.time_period else None,
                last_period=latest.time_period[4:] if len(latest.time_period) > 4 else 'A',
                created_at=now,
                updated_at=now,
            )
            self.session.add(sentinel)

        self.session.commit()

        return {
            'selected': sentinel_order,
            'total_series': total_series,
            'percentage': round(sentinel_order / total_series * 100, 1) if total_series > 0 else 0,
            'message': f"Selected {sentinel_order} sentinels from {total_series} series"
        }

    def _select_regional_sentinels(self) -> Dict[str, Any]:
        """Select sentinel series for Regional dataset."""
        from src.database.bea_models import RegionalLineCode, RegionalData

        # Get total unique table/line_code combinations
        total_series = self.session.query(func.count()).select_from(RegionalLineCode).scalar() or 0

        if total_series == 0:
            return {'selected': 0, 'total_series': 0, 'percentage': 0, 'message': 'No series found. Run backfill first.'}

        target_count = max(
            self.MIN_SENTINELS,
            min(self.MAX_SENTINELS, int(total_series * self.SENTINEL_PERCENTAGE))
        )

        # Priority tables with their key line codes
        priority_configs = [
            ('SAGDP1', 1, 'State GDP Total'),
            ('SAINC1', 1, 'State Personal Income'),
            ('CAINC1', 1, 'County Personal Income'),
            ('SAGDP2', 1, 'State GDP by Industry'),
            ('SQGDP1', 1, 'Quarterly State GDP'),
        ]

        # Sample states (national + large states for coverage)
        sample_geos = ['00000', '06000', '48000', '12000', '36000', '17000']  # US, CA, TX, FL, NY, IL

        # Clear existing sentinels
        self.session.query(BEASentinelSeries).filter(
            BEASentinelSeries.dataset_name == 'Regional'
        ).delete()

        now = datetime.now(UTC)
        sentinel_order = 0

        for table_name, line_code, reason in priority_configs:
            for geo_fips in sample_geos:
                # Check if data exists for this combination
                latest = self.session.query(RegionalData).filter(
                    RegionalData.table_name == table_name,
                    RegionalData.line_code == line_code,
                    RegionalData.geo_fips == geo_fips
                ).order_by(RegionalData.time_period.desc()).first()

                if latest:
                    sentinel_order += 1
                    sentinel = BEASentinelSeries(
                        dataset_name='Regional',
                        sentinel_id=f"{table_name}_{line_code}_{geo_fips}",
                        sentinel_order=sentinel_order,
                        table_name=table_name,
                        line_code=line_code,
                        geo_fips=geo_fips,
                        selection_reason=reason,
                        last_value=latest.value,
                        last_year=int(latest.time_period) if latest.time_period and latest.time_period.isdigit() else None,
                        last_period='A',
                        created_at=now,
                        updated_at=now,
                    )
                    self.session.add(sentinel)

                    if sentinel_order >= target_count:
                        break

            if sentinel_order >= target_count:
                break

        self.session.commit()

        return {
            'selected': sentinel_order,
            'total_series': total_series,
            'percentage': round(sentinel_order / total_series * 100, 1),
            'message': f"Selected {sentinel_order} sentinels from {total_series} series"
        }

    def _select_gdpbyindustry_sentinels(self, frequency: str = 'A') -> Dict[str, Any]:
        """Select sentinel series for GDPbyIndustry dataset."""
        from src.database.bea_models import GDPByIndustryIndustry, GDPByIndustryData

        # Get total unique table/industry combinations
        total_series = self.session.query(
            func.count(func.distinct(
                GDPByIndustryData.table_id.concat('_').concat(GDPByIndustryData.industry_code)
            ))
        ).filter(
            GDPByIndustryData.frequency == frequency
        ).scalar() or 0

        if total_series == 0:
            return {'selected': 0, 'total_series': 0, 'percentage': 0, 'message': 'No series found. Run backfill first.'}

        target_count = max(
            self.MIN_SENTINELS,
            min(self.MAX_SENTINELS, int(total_series * self.SENTINEL_PERCENTAGE))
        )

        # Priority tables (including tables 6 & 7 which have component rows)
        priority_tables = [1, 5, 6, 7, 10, 13]  # Value Added, Percentage, Components, Contributions

        # Key industry codes (major sectors)
        key_industries = ['ALL', 'GDP', '11', '21', '22', '23', '31G', '42', '44RT', '48TW', '51', '52', '54']

        # Clear existing sentinels for this dataset (including legacy ones with NULL frequency)
        self.session.query(BEASentinelSeries).filter(
            BEASentinelSeries.dataset_name == 'GDPbyIndustry'
        ).delete()

        now = datetime.now(UTC)
        sentinel_order = 0

        for table_id in priority_tables:
            for industry_code in key_industries:
                # Get latest data (exclude rows with empty time_period from old buggy inserts)
                # Filter by row_type='total' to get industry totals, not component rows (for tables 6 & 7)
                latest = self.session.query(GDPByIndustryData).filter(
                    GDPByIndustryData.table_id == table_id,
                    GDPByIndustryData.industry_code == industry_code,
                    GDPByIndustryData.frequency == frequency,
                    GDPByIndustryData.row_type == 'total',
                    GDPByIndustryData.time_period != '',
                    GDPByIndustryData.time_period.isnot(None)
                ).order_by(GDPByIndustryData.time_period.desc()).first()

                if latest:
                    sentinel_order += 1

                    # Parse time period
                    last_year = None
                    last_period = frequency
                    if latest.time_period:
                        if len(latest.time_period) >= 4:
                            last_year = int(latest.time_period[:4])
                        if len(latest.time_period) > 4:
                            last_period = latest.time_period[4:]

                    sentinel = BEASentinelSeries(
                        dataset_name='GDPbyIndustry',
                        sentinel_id=f"{table_id}_{industry_code}_{frequency}",
                        sentinel_order=sentinel_order,
                        table_name=str(table_id),
                        industry_code=industry_code,
                        frequency=frequency,
                        selection_reason=f"Table {table_id}, Industry {industry_code}",
                        last_value=latest.value,
                        last_year=last_year,
                        last_period=last_period,
                        created_at=now,
                        updated_at=now,
                    )
                    self.session.add(sentinel)

                    if sentinel_order >= target_count:
                        break

            if sentinel_order >= target_count:
                break

        self.session.commit()

        return {
            'selected': sentinel_order,
            'total_series': total_series,
            'percentage': round(sentinel_order / total_series * 100, 1),
            'message': f"Selected {sentinel_order} sentinels from {total_series} series"
        }

    def check_sentinels(self, dataset_name: str) -> Dict[str, Any]:
        """
        Check sentinel series for new data.

        Fetches current values from BEA API and compares against stored values.

        Args:
            dataset_name: 'NIPA', 'Regional', or 'GDPbyIndustry'

        Returns:
            Dict with 'checked', 'changed', 'new_data_detected', 'details'
        """
        sentinels = self.session.query(BEASentinelSeries).filter(
            BEASentinelSeries.dataset_name == dataset_name
        ).order_by(BEASentinelSeries.sentinel_order).all()

        if not sentinels:
            return {
                'checked': 0,
                'changed': 0,
                'new_data_detected': False,
                'message': 'No sentinels configured. Run sentinel selection first.'
            }

        now = datetime.now(UTC)
        checked = 0
        changed = 0
        changes = []

        for sentinel in sentinels:
            try:
                current = self._fetch_sentinel_value(sentinel)
                checked += 1

                sentinel.last_checked_at = now

                if current:
                    # Check if value or period changed
                    value_changed = current['value'] != sentinel.last_value
                    period_changed = (current['year'] != sentinel.last_year or
                                     current['period'] != sentinel.last_period)

                    if value_changed or period_changed:
                        changed += 1
                        sentinel.has_changed = True

                        changes.append({
                            'sentinel_id': sentinel.sentinel_id,
                            'table': sentinel.table_name,
                            'old_value': float(sentinel.last_value) if sentinel.last_value else None,
                            'new_value': float(current['value']) if current['value'] else None,
                            'old_period': f"{sentinel.last_year}{sentinel.last_period or ''}",
                            'new_period': f"{current['year']}{current['period'] or ''}",
                        })
                        # Do NOT update stored values - keep showing difference until local data is updated
                    else:
                        sentinel.has_changed = False

            except Exception as e:
                log.warning(f"Failed to check sentinel {sentinel.sentinel_id}: {e}")

        self.session.commit()

        # Update dataset freshness if changes detected
        if changed > 0:
            self._update_freshness_needs_update(dataset_name, True)

        return {
            'checked': checked,
            'changed': changed,
            'new_data_detected': changed > 0,
            'changes': changes[:10],  # Limit to first 10 changes
            'message': f"Checked {checked} sentinels, {changed} have new data" if changed > 0
                      else f"Checked {checked} sentinels, no new data detected"
        }

    def _fetch_sentinel_value(self, sentinel: BEASentinelSeries) -> Optional[Dict[str, Any]]:
        """Fetch current value for a sentinel from BEA API."""
        # Convert LAST5 to actual years
        year_spec = convert_year_spec('LAST5')

        if sentinel.dataset_name == 'NIPA':
            # Get latest data for this series
            result = self.client.get_nipa_data(
                table_name=sentinel.table_name,
                frequency=sentinel.frequency or 'A',
                year=year_spec
            )
            data = self.client._extract_data(result)

            # Find matching series and get latest period
            for row in sorted(data, key=lambda x: x.get('TimePeriod', ''), reverse=True):
                if row.get('SeriesCode') == sentinel.series_code:
                    value_str = row.get('DataValue', '')
                    try:
                        value = Decimal(value_str.replace(',', '')) if value_str and value_str not in ('', 'ND') else None
                    except:
                        value = None

                    time_period = row.get('TimePeriod', '')
                    return {
                        'value': value,
                        'year': int(time_period[:4]) if time_period else None,
                        'period': time_period[4:] if len(time_period) > 4 else 'A'
                    }

        elif sentinel.dataset_name == 'Regional':
            result = self.client.get_regional_data(
                table_name=sentinel.table_name,
                line_code=sentinel.line_code,
                geo_fips=sentinel.geo_fips,
                year=year_spec
            )
            data = self.client._extract_data(result)

            # Get latest period
            for row in sorted(data, key=lambda x: x.get('TimePeriod', ''), reverse=True):
                if row.get('GeoFips') == sentinel.geo_fips:
                    value_str = row.get('DataValue', '')
                    try:
                        value = Decimal(value_str.replace(',', '')) if value_str and value_str not in ('', 'NA', '(NA)') else None
                    except:
                        value = None

                    time_period = row.get('TimePeriod', '')
                    return {
                        'value': value,
                        'year': int(time_period) if time_period and time_period.isdigit() else None,
                        'period': 'A'
                    }

        elif sentinel.dataset_name == 'GDPbyIndustry':
            result = self.client.get_gdpbyindustry_data(
                table_id=int(sentinel.table_name),
                frequency=sentinel.frequency or 'A',
                year=year_spec,
                industry=sentinel.industry_code
            )
            data = self.client._extract_data(result)

            # Get latest period - GDPbyIndustry uses 'Year' and 'Quarter', not 'TimePeriod'
            # For tables 6 & 7, filter to get 'total' row (industry description matches industry name)
            # Component rows have descriptions like 'Compensation of employees', 'Taxes...', 'Gross operating surplus'
            component_descriptions = {'Compensation of employees', 'Taxes on production and imports less subsidies', 'Gross operating surplus'}

            for row in sorted(data, key=lambda x: (x.get('Year', ''), x.get('Quarter', '')), reverse=True):
                if row.get('Industry') == sentinel.industry_code:
                    # Skip component rows - we only want the 'total' row
                    industry_desc = row.get('IndustrYDescription') or row.get('IndustryDescription', '')
                    if industry_desc in component_descriptions:
                        continue

                    value_str = row.get('DataValue', '')
                    try:
                        value = Decimal(value_str.replace(',', '')) if value_str and value_str not in ('', 'ND') else None
                    except:
                        value = None

                    year_val = row.get('Year', '')
                    quarter_val = row.get('Quarter', '')
                    frequency = sentinel.frequency or 'A'

                    if frequency == 'Q' and quarter_val and quarter_val != year_val:
                        period = f"Q{quarter_val}" if quarter_val.isdigit() else quarter_val
                    else:
                        period = 'A'

                    return {
                        'value': value,
                        'year': int(year_val) if year_val else None,
                        'period': period
                    }

        return None

    def _update_freshness_needs_update(self, dataset_name: str, needs_update: bool):
        """Update dataset freshness to indicate new data available."""
        now = datetime.now(UTC)
        self.session.query(BEADatasetFreshness).filter(
            BEADatasetFreshness.dataset_name == dataset_name
        ).update({
            'needs_update': needs_update,
            'last_checked_at': now,
            'last_bea_update_detected': now if needs_update else BEADatasetFreshness.last_bea_update_detected,
            'updated_at': now,
        })
        self.session.commit()

    def get_sentinel_stats(self, dataset_name: Optional[str] = None) -> Dict[str, Any]:
        """Get sentinel statistics for dashboard display."""
        query = self.session.query(BEASentinelSeries)
        if dataset_name:
            query = query.filter(BEASentinelSeries.dataset_name == dataset_name)

        sentinels = query.all()

        if not sentinels:
            return {'total': 0, 'by_dataset': {}}

        by_dataset = {}
        for s in sentinels:
            if s.dataset_name not in by_dataset:
                by_dataset[s.dataset_name] = {
                    'count': 0,
                    'last_checked': None,
                    'changes_detected': 0,  # Number of sentinels with has_changed=True
                }
            stats = by_dataset[s.dataset_name]
            stats['count'] += 1

            # Count sentinels that currently show a difference
            if s.has_changed:
                stats['changes_detected'] += 1

            if s.last_checked_at:
                if not stats['last_checked'] or s.last_checked_at > stats['last_checked']:
                    stats['last_checked'] = s.last_checked_at

        return {
            'total': len(sentinels),
            'by_dataset': by_dataset
        }

    def list_sentinels(self, dataset_name: str) -> List[Dict[str, Any]]:
        """List all sentinels for a dataset."""
        sentinels = self.session.query(BEASentinelSeries).filter(
            BEASentinelSeries.dataset_name == dataset_name
        ).order_by(BEASentinelSeries.sentinel_order).all()

        return [
            {
                'sentinel_id': s.sentinel_id,
                'table_name': s.table_name,
                'series_code': s.series_code,
                'line_code': s.line_code,
                'geo_fips': s.geo_fips,
                'industry_code': s.industry_code,
                'frequency': s.frequency,
                'selection_reason': s.selection_reason,
                'last_value': float(s.last_value) if s.last_value else None,
                'last_year': s.last_year,
                'last_period': s.last_period,
                'last_checked_at': s.last_checked_at.isoformat() if s.last_checked_at else None,
                'last_changed_at': s.last_changed_at.isoformat() if s.last_changed_at else None,
                'check_count': s.check_count or 0,
                'change_count': s.change_count or 0,
            }
            for s in sentinels
        ]

    def delete_sentinel(self, dataset_name: str, sentinel_id: str) -> bool:
        """Delete a specific sentinel."""
        result = self.session.query(BEASentinelSeries).filter(
            BEASentinelSeries.dataset_name == dataset_name,
            BEASentinelSeries.sentinel_id == sentinel_id
        ).delete()
        self.session.commit()
        return result > 0

    def clear_sentinels(self, dataset_name: str) -> int:
        """Clear all sentinels for a dataset."""
        result = self.session.query(BEASentinelSeries).filter(
            BEASentinelSeries.dataset_name == dataset_name
        ).delete()
        self.session.commit()
        return result

    def sync_sentinels_from_data(self, dataset_name: str) -> Dict[str, Any]:
        """
        Sync sentinel values from the actual data tables.

        Call this after a data update to keep sentinels in sync.
        This updates last_value/last_year/last_period from the database,
        NOT from the BEA API.
        """
        from src.database.bea_models import NIPAData, RegionalData, GDPByIndustryData

        sentinels = self.session.query(BEASentinelSeries).filter(
            BEASentinelSeries.dataset_name == dataset_name
        ).all()

        if not sentinels:
            return {'synced': 0, 'message': 'No sentinels to sync'}

        now = datetime.now(UTC)
        synced = 0

        for sentinel in sentinels:
            try:
                latest = None

                if dataset_name == 'NIPA':
                    latest = self.session.query(NIPAData).filter(
                        NIPAData.series_code == sentinel.series_code
                    ).order_by(NIPAData.time_period.desc()).first()

                    if latest:
                        sentinel.last_value = latest.value
                        sentinel.last_year = int(latest.time_period[:4]) if latest.time_period else None
                        sentinel.last_period = latest.time_period[4:] if len(latest.time_period) > 4 else 'A'

                elif dataset_name == 'Regional':
                    latest = self.session.query(RegionalData).filter(
                        RegionalData.table_name == sentinel.table_name,
                        RegionalData.line_code == sentinel.line_code,
                        RegionalData.geo_fips == sentinel.geo_fips
                    ).order_by(RegionalData.time_period.desc()).first()

                    if latest:
                        sentinel.last_value = latest.value
                        sentinel.last_year = int(latest.time_period) if latest.time_period and latest.time_period.isdigit() else None
                        sentinel.last_period = 'A'

                elif dataset_name == 'GDPbyIndustry':
                    latest = self.session.query(GDPByIndustryData).filter(
                        GDPByIndustryData.table_id == int(sentinel.table_name),
                        GDPByIndustryData.industry_code == sentinel.industry_code,
                        GDPByIndustryData.frequency == sentinel.frequency,
                        GDPByIndustryData.time_period != '',
                        GDPByIndustryData.time_period.isnot(None)
                    ).order_by(GDPByIndustryData.time_period.desc()).first()

                    if latest:
                        sentinel.last_value = latest.value
                        sentinel.last_year = int(latest.time_period[:4]) if latest.time_period else None
                        sentinel.last_period = latest.time_period[4:] if len(latest.time_period) > 4 else 'A'

                if latest:
                    sentinel.updated_at = now
                    synced += 1

            except Exception as e:
                log.warning(f"Failed to sync sentinel {sentinel.sentinel_id}: {e}")

        self.session.commit()
        log.info(f"Synced {synced} sentinels for {dataset_name}")

        return {
            'synced': synced,
            'total': len(sentinels),
            'message': f"Synced {synced}/{len(sentinels)} sentinels for {dataset_name}"
        }
