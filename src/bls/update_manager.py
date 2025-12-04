"""
BLS Survey Update Manager

Reusable core logic for updating BLS survey data using the Update Cycle system.
Can be called from CLI scripts or API endpoints.

Update Cycle System:
- Soft Update: Uses existing current cycle, continues where left off
- Force Update: Creates new cycle, marks old one not current, starts fresh
"""
from datetime import datetime, date, UTC
from typing import Any, Dict, List, Optional, Callable

from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

try:
    from src.bls.bls_client import BLSClient
    from src.database.bls_tracking_models import (
        BLSUpdateCycle,
        BLSUpdateCycleSeries,
        BLSAPIUsageLog,
    )
    from src.database.bls_models import (
        APSeries, APData, CUSeries, CUData, LASeries, LAData, CESeries, CEData,
        PCSeries, PCData, WPSeries, WPData, SMSeries, SMData, JTSeries, JTData,
        ECSeries, ECData, OESeries, OEData, PRSeries, PRData, TUSeries, TUData,
        IPSeries, IPData, LNSeries, LNData, CWSeries, CWData, SUSeries, SUData,
        BDSeries, BDData, EISeries, EIData
    )
except ImportError:
    from bls.bls_client import BLSClient
    from database.bls_tracking_models import (
        BLSUpdateCycle,
        BLSUpdateCycleSeries,
        BLSAPIUsageLog,
    )
    from database.bls_models import (
        APSeries, APData, CUSeries, CUData, LASeries, LAData, CESeries, CEData,
        PCSeries, PCData, WPSeries, WPData, SMSeries, SMData, JTSeries, JTData,
        ECSeries, ECData, OESeries, OEData, PRSeries, PRData, TUSeries, TUData,
        IPSeries, IPData, LNSeries, LNData, CWSeries, CWData, SUSeries, SUData,
        BDSeries, BDData, EISeries, EIData
    )


# Survey configuration: code -> (SeriesModel, DataModel, survey_name)
SURVEYS = {
    'AP': (APSeries, APData, 'Average Price Data'),
    'CU': (CUSeries, CUData, 'Consumer Price Index'),
    'LA': (LASeries, LAData, 'Local Area Unemployment'),
    'CE': (CESeries, CEData, 'Current Employment Statistics'),
    'PC': (PCSeries, PCData, 'Producer Price Index - Commodity'),
    'WP': (WPSeries, WPData, 'Producer Price Index'),
    'SM': (SMSeries, SMData, 'State and Metro Area Employment'),
    'JT': (JTSeries, JTData, 'JOLTS'),
    'EC': (ECSeries, ECData, 'Employment Cost Index'),
    'OE': (OESeries, OEData, 'Occupational Employment'),
    'PR': (PRSeries, PRData, 'Major Sector Productivity'),
    'TU': (TUSeries, TUData, 'American Time Use Survey'),
    'IP': (IPSeries, IPData, 'Industry Productivity'),
    'LN': (LNSeries, LNData, 'Labor Force Statistics'),
    'CW': (CWSeries, CWData, 'CPI - Urban Wage Earners'),
    'SU': (SUSeries, SUData, 'Chained CPI'),
    'BD': (BDSeries, BDData, 'Business Employment Dynamics'),
    'EI': (EISeries, EIData, 'Import/Export Price Indexes'),
}


class UpdateProgress:
    """Progress tracking for survey updates"""
    def __init__(self, survey_code: str, total_series: int, cycle_id: int):
        self.survey_code = survey_code
        self.total_series = total_series
        self.cycle_id = cycle_id
        self.series_updated = 0
        self.observations_added = 0
        self.requests_used = 0
        self.errors: List[str] = []
        self.completed = False
        self.start_time = datetime.now()
        self.end_time: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'survey_code': self.survey_code,
            'cycle_id': self.cycle_id,
            'total_series': self.total_series,
            'series_updated': self.series_updated,
            'observations_added': self.observations_added,
            'requests_used': self.requests_used,
            'progress_pct': (self.series_updated / self.total_series * 100) if self.total_series > 0 else 0,
            'errors': self.errors,
            'completed': self.completed,
            'duration_seconds': ((self.end_time or datetime.now()) - self.start_time).total_seconds()
        }


def get_remaining_quota(session: Session, daily_limit: int = 500) -> int:
    """Check how many API requests remaining today"""
    today = date.today()
    used_today = session.query(
        func.sum(BLSAPIUsageLog.requests_used)
    ).filter(
        BLSAPIUsageLog.usage_date == today
    ).scalar() or 0

    remaining = daily_limit - used_today
    return max(0, remaining)


def get_current_cycle(session: Session, survey_code: str) -> Optional[BLSUpdateCycle]:
    """Get the current update cycle for a survey, if any"""
    return session.query(BLSUpdateCycle).filter(
        BLSUpdateCycle.survey_code == survey_code,
        BLSUpdateCycle.is_current == True
    ).first()


def create_new_cycle(session: Session, survey_code: str, total_series: int) -> BLSUpdateCycle:
    """
    Create a new update cycle for a survey.
    Marks any existing current cycle as not current.
    """
    # Mark existing current cycle as not current
    session.query(BLSUpdateCycle).filter(
        BLSUpdateCycle.survey_code == survey_code,
        BLSUpdateCycle.is_current == True
    ).update({'is_current': False})

    # Create new cycle
    cycle = BLSUpdateCycle(
        survey_code=survey_code,
        is_current=True,
        started_at=datetime.now(),
        total_series=total_series,
        series_updated=0,
        requests_used=0
    )
    session.add(cycle)
    session.commit()

    return cycle


def get_series_needing_update(session: Session, survey_code: str, series_model,
                               cycle: BLSUpdateCycle) -> List[str]:
    """
    Find series that need updates for the given cycle.

    Returns list of series IDs that are:
    - Active in the series table
    - NOT already updated in this cycle
    """
    # Get all active series for this survey
    active_series = session.query(series_model.series_id).filter(
        series_model.is_active == True
    ).all()
    active_series_ids = set(row[0] for row in active_series)

    # Get series already updated in this cycle
    updated_series = session.query(BLSUpdateCycleSeries.series_id).filter(
        BLSUpdateCycleSeries.cycle_id == cycle.id
    ).all()
    updated_series_ids = set(row[0] for row in updated_series)

    # Return series that need updates
    needs_update = list(active_series_ids - updated_series_ids)
    return needs_update


def update_series_batch(session: Session, client: BLSClient, series_ids: List[str],
                        cycle: BLSUpdateCycle, data_model,
                        start_year: int, end_year: int) -> Dict[str, Any]:
    """
    Update a batch of series and return statistics.
    Records each series in the cycle_series table.
    """
    # Fetch from API
    rows = client.get_many(
        series_ids,
        start_year=start_year,
        end_year=end_year,
        calculations=False,
        catalog=False,
        as_dataframe=False
    )

    # Convert to database format
    data_to_upsert = []
    for row in rows:
        data_to_upsert.append({
            'series_id': row['series_id'],
            'year': row['year'],
            'period': row['period'],
            'value': row['value'],
            'footnote_codes': row.get('footnotes'),
        })

    # Upsert data to survey data table
    if data_to_upsert:
        stmt = insert(data_model).values(data_to_upsert)
        stmt = stmt.on_conflict_do_update(
            index_elements=['series_id', 'year', 'period'],
            set_={
                'value': stmt.excluded.value,
                'footnote_codes': stmt.excluded.footnote_codes,
                'updated_at': datetime.now(UTC),
            }
        )
        session.execute(stmt)

    # Record series in cycle_series table
    now = datetime.now()
    cycle_series_records = [
        {'cycle_id': cycle.id, 'series_id': sid, 'updated_at': now}
        for sid in series_ids
    ]
    if cycle_series_records:
        stmt = insert(BLSUpdateCycleSeries).values(cycle_series_records)
        stmt = stmt.on_conflict_do_nothing()  # In case of retry
        session.execute(stmt)

    session.commit()

    return {
        'observations': len(data_to_upsert),
        'series_updated': len(series_ids),
    }


def record_api_usage(session: Session, requests_used: int, series_count: int,
                     survey_code: str, script_name: str = 'update_manager'):
    """Record API usage in log"""
    log = BLSAPIUsageLog(
        usage_date=date.today(),
        requests_used=requests_used,
        series_count=series_count,
        survey_code=survey_code,
        script_name=script_name
    )
    session.add(log)
    session.commit()


def update_survey(
    survey_code: str,
    session: Session,
    client: BLSClient,
    *,
    force: bool = False,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
    max_quota: Optional[int] = None,
    progress_callback: Optional[Callable[[UpdateProgress], None]] = None,
    skip_usage_logging: bool = False
) -> UpdateProgress:
    """
    Update a single survey with all its series.

    Args:
        survey_code: BLS survey code (e.g., 'CU', 'LA')
        session: Database session
        client: BLS API client
        force: If True, create new cycle; if False, use existing current cycle
        start_year: Start year for data fetch (default: last year)
        end_year: End year for data fetch (default: current year)
        max_quota: Maximum API requests to use (default: unlimited)
        progress_callback: Optional callback function to report progress
        skip_usage_logging: If True, don't log API usage (for custom API keys)

    Returns:
        UpdateProgress object with results
    """
    survey_code = survey_code.upper()

    if survey_code not in SURVEYS:
        raise ValueError(f"Invalid survey code: {survey_code}")

    # Get survey config
    series_model, data_model, survey_name = SURVEYS[survey_code]

    # Default year range
    if start_year is None:
        start_year = datetime.now().year - 1
    if end_year is None:
        end_year = datetime.now().year

    # Get total active series count
    total_active = session.query(series_model.series_id).filter(
        series_model.is_active == True
    ).count()

    # Get or create cycle
    if force:
        # Force: Create new cycle
        cycle = create_new_cycle(session, survey_code, total_active)
        print(f"[UpdateManager] Created new cycle #{cycle.id} for {survey_code}")
    else:
        # Soft: Use existing current cycle or create new one
        cycle = get_current_cycle(session, survey_code)
        if cycle:
            print(f"[UpdateManager] Resuming cycle #{cycle.id} for {survey_code}")
        else:
            cycle = create_new_cycle(session, survey_code, total_active)
            print(f"[UpdateManager] No current cycle, created new cycle #{cycle.id} for {survey_code}")

    # Get series needing update
    series_ids = get_series_needing_update(session, survey_code, series_model, cycle)

    if not series_ids:
        # No series need update - cycle is complete
        cycle.completed_at = datetime.now()
        cycle.is_running = False
        session.commit()

        progress = UpdateProgress(survey_code, total_active, cycle.id)
        progress.series_updated = cycle.series_updated
        progress.completed = True
        progress.end_time = datetime.now()
        print(f"[UpdateManager] No series need update for {survey_code}, cycle complete")
        return progress

    # Initialize progress tracking
    progress = UpdateProgress(survey_code, total_active, cycle.id)
    progress.series_updated = cycle.series_updated  # Start from current progress

    # Mark cycle as running
    cycle.is_running = True
    session.commit()

    print(f"[UpdateManager] {len(series_ids)} series to update for {survey_code}")

    try:
        # Update in batches of 50
        for i in range(0, len(series_ids), 50):
            # Check quota limit
            if max_quota is not None and progress.requests_used >= max_quota:
                print(f"[UpdateManager] Session quota reached ({max_quota} requests), pausing")
                break

            batch = series_ids[i:i+50]

            try:
                stats = update_series_batch(
                    session, client, batch, cycle, data_model,
                    start_year, end_year
                )

                progress.observations_added += stats['observations']
                progress.series_updated += stats['series_updated']
                progress.requests_used += 1

                # Update cycle progress
                cycle.series_updated = progress.series_updated
                cycle.requests_used += 1
                session.commit()

                # Record usage (skip if using custom API key)
                if not skip_usage_logging:
                    record_api_usage(session, 1, len(batch), survey_code)

                # Call progress callback if provided
                if progress_callback:
                    progress_callback(progress)

            except Exception as e:
                error_msg = f"Batch {i//50 + 1} failed: {str(e)}"
                progress.errors.append(error_msg)

                # Check if it's an API limit error
                error_str = str(e).lower()
                if 'quota' in error_str or 'limit' in error_str or 'exceeded' in error_str or 'threshold' in error_str:
                    progress.errors.append("API quota exceeded, stopping")
                    break

                # For other errors, continue with next batch
                session.rollback()
                continue

        # Check if cycle is complete
        if progress.series_updated >= total_active:
            cycle.completed_at = datetime.now()
            progress.completed = True
            print(f"[UpdateManager] Cycle #{cycle.id} completed for {survey_code}")
        else:
            print(f"[UpdateManager] Cycle #{cycle.id} paused at {progress.series_updated}/{total_active} series")

        # Mark cycle as not running
        cycle.is_running = False
        progress.end_time = datetime.now()
        session.commit()

    except Exception as e:
        # Mark cycle as not running on error
        cycle.is_running = False
        session.commit()

        progress.errors.append(f"Update failed: {str(e)}")
        progress.end_time = datetime.now()
        raise

    return progress


def get_survey_status(session: Session, survey_code: str) -> Dict[str, Any]:
    """
    Get the current update status for a survey.

    Returns dict with:
    - has_current_cycle: bool
    - cycle_id: int or None
    - total_series: int
    - series_updated: int
    - progress_pct: float
    - started_at: datetime or None
    - is_complete: bool
    - is_running: bool
    """
    survey_code = survey_code.upper()

    if survey_code not in SURVEYS:
        raise ValueError(f"Invalid survey code: {survey_code}")

    series_model, _, _ = SURVEYS[survey_code]

    # Get total active series
    total_active = session.query(series_model.series_id).filter(
        series_model.is_active == True
    ).count()

    # Get current cycle
    cycle = get_current_cycle(session, survey_code)

    if cycle:
        return {
            'has_current_cycle': True,
            'cycle_id': cycle.id,
            'total_series': total_active,
            'series_updated': cycle.series_updated,
            'progress_pct': (cycle.series_updated / total_active * 100) if total_active > 0 else 0,
            'started_at': cycle.started_at,
            'is_complete': cycle.completed_at is not None,
            'is_running': cycle.is_running,
        }
    else:
        return {
            'has_current_cycle': False,
            'cycle_id': None,
            'total_series': total_active,
            'series_updated': 0,
            'progress_pct': 0,
            'started_at': None,
            'is_complete': False,
            'is_running': False,
        }
