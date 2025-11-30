"""
BLS Survey Update Manager

Reusable core logic for updating BLS survey data.
Can be called from CLI scripts or API endpoints.
"""
from datetime import datetime, date, timedelta, UTC
from typing import Any, Dict, List, Optional, Callable
from decimal import Decimal

from sqlalchemy import func, update as sql_update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from src.bls.bls_client import BLSClient
from src.database.bls_tracking_models import (
    BLSSeriesUpdateStatus,
    BLSAPIUsageLog,
    BLSSurveyFreshness
)
from src.database.bls_models import (
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
    def __init__(self, survey_code: str, total_series: int):
        self.survey_code = survey_code
        self.total_series = total_series
        self.series_updated = 0
        self.observations_added = 0
        self.requests_used = 0
        self.errors = []
        self.completed = False
        self.start_time = datetime.now()
        self.end_time: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'survey_code': self.survey_code,
            'total_series': self.total_series,
            'series_updated': self.series_updated,
            'observations_added': self.observations_added,
            'requests_used': self.requests_used,
            'progress_pct': (self.series_updated / self.total_series * 100) if self.total_series > 0 else 0,
            'errors': self.errors,
            'completed': self.completed,
            'duration_seconds': (self.end_time or datetime.now() - self.start_time).total_seconds()
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


def reset_series_status(session: Session, survey_code: str) -> int:
    """
    Reset is_current=False for all series in a survey.
    Used when sentinel detects new data or before force update.

    Returns number of series reset.
    """
    result = session.query(BLSSeriesUpdateStatus).filter(
        BLSSeriesUpdateStatus.survey_code == survey_code
    ).update({'is_current': False})
    session.commit()
    return result


def get_series_needing_update(session: Session, survey_code: str, series_model,
                               data_model, force: bool = False) -> List[str]:
    """
    Find series that need updates

    Returns list of series IDs that either:
    - Are not marked as current (is_current = False)
    - Have no status record

    If force=True, first resets all series to is_current=False,
    then returns all active series.
    """
    # Get all active series for this survey
    active_series = session.query(series_model.series_id).filter(
        series_model.is_active == True
    ).all()
    active_series_ids = [row[0] for row in active_series]

    if force:
        # Reset all series status first, then return all active series
        reset_series_status(session, survey_code)
        return active_series_ids

    # Get series marked as current - these will be skipped
    current_series = session.query(
        BLSSeriesUpdateStatus.series_id
    ).filter(
        BLSSeriesUpdateStatus.survey_code == survey_code,
        BLSSeriesUpdateStatus.is_current == True
    ).all()
    current_series_ids = set([row[0] for row in current_series])

    # Return series that need updates
    needs_update = [sid for sid in active_series_ids if sid not in current_series_ids]
    return needs_update


def check_if_series_current(session: Session, series_id: str, data_model,
                            start_year: int, end_year: int) -> bool:
    """
    Check if a series has data up to expected timeframe

    A series is considered current if it has data for the most recent
    expected period (accounting for reporting lag)
    """
    # Get latest data point for this series
    latest = session.query(
        func.max(data_model.year).label('max_year'),
        func.max(data_model.period).label('max_period')
    ).filter(
        data_model.series_id == series_id
    ).first()

    if not latest or not latest.max_year:
        return False  # No data, needs update

    # Simple check: if latest year is >= end_year - 1, consider current
    # (accounts for reporting lag)
    return latest.max_year >= end_year - 1


def update_series_batch(session: Session, client: BLSClient, series_ids: List[str],
                        survey_code: str, data_model,
                        start_year: int, end_year: int) -> Dict[str, Any]:
    """
    Update a batch of series and return statistics
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

    # Upsert to database
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
        session.commit()

    # Update status for each series
    now = datetime.now()
    for series_id in series_ids:
        is_current = check_if_series_current(session, series_id, data_model,
                                             start_year, end_year)

        # Upsert status
        stmt = insert(BLSSeriesUpdateStatus).values({
            'series_id': series_id,
            'survey_code': survey_code,
            'last_checked_at': now,
            'last_updated_at': now,
            'is_current': is_current,
        })
        stmt = stmt.on_conflict_do_update(
            index_elements=['series_id'],
            set_={
                'last_checked_at': stmt.excluded.last_checked_at,
                'last_updated_at': stmt.excluded.last_updated_at,
                'is_current': stmt.excluded.is_current,
            }
        )
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
    progress_callback: Optional[Callable[[UpdateProgress], None]] = None
) -> UpdateProgress:
    """
    Update a single survey with all its series

    Args:
        survey_code: BLS survey code (e.g., 'CU', 'LA')
        session: Database session
        client: BLS API client
        force: Force update even if series marked current
        start_year: Start year for data fetch (default: last year)
        end_year: End year for data fetch (default: current year)
        max_quota: Maximum API requests to use (default: unlimited)
        progress_callback: Optional callback function to report progress

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

    # Get series needing update
    series_ids = get_series_needing_update(
        session, survey_code, series_model, data_model, force=force
    )

    if not series_ids:
        # No series need update
        progress = UpdateProgress(survey_code, 0)
        progress.completed = True
        progress.end_time = datetime.now()
        return progress

    # Initialize progress tracking
    progress = UpdateProgress(survey_code, len(series_ids))

    # Mark freshness as update in progress
    freshness_update = sql_update(BLSSurveyFreshness).where(
        BLSSurveyFreshness.survey_code == survey_code
    ).values(
        full_update_in_progress=True,
        last_full_update_started=datetime.now(),
        series_total_count=len(series_ids),
        series_updated_count=0
    )
    session.execute(freshness_update)
    session.commit()

    try:
        # Update in batches of 50
        for i in range(0, len(series_ids), 50):
            # Check quota limit
            if max_quota is not None and progress.requests_used >= max_quota:
                progress.errors.append(f"Quota limit reached: {max_quota} requests")
                break

            batch = series_ids[i:i+50]

            try:
                stats = update_series_batch(
                    session, client, batch, survey_code, data_model,
                    start_year, end_year
                )

                progress.observations_added += stats['observations']
                progress.series_updated += stats['series_updated']
                progress.requests_used += 1

                # Record usage
                record_api_usage(session, 1, len(batch), survey_code)

                # Update freshness progress
                freshness_update = sql_update(BLSSurveyFreshness).where(
                    BLSSurveyFreshness.survey_code == survey_code
                ).values(
                    series_updated_count=progress.series_updated
                )
                session.execute(freshness_update)
                session.commit()

                # Call progress callback if provided
                if progress_callback:
                    progress_callback(progress)

            except Exception as e:
                error_msg = f"Batch {i//50 + 1} failed: {str(e)}"
                progress.errors.append(error_msg)

                # Check if it's an API limit error
                error_str = str(e).lower()
                if 'quota' in error_str or 'limit' in error_str or 'exceeded' in error_str:
                    progress.errors.append("API quota exceeded, stopping")
                    break

                # For other errors, continue with next batch
                session.rollback()
                continue

        # Mark as complete
        progress.completed = True
        progress.end_time = datetime.now()

        # Update freshness tracking
        freshness_update = sql_update(BLSSurveyFreshness).where(
            BLSSurveyFreshness.survey_code == survey_code
        ).values(
            full_update_in_progress=False,
            last_full_update_completed=datetime.now(),
            needs_full_update=False
        )
        session.execute(freshness_update)
        session.commit()

    except Exception as e:
        # Mark as failed
        progress.errors.append(f"Update failed: {str(e)}")
        progress.end_time = datetime.now()

        # Clear in-progress flag
        freshness_update = sql_update(BLSSurveyFreshness).where(
            BLSSurveyFreshness.survey_code == survey_code
        ).values(
            full_update_in_progress=False
        )
        session.execute(freshness_update)
        session.commit()

        raise

    return progress
