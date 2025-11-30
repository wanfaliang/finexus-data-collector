"""
BLS Actions API Endpoints

Endpoints for triggering BLS operations (checks, updates, resets).
"""
import sys
import subprocess
from pathlib import Path
from typing import Optional, List
from datetime import datetime
from decimal import Decimal
from collections import defaultdict
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy import func

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.admin.core.database import get_db
from src.admin.schemas.actions import (
    FreshnessCheckRequest,
    FreshnessCheckResponse,
    FreshnessCheckResult,
    UpdateTriggerRequest,
    UpdateTriggerResponse,
    ResetFreshnessRequest,
    ResetFreshnessResponse,
)
from src.database.bls_tracking_models import BLSSurveyFreshness, BLSSurveySentinel
from src.bls.bls_client import BLSClient
from src.bls import update_manager
from src.config import settings

router = APIRouter()

# Survey codes that are supported
SUPPORTED_SURVEYS = [
    'AP', 'CU', 'LA', 'CE', 'PC', 'WP', 'SM', 'JT', 'EC', 'OE',
    'PR', 'TU', 'IP', 'LN', 'CW', 'SU', 'BD', 'EI'
]


def _check_survey_freshness(survey_code: str, db: Session) -> FreshnessCheckResult:
    """
    Check freshness for a single survey by comparing current sentinel values
    with stored values.

    This is a lightweight check that queries the BLS API for sentinel series
    and compares with last known values.
    """
    try:
        # Get sentinels for this survey
        sentinels = db.query(BLSSurveySentinel).filter(
            BLSSurveySentinel.survey_code == survey_code
        ).order_by(BLSSurveySentinel.sentinel_order).all()

        if not sentinels:
            return FreshnessCheckResult(
                survey_code=survey_code,
                sentinels_checked=0,
                sentinels_changed=0,
                needs_update=False,
                error="No sentinels configured for this survey"
            )

        # Initialize BLS API client
        api_key = settings.api.bls_api_key
        if not api_key:
            return FreshnessCheckResult(
                survey_code=survey_code,
                sentinels_checked=len(sentinels),
                sentinels_changed=0,
                needs_update=False,
                error="BLS API key not configured"
            )

        client = BLSClient(api_key)

        # Fetch latest data for sentinel series
        series_ids = [s.series_id for s in sentinels]

        # Fetch last 5 years of data to ensure we get latest values
        current_year = datetime.now().year
        rows = client.get_many(series_ids, start_year=current_year - 5, end_year=current_year) # type: ignore

        if not rows:
            return FreshnessCheckResult(
                survey_code=survey_code,
                sentinels_checked=len(sentinels),
                sentinels_changed=0,
                needs_update=False,
                error="Failed to fetch data from BLS API or no data returned"
            )

        # Group rows by series_id and get latest observation for each
        series_data = defaultdict(list)
        for row in rows:
            series_data[row['series_id']].append(row) # type: ignore

        # Compare with stored sentinel values
        changes_detected = 0

        for sentinel in sentinels:
            series_id = sentinel.series_id

            # Get observations for this series
            observations = series_data.get(series_id, [])
            if not observations:
                continue

            # Find latest observation (marked with latest=True or first one)
            latest_obs = next((obs for obs in observations if obs.get('latest')), None)
            if not latest_obs:
                # Sort by year and period to get most recent
                observations.sort(key=lambda x: (x.get('year') or 0, x.get('period') or ''), reverse=True)
                latest_obs = observations[0]

            # Get values from API response
            latest_year = latest_obs.get('year')  # Already int from BLSClient
            latest_period = latest_obs.get('period')
            latest_value = latest_obs.get('value')  # Already float from BLSClient

            # Convert to match database types for comparison
            # Year: int, Period: str, Value: Decimal
            if latest_value is not None and not isinstance(latest_value, Decimal):
                latest_value = Decimal(str(latest_value))

            # Check if values have changed
            if (latest_year != sentinel.last_year or
                latest_period != sentinel.last_period or
                latest_value != sentinel.last_value):

                changes_detected += 1

                # Update sentinel record
                sentinel.last_value = latest_value # type: ignore
                sentinel.last_year = latest_year
                sentinel.last_period = latest_period
                sentinel.last_changed_at = datetime.now() # type: ignore
                sentinel.change_count = (sentinel.change_count or 0) + 1 # type: ignore

            # Update check count
            sentinel.check_count = (sentinel.check_count or 0) + 1 # type: ignore

        # Commit sentinel updates
        db.commit()

        return FreshnessCheckResult(
            survey_code=survey_code,
            sentinels_checked=len(sentinels),
            sentinels_changed=changes_detected,
            needs_update=(changes_detected > 0),
            error=None
        )

    except Exception as e:
        return FreshnessCheckResult(
            survey_code=survey_code,
            sentinels_checked=0,
            sentinels_changed=0,
            needs_update=False,
            error=str(e)
        )


@router.post("/freshness/check", response_model=FreshnessCheckResponse)
async def trigger_freshness_check(
    request: FreshnessCheckRequest,
    db: Session = Depends(get_db)
):
    """
    Trigger freshness check for surveys

    Checks sentinel series to detect if BLS has published new data.
    This is a lightweight operation that uses minimal API quota.

    Args:
        request: Survey codes to check (empty = all surveys with sentinels)
    """
    check_time = datetime.now()

    # Determine which surveys to check
    if request.survey_codes:
        # Validate survey codes
        invalid_codes = [s for s in request.survey_codes if s.upper() not in SUPPORTED_SURVEYS]
        if invalid_codes:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid survey codes: {', '.join(invalid_codes)}"
            )
        survey_codes = [s.upper() for s in request.survey_codes]
    else:
        # Check all surveys that have sentinels
        survey_codes = db.query(BLSSurveySentinel.survey_code).distinct().all()
        survey_codes = [s[0] for s in survey_codes]

    # Run freshness check for each survey
    results = []
    for survey_code in survey_codes:
        result = _check_survey_freshness(survey_code, db)
        results.append(result)

    # Update last check time in freshness tracking
    for result in results:
        freshness = db.query(BLSSurveyFreshness).filter(
            BLSSurveyFreshness.survey_code == result.survey_code
        ).first()

        if freshness:
            freshness.last_sentinel_check = check_time # type: ignore
            freshness.sentinels_total = result.sentinels_checked
            freshness.sentinels_changed = result.sentinels_changed
            freshness.total_checks = (freshness.total_checks or 0) + 1 # type: ignore

            if result.needs_update:
                freshness.needs_full_update = True # type: ignore
                freshness.last_bls_update_detected = check_time # type: ignore
                freshness.total_updates_detected = (freshness.total_updates_detected or 0) + 1 # type: ignore
                # Reset is_current=False for all series in this survey so they get updated
                update_manager.reset_series_status(db, result.survey_code)
            else:
                # Clear flag if no changes detected - data is current
                freshness.needs_full_update = False # type: ignore

    db.commit()

    surveys_needing_update = sum(1 for r in results if r.needs_update)

    return FreshnessCheckResponse(
        check_time=check_time,
        surveys_checked=len(results),
        surveys_needing_update=surveys_needing_update,
        results=results
    )


@router.post("/freshness/execute/{survey_code}", response_model=UpdateTriggerResponse)
async def execute_survey_update(
    survey_code: str,
    request: UpdateTriggerRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Execute full data update for a survey immediately

    This actually runs the data collection process in the background.
    The update will consume BLS API quota.

    Args:
        survey_code: BLS survey code (e.g., CU, LA, CE)
        request: Update options (force, etc.)
    """
    survey_code = survey_code.upper()

    if survey_code not in SUPPORTED_SURVEYS:
        raise HTTPException(
            status_code=404,
            detail=f"Survey {survey_code} not supported"
        )

    # Get freshness record
    freshness = db.query(BLSSurveyFreshness).filter(
        BLSSurveyFreshness.survey_code == survey_code
    ).first()

    if not freshness:
        raise HTTPException(
            status_code=404,
            detail=f"No freshness tracking for survey {survey_code}. Run select_sentinels.py first."
        )

    # Check if update is already in progress
    if freshness.full_update_in_progress and not request.force:
        return UpdateTriggerResponse(
            survey_code=survey_code,
            status="already_running",
            message="Update is already in progress for this survey",
            series_count=freshness.series_total_count, # type: ignore
            estimated_requests=None
        )

    # Set update flags
    freshness.needs_full_update = True # type: ignore
    freshness.full_update_in_progress = True # type: ignore
    freshness.last_full_update_started = datetime.now() # type: ignore
    db.commit()

    # Calculate estimated requests
    series_count = freshness.series_total_count or 0
    estimated_requests = (series_count + 49) // 50  # 50 series per request

    # Execute update in background using update_manager
    def run_update():
        """Run survey update using update_manager"""
        print(f"[Execute] Starting update for {survey_code}")

        try:
            # Create BLS client
            client = BLSClient(api_key=settings.api.bls_api_key)

            # Define progress callback to print updates
            def progress_callback(progress: update_manager.UpdateProgress):
                print(f"[Execute] {survey_code} progress: {progress.series_updated}/{progress.total_series} series, {progress.requests_used} requests")

            # Run the update
            result = update_manager.update_survey(
                survey_code=survey_code,
                session=db,
                client=client,
                force=request.force,
                progress_callback=progress_callback
            )

            print(f"[Execute] Update completed for {survey_code}")
            print(f"[Execute] Results: {result.series_updated} series, {result.observations_added} observations, {result.requests_used} requests")

            if result.errors:
                print(f"[Execute] Errors encountered: {', '.join(result.errors)}")

        except Exception as e:
            print(f"[Execute] Exception running update for {survey_code}: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()

    # Add to background tasks
    background_tasks.add_task(run_update)

    return UpdateTriggerResponse(
        survey_code=survey_code,
        status="started",
        message=f"Update execution started for {survey_code} in background. Check freshness status for progress.",
        series_count=series_count, # type: ignore
        estimated_requests=estimated_requests # type: ignore
    )


@router.post("/freshness/reset/{survey_code}", response_model=ResetFreshnessResponse)
async def reset_survey_freshness(
    survey_code: str,
    request: ResetFreshnessRequest,
    db: Session = Depends(get_db)
):
    """
    Reset freshness tracking for a survey

    Useful for clearing flags after manual updates or fixing stuck states.

    Args:
        survey_code: BLS survey code
        request: What to reset (flags, sentinels, etc.)
    """
    survey_code = survey_code.upper()

    if survey_code not in SUPPORTED_SURVEYS:
        raise HTTPException(
            status_code=404,
            detail=f"Survey {survey_code} not supported"
        )

    freshness = db.query(BLSSurveyFreshness).filter(
        BLSSurveyFreshness.survey_code == survey_code
    ).first()

    if not freshness:
        raise HTTPException(
            status_code=404,
            detail=f"No freshness tracking for survey {survey_code}"
        )

    # Reset flags as requested
    changes = []

    if request.clear_update_flag:
        freshness.needs_full_update = False # type: ignore
        freshness.full_update_in_progress = False # type: ignore
        freshness.series_updated_count = 0 # type: ignore
        changes.append("cleared update flags")

    if request.reset_sentinels:
        freshness.sentinels_changed = 0 # type: ignore
        freshness.sentinels_total = 0 # type: ignore
        changes.append("reset sentinel counters")

    db.commit()

    return ResetFreshnessResponse(
        survey_code=survey_code,
        success=True,
        message=f"Reset complete: {', '.join(changes)}" if changes else "No changes requested"
    )


@router.get("/surveys", response_model=List[str])
async def list_supported_surveys():
    """
    Get list of supported survey codes
    """
    return SUPPORTED_SURVEYS
