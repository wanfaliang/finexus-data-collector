"""
BLS Actions API Endpoints

Endpoints for triggering BLS operations:
- Check freshness (compare BLS API with database)
- Execute updates (soft or force)
- View update cycle status
"""
import sys
from pathlib import Path
from typing import Optional, List
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.admin.core.database import get_db
from src.database.bls_tracking_models import BLSUpdateCycle, BLSAPIUsageLog
from src.bls.bls_client import BLSClient
from src.bls import update_manager
from src.bls import freshness_checker
from src.config import settings

# Daily BLS API limit
DAILY_API_LIMIT = 500

router = APIRouter()

# Survey codes that are supported
SUPPORTED_SURVEYS = [
    'AP', 'CU', 'LA', 'CE', 'PC', 'WP', 'SM', 'JT', 'OE',
    'PR', 'TU', 'IP', 'LN', 'CW', 'SU', 'BD', 'EI'
]


# ============================================================================
# Request/Response Models
# ============================================================================

class FreshnessCheckRequest(BaseModel):
    """Request to check freshness for surveys"""
    survey_codes: Optional[List[str]] = Field(
        None,
        description="Survey codes to check. Empty = all surveys."
    )


class FreshnessCheckResult(BaseModel):
    """Result of freshness check for a single survey"""
    survey_code: str
    survey_name: str
    has_new_data: bool
    series_checked: int
    series_with_new_data: int
    our_latest: Optional[str]
    bls_latest: Optional[str]
    error: Optional[str]


class FreshnessCheckResponse(BaseModel):
    """Response for freshness check"""
    checked_at: datetime
    surveys_checked: int
    surveys_with_new_data: int
    results: List[FreshnessCheckResult]


class UpdateTriggerRequest(BaseModel):
    """Request to trigger an update"""
    force: bool = Field(
        False,
        description="If true, create new cycle and start fresh. If false, resume existing cycle."
    )
    max_requests: Optional[int] = Field(
        None,
        description="Maximum API requests for this session. If None, uses all remaining daily quota."
    )
    api_key: Optional[str] = Field(
        None,
        description="Optional custom BLS API key. If provided, skips quota validation and usage logging."
    )
    user_agent: Optional[str] = Field(
        None,
        description="Optional custom User-Agent string. Should match API key registration with BLS."
    )


class UpdateTriggerResponse(BaseModel):
    """Response for update trigger"""
    survey_code: str
    status: str  # 'started', 'already_running', 'error'
    message: str
    cycle_id: Optional[int]
    series_total: Optional[int]
    series_remaining: Optional[int]


class SurveyStatusResponse(BaseModel):
    """Status of a survey's update cycle"""
    survey_code: str
    has_current_cycle: bool
    cycle_id: Optional[int]
    total_series: int
    series_updated: int
    progress_pct: float
    started_at: Optional[datetime]
    is_complete: bool


class AllSurveysStatusResponse(BaseModel):
    """Status of all surveys"""
    surveys: List[SurveyStatusResponse]


# ============================================================================
# Endpoints
# ============================================================================

@router.post("/freshness/check", response_model=FreshnessCheckResponse)
async def check_freshness(
    request: FreshnessCheckRequest,
    db: Session = Depends(get_db)
):
    """
    Check if BLS has published new data.

    Compares our latest data with BLS API for 50 series per survey.
    This is a lightweight check that helps decide if updates are needed.
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
        survey_codes = SUPPORTED_SURVEYS

    # Initialize BLS API client
    api_key = settings.api.bls_api_key
    if not api_key:
        raise HTTPException(
            status_code=500,
            detail="BLS API key not configured"
        )

    client = BLSClient(api_key)

    # Check freshness
    check_results = freshness_checker.check_all_surveys(db, client, survey_codes)

    # Convert to response format
    results = []
    for r in check_results:
        results.append(FreshnessCheckResult(
            survey_code=r.survey_code,
            survey_name=r.survey_name,
            has_new_data=r.has_new_data,
            series_checked=r.series_checked,
            series_with_new_data=r.series_with_new_data,
            our_latest=r.our_latest,
            bls_latest=r.bls_latest,
            error=r.error
        ))

    surveys_with_new_data = sum(1 for r in results if r.has_new_data)

    return FreshnessCheckResponse(
        checked_at=check_time,
        surveys_checked=len(results),
        surveys_with_new_data=surveys_with_new_data,
        results=results
    )


@router.post("/update/{survey_code}", response_model=UpdateTriggerResponse)
async def trigger_update(
    survey_code: str,
    request: UpdateTriggerRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Trigger an update for a survey.

    - force=False (default): Resume existing cycle or create new one if none exists
    - force=True: Create new cycle and start fresh
    """
    survey_code = survey_code.upper()

    if survey_code not in SUPPORTED_SURVEYS:
        raise HTTPException(
            status_code=404,
            detail=f"Survey {survey_code} not supported"
        )

    # Check current status
    status = update_manager.get_survey_status(db, survey_code)

    # Check if update is already running
    if status.get('is_running'):
        return UpdateTriggerResponse(
            survey_code=survey_code,
            status="already_running",
            message=f"Update cycle #{status['cycle_id']} is already in progress",
            cycle_id=status['cycle_id'],
            series_total=status['total_series'],
            series_remaining=status['total_series'] - status['series_updated']
        )

    # Calculate remaining series
    if status['has_current_cycle'] and not request.force:
        series_remaining = status['total_series'] - status['series_updated']
    else:
        series_remaining = status['total_series']

    # Check if using custom API key
    using_custom_key = bool(request.api_key)

    # Determine max_requests to use
    max_requests = request.max_requests
    if using_custom_key:
        # Custom key: skip quota validation, user manages their own limits
        if max_requests is None:
            max_requests = 500  # Default to full daily limit for custom key
    else:
        # System key: validate against tracked quota
        if max_requests is None:
            # Use all remaining quota
            max_requests = update_manager.get_remaining_quota(db, DAILY_API_LIMIT)
        else:
            # Validate against remaining quota
            remaining = update_manager.get_remaining_quota(db, DAILY_API_LIMIT)
            if max_requests > remaining:
                raise HTTPException(
                    status_code=400,
                    detail=f"Requested {max_requests} requests but only {remaining} remaining today"
                )

        if max_requests <= 0:
            raise HTTPException(
                status_code=400,
                detail="No API quota remaining today. Please try again tomorrow."
            )

    # Determine which API key and user agent to use
    api_key_to_use = request.api_key if using_custom_key else settings.api.bls_api_key
    user_agent_to_use = request.user_agent if (using_custom_key and request.user_agent) else None

    # Capture values for background task (avoid closure issues)
    _survey_code = survey_code
    _force = request.force
    _max_requests = max_requests
    _using_custom_key = using_custom_key
    _api_key = api_key_to_use
    _user_agent = user_agent_to_use

    # Execute update in background
    def run_update():
        """Run survey update in background"""
        from src.database.connection import get_session

        key_info = "custom key" if _using_custom_key else "system key"
        agent_info = f", custom user-agent" if _user_agent else ""
        print(f"[Actions] Starting {'force ' if _force else ''}update for {_survey_code} (max {_max_requests} requests, {key_info}{agent_info})")

        try:
            # Create a new database session for background task
            with get_session() as bg_session:
                # Create BLS client with appropriate key and user agent
                client_kwargs = {"api_key": _api_key}
                if _user_agent:
                    client_kwargs["user_agent"] = _user_agent
                client = BLSClient(**client_kwargs)

                # Define progress callback
                def progress_callback(progress):
                    print(f"[Actions] {_survey_code} progress: {progress.series_updated}/{progress.total_series} series")

                # Run the update (skip logging if using custom key)
                result = update_manager.update_survey(
                    survey_code=_survey_code,
                    session=bg_session,
                    client=client,
                    force=_force,
                    max_quota=_max_requests,
                    progress_callback=progress_callback,
                    skip_usage_logging=_using_custom_key
                )

                status_msg = "completed" if result.completed else "paused (session quota reached)"
                print(f"[Actions] Update {status_msg} for {_survey_code}")
                print(f"[Actions] Results: {result.series_updated} series, {result.observations_added} observations, {result.requests_used} requests")

                if result.errors:
                    print(f"[Actions] Errors encountered: {len(result.errors)}")
                    for err in result.errors[:5]:
                        print(f"[Actions]   - {err[:100]}")

        except Exception as e:
            print(f"[Actions] Exception running update for {_survey_code}: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()

    # Add to background tasks
    background_tasks.add_task(run_update)

    # Get cycle ID (will be created by update_survey)
    # For now, return the current one or indicate new will be created
    cycle_id = status['cycle_id'] if status['has_current_cycle'] and not request.force else None

    return UpdateTriggerResponse(
        survey_code=survey_code,
        status="started",
        message=f"{'Force update' if request.force else 'Update'} started for {survey_code}",
        cycle_id=cycle_id,
        series_total=status['total_series'],
        series_remaining=series_remaining
    )


@router.get("/status/{survey_code}", response_model=SurveyStatusResponse)
async def get_survey_status(
    survey_code: str,
    db: Session = Depends(get_db)
):
    """
    Get the current update status for a survey.
    """
    survey_code = survey_code.upper()

    if survey_code not in SUPPORTED_SURVEYS:
        raise HTTPException(
            status_code=404,
            detail=f"Survey {survey_code} not supported"
        )

    status = update_manager.get_survey_status(db, survey_code)

    return SurveyStatusResponse(
        survey_code=survey_code,
        has_current_cycle=status['has_current_cycle'],
        cycle_id=status['cycle_id'],
        total_series=status['total_series'],
        series_updated=status['series_updated'],
        progress_pct=status['progress_pct'],
        started_at=status['started_at'],
        is_complete=status['is_complete']
    )


@router.get("/status", response_model=AllSurveysStatusResponse)
async def get_all_surveys_status(
    db: Session = Depends(get_db)
):
    """
    Get update status for all surveys.
    """
    surveys = []
    for code in SUPPORTED_SURVEYS:
        try:
            status = update_manager.get_survey_status(db, code)
            surveys.append(SurveyStatusResponse(
                survey_code=code,
                has_current_cycle=status['has_current_cycle'],
                cycle_id=status['cycle_id'],
                total_series=status['total_series'],
                series_updated=status['series_updated'],
                progress_pct=status['progress_pct'],
                started_at=status['started_at'],
                is_complete=status['is_complete']
            ))
        except Exception as e:
            # Survey might not have series table yet
            surveys.append(SurveyStatusResponse(
                survey_code=code,
                has_current_cycle=False,
                cycle_id=None,
                total_series=0,
                series_updated=0,
                progress_pct=0,
                started_at=None,
                is_complete=False
            ))

    return AllSurveysStatusResponse(surveys=surveys)


@router.get("/surveys", response_model=List[str])
async def list_supported_surveys():
    """
    Get list of supported survey codes
    """
    return SUPPORTED_SURVEYS


class QuotaResponse(BaseModel):
    """Response for quota status"""
    daily_limit: int
    used_today: int
    remaining: int


@router.get("/quota", response_model=QuotaResponse)
async def get_quota_status(db: Session = Depends(get_db)):
    """
    Get today's API quota status.

    Returns daily limit, used requests, and remaining quota.
    """
    remaining = update_manager.get_remaining_quota(db, DAILY_API_LIMIT)
    used = DAILY_API_LIMIT - remaining

    return QuotaResponse(
        daily_limit=DAILY_API_LIMIT,
        used_today=used,
        remaining=remaining
    )


# ============================================================================
# Compatibility Endpoints (for existing Dashboard)
# ============================================================================

# Survey names mapping
SURVEY_NAMES = {
    'AP': 'Average Price Data',
    'CU': 'Consumer Price Index',
    'LA': 'Local Area Unemployment',
    'CE': 'Current Employment Statistics',
    'PC': 'Producer Price Index - Commodity',
    'WP': 'Producer Price Index',
    'SM': 'State and Metro Area Employment',
    'JT': 'JOLTS',
    'OE': 'Occupational Employment',
    'PR': 'Major Sector Productivity',
    'TU': 'American Time Use Survey',
    'IP': 'Industry Productivity',
    'LN': 'Labor Force Statistics',
    'CW': 'CPI - Urban Wage Earners',
    'SU': 'Chained CPI',
    'BD': 'Business Employment Dynamics',
    'EI': 'Import/Export Price Indexes',
}


class LegacySurveyFreshness(BaseModel):
    """Legacy format for backward compatibility with Dashboard"""
    survey_code: str
    survey_name: str
    status: str  # 'current', 'needs_update', 'updating'
    last_bls_update: Optional[str]
    last_check: Optional[str]
    sentinels_changed: int
    sentinels_total: int
    update_frequency_days: Optional[float]
    update_progress: Optional[float]
    series_updated: int
    series_total: int
    last_full_update_completed: Optional[str]


class LegacyFreshnessOverview(BaseModel):
    """Legacy format for backward compatibility with Dashboard"""
    total_surveys: int
    surveys_current: int
    surveys_need_update: int
    surveys_updating: int
    surveys: List[LegacySurveyFreshness]


@router.get("/freshness/overview", response_model=LegacyFreshnessOverview)
async def get_freshness_overview_legacy(db: Session = Depends(get_db)):
    """
    Legacy endpoint for Dashboard compatibility.
    Converts cycle-based status to the old sentinel-based format.
    """
    surveys = []
    surveys_current = 0
    surveys_need_update = 0
    surveys_updating = 0

    for code in SUPPORTED_SURVEYS:
        try:
            status = update_manager.get_survey_status(db, code)

            # Determine status string
            if status['has_current_cycle']:
                if status['is_complete']:
                    survey_status = 'current'
                    surveys_current += 1
                else:
                    # Has incomplete cycle - check if recently active
                    if status['started_at']:
                        time_since_start = (datetime.now() - status['started_at']).total_seconds()
                        if time_since_start < 300:  # Started in last 5 minutes
                            survey_status = 'updating'
                            surveys_updating += 1
                        else:
                            survey_status = 'needs_update'  # Paused, can be resumed
                            surveys_need_update += 1
                    else:
                        survey_status = 'needs_update'
                        surveys_need_update += 1
            else:
                survey_status = 'needs_update'
                surveys_need_update += 1

            # Calculate progress
            progress = None
            if status['total_series'] > 0:
                progress = status['series_updated'] / status['total_series']

            surveys.append(LegacySurveyFreshness(
                survey_code=code,
                survey_name=SURVEY_NAMES.get(code, code),
                status=survey_status,
                last_bls_update=None,  # No longer tracked
                last_check=None,  # No longer tracked
                sentinels_changed=0,  # Sentinel system removed
                sentinels_total=0,
                update_frequency_days=None,
                update_progress=progress,
                series_updated=status['series_updated'],
                series_total=status['total_series'],
                last_full_update_completed=status['started_at'].isoformat() if status['is_complete'] and status['started_at'] else None
            ))

        except Exception as e:
            # Survey might not have series table yet
            surveys.append(LegacySurveyFreshness(
                survey_code=code,
                survey_name=SURVEY_NAMES.get(code, code),
                status='needs_update',
                last_bls_update=None,
                last_check=None,
                sentinels_changed=0,
                sentinels_total=0,
                update_frequency_days=None,
                update_progress=None,
                series_updated=0,
                series_total=0,
                last_full_update_completed=None
            ))
            surveys_need_update += 1

    return LegacyFreshnessOverview(
        total_surveys=len(SUPPORTED_SURVEYS),
        surveys_current=surveys_current,
        surveys_need_update=surveys_need_update,
        surveys_updating=surveys_updating,
        surveys=surveys
    )
