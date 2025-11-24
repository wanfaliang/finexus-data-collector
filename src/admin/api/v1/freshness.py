"""
BLS Freshness API Endpoints

Endpoints for managing BLS survey freshness detection and sentinel system.
"""
from typing import List, Optional
from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func

from src.admin.core.database import get_db
from src.admin.schemas.freshness import (
    SurveyFreshnessResponse,
    SentinelResponse,
    FreshnessOverviewResponse,
)
from src.database.bls_tracking_models import BLSSurveyFreshness, BLSSurveySentinel

router = APIRouter()

# Survey name mapping
SURVEY_NAMES = {
    'AP': 'Average Price Data',
    'CU': 'Consumer Price Index',
    'LA': 'Local Area Unemployment',
    'CE': 'Current Employment Statistics',
    'PC': 'Producer Price Index - Commodity',
    'WP': 'Producer Price Index',
    'SM': 'State and Metro Area Employment',
    'JT': 'JOLTS',
    'EC': 'Employment Cost Index',
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


def _get_survey_status(freshness: Optional[BLSSurveyFreshness]) -> str:
    """Determine survey status from freshness record"""
    if not freshness:
        return "unknown"
    if freshness.full_update_in_progress:
        return "updating"
    if freshness.needs_full_update:
        return "needs_update"
    return "current"


def _calculate_update_progress(freshness: Optional[BLSSurveyFreshness]) -> Optional[float]:
    """Calculate update progress (0.0 to 1.0)"""
    if not freshness or not freshness.full_update_in_progress:
        return None
    if freshness.series_total_count == 0:
        return 0.0
    return freshness.series_updated_count / freshness.series_total_count


@router.get("/overview", response_model=FreshnessOverviewResponse)
async def get_freshness_overview(db: Session = Depends(get_db)):
    """
    Get overview of freshness status for all surveys

    Returns summary counts and detailed status for each survey.
    """
    # Get all freshness records
    freshness_records = db.query(BLSSurveyFreshness).all()

    # Build survey responses
    surveys = []
    for survey_code, survey_name in SURVEY_NAMES.items():
        # Find freshness record for this survey
        freshness = next(
            (f for f in freshness_records if f.survey_code == survey_code),
            None
        )

        status = _get_survey_status(freshness)
        progress = _calculate_update_progress(freshness)

        surveys.append(SurveyFreshnessResponse(
            survey_code=survey_code,
            survey_name=survey_name,
            status=status,
            last_bls_update=freshness.last_bls_update_detected if freshness else None,
            last_check=freshness.last_sentinel_check if freshness else None,
            sentinels_changed=freshness.sentinels_changed if freshness else 0,
            sentinels_total=freshness.sentinels_total if freshness else 0,
            update_frequency_days=float(freshness.bls_update_frequency_days) if freshness and freshness.bls_update_frequency_days else None,
            update_progress=progress,
            series_updated=freshness.series_updated_count if freshness else 0,
            series_total=freshness.series_total_count if freshness else 0,
            last_full_update_completed=freshness.last_full_update_completed if freshness else None,
        ))

    # Calculate summary counts
    total_surveys = len(surveys)
    surveys_current = sum(1 for s in surveys if s.status == "current")
    surveys_need_update = sum(1 for s in surveys if s.status == "needs_update")
    surveys_updating = sum(1 for s in surveys if s.status == "updating")

    return FreshnessOverviewResponse(
        total_surveys=total_surveys,
        surveys_current=surveys_current,
        surveys_need_update=surveys_need_update,
        surveys_updating=surveys_updating,
        surveys=surveys,
    )


@router.get("/surveys/needs-update", response_model=List[str])
async def get_surveys_needing_update(db: Session = Depends(get_db)):
    """
    Get list of survey codes that need updates

    Returns list of survey codes where needs_full_update = true
    """
    surveys = db.query(BLSSurveyFreshness.survey_code).filter(
        BLSSurveyFreshness.needs_full_update == True
    ).all()

    return [s[0] for s in surveys]


@router.get("/surveys/{survey_code}", response_model=SurveyFreshnessResponse)
async def get_survey_freshness(survey_code: str, db: Session = Depends(get_db)):
    """
    Get detailed freshness status for a specific survey

    Args:
        survey_code: BLS survey code (e.g., CU, LA, CE)
    """
    survey_code = survey_code.upper()

    if survey_code not in SURVEY_NAMES:
        raise HTTPException(status_code=404, detail=f"Survey {survey_code} not found")

    # Get freshness record
    freshness = db.query(BLSSurveyFreshness).filter(
        BLSSurveyFreshness.survey_code == survey_code
    ).first()

    status = _get_survey_status(freshness)
    progress = _calculate_update_progress(freshness)

    return SurveyFreshnessResponse(
        survey_code=survey_code,
        survey_name=SURVEY_NAMES[survey_code],
        status=status,
        last_bls_update=freshness.last_bls_update_detected if freshness else None,
        last_check=freshness.last_sentinel_check if freshness else None,
        sentinels_changed=freshness.sentinels_changed if freshness else 0,
        sentinels_total=freshness.sentinels_total if freshness else 0,
        update_frequency_days=float(freshness.bls_update_frequency_days) if freshness and freshness.bls_update_frequency_days else None,
        update_progress=progress,
        series_updated=freshness.series_updated_count if freshness else 0,
        series_total=freshness.series_total_count if freshness else 0,
        last_full_update_completed=freshness.last_full_update_completed if freshness else None,
    )


@router.get("/surveys/{survey_code}/sentinels", response_model=List[SentinelResponse])
async def get_survey_sentinels(
    survey_code: str,
    limit: Optional[int] = None,
    db: Session = Depends(get_db)
):
    """
    Get sentinel series for a survey

    Args:
        survey_code: BLS survey code
        limit: Optional limit on number of sentinels to return
    """
    survey_code = survey_code.upper()

    if survey_code not in SURVEY_NAMES:
        raise HTTPException(status_code=404, detail=f"Survey {survey_code} not found")

    # Query sentinels
    query = db.query(BLSSurveySentinel).filter(
        BLSSurveySentinel.survey_code == survey_code
    ).order_by(BLSSurveySentinel.sentinel_order)

    if limit:
        query = query.limit(limit)

    sentinels = query.all()

    return [
        SentinelResponse(
            series_id=s.series_id,
            sentinel_order=s.sentinel_order,
            last_value=s.last_value,
            last_year=s.last_year,
            last_period=s.last_period,
            check_count=s.check_count,
            change_count=s.change_count,
            last_changed_at=s.last_changed_at,
        )
        for s in sentinels
    ]
