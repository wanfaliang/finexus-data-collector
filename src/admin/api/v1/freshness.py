"""
BLS Freshness API Endpoints

Endpoints for managing BLS survey freshness detection.
Uses the new cycle-based update system.
"""
from typing import List, Optional
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from src.admin.core.database import get_db
from src.admin.schemas.freshness import (
    SurveyFreshnessResponse,
    FreshnessOverviewResponse,
)
from src.bls import update_manager

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

# Supported surveys
SUPPORTED_SURVEYS = [
    'AP', 'CU', 'LA', 'CE', 'PC', 'WP', 'SM', 'JT', 'EC', 'OE',
    'PR', 'TU', 'IP', 'LN', 'CW', 'SU', 'BD', 'EI'
]


def _get_survey_status_from_cycle(status: dict) -> str:
    """Determine survey status from cycle-based status"""
    if not status['has_current_cycle']:
        return "needs_update"
    if status.get('is_running'):
        return "updating"
    if status['is_complete']:
        return "current"
    return "needs_update"  # Paused/incomplete, can be resumed


@router.get("/overview", response_model=FreshnessOverviewResponse)
async def get_freshness_overview(db: Session = Depends(get_db)):
    """
    Get overview of freshness status for all surveys

    Returns summary counts and detailed status for each survey.
    Uses the new cycle-based update system.
    """
    surveys = []

    for survey_code in SUPPORTED_SURVEYS:
        survey_name = SURVEY_NAMES.get(survey_code, survey_code)

        try:
            status = update_manager.get_survey_status(db, survey_code)
            survey_status = _get_survey_status_from_cycle(status)

            # Calculate progress
            progress = None
            if status['total_series'] > 0:
                progress = status['series_updated'] / status['total_series']

            surveys.append(SurveyFreshnessResponse(
                survey_code=survey_code,
                survey_name=survey_name,
                status=survey_status,
                last_bls_update=None,  # No longer tracked in cycle system
                last_check=None,  # No longer tracked
                sentinels_changed=0,  # Sentinel system removed
                sentinels_total=0,
                update_frequency_days=None,
                update_progress=progress,
                series_updated=status['series_updated'],
                series_total=status['total_series'],
                last_full_update_completed=status['started_at'] if status['is_complete'] and status['started_at'] else None,
            ))
        except Exception as e:
            # Survey might not have series table yet
            surveys.append(SurveyFreshnessResponse(
                survey_code=survey_code,
                survey_name=survey_name,
                status="needs_update",
                last_bls_update=None,
                last_check=None,
                sentinels_changed=0,
                sentinels_total=0,
                update_frequency_days=None,
                update_progress=None,
                series_updated=0,
                series_total=0,
                last_full_update_completed=None,
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

    Returns list of survey codes where update cycle is not complete.
    """
    needs_update = []

    for survey_code in SUPPORTED_SURVEYS:
        try:
            status = update_manager.get_survey_status(db, survey_code)
            if not status['has_current_cycle'] or not status['is_complete']:
                needs_update.append(survey_code)
        except Exception:
            needs_update.append(survey_code)

    return needs_update


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

    survey_name = SURVEY_NAMES[survey_code]

    try:
        status = update_manager.get_survey_status(db, survey_code)
        survey_status = _get_survey_status_from_cycle(status)

        # Calculate progress
        progress = None
        if status['total_series'] > 0:
            progress = status['series_updated'] / status['total_series']

        return SurveyFreshnessResponse(
            survey_code=survey_code,
            survey_name=survey_name,
            status=survey_status,
            last_bls_update=None,
            last_check=None,
            sentinels_changed=0,
            sentinels_total=0,
            update_frequency_days=None,
            update_progress=progress,
            series_updated=status['series_updated'],
            series_total=status['total_series'],
            last_full_update_completed=status['started_at'].isoformat() if status['is_complete'] and status['started_at'] else None,
        )
    except Exception as e:
        return SurveyFreshnessResponse(
            survey_code=survey_code,
            survey_name=survey_name,
            status="needs_update",
            last_bls_update=None,
            last_check=None,
            sentinels_changed=0,
            sentinels_total=0,
            update_frequency_days=None,
            update_progress=None,
            series_updated=0,
            series_total=0,
            last_full_update_completed=None,
        )
