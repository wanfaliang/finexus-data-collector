"""
BLS Quota API Endpoints

Endpoints for tracking and managing BLS API quota usage.
"""
from typing import List, Optional
from datetime import date, timedelta
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func

from src.admin.core.database import get_db
from src.admin.schemas.quota import (
    QuotaUsageResponse,
    QuotaBreakdownResponse,
    QuotaBreakdownItem,
    UsageLogEntry,
)
from src.database.bls_tracking_models import BLSAPIUsageLog

router = APIRouter()


@router.get("/today", response_model=QuotaUsageResponse)
async def get_today_quota(
    daily_limit: int = Query(500, description="Daily quota limit"),
    db: Session = Depends(get_db)
):
    """
    Get today's quota usage

    Args:
        daily_limit: Daily API request limit (default: 500)
    """
    today = date.today()

    # Sum requests used today
    used = db.query(
        func.sum(BLSAPIUsageLog.requests_used)
    ).filter(
        BLSAPIUsageLog.usage_date == today
    ).scalar() or 0

    remaining = max(0, daily_limit - used)
    percentage_used = (used / daily_limit * 100) if daily_limit > 0 else 0

    return QuotaUsageResponse(
        date=today,
        used=used,
        limit=daily_limit,
        remaining=remaining,
        percentage_used=round(percentage_used, 2),
    )


@router.get("/history", response_model=List[QuotaUsageResponse])
async def get_quota_history(
    days: int = Query(7, description="Number of days of history", ge=1, le=90),
    daily_limit: int = Query(500, description="Daily quota limit"),
    db: Session = Depends(get_db)
):
    """
    Get quota usage history

    Args:
        days: Number of days to retrieve (1-90)
        daily_limit: Daily API request limit
    """
    end_date = date.today()
    start_date = end_date - timedelta(days=days - 1)

    # Query usage grouped by date
    usage_by_date = db.query(
        BLSAPIUsageLog.usage_date,
        func.sum(BLSAPIUsageLog.requests_used).label('total_used')
    ).filter(
        BLSAPIUsageLog.usage_date >= start_date,
        BLSAPIUsageLog.usage_date <= end_date
    ).group_by(
        BLSAPIUsageLog.usage_date
    ).order_by(
        BLSAPIUsageLog.usage_date
    ).all()

    # Build response with all dates (including zeros)
    result = []
    current_date = start_date
    usage_dict = {row[0]: row[1] for row in usage_by_date}

    while current_date <= end_date:
        used = usage_dict.get(current_date, 0)
        remaining = max(0, daily_limit - used)
        percentage_used = (used / daily_limit * 100) if daily_limit > 0 else 0

        result.append(QuotaUsageResponse(
            date=current_date,
            used=used,
            limit=daily_limit,
            remaining=remaining,
            percentage_used=round(percentage_used, 2),
        ))
        current_date += timedelta(days=1)

    return result


@router.get("/breakdown", response_model=QuotaBreakdownResponse)
async def get_quota_breakdown(
    usage_date: Optional[date] = Query(None, description="Date to get breakdown for (default: today)"),
    db: Session = Depends(get_db)
):
    """
    Get quota usage breakdown by survey and script

    Args:
        usage_date: Date to analyze (default: today)
    """
    target_date = usage_date or date.today()

    # Get all logs for the date
    logs = db.query(BLSAPIUsageLog).filter(
        BLSAPIUsageLog.usage_date == target_date
    ).all()

    # Calculate totals
    total_requests = sum(log.requests_used for log in logs)
    total_series = sum(log.series_count for log in logs)

    # Breakdown by survey
    by_survey = {}
    for log in logs:
        survey = log.survey_code or "unknown"
        if survey not in by_survey:
            by_survey[survey] = {"requests": 0, "series": 0}
        by_survey[survey]["requests"] += log.requests_used
        by_survey[survey]["series"] += log.series_count

    survey_breakdown = [
        QuotaBreakdownItem(
            label=survey,
            requests=data["requests"],
            series=data["series"]
        )
        for survey, data in sorted(by_survey.items(), key=lambda x: x[1]["requests"], reverse=True)
    ]

    # Breakdown by script
    by_script = {}
    for log in logs:
        script = log.script_name or "unknown"
        if script not in by_script:
            by_script[script] = {"requests": 0, "series": 0}
        by_script[script]["requests"] += log.requests_used
        by_script[script]["series"] += log.series_count

    script_breakdown = [
        QuotaBreakdownItem(
            label=script,
            requests=data["requests"],
            series=data["series"]
        )
        for script, data in sorted(by_script.items(), key=lambda x: x[1]["requests"], reverse=True)
    ]

    return QuotaBreakdownResponse(
        date=target_date,
        total_requests=total_requests,
        total_series=total_series,
        by_survey=survey_breakdown,
        by_script=script_breakdown,
    )


@router.get("/logs", response_model=List[UsageLogEntry])
async def get_usage_logs(
    start_date: Optional[date] = Query(None, description="Start date"),
    end_date: Optional[date] = Query(None, description="End date"),
    survey_code: Optional[str] = Query(None, description="Filter by survey"),
    script_name: Optional[str] = Query(None, description="Filter by script"),
    limit: int = Query(100, description="Maximum number of logs", ge=1, le=1000),
    db: Session = Depends(get_db)
):
    """
    Get usage log entries

    Args:
        start_date: Filter logs from this date
        end_date: Filter logs to this date
        survey_code: Filter by survey code
        script_name: Filter by script name
        limit: Maximum number of logs to return
    """
    query = db.query(BLSAPIUsageLog)

    # Apply filters
    if start_date:
        query = query.filter(BLSAPIUsageLog.usage_date >= start_date)
    if end_date:
        query = query.filter(BLSAPIUsageLog.usage_date <= end_date)
    if survey_code:
        query = query.filter(BLSAPIUsageLog.survey_code == survey_code.upper())
    if script_name:
        query = query.filter(BLSAPIUsageLog.script_name.ilike(f"%{script_name}%"))

    # Order and limit
    logs = query.order_by(BLSAPIUsageLog.execution_time.desc()).limit(limit).all()

    return [
        UsageLogEntry(
            log_id=log.log_id,
            usage_date=log.usage_date,
            execution_time=log.execution_time,
            survey_code=log.survey_code or "unknown",
            script_name=log.script_name or "unknown",
            requests_used=log.requests_used,
            series_count=log.series_count,
        )
        for log in logs
    ]
