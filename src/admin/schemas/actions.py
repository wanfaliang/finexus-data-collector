"""
Action Request/Response Schemas
"""
from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel, Field


class FreshnessCheckRequest(BaseModel):
    """Request to check freshness"""
    survey_codes: Optional[List[str]] = Field(
        None,
        description="Specific surveys to check (empty = all surveys)"
    )


class FreshnessCheckResult(BaseModel):
    """Result for a single survey freshness check"""
    survey_code: str
    sentinels_checked: int
    sentinels_changed: int
    needs_update: bool
    error: Optional[str] = None


class FreshnessCheckResponse(BaseModel):
    """Response from freshness check"""
    check_time: datetime
    surveys_checked: int
    surveys_needing_update: int
    results: List[FreshnessCheckResult]


class UpdateTriggerRequest(BaseModel):
    """Request to trigger survey update"""
    force: bool = Field(
        False,
        description="Force update even if not flagged as needing update"
    )


class UpdateTriggerResponse(BaseModel):
    """Response from update trigger"""
    survey_code: str
    status: str  # "started", "already_running", "not_needed", "error"
    message: str
    series_count: Optional[int] = None
    estimated_requests: Optional[int] = None


class ResetFreshnessRequest(BaseModel):
    """Request to reset freshness status"""
    clear_update_flag: bool = Field(
        True,
        description="Clear needs_full_update flag"
    )
    reset_sentinels: bool = Field(
        False,
        description="Reset sentinel change tracking"
    )


class ResetFreshnessResponse(BaseModel):
    """Response from freshness reset"""
    survey_code: str
    success: bool
    message: str
