"""
Freshness API Schemas

Pydantic models for BLS freshness detection API endpoints.
"""
from datetime import datetime
from typing import Optional, List
from decimal import Decimal
from pydantic import BaseModel, Field


class SentinelResponse(BaseModel):
    """Response model for a single sentinel series"""

    series_id: str
    sentinel_order: int
    last_value: Optional[Decimal] = None
    last_year: Optional[int] = None
    last_period: Optional[str] = None
    check_count: int
    change_count: int
    last_changed_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class SurveyFreshnessResponse(BaseModel):
    """Response model for survey freshness status"""

    survey_code: str
    survey_name: str
    status: str = Field(
        ..., description="current | needs_update | updating"
    )
    last_bls_update: Optional[datetime] = Field(
        None, description="When BLS last updated this survey (detected by sentinels)"
    )
    last_check: Optional[datetime] = Field(
        None, description="When we last checked sentinels"
    )
    sentinels_changed: int = Field(
        0, description="Number of sentinels that changed in last check"
    )
    sentinels_total: int = Field(50, description="Total number of sentinels")
    update_frequency_days: Optional[float] = Field(
        None, description="Average days between BLS updates"
    )
    update_progress: Optional[float] = Field(
        None, description="Progress of current update (0.0 to 1.0)"
    )
    series_updated: int = Field(0, description="Series updated in current/last update")
    series_total: int = Field(0, description="Total series in survey")
    last_full_update_completed: Optional[datetime] = None

    class Config:
        from_attributes = True


class FreshnessOverviewResponse(BaseModel):
    """Overview of all surveys freshness status"""

    total_surveys: int
    surveys_current: int
    surveys_need_update: int
    surveys_updating: int
    surveys: List[SurveyFreshnessResponse]


class CheckFreshnessRequest(BaseModel):
    """Request to check survey freshness"""

    surveys: Optional[List[str]] = Field(
        None, description="List of survey codes to check. None = all surveys"
    )
    verbose: bool = Field(False, description="Show detailed change information")


class UpdateRequest(BaseModel):
    """Request to update survey data"""

    surveys: List[str] = Field(..., description="Survey codes to update")
    start_year: Optional[int] = Field(None, description="Start year for data fetch")
    end_year: Optional[int] = Field(None, description="End year for data fetch")
    force: bool = Field(False, description="Force update even if current")
