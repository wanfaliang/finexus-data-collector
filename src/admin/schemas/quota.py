"""
Quota API Schemas

Pydantic models for API quota tracking endpoints.
"""
from datetime import date, datetime
from typing import Dict, List
from pydantic import BaseModel, Field


class QuotaUsageResponse(BaseModel):
    """Daily quota usage for a data source"""

    date: date
    used: int
    limit: int
    remaining: int
    percentage_used: float = Field(..., description="Percentage of quota used (0-100)")

    class Config:
        from_attributes = True


class QuotaBreakdownItem(BaseModel):
    """Breakdown of quota usage"""

    label: str = Field(..., description="Survey code or script name")
    requests: int = Field(..., description="Number of requests")
    series: int = Field(..., description="Number of series")


class QuotaBreakdownResponse(BaseModel):
    """Quota usage breakdown by survey and script"""

    date: date
    total_requests: int
    total_series: int
    by_survey: List[QuotaBreakdownItem]
    by_script: List[QuotaBreakdownItem]


class UsageLogEntry(BaseModel):
    """Single usage log entry"""

    log_id: int
    usage_date: date
    execution_time: datetime
    survey_code: str
    script_name: str
    requests_used: int
    series_count: int

    class Config:
        from_attributes = True
