"""
BEA API Schemas

Pydantic models for BEA (Bureau of Economic Analysis) API endpoints.
"""
from datetime import datetime
from typing import Optional, List, Dict, Any
from decimal import Decimal
from pydantic import BaseModel, Field


# ===================== Dataset Freshness ===================== #

class BEADatasetFreshnessResponse(BaseModel):
    """Response model for BEA dataset freshness status"""

    dataset_name: str
    latest_data_year: Optional[int] = None
    latest_data_period: Optional[str] = None
    last_checked_at: Optional[datetime] = None
    last_bea_update_detected: Optional[datetime] = None
    needs_update: bool = False
    update_in_progress: bool = False
    last_update_completed: Optional[datetime] = None
    tables_count: int = 0
    series_count: int = 0
    data_points_count: int = 0
    total_checks: int = 0
    total_updates_detected: int = 0

    class Config:
        from_attributes = True


class BEAFreshnessOverviewResponse(BaseModel):
    """Overview of BEA datasets freshness"""

    total_datasets: int
    datasets_current: int
    datasets_need_update: int
    datasets_updating: int
    total_data_points: int
    datasets: List[BEADatasetFreshnessResponse]


# ===================== API Usage ===================== #

class BEAAPIUsageResponse(BaseModel):
    """Response model for BEA API usage statistics"""

    date: str
    total_requests: int
    total_data_mb: float
    total_errors: int
    requests_remaining: int = Field(
        100, description="Requests remaining in current minute"
    )
    data_mb_remaining: float = Field(
        100.0, description="Data MB remaining in current minute"
    )


class BEAAPIUsageDetailResponse(BaseModel):
    """Detailed API usage by dataset"""

    date: str
    dataset_name: Optional[str]
    method_name: Optional[str]
    requests_count: int
    data_bytes: int
    error_count: int


# ===================== Collection Runs ===================== #

class BEACollectionRunResponse(BaseModel):
    """Response model for a collection run"""

    run_id: int
    dataset_name: str
    run_type: str
    frequency: Optional[str] = None  # 'A', 'Q', 'M' for NIPA/GDPbyIndustry
    geo_scope: Optional[str] = None  # 'STATE', 'COUNTY', 'MSA' for Regional
    year_spec: Optional[str] = None  # 'ALL', 'LAST5', etc.
    started_at: datetime
    completed_at: Optional[datetime] = None
    status: str
    error_message: Optional[str] = None
    tables_processed: int = 0
    series_processed: int = 0
    data_points_inserted: int = 0
    data_points_updated: int = 0
    api_requests_made: int = 0
    start_year: Optional[int] = None
    end_year: Optional[int] = None
    duration_seconds: Optional[float] = None

    class Config:
        from_attributes = True


# ===================== NIPA Explorer ===================== #

class NIPATableResponse(BaseModel):
    """Response model for NIPA table metadata"""

    table_name: str
    table_description: str
    has_annual: bool = True
    has_quarterly: bool = False
    has_monthly: bool = False
    first_year: Optional[int] = None
    last_year: Optional[int] = None
    series_count: int = 0
    is_active: bool = True

    class Config:
        from_attributes = True


class NIPASeriesResponse(BaseModel):
    """Response model for NIPA series metadata"""

    series_code: str
    table_name: str
    line_number: int
    line_description: str
    metric_name: Optional[str] = None
    cl_unit: Optional[str] = None
    unit_mult: Optional[int] = None
    data_points_count: int = 0

    class Config:
        from_attributes = True


class NIPADataPointResponse(BaseModel):
    """Response model for a NIPA data point"""

    series_code: str
    time_period: str
    value: Optional[Decimal] = None
    note_ref: Optional[str] = None
    year: Optional[int] = None
    period_type: Optional[str] = None  # 'A', 'Q', 'M'

    class Config:
        from_attributes = True


class NIPATimeSeriesResponse(BaseModel):
    """Response model for NIPA time series data"""

    series_code: str
    line_description: str
    metric_name: Optional[str] = None
    unit: Optional[str] = None
    data: List[Dict[str, Any]]


# ===================== Regional Explorer ===================== #

class RegionalTableResponse(BaseModel):
    """Response model for Regional table metadata"""

    table_name: str
    table_description: str
    geo_scope: Optional[str] = None
    first_year: Optional[int] = None
    last_year: Optional[int] = None
    line_codes_count: int = 0
    is_active: bool = True

    class Config:
        from_attributes = True


class RegionalLineCodeResponse(BaseModel):
    """Response model for Regional line code"""

    table_name: str
    line_code: int
    line_description: str
    cl_unit: Optional[str] = None
    unit_mult: Optional[int] = None

    class Config:
        from_attributes = True


class RegionalGeoResponse(BaseModel):
    """Response model for Regional geographic area"""

    geo_fips: str
    geo_name: str
    geo_type: Optional[str] = None
    parent_fips: Optional[str] = None

    class Config:
        from_attributes = True


class RegionalDataPointResponse(BaseModel):
    """Response model for a Regional data point"""

    table_name: str
    line_code: int
    geo_fips: str
    geo_name: Optional[str] = None
    time_period: str
    value: Optional[Decimal] = None
    cl_unit: Optional[str] = None
    unit_mult: Optional[int] = None

    class Config:
        from_attributes = True


class RegionalTimeSeriesResponse(BaseModel):
    """Response model for Regional time series data"""

    table_name: str
    line_code: int
    line_description: str
    geo_fips: str
    geo_name: str
    unit: Optional[str] = None
    data: List[Dict[str, Any]]


# ===================== Request Models ===================== #

class BEAUpdateRequest(BaseModel):
    """Request to update BEA data"""

    dataset: str = Field(..., description="Dataset to update: NIPA, Regional, or all")
    year: str = Field("LAST5", description="Year specification")
    force: bool = Field(False, description="Force update even if recent")


class NIPADataRequest(BaseModel):
    """Request for NIPA data"""

    table_name: str
    frequency: str = Field("A", description="A=Annual, Q=Quarterly, M=Monthly")
    start_year: Optional[int] = None
    end_year: Optional[int] = None
    line_numbers: Optional[List[int]] = None


class RegionalDataRequest(BaseModel):
    """Request for Regional data"""

    table_name: str
    line_code: int
    geo_fips: str = Field("STATE", description="Geographic scope or specific FIPS")
    start_year: Optional[int] = None
    end_year: Optional[int] = None
