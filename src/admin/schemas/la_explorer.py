"""
Pydantic schemas for LA (Local Area Unemployment Statistics) Survey Explorer

LA survey dimensions: Area + Measure
"""
from typing import List, Optional
from pydantic import BaseModel


# ==================== Dimension Models ====================

class LAAreaItem(BaseModel):
    """LA Area dimension item"""
    area_code: str
    area_name: str
    area_type: Optional[str] = None

    class Config:
        from_attributes = True


class LAMeasureItem(BaseModel):
    """LA Measure dimension item"""
    measure_code: str
    measure_name: str

    class Config:
        from_attributes = True


class LADimensions(BaseModel):
    """Available dimensions for LA survey"""
    areas: List[LAAreaItem]
    measures: List[LAMeasureItem]


# ==================== Series Models ====================

class LASeriesInfo(BaseModel):
    """LA Series metadata with dimensions"""
    series_id: str
    series_title: str
    area_code: str
    area_name: str
    measure_code: str
    measure_name: str
    seasonal_code: Optional[str] = None
    begin_year: Optional[int] = None
    begin_period: Optional[str] = None
    end_year: Optional[int] = None
    end_period: Optional[str] = None
    is_active: bool = True

    class Config:
        from_attributes = True


class LASeriesListResponse(BaseModel):
    """Response for LA series list with filters"""
    survey_code: str = "LA"
    total: int
    limit: int
    offset: int
    series: List[LASeriesInfo]


# ==================== Data Models ====================

class LADataPoint(BaseModel):
    """A single LA time series observation"""
    year: int
    period: str
    period_name: str  # "January 2024", etc.
    value: Optional[float] = None
    footnote_codes: Optional[str] = None

    class Config:
        from_attributes = True


class LASeriesData(BaseModel):
    """Time series data for a single LA series"""
    series_id: str
    series_title: str
    area_name: str
    measure_name: str
    data_points: List[LADataPoint]


class LADataResponse(BaseModel):
    """Response for LA series data request"""
    survey_code: str = "LA"
    series: List[LASeriesData]


# ==================== Explorer Models ====================

class UnemploymentMetric(BaseModel):
    """Single area's unemployment metrics"""
    series_id: str
    area_code: str
    area_name: str
    area_type: str
    unemployment_rate: Optional[float] = None
    unemployment_level: Optional[float] = None  # in thousands
    employment_level: Optional[float] = None  # in thousands
    labor_force: Optional[float] = None  # in thousands
    latest_date: str
    month_over_month: Optional[float] = None  # percentage points
    year_over_year: Optional[float] = None  # percentage points


class LAOverviewResponse(BaseModel):
    """Overview of national unemployment statistics"""
    survey_code: str = "LA"
    national_unemployment: UnemploymentMetric
    last_updated: str


class LAStateAnalysisResponse(BaseModel):
    """State-level unemployment analysis"""
    survey_code: str = "LA"
    states: List[UnemploymentMetric]
    rankings: dict  # {"highest": [...], "lowest": [...]}


class LAMetroAnalysisResponse(BaseModel):
    """Metropolitan area unemployment analysis"""
    survey_code: str = "LA"
    metros: List[UnemploymentMetric]
    total_count: int


class OverviewTimelinePoint(BaseModel):
    """Single point in time for overview timeline"""
    year: int
    period: str
    period_name: str
    unemployment_rate: Optional[float] = None
    unemployment_level: Optional[float] = None
    employment_level: Optional[float] = None
    labor_force: Optional[float] = None


class LAOverviewTimelineResponse(BaseModel):
    """Timeline data for national overview"""
    survey_code: str = "LA"
    area_name: str
    timeline: List[OverviewTimelinePoint]


class StateTimelinePoint(BaseModel):
    """Timeline point with data for multiple states"""
    year: int
    period: str
    period_name: str
    states: dict  # {area_code: unemployment_rate}


class LAStateTimelineResponse(BaseModel):
    """Timeline data for states"""
    survey_code: str = "LA"
    timeline: List[StateTimelinePoint]
    state_names: dict  # {area_code: area_name}


class MetroTimelinePoint(BaseModel):
    """Timeline point for metro areas"""
    year: int
    period: str
    period_name: str
    metros: dict  # {area_code: unemployment_rate}


class LAMetroTimelineResponse(BaseModel):
    """Timeline data for metro areas"""
    survey_code: str = "LA"
    timeline: List[MetroTimelinePoint]
    metro_names: dict  # {area_code: area_name}
