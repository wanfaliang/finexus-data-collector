"""
Pydantic schemas for CU (Consumer Price Index) Survey Explorer

CU survey dimensions: Area + Item
"""
from typing import List, Optional
from pydantic import BaseModel


# ==================== Dimension Models ====================

class CUAreaItem(BaseModel):
    """CU Area dimension item"""
    area_code: str
    area_name: str
    display_level: int
    selectable: bool
    sort_sequence: int

    class Config:
        from_attributes = True


class CUItemItem(BaseModel):
    """CU Item dimension item"""
    item_code: str
    item_name: str
    display_level: int
    selectable: bool
    sort_sequence: int

    class Config:
        from_attributes = True


class CUDimensions(BaseModel):
    """Available dimensions for CU survey"""
    areas: List[CUAreaItem]
    items: List[CUItemItem]


# ==================== Series Models ====================

class CUSeriesInfo(BaseModel):
    """CU Series metadata with dimensions"""
    series_id: str
    series_title: str
    area_code: str
    area_name: str
    item_code: str
    item_name: str
    seasonal_code: str
    periodicity_code: Optional[str] = None
    base_period: Optional[str] = None
    begin_year: Optional[int] = None
    begin_period: Optional[str] = None
    end_year: Optional[int] = None
    end_period: Optional[str] = None
    is_active: bool = True

    class Config:
        from_attributes = True


class CUSeriesListResponse(BaseModel):
    """Response for CU series list with filters"""
    survey_code: str = "CU"
    total: int
    limit: int
    offset: int
    series: List[CUSeriesInfo]


# ==================== Data Models ====================

class CUDataPoint(BaseModel):
    """A single CU time series observation"""
    year: int
    period: str
    period_name: str  # "January 2024", etc.
    value: Optional[float] = None
    footnote_codes: Optional[str] = None

    class Config:
        from_attributes = True


class CUSeriesData(BaseModel):
    """Time series data for a single CU series"""
    series_id: str
    series_title: str
    area_name: str
    item_name: str
    data_points: List[CUDataPoint]


class CUDataResponse(BaseModel):
    """Response for CU series data request"""
    survey_code: str = "CU"
    series: List[CUSeriesData]


# ==================== Analytics Models ====================

class InflationMetric(BaseModel):
    """Inflation rate metrics for a single series"""
    series_id: str
    item_name: str
    latest_value: Optional[float] = None
    latest_date: Optional[str] = None  # "2024-01"
    month_over_month: Optional[float] = None  # % change
    year_over_year: Optional[float] = None  # % change


class CUOverviewResponse(BaseModel):
    """Overview dashboard data"""
    survey_code: str = "CU"
    headline_cpi: Optional[InflationMetric] = None  # All items
    core_cpi: Optional[InflationMetric] = None  # All items less food and energy
    last_updated: Optional[str] = None


class CategoryMetric(BaseModel):
    """Metrics for a major CPI category"""
    category_code: str
    category_name: str
    latest_value: Optional[float] = None
    latest_date: Optional[str] = None
    month_over_month: Optional[float] = None
    year_over_year: Optional[float] = None
    series_id: str


class CUCategoryAnalysisResponse(BaseModel):
    """Category analysis with aggregated trends"""
    survey_code: str = "CU"
    area_code: str
    area_name: str
    categories: List[CategoryMetric]


class AreaComparisonMetric(BaseModel):
    """Comparison metric for a single area"""
    area_code: str
    area_name: str
    series_id: str
    latest_value: Optional[float] = None
    latest_date: Optional[str] = None
    month_over_month: Optional[float] = None
    year_over_year: Optional[float] = None


class CUAreaComparisonResponse(BaseModel):
    """Compare same item across different areas"""
    survey_code: str = "CU"
    item_code: str
    item_name: str
    areas: List[AreaComparisonMetric]


# ==================== Timeline Models ====================

class TimelineDataPoint(BaseModel):
    """A single point in the timeline with inflation metrics"""
    year: int
    period: str  # "M01", "M02", etc.
    period_name: str  # "Jan 2024"
    headline_value: Optional[float] = None
    headline_yoy: Optional[float] = None
    headline_mom: Optional[float] = None
    core_value: Optional[float] = None
    core_yoy: Optional[float] = None
    core_mom: Optional[float] = None


class CUOverviewTimelineResponse(BaseModel):
    """Timeline data for overview dashboard"""
    survey_code: str = "CU"
    area_code: str
    area_name: str
    timeline: List[TimelineDataPoint]


class CategoryTimelinePoint(BaseModel):
    """Timeline point for category analysis"""
    year: int
    period: str
    period_name: str
    categories: List[CategoryMetric]  # All 8 categories for this month


class CUCategoryTimelineResponse(BaseModel):
    """Timeline data for category analysis"""
    survey_code: str = "CU"
    area_code: str
    area_name: str
    timeline: List[CategoryTimelinePoint]


class AreaTimelinePoint(BaseModel):
    """Timeline point for area comparison"""
    year: int
    period: str
    period_name: str
    areas: List[AreaComparisonMetric]  # All areas for this month


class CUAreaComparisonTimelineResponse(BaseModel):
    """Timeline data for area comparison"""
    survey_code: str = "CU"
    item_code: str
    item_name: str
    timeline: List[AreaTimelinePoint]
