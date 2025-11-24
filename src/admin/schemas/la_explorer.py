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
