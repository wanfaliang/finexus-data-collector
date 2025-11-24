"""
Pydantic schemas for CE (Current Employment Statistics) Survey Explorer

CE survey dimensions: Industry + Supersector
"""
from typing import List, Optional
from pydantic import BaseModel


# ==================== Dimension Models ====================

class CEIndustryItem(BaseModel):
    """CE Industry dimension item"""
    industry_code: str
    industry_name: str
    display_level: int
    selectable: bool
    sort_sequence: int
    supersector_code: Optional[str] = None

    class Config:
        from_attributes = True


class CESupersectorItem(BaseModel):
    """CE Supersector dimension item"""
    supersector_code: str
    supersector_name: str

    class Config:
        from_attributes = True


class CEDimensions(BaseModel):
    """Available dimensions for CE survey"""
    industries: List[CEIndustryItem]
    supersectors: List[CESupersectorItem]


# ==================== Series Models ====================

class CESeriesInfo(BaseModel):
    """CE Series metadata with dimensions"""
    series_id: str
    series_title: str
    industry_code: str
    industry_name: str
    supersector_code: Optional[str] = None
    supersector_name: Optional[str] = None
    seasonal_code: Optional[str] = None
    begin_year: Optional[int] = None
    begin_period: Optional[str] = None
    end_year: Optional[int] = None
    end_period: Optional[str] = None
    is_active: bool = True

    class Config:
        from_attributes = True


class CESeriesListResponse(BaseModel):
    """Response for CE series list with filters"""
    survey_code: str = "CE"
    total: int
    limit: int
    offset: int
    series: List[CESeriesInfo]


# ==================== Data Models ====================

class CEDataPoint(BaseModel):
    """A single CE time series observation"""
    year: int
    period: str
    period_name: str  # "January 2024", etc.
    value: Optional[float] = None
    footnote_codes: Optional[str] = None

    class Config:
        from_attributes = True


class CESeriesData(BaseModel):
    """Time series data for a single CE series"""
    series_id: str
    series_title: str
    industry_name: str
    data_points: List[CEDataPoint]


class CEDataResponse(BaseModel):
    """Response for CE series data request"""
    survey_code: str = "CE"
    series: List[CESeriesData]
