"""
Pydantic schemas for CE (Current Employment Statistics) Survey Explorer

CE survey dimensions: Industry + Supersector + DataType
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


class CEDataTypeItem(BaseModel):
    """CE Data Type dimension item"""
    data_type_code: str
    data_type_text: str
    category: Optional[str] = None  # Grouped category for UI

    class Config:
        from_attributes = True


class CEDimensions(BaseModel):
    """Available dimensions for CE survey"""
    industries: List[CEIndustryItem]
    supersectors: List[CESupersectorItem]
    data_types: List[CEDataTypeItem]


# ==================== Series Models ====================

class CESeriesInfo(BaseModel):
    """CE Series metadata with dimensions"""
    series_id: str
    series_title: str
    industry_code: str
    industry_name: str
    supersector_code: Optional[str] = None
    supersector_name: Optional[str] = None
    data_type_code: Optional[str] = None
    data_type_text: Optional[str] = None
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


# ==================== Overview/Analytics Models ====================

class CEEmploymentMetric(BaseModel):
    """Employment metric for overview"""
    series_id: str
    name: str  # "Total Nonfarm", "Total Private", etc.
    latest_value: Optional[float] = None
    latest_date: Optional[str] = None  # "October 2024"
    month_over_month: Optional[float] = None  # Change from prior month (thousands)
    month_over_month_pct: Optional[float] = None  # Percent change
    year_over_year: Optional[float] = None  # Change from same month last year
    year_over_year_pct: Optional[float] = None  # Percent change


class CEOverviewResponse(BaseModel):
    """Overview of headline employment statistics"""
    survey_code: str = "CE"
    total_nonfarm: Optional[CEEmploymentMetric] = None
    total_private: Optional[CEEmploymentMetric] = None
    goods_producing: Optional[CEEmploymentMetric] = None
    service_providing: Optional[CEEmploymentMetric] = None
    government: Optional[CEEmploymentMetric] = None
    last_updated: Optional[str] = None


class CEOverviewTimelinePoint(BaseModel):
    """Single point in overview timeline"""
    year: int
    period: str
    period_name: str
    total_nonfarm: Optional[float] = None
    total_private: Optional[float] = None
    goods_producing: Optional[float] = None
    service_providing: Optional[float] = None
    government: Optional[float] = None


class CEOverviewTimelineResponse(BaseModel):
    """Timeline data for overview charts"""
    survey_code: str = "CE"
    timeline: List[CEOverviewTimelinePoint]


# ==================== Supersector Analysis Models ====================

class CESupersectorMetric(BaseModel):
    """Employment metric for a supersector"""
    supersector_code: str
    supersector_name: str
    series_id: str
    latest_value: Optional[float] = None
    latest_date: Optional[str] = None
    month_over_month: Optional[float] = None
    month_over_month_pct: Optional[float] = None
    year_over_year: Optional[float] = None
    year_over_year_pct: Optional[float] = None


class CESupersectorAnalysisResponse(BaseModel):
    """Supersector employment analysis"""
    survey_code: str = "CE"
    supersectors: List[CESupersectorMetric]
    last_updated: Optional[str] = None


class CESupersectorTimelinePoint(BaseModel):
    """Single point in supersector timeline"""
    year: int
    period: str
    period_name: str
    supersectors: dict  # supersector_code -> value


class CESupersectorTimelineResponse(BaseModel):
    """Timeline data for supersector comparison charts"""
    survey_code: str = "CE"
    timeline: List[CESupersectorTimelinePoint]
    supersector_names: dict  # supersector_code -> name


# ==================== Industry Analysis Models ====================

class CEIndustryMetric(BaseModel):
    """Employment metric for an industry"""
    industry_code: str
    industry_name: str
    series_id: str
    display_level: int
    latest_value: Optional[float] = None
    latest_date: Optional[str] = None
    month_over_month: Optional[float] = None
    month_over_month_pct: Optional[float] = None
    year_over_year: Optional[float] = None
    year_over_year_pct: Optional[float] = None


class CEIndustryAnalysisResponse(BaseModel):
    """Industry employment analysis"""
    survey_code: str = "CE"
    industries: List[CEIndustryMetric]
    total_count: int
    last_updated: Optional[str] = None


class CEIndustryTimelinePoint(BaseModel):
    """Single point in industry timeline"""
    year: int
    period: str
    period_name: str
    industries: dict  # industry_code -> value


class CEIndustryTimelineResponse(BaseModel):
    """Timeline data for industry comparison charts"""
    survey_code: str = "CE"
    timeline: List[CEIndustryTimelinePoint]
    industry_names: dict  # industry_code -> name


# ==================== Data Type Analysis Models ====================

class CEDataTypeMetric(BaseModel):
    """Metric for a specific data type"""
    data_type_code: str
    data_type_text: str
    series_id: str
    industry_code: str
    industry_name: str
    latest_value: Optional[float] = None
    latest_date: Optional[str] = None
    month_over_month: Optional[float] = None
    month_over_month_pct: Optional[float] = None
    year_over_year: Optional[float] = None
    year_over_year_pct: Optional[float] = None


class CEDataTypeAnalysisResponse(BaseModel):
    """Data type analysis for an industry"""
    survey_code: str = "CE"
    industry_code: str
    industry_name: str
    data_types: List[CEDataTypeMetric]
    last_updated: Optional[str] = None


class CEDataTypeTimelinePoint(BaseModel):
    """Single point in data type timeline"""
    year: int
    period: str
    period_name: str
    data_types: dict  # data_type_code -> value


class CEDataTypeTimelineResponse(BaseModel):
    """Timeline data for data type comparison charts"""
    survey_code: str = "CE"
    industry_code: str
    industry_name: str
    timeline: List[CEDataTypeTimelinePoint]
    data_type_names: dict  # data_type_code -> text


# ==================== Earnings Analysis Models ====================

class CEEarningsMetric(BaseModel):
    """Earnings metric for an industry"""
    industry_code: str
    industry_name: str
    supersector_code: Optional[str] = None
    supersector_name: Optional[str] = None
    avg_hourly_earnings: Optional[float] = None
    avg_weekly_earnings: Optional[float] = None
    avg_weekly_hours: Optional[float] = None
    latest_date: Optional[str] = None
    hourly_mom_change: Optional[float] = None
    hourly_mom_pct: Optional[float] = None
    hourly_yoy_change: Optional[float] = None
    hourly_yoy_pct: Optional[float] = None


class CEEarningsAnalysisResponse(BaseModel):
    """Earnings analysis across industries"""
    survey_code: str = "CE"
    earnings: List[CEEarningsMetric]
    total_count: int
    last_updated: Optional[str] = None


class CEEarningsTimelinePoint(BaseModel):
    """Single point in earnings timeline"""
    year: int
    period: str
    period_name: str
    avg_hourly_earnings: Optional[float] = None
    avg_weekly_earnings: Optional[float] = None
    avg_weekly_hours: Optional[float] = None


class CEEarningsTimelineResponse(BaseModel):
    """Timeline data for earnings charts"""
    survey_code: str = "CE"
    industry_code: str
    industry_name: str
    timeline: List[CEEarningsTimelinePoint]
