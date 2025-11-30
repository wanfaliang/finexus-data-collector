"""
LN Explorer Pydantic Schemas
Labor Force Statistics from the Current Population Survey (CPS)
"""
from typing import List, Optional
from pydantic import BaseModel


# ==================== Dimension Models ====================

class LNDimensionItem(BaseModel):
    """Generic dimension item (age, sex, race, education, etc.)"""
    code: str
    text: str

class LNDimensions(BaseModel):
    """All available LN dimensions for filtering"""
    labor_force_statuses: List[LNDimensionItem]
    ages: List[LNDimensionItem]
    sexes: List[LNDimensionItem]
    races: List[LNDimensionItem]
    educations: List[LNDimensionItem]
    occupations: List[LNDimensionItem]
    industries: List[LNDimensionItem]
    marital_statuses: List[LNDimensionItem]
    veteran_statuses: List[LNDimensionItem]
    disability_statuses: List[LNDimensionItem]
    telework_statuses: List[LNDimensionItem]


# ==================== Series Models ====================

class LNSeriesInfo(BaseModel):
    """LN Series metadata"""
    series_id: str
    series_title: str
    seasonal: str  # S = Seasonally adjusted, U = Not seasonally adjusted

    # Dimension codes
    lfst_code: Optional[str] = None
    ages_code: Optional[str] = None
    sexs_code: Optional[str] = None
    race_code: Optional[str] = None
    education_code: Optional[str] = None
    occupation_code: Optional[str] = None
    indy_code: Optional[str] = None
    mari_code: Optional[str] = None
    vets_code: Optional[str] = None
    disa_code: Optional[str] = None
    tlwk_code: Optional[str] = None

    # Time range
    begin_year: Optional[int] = None
    begin_period: Optional[str] = None
    end_year: Optional[int] = None
    end_period: Optional[str] = None

    is_active: bool = True

    class Config:
        from_attributes = True


class LNSeriesListResponse(BaseModel):
    """Response for series list endpoint"""
    survey_code: str = "LN"
    total: int
    limit: int
    offset: int
    series: List[LNSeriesInfo]


# ==================== Data Models ====================

class DataPoint(BaseModel):
    """Single data observation"""
    year: int
    period: str
    period_name: str
    value: Optional[float] = None
    footnote_codes: Optional[str] = None


class LNSeriesData(BaseModel):
    """Time series data for a single series"""
    series_id: str
    series_title: str
    data_points: List[DataPoint]


class LNDataResponse(BaseModel):
    """Response for series data endpoint"""
    survey_code: str = "LN"
    series: List[LNSeriesData]


# ==================== Analytics Models ====================

class UnemploymentMetric(BaseModel):
    """Unemployment rate and related metrics"""
    series_id: str
    dimension_name: str
    latest_value: Optional[float] = None
    latest_date: Optional[str] = None
    month_over_month: Optional[float] = None  # Change from previous month
    year_over_year: Optional[float] = None   # Change from same month last year


class LNOverviewResponse(BaseModel):
    """Overview dashboard with headline unemployment rate"""
    survey_code: str = "LN"
    headline_unemployment: Optional[UnemploymentMetric] = None
    labor_force_participation: Optional[UnemploymentMetric] = None
    employment_population_ratio: Optional[UnemploymentMetric] = None
    last_updated: Optional[str] = None


class DemographicBreakdown(BaseModel):
    """Unemployment breakdown by a single demographic dimension"""
    dimension_type: str  # "age", "sex", "race", "education", etc.
    dimension_name: str
    metrics: List[UnemploymentMetric]


class LNDemographicAnalysisResponse(BaseModel):
    """Demographic breakdown analysis"""
    survey_code: str = "LN"
    breakdowns: List[DemographicBreakdown]


# ==================== Timeline Models ====================

class TimelineDataPoint(BaseModel):
    """Data point in a timeline"""
    year: int
    period: str
    period_name: str
    value: Optional[float] = None


class LNTimelineResponse(BaseModel):
    """Timeline data for comparison"""
    survey_code: str = "LN"
    series_id: str
    series_title: str
    timeline: List[TimelineDataPoint]


class OverviewTimelinePoint(BaseModel):
    """Timeline point for overview metrics"""
    year: int
    period: str
    period_name: str
    headline_value: Optional[float] = None
    lfpr_value: Optional[float] = None
    epop_value: Optional[float] = None


class LNOverviewTimelineResponse(BaseModel):
    """Timeline data for overview metrics"""
    survey_code: str = "LN"
    timeline: List[OverviewTimelinePoint]


class DemographicTimelinePoint(BaseModel):
    """Timeline point for demographic breakdown"""
    year: int
    period: str
    period_name: str
    metrics: List[UnemploymentMetric]


class LNDemographicTimelineResponse(BaseModel):
    """Timeline data for demographic breakdowns"""
    survey_code: str = "LN"
    dimension_type: str
    dimension_name: str
    timeline: List[DemographicTimelinePoint]


# ==================== Occupation Analysis Models ====================

class LNOccupationAnalysisResponse(BaseModel):
    """Unemployment breakdown by occupation (latest snapshot)"""
    survey_code: str = "LN"
    occupations: List[UnemploymentMetric]


class LNOccupationTimelineResponse(BaseModel):
    """Timeline data for occupation unemployment"""
    survey_code: str = "LN"
    timeline: List[DemographicTimelinePoint]


# ==================== Industry Analysis Models ====================

class LNIndustryAnalysisResponse(BaseModel):
    """Unemployment breakdown by industry (latest snapshot)"""
    survey_code: str = "LN"
    industries: List[UnemploymentMetric]


class LNIndustryTimelineResponse(BaseModel):
    """Timeline data for industry unemployment"""
    survey_code: str = "LN"
    timeline: List[DemographicTimelinePoint]


# ==================== Pivot Table Models ====================

class PivotCell(BaseModel):
    """Single cell in pivot table"""
    value: Optional[float] = None
    footnote: Optional[str] = None


class PivotRow(BaseModel):
    """Single row in pivot table"""
    row_label: str
    row_code: str
    cells: List[PivotCell]


class PivotTableResponse(BaseModel):
    """Pivot table data response"""
    survey_code: str = "LN"
    row_dimension: str  # "age", "sex", "race", etc.
    column_dimension: str  # "period" or another dimension
    column_labels: List[str]  # Column headers
    rows: List[PivotRow]
    filters: dict  # Active filters
