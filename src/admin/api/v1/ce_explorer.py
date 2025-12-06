"""
CE (Current Employment Statistics) Survey Explorer API Endpoints
"""
from typing import Optional, List, Dict
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, and_

from ...core.database import get_db
from ...schemas.ce_explorer import (
    CEDimensions, CEIndustryItem, CESupersectorItem, CEDataTypeItem,
    CESeriesListResponse, CESeriesInfo,
    CEDataResponse, CESeriesData, CEDataPoint,
    CEEmploymentMetric, CEOverviewResponse, CEOverviewTimelinePoint, CEOverviewTimelineResponse,
    CESupersectorMetric, CESupersectorAnalysisResponse, CESupersectorTimelinePoint, CESupersectorTimelineResponse,
    CEIndustryMetric, CEIndustryAnalysisResponse, CEIndustryTimelinePoint, CEIndustryTimelineResponse,
    CEDataTypeMetric, CEDataTypeAnalysisResponse, CEDataTypeTimelinePoint, CEDataTypeTimelineResponse,
    CEEarningsMetric, CEEarningsAnalysisResponse, CEEarningsTimelinePoint, CEEarningsTimelineResponse
)
from src.database.bls_models import (
    CEIndustry, CESupersector, CEDataType, CESeries, CEData, BLSPeriod
)

# Key headline series IDs (seasonally adjusted, all employees, thousands)
HEADLINE_SERIES = {
    'total_nonfarm': 'CES0000000001',
    'total_private': 'CES0500000001',
    'goods_producing': 'CES0600000001',
    'service_providing': 'CES0700000001',
    'government': 'CES9000000001',
}

# Major supersectors for analysis (exclude aggregates like 00, 05, 06, 07, 08)
MAJOR_SUPERSECTORS = ['10', '20', '30', '40', '41', '42', '43', '44', '45', '50', '55', '60', '65', '70', '80', '90']

# Data type categories for better UI grouping
DATA_TYPE_CATEGORIES = {
    '01': 'Employment',
    '06': 'Employment',
    '10': 'Employment',
    '02': 'Hours',
    '04': 'Hours',
    '07': 'Hours',
    '09': 'Hours',
    '19': 'Hours',
    '20': 'Hours',
    '36': 'Hours',
    '37': 'Hours',
    '03': 'Earnings',
    '08': 'Earnings',
    '11': 'Earnings',
    '12': 'Earnings',
    '13': 'Earnings',
    '15': 'Earnings',
    '30': 'Earnings',
    '31': 'Earnings',
    '32': 'Earnings',
    '33': 'Earnings',
    '16': 'Indexes',
    '17': 'Indexes',
    '34': 'Indexes',
    '35': 'Indexes',
    '21': 'Diffusion',
    '22': 'Diffusion',
    '23': 'Diffusion',
    '24': 'Diffusion',
    '25': 'Aggregates',
    '26': 'Aggregates',
    '56': 'Aggregates',
    '57': 'Aggregates',
    '58': 'Aggregates',
    '81': 'Aggregates',
    '82': 'Aggregates',
    '83': 'Aggregates',
}

# Key data types for the main analysis view
KEY_DATA_TYPES = ['01', '02', '03', '04', '06', '07', '08', '11']

router = APIRouter(prefix="/ce", tags=["CE Explorer"])


@router.get("/dimensions", response_model=CEDimensions)
def get_ce_dimensions(db: Session = Depends(get_db)):
    """Get all available dimensions for CE survey (industries, supersectors, data types)"""

    # Get all industries
    industries = db.query(CEIndustry).order_by(CEIndustry.sort_sequence).all()
    industry_items = [
        CEIndustryItem(
            industry_code=i.industry_code,
            industry_name=i.industry_name,
            display_level=i.display_level or 0,
            selectable=i.selectable == 'T',
            sort_sequence=i.sort_sequence or 0,
            supersector_code=None  # CEIndustry doesn't have supersector_code field
        )
        for i in industries
    ]

    # Get all supersectors
    supersectors = db.query(CESupersector).order_by(CESupersector.supersector_code).all()
    supersector_items = [
        CESupersectorItem(
            supersector_code=ss.supersector_code,
            supersector_name=ss.supersector_name
        )
        for ss in supersectors
    ]

    # Get all data types
    data_types = db.query(CEDataType).order_by(CEDataType.data_type_code).all()
    data_type_items = [
        CEDataTypeItem(
            data_type_code=dt.data_type_code,
            data_type_text=dt.data_type_text,
            category=DATA_TYPE_CATEGORIES.get(dt.data_type_code, 'Other')
        )
        for dt in data_types
    ]

    return CEDimensions(industries=industry_items, supersectors=supersector_items, data_types=data_type_items)


@router.get("/series", response_model=CESeriesListResponse)
def get_ce_series(
    industry_code: Optional[str] = Query(None, description="Filter by industry code"),
    supersector_code: Optional[str] = Query(None, description="Filter by supersector code"),
    data_type_code: Optional[str] = Query(None, description="Filter by data type code"),
    seasonal_code: Optional[str] = Query(None, description="Filter by seasonal adjustment (S/U)"),
    active_only: bool = Query(True, description="Only return active series"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db)
):
    """Get CE series list with optional filters"""

    # Build query with joins
    query = db.query(
        CESeries,
        CEIndustry.industry_name,
        CESupersector.supersector_name,
        CEDataType.data_type_text
    ).join(
        CEIndustry, CESeries.industry_code == CEIndustry.industry_code
    ).outerjoin(
        CESupersector, CESeries.supersector_code == CESupersector.supersector_code
    ).outerjoin(
        CEDataType, CESeries.data_type_code == CEDataType.data_type_code
    )

    # Apply filters
    if industry_code:
        query = query.filter(CESeries.industry_code == industry_code)
    if supersector_code:
        query = query.filter(CESeries.supersector_code == supersector_code)
    if data_type_code:
        query = query.filter(CESeries.data_type_code == data_type_code)
    if seasonal_code:
        query = query.filter(CESeries.seasonal_code == seasonal_code)
    if active_only:
        query = query.filter(CESeries.is_active == True)

    # Get total count
    total = query.count()

    # Apply pagination
    results = query.order_by(CESeries.series_id).offset(offset).limit(limit).all()

    # Build response
    series_list = [
        CESeriesInfo(
            series_id=s.series_id,
            series_title=s.series_title,
            industry_code=s.industry_code,
            industry_name=industry_name,
            supersector_code=s.supersector_code,
            supersector_name=supersector_name,
            data_type_code=s.data_type_code,
            data_type_text=data_type_text,
            seasonal_code=s.seasonal_code,
            begin_year=s.begin_year,
            begin_period=s.begin_period,
            end_year=s.end_year,
            end_period=s.end_period,
            is_active=s.is_active
        )
        for s, industry_name, supersector_name, data_type_text in results
    ]

    return CESeriesListResponse(
        total=total,
        limit=limit,
        offset=offset,
        series=series_list
    )


@router.get("/series/{series_id}/data", response_model=CEDataResponse)
def get_ce_series_data(
    series_id: str,
    start_year: Optional[int] = Query(None, description="Filter data from this year"),
    end_year: Optional[int] = Query(None, description="Filter data up to this year"),
    db: Session = Depends(get_db)
):
    """Get time series data for a specific CE series"""

    # Get series metadata
    series = db.query(
        CESeries,
        CEIndustry.industry_name
    ).join(
        CEIndustry, CESeries.industry_code == CEIndustry.industry_code
    ).filter(
        CESeries.series_id == series_id
    ).first()

    if not series:
        raise HTTPException(status_code=404, detail=f"Series {series_id} not found")

    s, industry_name = series

    # Get data points
    data_query = db.query(CEData).filter(CEData.series_id == series_id)

    if start_year:
        data_query = data_query.filter(CEData.year >= start_year)
    if end_year:
        data_query = data_query.filter(CEData.year <= end_year)

    data_points = data_query.order_by(CEData.year, CEData.period).all()

    # Get period names for formatting
    period_map = {p.period_code: p.period_name for p in db.query(BLSPeriod).all()}

    # Build data points with period names
    formatted_points = []
    for dp in data_points:
        period_name = period_map.get(dp.period, dp.period)
        # Format as "January 2024"
        if period_name and dp.year:
            period_display = f"{period_name} {dp.year}"
        else:
            period_display = f"{dp.period} {dp.year}"

        formatted_points.append(
            CEDataPoint(
                year=dp.year,
                period=dp.period,
                period_name=period_display,
                value=float(dp.value) if dp.value is not None else None,
                footnote_codes=dp.footnote_codes
            )
        )

    series_data = CESeriesData(
        series_id=s.series_id,
        series_title=s.series_title,
        industry_name=industry_name,
        data_points=formatted_points
    )

    return CEDataResponse(series=[series_data])


# ==================== Helper Functions ====================

def get_period_name(period: str, year: int, period_map: Dict[str, str]) -> str:
    """Format period as 'January 2024'"""
    name = period_map.get(period, period)
    return f"{name} {year}"


def get_latest_periods(db: Session, series_id: str, count: int = 13) -> List[CEData]:
    """Get the most recent data points for a series. If count is 0, return all data."""
    query = db.query(CEData).filter(
        CEData.series_id == series_id
    ).order_by(CEData.year.desc(), CEData.period.desc())
    if count > 0:
        query = query.limit(count)
    return query.all()


def calculate_changes(data_points: List[CEData]) -> Dict:
    """Calculate month-over-month and year-over-year changes"""
    if not data_points:
        return {}

    latest = data_points[0]
    result = {
        'latest_value': float(latest.value) if latest.value else None,
        'latest_year': latest.year,
        'latest_period': latest.period,
    }

    # Month-over-month (compare with previous month)
    if len(data_points) > 1 and data_points[1].value and latest.value:
        mom = float(latest.value) - float(data_points[1].value)
        mom_pct = (mom / float(data_points[1].value)) * 100 if data_points[1].value else None
        result['month_over_month'] = round(mom, 1)
        result['month_over_month_pct'] = round(mom_pct, 2) if mom_pct else None

    # Year-over-year (compare with same period last year - 12 months back)
    if len(data_points) >= 13 and data_points[12].value and latest.value:
        yoy = float(latest.value) - float(data_points[12].value)
        yoy_pct = (yoy / float(data_points[12].value)) * 100 if data_points[12].value else None
        result['year_over_year'] = round(yoy, 1)
        result['year_over_year_pct'] = round(yoy_pct, 2) if yoy_pct else None

    return result


# ==================== Overview Endpoints ====================

@router.get("/overview", response_model=CEOverviewResponse)
def get_ce_overview(db: Session = Depends(get_db)):
    """Get headline employment statistics overview"""

    period_map = {p.period_code: p.period_name for p in db.query(BLSPeriod).all()}

    def build_metric(series_id: str, name: str) -> Optional[CEEmploymentMetric]:
        data_points = get_latest_periods(db, series_id, 13)
        if not data_points:
            return None

        changes = calculate_changes(data_points)
        latest_date = get_period_name(changes.get('latest_period', ''), changes.get('latest_year', 0), period_map)

        return CEEmploymentMetric(
            series_id=series_id,
            name=name,
            latest_value=changes.get('latest_value'),
            latest_date=latest_date,
            month_over_month=changes.get('month_over_month'),
            month_over_month_pct=changes.get('month_over_month_pct'),
            year_over_year=changes.get('year_over_year'),
            year_over_year_pct=changes.get('year_over_year_pct')
        )

    # Get latest data timestamp
    latest = db.query(func.max(CEData.year), func.max(CEData.period)).filter(
        CEData.series_id == HEADLINE_SERIES['total_nonfarm']
    ).first()
    last_updated = get_period_name(latest[1], latest[0], period_map) if latest[0] else None

    return CEOverviewResponse(
        total_nonfarm=build_metric(HEADLINE_SERIES['total_nonfarm'], 'Total Nonfarm'),
        total_private=build_metric(HEADLINE_SERIES['total_private'], 'Total Private'),
        goods_producing=build_metric(HEADLINE_SERIES['goods_producing'], 'Goods-Producing'),
        service_providing=build_metric(HEADLINE_SERIES['service_providing'], 'Service-Providing'),
        government=build_metric(HEADLINE_SERIES['government'], 'Government'),
        last_updated=last_updated
    )


@router.get("/overview/timeline", response_model=CEOverviewTimelineResponse)
def get_ce_overview_timeline(
    months_back: int = Query(24, ge=0, le=600, description="Number of months of history (0 for all time)"),
    db: Session = Depends(get_db)
):
    """Get timeline data for headline employment metrics"""

    period_map = {p.period_code: p.period_name for p in db.query(BLSPeriod).all()}

    # Get data for all headline series
    series_data = {}
    for key, series_id in HEADLINE_SERIES.items():
        data = get_latest_periods(db, series_id, months_back)
        series_data[key] = {(d.year, d.period): float(d.value) if d.value else None for d in data}

    # Build timeline (get unique periods from total_nonfarm)
    nonfarm_data = get_latest_periods(db, HEADLINE_SERIES['total_nonfarm'], months_back)

    timeline = []
    for dp in reversed(nonfarm_data):  # Reverse to get chronological order
        point = CEOverviewTimelinePoint(
            year=dp.year,
            period=dp.period,
            period_name=get_period_name(dp.period, dp.year, period_map),
            total_nonfarm=series_data['total_nonfarm'].get((dp.year, dp.period)),
            total_private=series_data['total_private'].get((dp.year, dp.period)),
            goods_producing=series_data['goods_producing'].get((dp.year, dp.period)),
            service_providing=series_data['service_providing'].get((dp.year, dp.period)),
            government=series_data['government'].get((dp.year, dp.period))
        )
        timeline.append(point)

    return CEOverviewTimelineResponse(timeline=timeline)


# ==================== Supersector Analysis Endpoints ====================

@router.get("/supersectors", response_model=CESupersectorAnalysisResponse)
def get_ce_supersector_analysis(db: Session = Depends(get_db)):
    """Get employment analysis by supersector"""

    period_map = {p.period_code: p.period_name for p in db.query(BLSPeriod).all()}

    # Get all supersectors
    supersectors = db.query(CESupersector).filter(
        CESupersector.supersector_code.in_(MAJOR_SUPERSECTORS)
    ).order_by(CESupersector.supersector_code).all()

    metrics = []
    last_updated = None

    for ss in supersectors:
        # Find the "all employees" series for this supersector
        # Pattern: CES{supersector}000001 (all employees, thousands, seasonally adjusted)
        series_id = f"CES{ss.supersector_code}00000001"

        series = db.query(CESeries).filter(CESeries.series_id == series_id).first()
        if not series:
            continue

        data_points = get_latest_periods(db, series_id, 13)
        if not data_points:
            continue

        changes = calculate_changes(data_points)
        latest_date = get_period_name(changes.get('latest_period', ''), changes.get('latest_year', 0), period_map)

        if not last_updated:
            last_updated = latest_date

        metrics.append(CESupersectorMetric(
            supersector_code=ss.supersector_code,
            supersector_name=ss.supersector_name,
            series_id=series_id,
            latest_value=changes.get('latest_value'),
            latest_date=latest_date,
            month_over_month=changes.get('month_over_month'),
            month_over_month_pct=changes.get('month_over_month_pct'),
            year_over_year=changes.get('year_over_year'),
            year_over_year_pct=changes.get('year_over_year_pct')
        ))

    return CESupersectorAnalysisResponse(
        supersectors=metrics,
        last_updated=last_updated
    )


@router.get("/supersectors/timeline", response_model=CESupersectorTimelineResponse)
def get_ce_supersector_timeline(
    supersector_codes: Optional[str] = Query(None, description="Comma-separated supersector codes (default: all major)"),
    months_back: int = Query(24, ge=0, le=600, description="Number of months of history (0 for all time)"),
    db: Session = Depends(get_db)
):
    """Get timeline data for supersector comparison"""

    period_map = {p.period_code: p.period_name for p in db.query(BLSPeriod).all()}

    # Determine which supersectors to include
    if supersector_codes:
        codes = [c.strip() for c in supersector_codes.split(',')]
    else:
        codes = MAJOR_SUPERSECTORS

    # Get supersector names
    supersectors = db.query(CESupersector).filter(
        CESupersector.supersector_code.in_(codes)
    ).all()
    supersector_names = {ss.supersector_code: ss.supersector_name for ss in supersectors}

    # Get data for each supersector
    series_data = {}
    reference_periods = []

    for code in codes:
        series_id = f"CES{code}00000001"
        data = get_latest_periods(db, series_id, months_back)
        series_data[code] = {(d.year, d.period): float(d.value) if d.value else None for d in data}

        if not reference_periods and data:
            reference_periods = [(d.year, d.period) for d in reversed(data)]

    # Build timeline
    timeline = []
    for year, period in reference_periods:
        supersector_values = {code: series_data.get(code, {}).get((year, period)) for code in codes}

        timeline.append(CESupersectorTimelinePoint(
            year=year,
            period=period,
            period_name=get_period_name(period, year, period_map),
            supersectors=supersector_values
        ))

    return CESupersectorTimelineResponse(
        timeline=timeline,
        supersector_names=supersector_names
    )


# ==================== Industry Analysis Endpoints ====================

@router.get("/industries", response_model=CEIndustryAnalysisResponse)
def get_ce_industry_analysis(
    display_level: Optional[int] = Query(None, description="Filter by display level (1-7)"),
    supersector_code: Optional[str] = Query(None, description="Filter by supersector"),
    limit: int = Query(50, ge=1, le=200, description="Maximum industries to return"),
    db: Session = Depends(get_db)
):
    """Get employment analysis by industry"""

    period_map = {p.period_code: p.period_name for p in db.query(BLSPeriod).all()}

    # Build query for industries
    query = db.query(CEIndustry)

    if display_level is not None:
        query = query.filter(CEIndustry.display_level == display_level)

    # Filter to selectable industries
    query = query.filter(CEIndustry.selectable == 'T')
    query = query.order_by(CEIndustry.sort_sequence)

    industries = query.limit(limit * 2).all()  # Get extra to account for missing series

    metrics = []
    last_updated = None

    for ind in industries:
        if len(metrics) >= limit:
            break

        # Find the "all employees" series for this industry
        # Pattern: CES{industry_code}01 (all employees, thousands, seasonally adjusted)
        series_id = f"CES{ind.industry_code}01"

        series = db.query(CESeries).filter(
            CESeries.series_id == series_id,
            CESeries.is_active == True
        ).first()

        if not series:
            continue

        # Filter by supersector if specified
        if supersector_code and series.supersector_code != supersector_code:
            continue

        data_points = get_latest_periods(db, series_id, 13)
        if not data_points:
            continue

        changes = calculate_changes(data_points)
        latest_date = get_period_name(changes.get('latest_period', ''), changes.get('latest_year', 0), period_map)

        if not last_updated:
            last_updated = latest_date

        metrics.append(CEIndustryMetric(
            industry_code=ind.industry_code,
            industry_name=ind.industry_name,
            series_id=series_id,
            display_level=ind.display_level or 0,
            latest_value=changes.get('latest_value'),
            latest_date=latest_date,
            month_over_month=changes.get('month_over_month'),
            month_over_month_pct=changes.get('month_over_month_pct'),
            year_over_year=changes.get('year_over_year'),
            year_over_year_pct=changes.get('year_over_year_pct')
        ))

    return CEIndustryAnalysisResponse(
        industries=metrics,
        total_count=len(metrics),
        last_updated=last_updated
    )


@router.get("/industries/timeline", response_model=CEIndustryTimelineResponse)
def get_ce_industry_timeline(
    industry_codes: str = Query(..., description="Comma-separated industry codes (required, max 10)"),
    months_back: int = Query(24, ge=0, le=600, description="Number of months of history (0 for all time)"),
    db: Session = Depends(get_db)
):
    """Get timeline data for industry comparison"""

    period_map = {p.period_code: p.period_name for p in db.query(BLSPeriod).all()}

    # Parse industry codes
    codes = [c.strip() for c in industry_codes.split(',')][:10]  # Limit to 10

    # Get industry names
    industries = db.query(CEIndustry).filter(
        CEIndustry.industry_code.in_(codes)
    ).all()
    industry_names = {ind.industry_code: ind.industry_name for ind in industries}

    # Get data for each industry
    series_data = {}
    reference_periods = []

    for code in codes:
        series_id = f"CES{code}01"
        data = get_latest_periods(db, series_id, months_back)
        series_data[code] = {(d.year, d.period): float(d.value) if d.value else None for d in data}

        if not reference_periods and data:
            reference_periods = [(d.year, d.period) for d in reversed(data)]

    # Build timeline
    timeline = []
    for year, period in reference_periods:
        industry_values = {code: series_data.get(code, {}).get((year, period)) for code in codes}

        timeline.append(CEIndustryTimelinePoint(
            year=year,
            period=period,
            period_name=get_period_name(period, year, period_map),
            industries=industry_values
        ))

    return CEIndustryTimelineResponse(
        timeline=timeline,
        industry_names=industry_names
    )


# ==================== Data Type Analysis Endpoints ====================

@router.get("/datatypes/{industry_code}", response_model=CEDataTypeAnalysisResponse)
def get_ce_datatype_analysis(
    industry_code: str,
    db: Session = Depends(get_db)
):
    """Get all available data types for a specific industry with current values"""

    period_map = {p.period_code: p.period_name for p in db.query(BLSPeriod).all()}

    # Get industry name
    industry = db.query(CEIndustry).filter(CEIndustry.industry_code == industry_code).first()
    if not industry:
        raise HTTPException(status_code=404, detail=f"Industry {industry_code} not found")

    # Get all series for this industry (seasonally adjusted)
    series_list = db.query(
        CESeries,
        CEDataType.data_type_text
    ).join(
        CEDataType, CESeries.data_type_code == CEDataType.data_type_code
    ).filter(
        CESeries.industry_code == industry_code,
        CESeries.seasonal_code == 'S',
        CESeries.is_active == True
    ).order_by(CESeries.data_type_code).all()

    metrics = []
    last_updated = None

    for series, data_type_text in series_list:
        data_points = get_latest_periods(db, series.series_id, 13)
        if not data_points:
            continue

        changes = calculate_changes(data_points)
        latest_date = get_period_name(changes.get('latest_period', ''), changes.get('latest_year', 0), period_map)

        if not last_updated:
            last_updated = latest_date

        metrics.append(CEDataTypeMetric(
            data_type_code=series.data_type_code,
            data_type_text=data_type_text,
            series_id=series.series_id,
            industry_code=industry_code,
            industry_name=industry.industry_name,
            latest_value=changes.get('latest_value'),
            latest_date=latest_date,
            month_over_month=changes.get('month_over_month'),
            month_over_month_pct=changes.get('month_over_month_pct'),
            year_over_year=changes.get('year_over_year'),
            year_over_year_pct=changes.get('year_over_year_pct')
        ))

    return CEDataTypeAnalysisResponse(
        industry_code=industry_code,
        industry_name=industry.industry_name,
        data_types=metrics,
        last_updated=last_updated
    )


@router.get("/datatypes/{industry_code}/timeline", response_model=CEDataTypeTimelineResponse)
def get_ce_datatype_timeline(
    industry_code: str,
    data_type_codes: Optional[str] = Query(None, description="Comma-separated data type codes (default: key types)"),
    months_back: int = Query(24, ge=0, le=600, description="Number of months of history (0 for all time)"),
    db: Session = Depends(get_db)
):
    """Get timeline data for data type comparison within an industry"""

    period_map = {p.period_code: p.period_name for p in db.query(BLSPeriod).all()}

    # Get industry name
    industry = db.query(CEIndustry).filter(CEIndustry.industry_code == industry_code).first()
    if not industry:
        raise HTTPException(status_code=404, detail=f"Industry {industry_code} not found")

    # Determine which data types to include
    if data_type_codes:
        codes = [c.strip() for c in data_type_codes.split(',')]
    else:
        codes = KEY_DATA_TYPES

    # Get data type names
    data_types = db.query(CEDataType).filter(CEDataType.data_type_code.in_(codes)).all()
    data_type_names = {dt.data_type_code: dt.data_type_text for dt in data_types}

    # Get data for each data type
    series_data = {}
    reference_periods = []

    for code in codes:
        # Build series ID: CES{industry_code}{data_type_code}
        series_id = f"CES{industry_code}{code}"
        data = get_latest_periods(db, series_id, months_back)
        series_data[code] = {(d.year, d.period): float(d.value) if d.value else None for d in data}

        if not reference_periods and data:
            reference_periods = [(d.year, d.period) for d in reversed(data)]

    # Build timeline
    timeline = []
    for year, period in reference_periods:
        data_type_values = {code: series_data.get(code, {}).get((year, period)) for code in codes}

        timeline.append(CEDataTypeTimelinePoint(
            year=year,
            period=period,
            period_name=get_period_name(period, year, period_map),
            data_types=data_type_values
        ))

    return CEDataTypeTimelineResponse(
        industry_code=industry_code,
        industry_name=industry.industry_name,
        timeline=timeline,
        data_type_names=data_type_names
    )


# ==================== Earnings Analysis Endpoints ====================

@router.get("/earnings", response_model=CEEarningsAnalysisResponse)
def get_ce_earnings_analysis(
    supersector_code: Optional[str] = Query(None, description="Filter by supersector"),
    display_level: Optional[int] = Query(None, description="Filter by display level (1-7)"),
    limit: int = Query(50, ge=1, le=200, description="Maximum industries to return"),
    db: Session = Depends(get_db)
):
    """Get earnings analysis across industries (hourly/weekly earnings and hours)"""

    period_map = {p.period_code: p.period_name for p in db.query(BLSPeriod).all()}

    # Build query for industries
    query = db.query(CEIndustry)

    if display_level is not None:
        query = query.filter(CEIndustry.display_level == display_level)

    query = query.filter(CEIndustry.selectable == 'T')
    query = query.order_by(CEIndustry.sort_sequence)

    industries = query.limit(limit * 2).all()

    metrics = []
    last_updated = None

    for ind in industries:
        if len(metrics) >= limit:
            break

        # Get series IDs for earnings data
        # 03 = Average Hourly Earnings, 11 = Average Weekly Earnings, 02 = Average Weekly Hours
        hourly_series_id = f"CES{ind.industry_code}03"
        weekly_series_id = f"CES{ind.industry_code}11"
        hours_series_id = f"CES{ind.industry_code}02"

        # Check if earnings series exists
        hourly_series = db.query(CESeries).filter(
            CESeries.series_id == hourly_series_id,
            CESeries.is_active == True
        ).first()

        if not hourly_series:
            continue

        # Filter by supersector if specified
        if supersector_code and hourly_series.supersector_code != supersector_code:
            continue

        # Get hourly earnings data
        hourly_data = get_latest_periods(db, hourly_series_id, 13)
        weekly_data = get_latest_periods(db, weekly_series_id, 13)
        hours_data = get_latest_periods(db, hours_series_id, 13)

        if not hourly_data:
            continue

        hourly_changes = calculate_changes(hourly_data)
        latest_date = get_period_name(hourly_changes.get('latest_period', ''), hourly_changes.get('latest_year', 0), period_map)

        if not last_updated:
            last_updated = latest_date

        # Get supersector name
        supersector = db.query(CESupersector).filter(
            CESupersector.supersector_code == hourly_series.supersector_code
        ).first()

        metrics.append(CEEarningsMetric(
            industry_code=ind.industry_code,
            industry_name=ind.industry_name,
            supersector_code=hourly_series.supersector_code,
            supersector_name=supersector.supersector_name if supersector else None,
            avg_hourly_earnings=hourly_changes.get('latest_value'),
            avg_weekly_earnings=float(weekly_data[0].value) if weekly_data and weekly_data[0].value else None,
            avg_weekly_hours=float(hours_data[0].value) if hours_data and hours_data[0].value else None,
            latest_date=latest_date,
            hourly_mom_change=hourly_changes.get('month_over_month'),
            hourly_mom_pct=hourly_changes.get('month_over_month_pct'),
            hourly_yoy_change=hourly_changes.get('year_over_year'),
            hourly_yoy_pct=hourly_changes.get('year_over_year_pct')
        ))

    return CEEarningsAnalysisResponse(
        earnings=metrics,
        total_count=len(metrics),
        last_updated=last_updated
    )


@router.get("/earnings/{industry_code}/timeline", response_model=CEEarningsTimelineResponse)
def get_ce_earnings_timeline(
    industry_code: str,
    months_back: int = Query(24, ge=0, le=600, description="Number of months of history (0 for all time)"),
    db: Session = Depends(get_db)
):
    """Get earnings timeline data for a specific industry"""

    period_map = {p.period_code: p.period_name for p in db.query(BLSPeriod).all()}

    # Get industry name
    industry = db.query(CEIndustry).filter(CEIndustry.industry_code == industry_code).first()
    if not industry:
        raise HTTPException(status_code=404, detail=f"Industry {industry_code} not found")

    # Get data for earnings series
    hourly_series_id = f"CES{industry_code}03"
    weekly_series_id = f"CES{industry_code}11"
    hours_series_id = f"CES{industry_code}02"

    hourly_data = get_latest_periods(db, hourly_series_id, months_back)
    weekly_data = get_latest_periods(db, weekly_series_id, months_back)
    hours_data = get_latest_periods(db, hours_series_id, months_back)

    # Build lookup dicts
    hourly_values = {(d.year, d.period): float(d.value) if d.value else None for d in hourly_data}
    weekly_values = {(d.year, d.period): float(d.value) if d.value else None for d in weekly_data}
    hours_values = {(d.year, d.period): float(d.value) if d.value else None for d in hours_data}

    # Get reference periods from hourly data
    reference_periods = [(d.year, d.period) for d in reversed(hourly_data)]

    # Build timeline
    timeline = []
    for year, period in reference_periods:
        timeline.append(CEEarningsTimelinePoint(
            year=year,
            period=period,
            period_name=get_period_name(period, year, period_map),
            avg_hourly_earnings=hourly_values.get((year, period)),
            avg_weekly_earnings=weekly_values.get((year, period)),
            avg_weekly_hours=hours_values.get((year, period))
        ))

    return CEEarningsTimelineResponse(
        industry_code=industry_code,
        industry_name=industry.industry_name,
        timeline=timeline
    )
