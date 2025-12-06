"""
CU (Consumer Price Index) Survey Explorer API Endpoints
"""
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_

from ...core.database import get_db
from ...schemas.cu_explorer import (
    CUDimensions, CUAreaItem, CUItemItem,
    CUSeriesListResponse, CUSeriesInfo,
    CUDataResponse, CUSeriesData, CUDataPoint,
    CUOverviewResponse, InflationMetric,
    CUCategoryAnalysisResponse, CategoryMetric,
    CUAreaComparisonResponse, AreaComparisonMetric,
    TimelineDataPoint, CUOverviewTimelineResponse,
    CategoryTimelinePoint, CUCategoryTimelineResponse,
    AreaTimelinePoint, CUAreaComparisonTimelineResponse
)
from src.database.bls_models import (
    CUArea, CUItem, CUSeries, CUData, BLSPeriod
)

router = APIRouter(prefix="/cu", tags=["CU Explorer"])


@router.get("/dimensions", response_model=CUDimensions)
def get_cu_dimensions(db: Session = Depends(get_db)):
    """Get all available dimensions for CU survey (areas and items)"""

    # Get all areas
    areas = db.query(CUArea).order_by(CUArea.sort_sequence).all()
    area_items = [
        CUAreaItem(
            area_code=a.area_code,
            area_name=a.area_name,
            display_level=a.display_level or 0,
            selectable=a.selectable == 'T',
            sort_sequence=a.sort_sequence or 0
        )
        for a in areas
    ]

    # Get all items
    items = db.query(CUItem).order_by(CUItem.sort_sequence).all()
    item_items = [
        CUItemItem(
            item_code=i.item_code,
            item_name=i.item_name,
            display_level=i.display_level or 0,
            selectable=i.selectable == 'T',
            sort_sequence=i.sort_sequence or 0
        )
        for i in items
    ]

    return CUDimensions(areas=area_items, items=item_items)


@router.get("/series", response_model=CUSeriesListResponse)
def get_cu_series(
    area_code: Optional[str] = Query(None, description="Filter by area code"),
    item_code: Optional[str] = Query(None, description="Filter by item code"),
    seasonal_code: Optional[str] = Query(None, description="Filter by seasonal adjustment (S/U)"),
    begin_year: Optional[int] = Query(None, description="Filter by begin year (>=)"),
    end_year: Optional[int] = Query(None, description="Filter by end year (<=)"),
    active_only: bool = Query(True, description="Only return active series"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db)
):
    """Get CU series list with optional filters"""

    # Build query with joins
    query = db.query(
        CUSeries,
        CUArea.area_name,
        CUItem.item_name
    ).join(
        CUArea, CUSeries.area_code == CUArea.area_code
    ).join(
        CUItem, CUSeries.item_code == CUItem.item_code
    )

    # Apply filters
    if area_code:
        query = query.filter(CUSeries.area_code == area_code)
    if item_code:
        query = query.filter(CUSeries.item_code == item_code)
    if seasonal_code:
        query = query.filter(CUSeries.seasonal_code == seasonal_code)
    if begin_year:
        # Show series that have data available FROM begin_year onwards
        # (series hasn't ended before begin_year)
        query = query.filter(or_(CUSeries.end_year == None, CUSeries.end_year >= begin_year))
    if end_year:
        # Show series that have data available UP TO end_year
        # (series started before or in end_year)
        query = query.filter(CUSeries.begin_year <= end_year)
    if active_only:
        query = query.filter(CUSeries.is_active == True)

    # Get total count
    total = query.count()

    # Apply pagination
    results = query.order_by(CUSeries.series_id).offset(offset).limit(limit).all()

    # Build response
    series_list = [
        CUSeriesInfo(
            series_id=s.series_id,
            series_title=s.series_title,
            area_code=s.area_code,
            area_name=area_name,
            item_code=s.item_code,
            item_name=item_name,
            seasonal_code=s.seasonal_code or '',
            periodicity_code=s.periodicity_code,
            base_period=s.base_period,
            begin_year=s.begin_year,
            begin_period=s.begin_period,
            end_year=s.end_year,
            end_period=s.end_period,
            is_active=s.is_active
        )
        for s, area_name, item_name in results
    ]

    return CUSeriesListResponse(
        total=total,
        limit=limit,
        offset=offset,
        series=series_list
    )


@router.get("/series/{series_id}/data", response_model=CUDataResponse)
def get_cu_series_data(
    series_id: str,
    start_year: Optional[int] = Query(None, description="Filter data from this year"),
    end_year: Optional[int] = Query(None, description="Filter data up to this year"),
    start_period: Optional[str] = Query(None, description="Filter data from this period (M01-M12)"),
    end_period: Optional[str] = Query(None, description="Filter data up to this period (M01-M12)"),
    db: Session = Depends(get_db)
):
    """Get time series data for a specific CU series"""

    # Get series metadata
    series = db.query(
        CUSeries,
        CUArea.area_name,
        CUItem.item_name
    ).join(
        CUArea, CUSeries.area_code == CUArea.area_code
    ).join(
        CUItem, CUSeries.item_code == CUItem.item_code
    ).filter(
        CUSeries.series_id == series_id
    ).first()

    if not series:
        raise HTTPException(status_code=404, detail=f"Series {series_id} not found")

    s, area_name, item_name = series

    # Get data points
    data_query = db.query(CUData).filter(CUData.series_id == series_id)

    # Apply year filters
    if start_year:
        data_query = data_query.filter(CUData.year >= start_year)
    if end_year:
        data_query = data_query.filter(CUData.year <= end_year)

    # Apply period/month filters
    # BLS periods are formatted as M01, M02, ..., M12 for monthly data
    if start_period:
        # For start period, filter: year > start_year OR (year == start_year AND period >= start_period)
        if start_year:
            data_query = data_query.filter(
                or_(
                    CUData.year > start_year,
                    and_(CUData.year == start_year, CUData.period >= f"M{start_period}")
                )
            )
        else:
            # If no start_year, just filter by period across all years
            data_query = data_query.filter(CUData.period >= f"M{start_period}")

    if end_period:
        # For end period, filter: year < end_year OR (year == end_year AND period <= end_period)
        if end_year:
            data_query = data_query.filter(
                or_(
                    CUData.year < end_year,
                    and_(CUData.year == end_year, CUData.period <= f"M{end_period}")
                )
            )
        else:
            # If no end_year, just filter by period across all years
            data_query = data_query.filter(CUData.period <= f"M{end_period}")

    data_points = data_query.order_by(CUData.year, CUData.period).all()

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
            CUDataPoint(
                year=dp.year,
                period=dp.period,
                period_name=period_display,
                value=float(dp.value) if dp.value is not None else None,
                footnote_codes=dp.footnote_codes
            )
        )

    series_data = CUSeriesData(
        series_id=s.series_id,
        series_title=s.series_title,
        area_name=area_name,
        item_name=item_name,
        data_points=formatted_points
    )

    return CUDataResponse(series=[series_data])


def _calculate_inflation_metrics(series_id: str, item_name: str, db: Session) -> Optional[InflationMetric]:
    """Helper function to calculate inflation metrics for a series"""
    # Get enough data for YoY calculations (13+ months)
    data_points = db.query(CUData).filter(
        CUData.series_id == series_id
    ).order_by(
        CUData.year.desc(), CUData.period.desc()
    ).limit(25).all()  # Get extra records to ensure we have year-ago data

    if not data_points or len(data_points) < 1:
        return None

    # Most recent data point
    latest = data_points[0]
    latest_value = float(latest.value) if latest.value else None
    latest_year = latest.year
    latest_period = latest.period

    # Build a dictionary for easier lookups
    data_dict = {(d.year, d.period): float(d.value) if d.value else None for d in data_points}

    # Calculate m/m change (vs previous month)
    month_over_month = None
    prev_month = int(latest_period[1:]) - 1  # Remove 'M' prefix and subtract 1
    prev_year = latest_year
    if prev_month < 1:
        prev_month = 12
        prev_year = latest_year - 1
    prev_period = f"M{prev_month:02d}"
    prev_month_value = data_dict.get((prev_year, prev_period))
    if prev_month_value and latest_value:
        month_over_month = ((latest_value - prev_month_value) / prev_month_value) * 100

    # Calculate y/y change (vs same month last year)
    year_over_year = None
    year_ago_value = data_dict.get((latest_year - 1, latest_period))
    if year_ago_value and latest_value:
        year_over_year = ((latest_value - year_ago_value) / year_ago_value) * 100

    return InflationMetric(
        series_id=series_id,
        item_name=item_name,
        latest_value=latest_value,
        latest_date=f"{latest.year}-{latest.period}",
        month_over_month=round(month_over_month, 2) if month_over_month else None,
        year_over_year=round(year_over_year, 2) if year_over_year else None
    )


@router.get("/overview", response_model=CUOverviewResponse)
def get_cu_overview(
    area_code: str = Query("0000", description="Area code (default: US City Average)"),
    db: Session = Depends(get_db)
):
    """Get overview dashboard with headline and core CPI"""

    # Determine if we should use seasonally adjusted or unadjusted series
    # National data (0000) has seasonally adjusted (S) series
    # Metro areas only have unadjusted (U) series
    seasonal_code = "S" if area_code == "0000" else "U"

    # Headline CPI: All items
    # Series ID format: CU{S/U}R{area}SA0 where area is 4 digits
    headline_series_id = f"CU{seasonal_code}R{area_code}SA0"

    # Core CPI: All items less food and energy
    # Series ID format: CU{S/U}R{area}SA0L1E
    core_series_id = f"CU{seasonal_code}R{area_code}SA0L1E"

    headline = _calculate_inflation_metrics(headline_series_id, "All items", db)
    core = _calculate_inflation_metrics(core_series_id, "All items less food and energy", db)

    return CUOverviewResponse(
        survey_code="CU",
        headline_cpi=headline,
        core_cpi=core,
        last_updated=headline.latest_date if headline else None
    )


@router.get("/overview/timeline", response_model=CUOverviewTimelineResponse)
def get_cu_overview_timeline(
    area_code: str = Query("0000", description="Area code (default: US City Average)"),
    months_back: int = Query(12, ge=0, le=600, description="Number of months to look back (0 for all time)"),
    db: Session = Depends(get_db)
):
    """Get historical timeline data for overview dashboard"""
    from datetime import datetime, timedelta
    from dateutil.relativedelta import relativedelta

    # Determine seasonal code
    seasonal_code = "S" if area_code == "0000" else "U"

    # Get area name
    area_name = db.query(CUArea.area_name).filter(CUArea.area_code == area_code).scalar() or "Unknown Area"

    # Series IDs
    headline_series_id = f"CU{seasonal_code}R{area_code}SA0"
    core_series_id = f"CU{seasonal_code}R{area_code}SA0L1E"

    # Get the most recent N months of data directly (0 = all time)
    # Fetch extra data for YoY calculations (need data from 13 months ago)
    fetch_limit = None if months_back == 0 else months_back + 13  # Get enough for YoY calculations

    # Get headline data - get most recent records
    headline_query = db.query(CUData).filter(
        CUData.series_id == headline_series_id
    ).order_by(CUData.year.desc(), CUData.period.desc())
    headline_data = headline_query.all() if fetch_limit is None else headline_query.limit(fetch_limit).all()
    headline_data = list(reversed(headline_data))  # Reverse to chronological order

    # Get core data
    core_query = db.query(CUData).filter(
        CUData.series_id == core_series_id
    ).order_by(CUData.year.desc(), CUData.period.desc())
    core_data = core_query.all() if fetch_limit is None else core_query.limit(fetch_limit).all()
    core_data = list(reversed(core_data))  # Reverse to chronological order

    # Create dictionaries for quick lookup
    headline_dict = {(d.year, d.period): float(d.value) if d.value else None for d in headline_data}
    core_dict = {(d.year, d.period): float(d.value) if d.value else None for d in core_data}

    # Get period names
    period_map = {p.period_code: p.period_name for p in db.query(BLSPeriod).all()}

    # Build timeline
    timeline = []
    for data_point in headline_data:
        year = data_point.year
        period = data_point.period

        headline_value = headline_dict.get((year, period))
        core_value = core_dict.get((year, period))

        # Calculate MoM (vs previous month)
        headline_mom = None
        core_mom = None
        prev_month = int(period[1:]) - 1
        prev_year = year
        if prev_month == 0:
            prev_month = 12
            prev_year = year - 1
        prev_period = f"M{prev_month:02d}"

        prev_headline = headline_dict.get((prev_year, prev_period))
        if headline_value and prev_headline:
            headline_mom = ((headline_value - prev_headline) / prev_headline) * 100

        prev_core = core_dict.get((prev_year, prev_period))
        if core_value and prev_core:
            core_mom = ((core_value - prev_core) / prev_core) * 100

        # Calculate YoY (vs 12 months ago)
        headline_yoy = None
        core_yoy = None
        yoy_period = period
        yoy_year = year - 1

        yoy_headline = headline_dict.get((yoy_year, yoy_period))
        if headline_value and yoy_headline:
            headline_yoy = ((headline_value - yoy_headline) / yoy_headline) * 100

        yoy_core = core_dict.get((yoy_year, yoy_period))
        if core_value and yoy_core:
            core_yoy = ((core_value - yoy_core) / yoy_core) * 100

        # Format period name
        period_name_str = period_map.get(period, period)
        period_display = f"{period_name_str} {year}"

        timeline.append(TimelineDataPoint(
            year=year,
            period=period,
            period_name=period_display,
            headline_value=round(headline_value, 3) if headline_value else None,
            headline_yoy=round(headline_yoy, 2) if headline_yoy else None,
            headline_mom=round(headline_mom, 2) if headline_mom else None,
            core_value=round(core_value, 3) if core_value else None,
            core_yoy=round(core_yoy, 2) if core_yoy else None,
            core_mom=round(core_mom, 2) if core_mom else None
        ))

    # Trim timeline to exact months_back requested
    if len(timeline) > months_back:
        timeline = timeline[-months_back:]

    return CUOverviewTimelineResponse(
        survey_code="CU",
        area_code=area_code,
        area_name=area_name,
        timeline=timeline
    )


@router.get("/categories", response_model=CUCategoryAnalysisResponse)
def get_cu_category_analysis(
    area_code: str = Query("0000", description="Area code (default: US City Average)"),
    db: Session = Depends(get_db)
):
    """Get major CPI categories with trends"""

    # Determine if we should use seasonally adjusted or unadjusted series
    # National data (0000) has seasonally adjusted (S) series
    # Metro areas only have unadjusted (U) series
    seasonal_code = "S" if area_code == "0000" else "U"

    # Define major categories (item codes for seasonally adjusted series)
    # These are the major groups in CPI
    categories = [
        ("SAF", "Food and beverages"),
        ("SAH", "Housing"),
        ("SAA", "Apparel"),
        ("SAT", "Transportation"),
        ("SAM", "Medical care"),
        ("SAR", "Recreation"),
        ("SAE", "Education and communication"),
        ("SAG", "Other goods and services"),
    ]

    area_name = db.query(CUArea.area_name).filter(CUArea.area_code == area_code).scalar() or "Unknown Area"

    category_metrics = []
    for item_code, category_name in categories:
        # Series ID format: CU{S/U}R{area}{item_code}
        series_id = f"CU{seasonal_code}R{area_code}{item_code}"
        metric = _calculate_inflation_metrics(series_id, category_name, db)
        if metric:
            category_metrics.append(CategoryMetric(
                category_code=item_code,
                category_name=category_name,
                latest_value=metric.latest_value,
                latest_date=metric.latest_date,
                month_over_month=metric.month_over_month,
                year_over_year=metric.year_over_year,
                series_id=series_id
            ))

    return CUCategoryAnalysisResponse(
        survey_code="CU",
        area_code=area_code,
        area_name=area_name,
        categories=category_metrics
    )


@router.get("/categories/timeline", response_model=CUCategoryTimelineResponse)
def get_cu_category_timeline(
    area_code: str = Query("0000", description="Area code (default: US City Average)"),
    months_back: int = Query(12, ge=0, le=600, description="Number of months to look back (0 for all time)"),
    db: Session = Depends(get_db)
):
    """Get historical timeline data for category analysis"""
    from datetime import datetime
    from dateutil.relativedelta import relativedelta
    from collections import defaultdict

    # Determine seasonal code
    seasonal_code = "S" if area_code == "0000" else "U"

    # Get area name
    area_name = db.query(CUArea.area_name).filter(CUArea.area_code == area_code).scalar() or "Unknown Area"

    # Define categories
    categories = [
        ("SAF", "Food and beverages"),
        ("SAH", "Housing"),
        ("SAA", "Apparel"),
        ("SAT", "Transportation"),
        ("SAM", "Medical care"),
        ("SAR", "Recreation"),
        ("SAE", "Education and communication"),
        ("SAG", "Other goods and services"),
    ]

    # Get data for all categories (0 = all time)
    # Fetch most recent N months directly using LIMIT
    fetch_limit = None if months_back == 0 else months_back + 13  # Extra records for YoY calculations

    category_data = {}
    for item_code, category_name in categories:
        series_id = f"CU{seasonal_code}R{area_code}{item_code}"
        query = db.query(CUData).filter(
            CUData.series_id == series_id
        ).order_by(CUData.year.desc(), CUData.period.desc())
        data = query.all() if fetch_limit is None else query.limit(fetch_limit).all()

        # Reverse to chronological order
        data = list(reversed(data))

        category_data[item_code] = {
            'name': category_name,
            'series_id': series_id,
            'data': {(d.year, d.period): float(d.value) if d.value else None for d in data},
            'points': data
        }

    # Get period names
    period_map = {p.period_code: p.period_name for p in db.query(BLSPeriod).all()}

    # Build timeline by iterating through months
    # Use the first category's data points as the timeline backbone
    timeline_points = []
    if categories and category_data.get(categories[0][0]):
        for data_point in category_data[categories[0][0]]['points']:
            year = data_point.year
            period = data_point.period

            # Calculate metrics for all categories at this time point
            category_metrics = []
            for item_code, category_name in categories:
                cat_info = category_data.get(item_code)
                if not cat_info:
                    continue

                value = cat_info['data'].get((year, period))

                # Calculate MoM
                mom = None
                prev_month = int(period[1:]) - 1
                prev_year = year
                if prev_month == 0:
                    prev_month = 12
                    prev_year = year - 1
                prev_period = f"M{prev_month:02d}"
                prev_value = cat_info['data'].get((prev_year, prev_period))
                if value and prev_value:
                    mom = ((value - prev_value) / prev_value) * 100

                # Calculate YoY
                yoy = None
                yoy_value = cat_info['data'].get((year - 1, period))
                if value and yoy_value:
                    yoy = ((value - yoy_value) / yoy_value) * 100

                category_metrics.append(CategoryMetric(
                    category_code=item_code,
                    category_name=category_name,
                    latest_value=round(value, 3) if value else None,
                    latest_date=f"{year}-{period}",
                    month_over_month=round(mom, 2) if mom else None,
                    year_over_year=round(yoy, 2) if yoy else None,
                    series_id=cat_info['series_id']
                ))

            # Format period name
            period_name_str = period_map.get(period, period)
            period_display = f"{period_name_str} {year}"

            timeline_points.append(CategoryTimelinePoint(
                year=year,
                period=period,
                period_name=period_display,
                categories=category_metrics
            ))

    # Trim timeline to exact months_back requested
    if len(timeline_points) > months_back:
        timeline_points = timeline_points[-months_back:]

    return CUCategoryTimelineResponse(
        survey_code="CU",
        area_code=area_code,
        area_name=area_name,
        timeline=timeline_points
    )


@router.get("/areas/compare", response_model=CUAreaComparisonResponse)
def compare_areas(
    item_code: str = Query("SA0", description="Item code to compare across areas"),
    db: Session = Depends(get_db)
):
    """Compare same item across different metropolitan areas"""

    # Query all selectable areas first
    areas_query = db.query(
        CUArea.area_code,
        CUArea.area_name,
        CUArea.sort_sequence
    ).filter(
        CUArea.selectable == 'T'
    ).order_by(CUArea.sort_sequence).all()

    item_name = db.query(CUItem.item_name).filter(CUItem.item_code == item_code).scalar() or "Unknown Item"

    area_comparisons = []
    for area_code, area_name, _ in areas_query:
        # Use seasonally adjusted for US City Average (0000), try both S and U for others
        seasonal_code = "S" if area_code == "0000" else "U"
        series_id = f"CU{seasonal_code}R{area_code}{item_code}"

        # Check if series exists, if not try the other seasonal code
        series_exists = db.query(CUSeries).filter(CUSeries.series_id == series_id).first()
        if not series_exists and area_code != "0000":
            # Try seasonally adjusted for this area
            seasonal_code = "S"
            series_id = f"CU{seasonal_code}R{area_code}{item_code}"
            series_exists = db.query(CUSeries).filter(CUSeries.series_id == series_id).first()

        if not series_exists:
            continue

        metric = _calculate_inflation_metrics(series_id, item_name, db)
        if metric:
            area_comparisons.append(AreaComparisonMetric(
                area_code=area_code,
                area_name=area_name,
                series_id=series_id,
                latest_value=metric.latest_value,
                latest_date=metric.latest_date,
                month_over_month=metric.month_over_month,
                year_over_year=metric.year_over_year
            ))

    return CUAreaComparisonResponse(
        item_code=item_code,
        item_name=item_name,
        areas=area_comparisons
    )


@router.get("/areas/compare/timeline", response_model=CUAreaComparisonTimelineResponse)
def get_area_comparison_timeline(
    item_code: str = Query("SA0", description="Item code to compare across areas"),
    months_back: int = Query(12, ge=0, le=600, description="Number of months to look back (0 for all time)"),
    db: Session = Depends(get_db)
):
    """Get historical timeline data for area comparison"""
    from datetime import datetime
    from dateutil.relativedelta import relativedelta

    # Get item name
    item_name = db.query(CUItem.item_name).filter(CUItem.item_code == item_code).scalar() or "Unknown Item"

    # Query all selectable areas first
    areas_query = db.query(
        CUArea.area_code,
        CUArea.area_name,
        CUArea.sort_sequence
    ).filter(
        CUArea.selectable == 'T'
    ).order_by(CUArea.sort_sequence).limit(15).all()

    # Get data for all areas (0 = all time)
    # Fetch most recent N months directly using LIMIT
    fetch_limit = None if months_back == 0 else months_back + 13  # Extra records for YoY calculations

    area_data = {}
    series_list = []
    for area_code, area_name, _ in areas_query:
        # Use seasonally adjusted for US City Average (0000), try both S and U for others
        seasonal_code = "S" if area_code == "0000" else "U"
        series_id = f"CU{seasonal_code}R{area_code}{item_code}"

        # Check if series exists, if not try the other seasonal code
        series_exists = db.query(CUSeries).filter(CUSeries.series_id == series_id).first()
        if not series_exists and area_code != "0000":
            # Try seasonally adjusted for this area
            seasonal_code = "S"
            series_id = f"CU{seasonal_code}R{area_code}{item_code}"
            series_exists = db.query(CUSeries).filter(CUSeries.series_id == series_id).first()

        if not series_exists:
            continue

        series_list.append((series_id, area_code, area_name))

    for series_id, area_code, area_name in series_list:
        query = db.query(CUData).filter(
            CUData.series_id == series_id
        ).order_by(CUData.year.desc(), CUData.period.desc())
        data = query.all() if fetch_limit is None else query.limit(fetch_limit).all()

        # Reverse to chronological order
        data = list(reversed(data))

        area_data[area_code] = {
            'name': area_name,
            'series_id': series_id,
            'data': {(d.year, d.period): float(d.value) if d.value else None for d in data},
            'points': data
        }

    # Get period names
    period_map = {p.period_code: p.period_name for p in db.query(BLSPeriod).all()}

    # Build timeline by iterating through months
    # Use the first area's data points as the timeline backbone
    timeline_points = []
    if series_list and area_data:
        first_area_code = series_list[0][1]
        if first_area_code in area_data:
            for data_point in area_data[first_area_code]['points']:
                year = data_point.year
                period = data_point.period

                # Calculate metrics for all areas at this time point
                area_metrics = []
                for series_id, area_code, area_name in series_list:
                    area_info = area_data.get(area_code)
                    if not area_info:
                        continue

                    value = area_info['data'].get((year, period))

                    # Calculate MoM
                    mom = None
                    prev_month = int(period[1:]) - 1
                    prev_year = year
                    if prev_month < 1:
                        prev_month = 12
                        prev_year = year - 1
                    prev_period = f"M{prev_month:02d}"
                    mom_value = area_info['data'].get((prev_year, prev_period))
                    if value and mom_value:
                        mom = ((value - mom_value) / mom_value) * 100

                    # Calculate YoY
                    yoy = None
                    yoy_value = area_info['data'].get((year - 1, period))
                    if value and yoy_value:
                        yoy = ((value - yoy_value) / yoy_value) * 100

                    area_metrics.append(AreaComparisonMetric(
                        area_code=area_code,
                        area_name=area_name,
                        series_id=series_id,
                        latest_value=round(value, 3) if value else None,
                        latest_date=f"{year}-{period}",
                        month_over_month=round(mom, 2) if mom else None,
                        year_over_year=round(yoy, 2) if yoy else None
                    ))

                # Format period name
                period_name_str = period_map.get(period, period)
                period_display = f"{period_name_str} {year}"

                timeline_points.append(AreaTimelinePoint(
                    year=year,
                    period=period,
                    period_name=period_display,
                    areas=area_metrics
                ))

    # Trim timeline to exact months_back requested
    if len(timeline_points) > months_back:
        timeline_points = timeline_points[-months_back:]

    return CUAreaComparisonTimelineResponse(
        survey_code="CU",
        item_code=item_code,
        item_name=item_name,
        timeline=timeline_points
    )
