"""
CE (Current Employment Statistics) Survey Explorer API Endpoints
"""
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from ...core.database import get_db
from ...schemas.ce_explorer import (
    CEDimensions, CEIndustryItem, CESupersectorItem,
    CESeriesListResponse, CESeriesInfo,
    CEDataResponse, CESeriesData, CEDataPoint
)
from src.database.bls_models import (
    CEIndustry, CESupersector, CESeries, CEData, BLSPeriod
)

router = APIRouter(prefix="/ce", tags=["CE Explorer"])


@router.get("/dimensions", response_model=CEDimensions)
def get_ce_dimensions(db: Session = Depends(get_db)):
    """Get all available dimensions for CE survey (industries and supersectors)"""

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

    return CEDimensions(industries=industry_items, supersectors=supersector_items)


@router.get("/series", response_model=CESeriesListResponse)
def get_ce_series(
    industry_code: Optional[str] = Query(None, description="Filter by industry code"),
    supersector_code: Optional[str] = Query(None, description="Filter by supersector code"),
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
        CESupersector.supersector_name
    ).join(
        CEIndustry, CESeries.industry_code == CEIndustry.industry_code
    ).outerjoin(
        CESupersector, CESeries.supersector_code == CESupersector.supersector_code
    )

    # Apply filters
    if industry_code:
        query = query.filter(CESeries.industry_code == industry_code)
    if supersector_code:
        query = query.filter(CESeries.supersector_code == supersector_code)
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
            seasonal_code=s.seasonal_code,
            begin_year=s.begin_year,
            begin_period=s.begin_period,
            end_year=s.end_year,
            end_period=s.end_period,
            is_active=s.is_active
        )
        for s, industry_name, supersector_name in results
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
