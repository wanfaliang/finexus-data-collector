"""
LA (Local Area Unemployment Statistics) Survey Explorer API Endpoints
"""
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from ...core.database import get_db
from ...schemas.la_explorer import (
    LADimensions, LAAreaItem, LAMeasureItem,
    LASeriesListResponse, LASeriesInfo,
    LADataResponse, LASeriesData, LADataPoint
)
from src.database.bls_models import (
    LAArea, LAMeasure, LASeries, LAData, BLSPeriod
)

router = APIRouter(prefix="/la", tags=["LA Explorer"])


@router.get("/dimensions", response_model=LADimensions)
def get_la_dimensions(db: Session = Depends(get_db)):
    """Get all available dimensions for LA survey (areas and measures)"""

    # Get all areas
    areas = db.query(LAArea).order_by(LAArea.area_text).all()
    area_items = [
        LAAreaItem(
            area_code=a.area_code,
            area_name=a.area_text,
            area_type=a.area_type_code
        )
        for a in areas
    ]

    # Get all measures
    measures = db.query(LAMeasure).order_by(LAMeasure.measure_code).all()
    measure_items = [
        LAMeasureItem(
            measure_code=m.measure_code,
            measure_name=m.measure_text
        )
        for m in measures
    ]

    return LADimensions(areas=area_items, measures=measure_items)


@router.get("/series", response_model=LASeriesListResponse)
def get_la_series(
    area_code: Optional[str] = Query(None, description="Filter by area code"),
    measure_code: Optional[str] = Query(None, description="Filter by measure code"),
    seasonal_code: Optional[str] = Query(None, description="Filter by seasonal adjustment (S/U)"),
    active_only: bool = Query(True, description="Only return active series"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db)
):
    """Get LA series list with optional filters"""

    # Build query with joins
    query = db.query(
        LASeries,
        LAArea.area_text,
        LAMeasure.measure_text
    ).join(
        LAArea, LASeries.area_code == LAArea.area_code
    ).join(
        LAMeasure, LASeries.measure_code == LAMeasure.measure_code
    )

    # Apply filters
    if area_code:
        query = query.filter(LASeries.area_code == area_code)
    if measure_code:
        query = query.filter(LASeries.measure_code == measure_code)
    if seasonal_code:
        query = query.filter(LASeries.seasonal_code == seasonal_code)
    if active_only:
        query = query.filter(LASeries.is_active == True)

    # Get total count
    total = query.count()

    # Apply pagination
    results = query.order_by(LASeries.series_id).offset(offset).limit(limit).all()

    # Build response
    series_list = [
        LASeriesInfo(
            series_id=s.series_id,
            series_title=s.series_title,
            area_code=s.area_code,
            area_name=area_name,
            measure_code=s.measure_code,
            measure_name=measure_name,
            seasonal_code=s.seasonal_code,
            begin_year=s.begin_year,
            begin_period=s.begin_period,
            end_year=s.end_year,
            end_period=s.end_period,
            is_active=s.is_active
        )
        for s, area_name, measure_name in results
    ]

    return LASeriesListResponse(
        total=total,
        limit=limit,
        offset=offset,
        series=series_list
    )


@router.get("/series/{series_id}/data", response_model=LADataResponse)
def get_la_series_data(
    series_id: str,
    start_year: Optional[int] = Query(None, description="Filter data from this year"),
    end_year: Optional[int] = Query(None, description="Filter data up to this year"),
    db: Session = Depends(get_db)
):
    """Get time series data for a specific LA series"""

    # Get series metadata
    series = db.query(
        LASeries,
        LAArea.area_text,
        LAMeasure.measure_text
    ).join(
        LAArea, LASeries.area_code == LAArea.area_code
    ).join(
        LAMeasure, LASeries.measure_code == LAMeasure.measure_code
    ).filter(
        LASeries.series_id == series_id
    ).first()

    if not series:
        raise HTTPException(status_code=404, detail=f"Series {series_id} not found")

    s, area_name, measure_name = series

    # Get data points
    data_query = db.query(LAData).filter(LAData.series_id == series_id)

    if start_year:
        data_query = data_query.filter(LAData.year >= start_year)
    if end_year:
        data_query = data_query.filter(LAData.year <= end_year)

    data_points = data_query.order_by(LAData.year, LAData.period).all()

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
            LADataPoint(
                year=dp.year,
                period=dp.period,
                period_name=period_display,
                value=float(dp.value) if dp.value is not None else None,
                footnote_codes=dp.footnote_codes
            )
        )

    series_data = LASeriesData(
        series_id=s.series_id,
        series_title=s.series_title,
        area_name=area_name,
        measure_name=measure_name,
        data_points=formatted_points
    )

    return LADataResponse(series=[series_data])
