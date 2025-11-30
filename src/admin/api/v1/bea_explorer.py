"""
BEA Data Explorer API Endpoints

Endpoints for exploring NIPA and Regional BEA data.
"""
from typing import List, Optional
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, desc

from src.admin.core.database import get_db
from src.admin.schemas.bea import (
    NIPATableResponse,
    NIPASeriesResponse,
    NIPADataPointResponse,
    NIPATimeSeriesResponse,
    RegionalTableResponse,
    RegionalLineCodeResponse,
    RegionalGeoResponse,
    RegionalDataPointResponse,
    RegionalTimeSeriesResponse,
)
from src.database.bea_models import (
    NIPATable, NIPASeries, NIPAData,
    RegionalTable, RegionalLineCode, RegionalGeoFips, RegionalData,
)

router = APIRouter()


# ===================== NIPA Explorer ===================== #

@router.get("/nipa/tables", response_model=List[NIPATableResponse])
async def get_nipa_tables(
    active_only: bool = True,
    db: Session = Depends(get_db)
):
    """
    Get list of NIPA tables

    Args:
        active_only: Only return active tables
    """
    query = db.query(NIPATable)

    if active_only:
        query = query.filter(NIPATable.is_active == True)

    tables = query.order_by(NIPATable.table_name).all()

    # Get series counts per table
    series_counts = dict(
        db.query(
            NIPASeries.table_name,
            func.count(NIPASeries.series_code)
        ).group_by(NIPASeries.table_name).all()
    )

    return [
        NIPATableResponse(
            table_name=t.table_name,
            table_description=t.table_description,
            has_annual=t.has_annual or False,
            has_quarterly=t.has_quarterly or False,
            has_monthly=t.has_monthly or False,
            first_year=t.first_year,
            last_year=t.last_year,
            series_count=series_counts.get(t.table_name, 0),
            is_active=t.is_active or False,
        )
        for t in tables
    ]


@router.get("/nipa/tables/{table_name}", response_model=NIPATableResponse)
async def get_nipa_table(
    table_name: str,
    db: Session = Depends(get_db)
):
    """
    Get details of a specific NIPA table

    Args:
        table_name: NIPA table name (e.g., T10101)
    """
    table = db.query(NIPATable).filter(
        NIPATable.table_name == table_name.upper()
    ).first()

    if not table:
        raise HTTPException(status_code=404, detail=f"Table {table_name} not found")

    series_count = db.query(func.count(NIPASeries.series_code)).filter(
        NIPASeries.table_name == table_name.upper()
    ).scalar() or 0

    return NIPATableResponse(
        table_name=table.table_name,
        table_description=table.table_description,
        has_annual=table.has_annual or False,
        has_quarterly=table.has_quarterly or False,
        has_monthly=table.has_monthly or False,
        first_year=table.first_year,
        last_year=table.last_year,
        series_count=series_count,
        is_active=table.is_active or False,
    )


@router.get("/nipa/tables/{table_name}/series", response_model=List[NIPASeriesResponse])
async def get_nipa_table_series(
    table_name: str,
    db: Session = Depends(get_db)
):
    """
    Get all series for a NIPA table

    Args:
        table_name: NIPA table name
    """
    series_list = db.query(NIPASeries).filter(
        NIPASeries.table_name == table_name.upper()
    ).order_by(NIPASeries.line_number).all()

    if not series_list:
        raise HTTPException(status_code=404, detail=f"No series found for table {table_name}")

    # Get data point counts
    data_counts = dict(
        db.query(
            NIPAData.series_code,
            func.count(NIPAData.time_period)
        ).filter(
            NIPAData.series_code.in_([s.series_code for s in series_list])
        ).group_by(NIPAData.series_code).all()
    )

    return [
        NIPASeriesResponse(
            series_code=s.series_code,
            table_name=s.table_name,
            line_number=s.line_number,
            line_description=s.line_description,
            metric_name=s.metric_name,
            cl_unit=s.cl_unit,
            unit_mult=s.unit_mult,
            data_points_count=data_counts.get(s.series_code, 0),
        )
        for s in series_list
    ]


@router.get("/nipa/series/{series_code}/data", response_model=NIPATimeSeriesResponse)
async def get_nipa_series_data(
    series_code: str,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
    frequency: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """
    Get time series data for a NIPA series

    Args:
        series_code: NIPA series code
        start_year: Optional filter - start year
        end_year: Optional filter - end year
        frequency: Optional filter - A/Q/M
    """
    # Get series metadata
    series = db.query(NIPASeries).filter(
        NIPASeries.series_code == series_code
    ).first()

    if not series:
        raise HTTPException(status_code=404, detail=f"Series {series_code} not found")

    # Get data points
    query = db.query(NIPAData).filter(
        NIPAData.series_code == series_code
    )

    if start_year:
        query = query.filter(NIPAData.time_period >= str(start_year))
    if end_year:
        query = query.filter(NIPAData.time_period <= str(end_year + 1))  # Include full year

    data_points = query.order_by(NIPAData.time_period).all()

    # Filter by frequency if specified
    if frequency:
        freq = frequency.upper()
        if freq == 'A':
            data_points = [d for d in data_points if len(d.time_period) == 4]
        elif freq == 'Q':
            data_points = [d for d in data_points if 'Q' in d.time_period]
        elif freq == 'M':
            data_points = [d for d in data_points if 'M' in d.time_period]

    return NIPATimeSeriesResponse(
        series_code=series_code,
        line_description=series.line_description,
        metric_name=series.metric_name,
        unit=series.cl_unit,
        data=[
            {
                "time_period": d.time_period,
                "value": float(d.value) if d.value else None,
                "note_ref": d.note_ref,
            }
            for d in data_points
        ]
    )


# ===================== Regional Explorer ===================== #

@router.get("/regional/tables", response_model=List[RegionalTableResponse])
async def get_regional_tables(
    active_only: bool = True,
    db: Session = Depends(get_db)
):
    """
    Get list of Regional tables

    Args:
        active_only: Only return active tables
    """
    query = db.query(RegionalTable)

    if active_only:
        query = query.filter(RegionalTable.is_active == True)

    tables = query.order_by(RegionalTable.table_name).all()

    # Get line code counts per table
    line_counts = dict(
        db.query(
            RegionalLineCode.table_name,
            func.count(RegionalLineCode.line_code)
        ).group_by(RegionalLineCode.table_name).all()
    )

    return [
        RegionalTableResponse(
            table_name=t.table_name,
            table_description=t.table_description,
            geo_scope=t.geo_scope,
            first_year=t.first_year,
            last_year=t.last_year,
            line_codes_count=line_counts.get(t.table_name, 0),
            is_active=t.is_active or False,
        )
        for t in tables
    ]


@router.get("/regional/tables/{table_name}", response_model=RegionalTableResponse)
async def get_regional_table(
    table_name: str,
    db: Session = Depends(get_db)
):
    """
    Get details of a specific Regional table

    Args:
        table_name: Regional table name (e.g., CAINC1, SAGDP1)
    """
    table = db.query(RegionalTable).filter(
        RegionalTable.table_name == table_name.upper()
    ).first()

    if not table:
        raise HTTPException(status_code=404, detail=f"Table {table_name} not found")

    line_count = db.query(func.count(RegionalLineCode.line_code)).filter(
        RegionalLineCode.table_name == table_name.upper()
    ).scalar() or 0

    return RegionalTableResponse(
        table_name=table.table_name,
        table_description=table.table_description,
        geo_scope=table.geo_scope,
        first_year=table.first_year,
        last_year=table.last_year,
        line_codes_count=line_count,
        is_active=table.is_active or False,
    )


@router.get("/regional/tables/{table_name}/linecodes", response_model=List[RegionalLineCodeResponse])
async def get_regional_table_linecodes(
    table_name: str,
    db: Session = Depends(get_db)
):
    """
    Get all line codes for a Regional table

    Args:
        table_name: Regional table name
    """
    line_codes = db.query(RegionalLineCode).filter(
        RegionalLineCode.table_name == table_name.upper()
    ).order_by(RegionalLineCode.line_code).all()

    if not line_codes:
        raise HTTPException(status_code=404, detail=f"No line codes found for table {table_name}")

    return [
        RegionalLineCodeResponse(
            table_name=lc.table_name,
            line_code=lc.line_code,
            line_description=lc.line_description,
            cl_unit=lc.cl_unit,
            unit_mult=lc.unit_mult,
        )
        for lc in line_codes
    ]


@router.get("/regional/geographies", response_model=List[RegionalGeoResponse])
async def get_regional_geographies(
    geo_type: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = Query(100, ge=1, le=1000),
    db: Session = Depends(get_db)
):
    """
    Get list of geographic areas

    Args:
        geo_type: Filter by type (State, County, MSA, etc.)
        search: Search in geo_name
        limit: Maximum results to return
    """
    query = db.query(RegionalGeoFips)

    if geo_type:
        query = query.filter(RegionalGeoFips.geo_type == geo_type)

    if search:
        query = query.filter(RegionalGeoFips.geo_name.ilike(f"%{search}%"))

    geos = query.order_by(RegionalGeoFips.geo_name).limit(limit).all()

    return [
        RegionalGeoResponse(
            geo_fips=g.geo_fips,
            geo_name=g.geo_name,
            geo_type=g.geo_type,
            parent_fips=g.parent_fips,
        )
        for g in geos
    ]


@router.get("/regional/data", response_model=RegionalTimeSeriesResponse)
async def get_regional_data(
    table_name: str,
    line_code: int,
    geo_fips: str,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
    db: Session = Depends(get_db)
):
    """
    Get time series data for a Regional table/line/geography combination

    Args:
        table_name: Regional table name
        line_code: Line code
        geo_fips: Geographic FIPS code
        start_year: Optional filter - start year
        end_year: Optional filter - end year
    """
    # Get line code info
    line_info = db.query(RegionalLineCode).filter(
        RegionalLineCode.table_name == table_name.upper(),
        RegionalLineCode.line_code == line_code
    ).first()

    if not line_info:
        raise HTTPException(
            status_code=404,
            detail=f"Line code {line_code} not found for table {table_name}"
        )

    # Get geography info
    geo_info = db.query(RegionalGeoFips).filter(
        RegionalGeoFips.geo_fips == geo_fips
    ).first()

    if not geo_info:
        raise HTTPException(status_code=404, detail=f"Geography {geo_fips} not found")

    # Get data
    query = db.query(RegionalData).filter(
        RegionalData.table_name == table_name.upper(),
        RegionalData.line_code == line_code,
        RegionalData.geo_fips == geo_fips
    )

    if start_year:
        query = query.filter(RegionalData.time_period >= str(start_year))
    if end_year:
        query = query.filter(RegionalData.time_period <= str(end_year))

    data_points = query.order_by(RegionalData.time_period).all()

    return RegionalTimeSeriesResponse(
        table_name=table_name.upper(),
        line_code=line_code,
        line_description=line_info.line_description,
        geo_fips=geo_fips,
        geo_name=geo_info.geo_name,
        unit=line_info.cl_unit,
        data=[
            {
                "time_period": d.time_period,
                "value": float(d.value) if d.value else None,
                "note_ref": d.note_ref,
            }
            for d in data_points
        ]
    )


@router.get("/regional/compare")
async def compare_regional_data(
    table_name: str,
    line_code: int,
    geo_fips_list: str = Query(..., description="Comma-separated list of FIPS codes"),
    year: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """
    Compare Regional data across multiple geographies

    Args:
        table_name: Regional table name
        line_code: Line code
        geo_fips_list: Comma-separated FIPS codes
        year: Optional year filter
    """
    fips_codes = [f.strip() for f in geo_fips_list.split(',')]

    if len(fips_codes) > 50:
        raise HTTPException(status_code=400, detail="Maximum 50 geographies for comparison")

    # Get data for all geographies
    query = db.query(RegionalData).filter(
        RegionalData.table_name == table_name.upper(),
        RegionalData.line_code == line_code,
        RegionalData.geo_fips.in_(fips_codes)
    )

    if year:
        query = query.filter(RegionalData.time_period == year)

    data_points = query.all()

    # Get geography names
    geo_names = dict(
        db.query(RegionalGeoFips.geo_fips, RegionalGeoFips.geo_name).filter(
            RegionalGeoFips.geo_fips.in_(fips_codes)
        ).all()
    )

    # Organize by geography
    result = {}
    for d in data_points:
        if d.geo_fips not in result:
            result[d.geo_fips] = {
                "geo_fips": d.geo_fips,
                "geo_name": geo_names.get(d.geo_fips, d.geo_fips),
                "data": []
            }
        result[d.geo_fips]["data"].append({
            "time_period": d.time_period,
            "value": float(d.value) if d.value else None,
        })

    return {
        "table_name": table_name.upper(),
        "line_code": line_code,
        "geographies": list(result.values())
    }
