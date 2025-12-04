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
    ITAIndicatorResponse,
    ITAAreaResponse,
    ITATimeSeriesResponse,
    FixedAssetsTableResponse,
    FixedAssetsSeriesResponse,
    FixedAssetsTimeSeriesResponse,
)
from src.database.bea_models import (
    NIPATable, NIPASeries, NIPAData,
    RegionalTable, RegionalLineCode, RegionalGeoFips, RegionalData,
    ITAIndicator, ITAArea, ITAData,
    FixedAssetsTable, FixedAssetsSeries, FixedAssetsData,
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
        return []  # Return empty list instead of 404

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
        unit_mult=series.unit_mult,
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

    # Get unit info from actual data (line_codes table often has NULL)
    unit_mult = None
    cl_unit = None
    for d in data_points:
        if unit_mult is None and d.unit_mult is not None:
            unit_mult = d.unit_mult
        if cl_unit is None and d.cl_unit is not None:
            cl_unit = d.cl_unit
        if unit_mult is not None and cl_unit is not None:
            break

    return RegionalTimeSeriesResponse(
        table_name=table_name.upper(),
        line_code=line_code,
        line_description=line_info.line_description,
        geo_fips=geo_fips,
        geo_name=geo_info.geo_name,
        unit=cl_unit or line_info.cl_unit,
        unit_mult=unit_mult if unit_mult is not None else line_info.unit_mult,
        data=[
            {
                "time_period": d.time_period,
                "value": float(d.value) if d.value else None,
                "note_ref": d.note_ref,
            }
            for d in data_points
        ]
    )


@router.get("/regional/snapshot")
async def get_regional_snapshot(
    table_name: str = "SAGDP1",
    line_code: int = 1,
    geo_type: str = "State",
    db: Session = Depends(get_db)
):
    """
    Get snapshot of latest data for all geographies of a type (for treemap/heatmap)

    Args:
        table_name: Regional table name (default: SAGDP1 - Real GDP)
        line_code: Line code (default: 1 - Real GDP)
        geo_type: Type of geography (State, County, MSA)
    """
    from sqlalchemy import and_

    # Get all geographies of the specified type
    geos = db.query(RegionalGeoFips).filter(
        RegionalGeoFips.geo_type == geo_type
    ).all()

    if not geos:
        return {"data": [], "table_name": table_name, "line_code": line_code}

    geo_fips_list = [g.geo_fips for g in geos]
    geo_names = {g.geo_fips: g.geo_name for g in geos}

    # Get the latest year available
    latest_year = db.query(func.max(RegionalData.time_period)).filter(
        RegionalData.table_name == table_name.upper(),
        RegionalData.line_code == line_code,
        RegionalData.geo_fips.in_(geo_fips_list)
    ).scalar()

    if not latest_year:
        return {"data": [], "table_name": table_name, "line_code": line_code, "year": None}

    # Get data for all geographies for the latest year
    data_points = db.query(RegionalData).filter(
        RegionalData.table_name == table_name.upper(),
        RegionalData.line_code == line_code,
        RegionalData.geo_fips.in_(geo_fips_list),
        RegionalData.time_period == latest_year
    ).all()

    # Get line code info for description
    line_info = db.query(RegionalLineCode).filter(
        RegionalLineCode.table_name == table_name.upper(),
        RegionalLineCode.line_code == line_code
    ).first()

    # Get unit info from actual data (line_codes table often has NULL)
    unit_mult = None
    cl_unit = None

    result = []
    for d in data_points:
        if d.value is not None:
            result.append({
                "geo_fips": d.geo_fips,
                "geo_name": geo_names.get(d.geo_fips, d.geo_fips),
                "value": float(d.value),
            })
            # Get unit info from first data point
            if unit_mult is None and d.unit_mult is not None:
                unit_mult = d.unit_mult
            if cl_unit is None and d.cl_unit is not None:
                cl_unit = d.cl_unit

    # Sort by value descending
    result.sort(key=lambda x: x["value"], reverse=True)

    return {
        "data": result,
        "table_name": table_name.upper(),
        "line_code": line_code,
        "line_description": line_info.line_description if line_info else None,
        "unit": cl_unit or (line_info.cl_unit if line_info else None),
        "unit_mult": unit_mult if unit_mult is not None else (line_info.unit_mult if line_info else None),
        "year": latest_year,
    }


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


# ===================== GDP by Industry Explorer ===================== #

from src.admin.schemas.bea import (
    GDPByIndustryTableResponse,
    GDPByIndustryIndustryResponse,
    GDPByIndustryTimeSeriesResponse,
)
from src.database.bea_models import (
    GDPByIndustryTable, GDPByIndustryIndustry, GDPByIndustryData,
)


@router.get("/gdpbyindustry/tables", response_model=List[GDPByIndustryTableResponse])
async def get_gdpbyindustry_tables(
    active_only: bool = True,
    db: Session = Depends(get_db)
):
    """
    Get list of GDP by Industry tables
    """
    query = db.query(GDPByIndustryTable)

    if active_only:
        query = query.filter(GDPByIndustryTable.is_active == True)

    tables = query.order_by(GDPByIndustryTable.table_id).all()

    return [
        GDPByIndustryTableResponse(
            table_id=t.table_id,
            table_description=t.table_description,
            has_annual=t.has_annual or False,
            has_quarterly=t.has_quarterly or False,
            first_annual_year=t.first_annual_year,
            last_annual_year=t.last_annual_year,
            first_quarterly_year=t.first_quarterly_year,
            last_quarterly_year=t.last_quarterly_year,
            is_active=t.is_active or False,
        )
        for t in tables
    ]


@router.get("/gdpbyindustry/industries", response_model=List[GDPByIndustryIndustryResponse])
async def get_gdpbyindustry_industries(
    active_only: bool = True,
    level: Optional[int] = None,
    db: Session = Depends(get_db)
):
    """
    Get list of industries for GDP by Industry data

    Args:
        active_only: Only return active industries
        level: Filter by industry level (1=sector, 2=subsector, etc.)
    """
    query = db.query(GDPByIndustryIndustry)

    if active_only:
        query = query.filter(GDPByIndustryIndustry.is_active == True)

    if level is not None:
        query = query.filter(GDPByIndustryIndustry.industry_level == level)

    industries = query.order_by(GDPByIndustryIndustry.industry_code).all()

    # Get industry descriptions from data table (industries table may have empty descriptions)
    # Use row_type='total' to get the main description, not component descriptions
    industry_codes = [i.industry_code for i in industries]
    descriptions_query = db.query(
        GDPByIndustryData.industry_code,
        GDPByIndustryData.industry_description
    ).filter(
        GDPByIndustryData.industry_code.in_(industry_codes),
        GDPByIndustryData.row_type == 'total',
        GDPByIndustryData.industry_description.isnot(None)
    ).distinct().all()

    # Build description map - prefer non-generic descriptions
    generic_descriptions = {'Value added', 'Compensation of employees', 'Gross operating surplus',
                           'Taxes on production and imports less subsidies', 'Energy inputs',
                           'Intermediate inputs', 'Materials inputs', 'Purchased-services inputs'}
    desc_map = {}
    for code, desc in descriptions_query:
        if desc:
            is_generic = desc in generic_descriptions
            if code not in desc_map:
                desc_map[code] = {'desc': desc, 'is_generic': is_generic}
            elif desc_map[code]['is_generic'] and not is_generic:
                desc_map[code] = {'desc': desc, 'is_generic': is_generic}

    desc_map = {k: v['desc'] for k, v in desc_map.items()}

    return [
        GDPByIndustryIndustryResponse(
            industry_code=i.industry_code,
            industry_description=desc_map.get(i.industry_code) or i.industry_description or i.industry_code,
            parent_code=i.parent_code,
            industry_level=i.industry_level,
        )
        for i in industries
    ]


@router.get("/gdpbyindustry/data", response_model=GDPByIndustryTimeSeriesResponse)
async def get_gdpbyindustry_data(
    table_id: int,
    industry_code: str,
    frequency: str = "A",
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
    db: Session = Depends(get_db)
):
    """
    Get time series data for a GDP by Industry table/industry combination

    Args:
        table_id: Table ID (1-39)
        industry_code: Industry code
        frequency: 'A' for Annual, 'Q' for Quarterly
        start_year: Optional filter - start year
        end_year: Optional filter - end year
    """
    # Get table info
    table_info = db.query(GDPByIndustryTable).filter(
        GDPByIndustryTable.table_id == table_id
    ).first()

    if not table_info:
        raise HTTPException(status_code=404, detail=f"Table {table_id} not found")

    # Get industry info
    industry_info = db.query(GDPByIndustryIndustry).filter(
        GDPByIndustryIndustry.industry_code == industry_code
    ).first()

    if not industry_info:
        raise HTTPException(status_code=404, detail=f"Industry {industry_code} not found")

    # Get data
    query = db.query(GDPByIndustryData).filter(
        GDPByIndustryData.table_id == table_id,
        GDPByIndustryData.industry_code == industry_code,
        GDPByIndustryData.frequency == frequency.upper()
    )

    if start_year:
        query = query.filter(GDPByIndustryData.time_period >= str(start_year))
    if end_year:
        query = query.filter(GDPByIndustryData.time_period <= str(end_year + 1))

    data_points = query.order_by(GDPByIndustryData.time_period).all()

    # Get unit info from actual data
    unit_mult = None
    cl_unit = None
    for d in data_points:
        if unit_mult is None and d.unit_mult is not None:
            unit_mult = d.unit_mult
        if cl_unit is None and d.cl_unit is not None:
            cl_unit = d.cl_unit
        if unit_mult is not None and cl_unit is not None:
            break

    return GDPByIndustryTimeSeriesResponse(
        table_id=table_id,
        table_description=table_info.table_description,
        industry_code=industry_code,
        industry_description=industry_info.industry_description or industry_code,
        frequency=frequency.upper(),
        unit=cl_unit,
        unit_mult=unit_mult,
        data=[
            {
                "time_period": d.time_period,
                "value": float(d.value) if d.value else None,
                "row_type": d.row_type,
                "note_ref": d.note_ref,
            }
            for d in data_points
        ]
    )


@router.get("/gdpbyindustry/snapshot")
async def get_gdpbyindustry_snapshot(
    table_id: int = 1,
    frequency: str = "A",
    db: Session = Depends(get_db)
):
    """
    Get snapshot of latest data for all industries (for treemap/charts)

    Args:
        table_id: Table ID (default: 1 - Value Added by Industry)
        frequency: 'A' for Annual, 'Q' for Quarterly
    """
    # Get the latest period available
    latest_period = db.query(func.max(GDPByIndustryData.time_period)).filter(
        GDPByIndustryData.table_id == table_id,
        GDPByIndustryData.frequency == frequency.upper()
    ).scalar()

    if not latest_period:
        return {"data": [], "table_id": table_id, "period": None}

    # Get data for all industries for the latest period
    data_points = db.query(GDPByIndustryData).filter(
        GDPByIndustryData.table_id == table_id,
        GDPByIndustryData.frequency == frequency.upper(),
        GDPByIndustryData.time_period == latest_period,
        GDPByIndustryData.row_type == 'total'
    ).all()

    # Get industry info
    industry_codes = [d.industry_code for d in data_points]
    industries = db.query(GDPByIndustryIndustry).filter(
        GDPByIndustryIndustry.industry_code.in_(industry_codes)
    ).all()
    industry_map = {i.industry_code: i for i in industries}

    # Get industry descriptions from data table (industries table may have empty descriptions)
    # Look for the actual industry name, not generic row descriptions
    generic_descriptions = {'Value added', 'Compensation of employees', 'Gross operating surplus',
                           'Taxes on production and imports less subsidies', 'Energy inputs',
                           'Intermediate inputs', 'Materials inputs', 'Purchased-services inputs'}

    descriptions_query = db.query(
        GDPByIndustryData.industry_code,
        GDPByIndustryData.industry_description
    ).filter(
        GDPByIndustryData.industry_code.in_(industry_codes),
        GDPByIndustryData.row_type == 'total',
        GDPByIndustryData.industry_description.isnot(None)
    ).distinct().all()

    # Build description map - prefer non-generic descriptions
    desc_map = {}
    for code, desc in descriptions_query:
        if desc:
            is_generic = desc in generic_descriptions
            # Only add if: no entry yet, OR current is generic and new one isn't
            if code not in desc_map:
                desc_map[code] = {'desc': desc, 'is_generic': is_generic}
            elif desc_map[code]['is_generic'] and not is_generic:
                # Replace generic with non-generic
                desc_map[code] = {'desc': desc, 'is_generic': is_generic}

    # Extract just the descriptions (prefer non-generic, but keep generic as fallback)
    desc_map = {k: v['desc'] for k, v in desc_map.items()}

    # Get unit info from first data point
    unit_mult = None
    cl_unit = None
    for d in data_points:
        if unit_mult is None and d.unit_mult is not None:
            unit_mult = d.unit_mult
        if cl_unit is None and d.cl_unit is not None:
            cl_unit = d.cl_unit
        if unit_mult is not None and cl_unit is not None:
            break

    # Get table info
    table_info = db.query(GDPByIndustryTable).filter(
        GDPByIndustryTable.table_id == table_id
    ).first()

    result = []
    for d in data_points:
        if d.value is not None:
            industry = industry_map.get(d.industry_code)
            # Use description from data table, fallback to industry table, then code
            description = desc_map.get(d.industry_code) or (industry.industry_description if industry and industry.industry_description else d.industry_code)
            result.append({
                "industry_code": d.industry_code,
                "industry_description": description,
                "value": float(d.value),
                "parent_code": industry.parent_code if industry else None,
                "industry_level": industry.industry_level if industry else None,
            })

    # Sort by value descending
    result.sort(key=lambda x: x["value"], reverse=True)

    return {
        "data": result,
        "table_id": table_id,
        "table_description": table_info.table_description if table_info else None,
        "frequency": frequency.upper(),
        "period": latest_period,
        "unit": cl_unit,
        "unit_mult": unit_mult,
    }


# ===================== ITA (International Trade) Explorer ===================== #

@router.get("/ita/indicators", response_model=List[ITAIndicatorResponse])
async def get_ita_indicators(
    active_only: bool = True,
    search: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """
    Get list of ITA indicators (transaction types)

    Args:
        active_only: Only return active indicators
        search: Optional search term to filter indicators
    """
    query = db.query(ITAIndicator)

    if active_only:
        query = query.filter(ITAIndicator.is_active == True)

    if search:
        query = query.filter(
            ITAIndicator.indicator_description.ilike(f'%{search}%') |
            ITAIndicator.indicator_code.ilike(f'%{search}%')
        )

    indicators = query.order_by(ITAIndicator.indicator_code).all()

    return [
        ITAIndicatorResponse(
            indicator_code=i.indicator_code,
            indicator_description=i.indicator_description or i.indicator_code,
            is_active=i.is_active,
        )
        for i in indicators
    ]


@router.get("/ita/areas", response_model=List[ITAAreaResponse])
async def get_ita_areas(
    active_only: bool = True,
    area_type: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """
    Get list of ITA areas/countries

    Args:
        active_only: Only return active areas
        area_type: Filter by area type ('Country', 'Region', 'Aggregate')
    """
    query = db.query(ITAArea)

    if active_only:
        query = query.filter(ITAArea.is_active == True)

    if area_type:
        query = query.filter(ITAArea.area_type == area_type)

    areas = query.order_by(ITAArea.area_name).all()

    return [
        ITAAreaResponse(
            area_code=a.area_code,
            area_name=a.area_name,
            area_type=a.area_type,
            is_active=a.is_active,
        )
        for a in areas
    ]


@router.get("/ita/data", response_model=ITATimeSeriesResponse)
async def get_ita_data(
    indicator_code: str,
    area_code: str = "AllCountries",
    frequency: str = "A",
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
    db: Session = Depends(get_db)
):
    """
    Get ITA time series data for a specific indicator and area

    Args:
        indicator_code: The indicator code (e.g., 'BalGds', 'ExpGds')
        area_code: The area/country code (default: AllCountries)
        frequency: 'A' for Annual, 'QSA' for Quarterly SA, 'QNSA' for Quarterly NSA
        start_year: Optional start year filter
        end_year: Optional end year filter
    """
    # Get indicator info
    indicator = db.query(ITAIndicator).filter(
        ITAIndicator.indicator_code == indicator_code
    ).first()

    if not indicator:
        raise HTTPException(status_code=404, detail=f"Indicator '{indicator_code}' not found")

    # Get area info
    area = db.query(ITAArea).filter(ITAArea.area_code == area_code).first()
    if not area:
        raise HTTPException(status_code=404, detail=f"Area '{area_code}' not found")

    # Build query
    query = db.query(ITAData).filter(
        ITAData.indicator_code == indicator_code,
        ITAData.area_code == area_code,
        ITAData.frequency == frequency.upper()
    )

    if start_year:
        query = query.filter(ITAData.time_period >= str(start_year))
    if end_year:
        query = query.filter(ITAData.time_period <= f"{end_year}Q4" if frequency != "A" else str(end_year))

    data_points = query.order_by(ITAData.time_period).all()

    if not data_points:
        raise HTTPException(
            status_code=404,
            detail=f"No data found for indicator '{indicator_code}' and area '{area_code}' with frequency '{frequency}'"
        )

    # Get unit info from first data point
    first_point = data_points[0]

    return ITATimeSeriesResponse(
        indicator_code=indicator_code,
        indicator_description=indicator.indicator_description or indicator_code,
        area_code=area_code,
        area_name=area.area_name,
        frequency=frequency.upper(),
        unit=first_point.cl_unit,
        unit_mult=first_point.unit_mult,
        data=[
            {
                "time_period": d.time_period,
                "value": float(d.value) if d.value is not None else None,
                "time_series_description": d.time_series_description,
                "note_ref": d.note_ref,
            }
            for d in data_points
        ]
    )


@router.get("/ita/snapshot")
async def get_ita_snapshot(
    indicator_code: str = "BalGds",
    frequency: str = "A",
    db: Session = Depends(get_db)
):
    """
    Get snapshot of latest data for an indicator across top trading partners

    Args:
        indicator_code: The indicator code (default: BalGds - Balance on Goods)
        frequency: 'A' for Annual, 'QSA' for Quarterly SA
    """
    # Get indicator info
    indicator = db.query(ITAIndicator).filter(
        ITAIndicator.indicator_code == indicator_code
    ).first()

    if not indicator:
        raise HTTPException(status_code=404, detail=f"Indicator '{indicator_code}' not found")

    # Get the latest period available
    latest_period = db.query(func.max(ITAData.time_period)).filter(
        ITAData.indicator_code == indicator_code,
        ITAData.frequency == frequency.upper()
    ).scalar()

    if not latest_period:
        return {"data": [], "indicator_code": indicator_code, "period": None}

    # Get data for all countries for the latest period (excluding AllCountries aggregate)
    data_points = db.query(ITAData).filter(
        ITAData.indicator_code == indicator_code,
        ITAData.frequency == frequency.upper(),
        ITAData.time_period == latest_period,
        ITAData.area_code != 'AllCountries'
    ).all()

    # Get area info for mapping
    area_codes = [d.area_code for d in data_points]
    areas = db.query(ITAArea).filter(ITAArea.area_code.in_(area_codes)).all()
    area_map = {a.area_code: a for a in areas}

    # Get unit info
    unit_mult = None
    cl_unit = None
    for d in data_points:
        if unit_mult is None and d.unit_mult is not None:
            unit_mult = d.unit_mult
        if cl_unit is None and d.cl_unit is not None:
            cl_unit = d.cl_unit
        if unit_mult is not None and cl_unit is not None:
            break

    result = []
    for d in data_points:
        if d.value is not None:
            area = area_map.get(d.area_code)
            result.append({
                "area_code": d.area_code,
                "area_name": area.area_name if area else d.area_code,
                "area_type": area.area_type if area else None,
                "value": float(d.value),
            })

    # Sort by absolute value descending (for trade balances, show largest deficits/surpluses)
    result.sort(key=lambda x: abs(x["value"]), reverse=True)

    return {
        "data": result,
        "indicator_code": indicator_code,
        "indicator_description": indicator.indicator_description,
        "frequency": frequency.upper(),
        "period": latest_period,
        "unit": cl_unit,
        "unit_mult": unit_mult,
    }


@router.get("/ita/headline")
async def get_ita_headline(
    frequency: str = "A",
    db: Session = Depends(get_db)
):
    """
    Get headline ITA metrics (key balance indicators) for the latest period

    Returns latest values for: Balance on Goods, Balance on Services,
    Balance on Goods and Services, Balance on Current Account
    """
    headline_indicators = ['BalGds', 'BalServ', 'BalGdsServ', 'BalCurrAcct', 'BalPrimInc', 'BalSecInc']

    results = []
    for ind_code in headline_indicators:
        # Get indicator info
        indicator = db.query(ITAIndicator).filter(
            ITAIndicator.indicator_code == ind_code
        ).first()

        if not indicator:
            continue

        # Get latest data for AllCountries
        latest_data = db.query(ITAData).filter(
            ITAData.indicator_code == ind_code,
            ITAData.area_code == 'AllCountries',
            ITAData.frequency == frequency.upper()
        ).order_by(desc(ITAData.time_period)).first()

        if latest_data:
            results.append({
                "indicator_code": ind_code,
                "indicator_description": indicator.indicator_description,
                "value": float(latest_data.value) if latest_data.value is not None else None,
                "time_period": latest_data.time_period,
                "unit": latest_data.cl_unit,
                "unit_mult": latest_data.unit_mult,
            })

    return {"data": results, "frequency": frequency.upper()}


# ===================== Fixed Assets Explorer ===================== #

@router.get("/fixedassets/tables", response_model=List[FixedAssetsTableResponse])
async def get_fixedassets_tables(
    active_only: bool = True,
    db: Session = Depends(get_db)
):
    """
    Get list of Fixed Assets tables

    Args:
        active_only: Only return active tables
    """
    query = db.query(FixedAssetsTable)

    if active_only:
        query = query.filter(FixedAssetsTable.is_active == True)

    tables = query.order_by(FixedAssetsTable.table_name).all()

    # Get series counts per table
    series_counts = dict(
        db.query(
            FixedAssetsSeries.table_name,
            func.count(FixedAssetsSeries.series_code)
        ).group_by(FixedAssetsSeries.table_name).all()
    )

    return [
        FixedAssetsTableResponse(
            table_name=t.table_name,
            table_description=t.table_description,
            first_year=t.first_year,
            last_year=t.last_year,
            series_count=series_counts.get(t.table_name, 0),
            is_active=t.is_active or False,
        )
        for t in tables
    ]


@router.get("/fixedassets/tables/{table_name}", response_model=FixedAssetsTableResponse)
async def get_fixedassets_table(
    table_name: str,
    db: Session = Depends(get_db)
):
    """
    Get details of a specific Fixed Assets table

    Args:
        table_name: Fixed Assets table name (e.g., FAAt201)
    """
    table = db.query(FixedAssetsTable).filter(
        FixedAssetsTable.table_name == table_name
    ).first()

    if not table:
        raise HTTPException(status_code=404, detail=f"Table {table_name} not found")

    series_count = db.query(func.count(FixedAssetsSeries.series_code)).filter(
        FixedAssetsSeries.table_name == table_name
    ).scalar() or 0

    return FixedAssetsTableResponse(
        table_name=table.table_name,
        table_description=table.table_description,
        first_year=table.first_year,
        last_year=table.last_year,
        series_count=series_count,
        is_active=table.is_active or False,
    )


@router.get("/fixedassets/tables/{table_name}/series", response_model=List[FixedAssetsSeriesResponse])
async def get_fixedassets_table_series(
    table_name: str,
    db: Session = Depends(get_db)
):
    """
    Get all series for a Fixed Assets table

    Args:
        table_name: Fixed Assets table name (e.g., FAAt201)
    """
    series_list = db.query(FixedAssetsSeries).filter(
        FixedAssetsSeries.table_name == table_name
    ).order_by(FixedAssetsSeries.line_number).all()

    if not series_list:
        raise HTTPException(status_code=404, detail=f"No series found for table {table_name}")

    # Get data point counts
    data_counts = dict(
        db.query(
            FixedAssetsData.series_code,
            func.count(FixedAssetsData.time_period)
        ).filter(
            FixedAssetsData.series_code.in_([s.series_code for s in series_list])
        ).group_by(FixedAssetsData.series_code).all()
    )

    return [
        FixedAssetsSeriesResponse(
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


@router.get("/fixedassets/series/{series_code}/data", response_model=FixedAssetsTimeSeriesResponse)
async def get_fixedassets_series_data(
    series_code: str,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
    db: Session = Depends(get_db)
):
    """
    Get time series data for a specific Fixed Assets series

    Args:
        series_code: The series code
        start_year: Optional start year filter
        end_year: Optional end year filter
    """
    series = db.query(FixedAssetsSeries).filter(
        FixedAssetsSeries.series_code == series_code
    ).first()

    if not series:
        raise HTTPException(status_code=404, detail=f"Series {series_code} not found")

    # Build data query
    query = db.query(FixedAssetsData).filter(
        FixedAssetsData.series_code == series_code
    )

    if start_year:
        query = query.filter(FixedAssetsData.time_period >= str(start_year))
    if end_year:
        query = query.filter(FixedAssetsData.time_period <= str(end_year))

    data_points = query.order_by(FixedAssetsData.time_period).all()

    return FixedAssetsTimeSeriesResponse(
        series_code=series.series_code,
        line_description=series.line_description,
        metric_name=series.metric_name,
        unit=series.cl_unit,
        unit_mult=series.unit_mult,
        data=[
            {
                "time_period": d.time_period,
                "value": float(d.value) if d.value is not None else None,
                "note_ref": d.note_ref,
            }
            for d in data_points
        ]
    )


@router.get("/fixedassets/snapshot")
async def get_fixedassets_snapshot(
    table_name: str = "FAAt101",
    db: Session = Depends(get_db)
):
    """
    Get snapshot of latest Fixed Assets data for a table's key series

    Args:
        table_name: The table name (default: FAAt101 - Current-Cost Net Stock of Private Fixed Assets)
    """
    # Get table info
    table = db.query(FixedAssetsTable).filter(
        FixedAssetsTable.table_name == table_name
    ).first()

    if not table:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

    # Get all series for this table
    series_list = db.query(FixedAssetsSeries).filter(
        FixedAssetsSeries.table_name == table_name
    ).order_by(FixedAssetsSeries.line_number).all()

    if not series_list:
        return {"data": [], "table_name": table_name, "period": None}

    # Get the latest period available
    latest_period = db.query(func.max(FixedAssetsData.time_period)).filter(
        FixedAssetsData.series_code.in_([s.series_code for s in series_list])
    ).scalar()

    if not latest_period:
        return {"data": [], "table_name": table_name, "period": None}

    # Get data for all series for the latest period
    data_points = db.query(FixedAssetsData).filter(
        FixedAssetsData.series_code.in_([s.series_code for s in series_list]),
        FixedAssetsData.time_period == latest_period
    ).all()

    data_map = {d.series_code: d for d in data_points}

    # Get unit info from first series
    unit = series_list[0].cl_unit if series_list else None
    unit_mult = series_list[0].unit_mult if series_list else None

    result = []
    for s in series_list:
        data = data_map.get(s.series_code)
        if data and data.value is not None:
            result.append({
                "series_code": s.series_code,
                "line_number": s.line_number,
                "line_description": s.line_description,
                "value": float(data.value),
            })

    return {
        "data": result,
        "table_name": table_name,
        "table_description": table.table_description,
        "period": latest_period,
        "unit": unit,
        "unit_mult": unit_mult,
    }


@router.get("/fixedassets/headline")
async def get_fixedassets_headline(
    db: Session = Depends(get_db)
):
    """
    Get headline Fixed Assets metrics

    Returns key metrics from major Fixed Assets categories:
    - Total Private Fixed Assets
    - Nonresidential Equipment
    - Nonresidential Structures
    - Residential Fixed Assets
    - Intellectual Property Products
    - Government Fixed Assets
    """
    # Key series codes for headline metrics (from FAAt101 - Current-Cost Net Stock)
    headline_series = [
        {'table': 'FAAt101', 'line': 1, 'name': 'Private Fixed Assets', 'description': 'Total private fixed assets'},
        {'table': 'FAAt101', 'line': 3, 'name': 'Equipment', 'description': 'Nonresidential equipment'},
        {'table': 'FAAt101', 'line': 4, 'name': 'Structures', 'description': 'Nonresidential structures'},
        {'table': 'FAAt101', 'line': 8, 'name': 'Residential', 'description': 'Residential fixed assets'},
        {'table': 'FAAt101', 'line': 5, 'name': 'IP Products', 'description': 'Intellectual property products'},
        {'table': 'FAAt201', 'line': 1, 'name': 'Government Assets', 'description': 'Government fixed assets'},
    ]

    results = []
    for item in headline_series:
        # Find the series
        series = db.query(FixedAssetsSeries).filter(
            FixedAssetsSeries.table_name == item['table'],
            FixedAssetsSeries.line_number == item['line']
        ).first()

        if not series:
            continue

        # Get latest data
        latest_data = db.query(FixedAssetsData).filter(
            FixedAssetsData.series_code == series.series_code
        ).order_by(desc(FixedAssetsData.time_period)).first()

        if latest_data:
            results.append({
                "series_code": series.series_code,
                "name": item['name'],
                "description": item['description'],
                "line_description": series.line_description,
                "value": float(latest_data.value) if latest_data.value is not None else None,
                "time_period": latest_data.time_period,
                "unit": series.cl_unit,
                "unit_mult": series.unit_mult,
            })

    return {"data": results}
