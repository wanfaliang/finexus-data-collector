"""
BEA Action Endpoints

API endpoints for triggering BEA data collection tasks.

Author: FinExus Data Collector
Created: 2025-11-27
"""
from typing import Optional, List
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from src.bea.task_runner import task_runner

router = APIRouter()


# ==================== Table Group Definitions ==================== #

# NIPA table sections (based on table number prefix)
NIPA_TABLE_GROUPS = {
    'priority': ['T10101', 'T10105', 'T10106', 'T10107', 'T20100', 'T20301'],  # Key GDP & Income tables
    'gdp': None,  # T1xxxx - will be filtered dynamically
    'income': None,  # T2xxxx
    'govt': None,  # T3xxxx
    'trade': None,  # T4xxxx
    'investment': None,  # T5xxxx
    'industry': None,  # T6xxxx
    'supplemental': None,  # T7xxxx
    'all': None,
}

# Regional table categories
# Note: SAINC5/6/7 don't exist - use SAINC5N (NAICS), SAINC6N, SAINC7N instead
# Similarly for CAINC5 - use CAINC5N
REGIONAL_TABLE_GROUPS = {
    'priority': ['SAGDP1', 'SAINC1', 'CAINC1'],  # Key state GDP & income
    'state_gdp': ['SAGDP1', 'SAGDP2', 'SAGDP2N', 'SAGDP9', 'SAGDP9N'],  # State GDP tables
    'state_income': ['SAINC1', 'SAINC4', 'SAINC5N', 'SAINC6N', 'SAINC7N'],  # State income tables (NAICS)
    'county': ['CAINC1', 'CAINC4', 'CAINC5N', 'CAINC30', 'CAGDP1', 'CAGDP2'],  # County tables
    'quarterly': ['SQGDP1', 'SQGDP2', 'SQGDP9'],  # Quarterly state tables
    'all': None,
}

# GDP by Industry table categories
GDPBYINDUSTRY_TABLE_GROUPS = {
    'priority': [1, 10, 13],  # Value Added, Real Value Added, Contributions
    'value_added': [1, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],  # Tables 1-14
    'gross_output': [15, 16, 17, 18, 19, 208],  # Tables 15-19, 208
    'inputs': [20, 21, 22, 23, 24, 25, 26, 209],  # Tables 20-26, 209
    'all': None,
}


# ==================== Request/Response Models ==================== #

class NIPABackfillRequest(BaseModel):
    """Request to start NIPA backfill"""
    frequency: str = Field("A", description="Data frequency: A (Annual), Q (Quarterly), M (Monthly)")
    year: str = Field("ALL", description="Year specification: ALL, LAST5, LAST10, or comma-separated years")
    tables: Optional[List[str]] = Field(None, description="Specific tables to backfill (optional)")


class RegionalBackfillRequest(BaseModel):
    """Request to start Regional backfill"""
    geo: str = Field("STATE", description="Geographic level: STATE, COUNTY, MSA")
    year: str = Field("ALL", description="Year specification: ALL, LAST5, LAST10, or comma-separated years")
    tables: Optional[List[str]] = Field(None, description="Specific tables to backfill (optional)")


class GDPByIndustryBackfillRequest(BaseModel):
    """Request to start GDP by Industry backfill"""
    frequency: str = Field("A", description="Data frequency: A (Annual), Q (Quarterly)")
    year: str = Field("ALL", description="Year specification: ALL, LAST5, LAST10, or comma-separated years")
    tables: Optional[List[int]] = Field(None, description="Specific table IDs to backfill (optional)")


class NIPAUpdateRequest(BaseModel):
    """Request to start NIPA incremental update"""
    section: str = Field("priority", description="Table section: priority, gdp, income, govt, trade, investment, industry, supplemental, all")
    frequency: str = Field("A", description="Data frequency: A, Q, M")
    year: str = Field("LAST5", description="Year specification")


class RegionalUpdateRequest(BaseModel):
    """Request to start Regional incremental update"""
    category: str = Field("priority", description="Category: priority, state_gdp, state_income, county, quarterly, all")
    year: str = Field("LAST5", description="Year specification")


class GDPByIndustryUpdateRequest(BaseModel):
    """Request to start GDP by Industry incremental update"""
    category: str = Field("priority", description="Category: priority, value_added, gross_output, inputs, all")
    frequency: str = Field("A", description="Data frequency: A, Q")
    year: str = Field("LAST5", description="Year specification")


class UpdateRequest(BaseModel):
    """Request to start incremental update (legacy - updates priority tables only)"""
    dataset: str = Field("all", description="Dataset to update: NIPA, Regional, GDPbyIndustry, or all")
    year: str = Field("LAST5", description="Year specification for update")
    force: bool = Field(False, description="Force update even if recently updated")


class TaskResponse(BaseModel):
    """Response for task start requests"""
    success: bool
    message: str
    run_id: Optional[int] = None


class TaskStatusResponse(BaseModel):
    """Response for task status check"""
    nipa_running: bool
    regional_running: bool
    gdpbyindustry_running: bool


# ==================== Endpoints ==================== #

@router.get("/actions/status", response_model=TaskStatusResponse)
async def get_task_status():
    """
    Get current status of running tasks.

    Returns which datasets have active background tasks.
    """
    status = task_runner.get_running_tasks()
    return TaskStatusResponse(
        nipa_running=status["NIPA"],
        regional_running=status["Regional"],
        gdpbyindustry_running=status["GDPbyIndustry"],
    )


@router.post("/actions/backfill/nipa", response_model=TaskResponse)
async def start_nipa_backfill(request: NIPABackfillRequest):
    """
    Start NIPA data backfill in background.

    - **frequency**: A (Annual), Q (Quarterly), M (Monthly)
    - **year**: ALL, LAST5, LAST10, or comma-separated years
    - **tables**: Optional list of specific tables to backfill

    Returns immediately with run_id. Check /actions/status or recent runs for progress.
    """
    # Validate frequency
    if request.frequency not in ("A", "Q", "M"):
        raise HTTPException(status_code=400, detail="frequency must be A, Q, or M")

    run_id = task_runner.start_nipa_backfill(
        frequency=request.frequency,
        year=request.year,
        tables=request.tables,
    )

    if run_id is None:
        return TaskResponse(
            success=False,
            message="NIPA backfill already running. Please wait for it to complete.",
        )

    return TaskResponse(
        success=True,
        message=f"NIPA backfill started (frequency={request.frequency}, year={request.year})",
        run_id=run_id,
    )


@router.post("/actions/backfill/regional", response_model=TaskResponse)
async def start_regional_backfill(request: RegionalBackfillRequest):
    """
    Start Regional data backfill in background.

    - **geo**: STATE, COUNTY, or MSA
    - **year**: ALL, LAST5, LAST10, or comma-separated years
    - **tables**: Optional list of specific tables to backfill

    Returns immediately with run_id. Check /actions/status or recent runs for progress.
    """
    # Validate geo
    if request.geo not in ("STATE", "COUNTY", "MSA"):
        raise HTTPException(status_code=400, detail="geo must be STATE, COUNTY, or MSA")

    run_id = task_runner.start_regional_backfill(
        geo_fips=request.geo,
        year=request.year,
        tables=request.tables,
    )

    if run_id is None:
        return TaskResponse(
            success=False,
            message="Regional backfill already running. Please wait for it to complete.",
        )

    return TaskResponse(
        success=True,
        message=f"Regional backfill started (geo={request.geo}, year={request.year})",
        run_id=run_id,
    )


@router.post("/actions/backfill/gdpbyindustry", response_model=TaskResponse)
async def start_gdpbyindustry_backfill(request: GDPByIndustryBackfillRequest):
    """
    Start GDP by Industry data backfill in background.

    - **frequency**: A (Annual) or Q (Quarterly)
    - **year**: ALL, LAST5, LAST10, or comma-separated years
    - **tables**: Optional list of specific table IDs to backfill (e.g., [1, 5, 6])

    Returns immediately with run_id. Check /actions/status or recent runs for progress.
    """
    # Validate frequency
    if request.frequency not in ("A", "Q"):
        raise HTTPException(status_code=400, detail="frequency must be A or Q")

    run_id = task_runner.start_gdpbyindustry_backfill(
        frequency=request.frequency,
        year=request.year,
        tables=request.tables,
    )

    if run_id is None:
        return TaskResponse(
            success=False,
            message="GDP by Industry backfill already running. Please wait for it to complete.",
        )

    return TaskResponse(
        success=True,
        message=f"GDP by Industry backfill started (frequency={request.frequency}, year={request.year})",
        run_id=run_id,
    )


@router.post("/actions/update", response_model=TaskResponse)
async def start_update(request: UpdateRequest):
    """
    Start incremental data update in background.

    - **dataset**: NIPA, Regional, GDPbyIndustry, or all
    - **year**: Year specification (default: LAST5)
    - **force**: Force update even if recently updated

    Returns immediately with run_id. Check /actions/status or recent runs for progress.
    """
    # Validate dataset
    if request.dataset not in ("NIPA", "Regional", "GDPbyIndustry", "all"):
        raise HTTPException(status_code=400, detail="dataset must be NIPA, Regional, GDPbyIndustry, or all")

    run_id = task_runner.start_update(
        dataset=request.dataset,
        year=request.year,
        force=request.force,
    )

    if run_id is None:
        return TaskResponse(
            success=False,
            message="Update task already running. Please wait for it to complete.",
        )

    return TaskResponse(
        success=True,
        message=f"Update started for {request.dataset} (year={request.year})",
        run_id=run_id,
    )


# ==================== Granular Update Endpoints ==================== #

@router.post("/actions/update/nipa", response_model=TaskResponse)
async def start_nipa_update(request: NIPAUpdateRequest):
    """
    Start NIPA incremental update for specific table section.

    - **section**: priority, gdp, income, govt, trade, investment, industry, supplemental, all
    - **frequency**: A, Q, M
    - **year**: Year specification (default: LAST5)
    """
    valid_sections = list(NIPA_TABLE_GROUPS.keys())
    if request.section not in valid_sections:
        raise HTTPException(status_code=400, detail=f"section must be one of: {valid_sections}")
    if request.frequency not in ("A", "Q", "M"):
        raise HTTPException(status_code=400, detail="frequency must be A, Q, or M")

    tables = NIPA_TABLE_GROUPS.get(request.section)

    # Use start_nipa_update (creates run_type='update') instead of start_nipa_backfill
    run_id = task_runner.start_nipa_update(
        frequency=request.frequency,
        year=request.year,
        tables=tables,
    )

    if run_id is None:
        return TaskResponse(
            success=False,
            message="NIPA task already running. Please wait for it to complete.",
        )

    return TaskResponse(
        success=True,
        message=f"NIPA update started (section={request.section}, freq={request.frequency})",
        run_id=run_id,
    )


@router.post("/actions/update/regional", response_model=TaskResponse)
async def start_regional_update(request: RegionalUpdateRequest):
    """
    Start Regional incremental update for specific category.

    - **category**: priority, state_gdp, state_income, county, quarterly, all
    - **year**: Year specification (default: LAST5)
    """
    valid_categories = list(REGIONAL_TABLE_GROUPS.keys())
    if request.category not in valid_categories:
        raise HTTPException(status_code=400, detail=f"category must be one of: {valid_categories}")

    tables = REGIONAL_TABLE_GROUPS.get(request.category)

    # Determine geo_fips based on category
    if request.category == 'county':
        geo_fips = 'COUNTY'
    elif request.category == 'quarterly':
        geo_fips = 'STATE'
    else:
        geo_fips = 'STATE'

    # Use start_regional_update (creates run_type='update') instead of start_regional_backfill
    run_id = task_runner.start_regional_update(
        geo_fips=geo_fips,
        year=request.year,
        tables=tables,
    )

    if run_id is None:
        return TaskResponse(
            success=False,
            message="Regional task already running. Please wait for it to complete.",
        )

    return TaskResponse(
        success=True,
        message=f"Regional update started (category={request.category})",
        run_id=run_id,
    )


@router.post("/actions/update/gdpbyindustry", response_model=TaskResponse)
async def start_gdpbyindustry_update(request: GDPByIndustryUpdateRequest):
    """
    Start GDP by Industry incremental update for specific category.

    - **category**: priority, value_added, gross_output, inputs, all
    - **frequency**: A, Q
    - **year**: Year specification (default: LAST5)
    """
    valid_categories = list(GDPBYINDUSTRY_TABLE_GROUPS.keys())
    if request.category not in valid_categories:
        raise HTTPException(status_code=400, detail=f"category must be one of: {valid_categories}")
    if request.frequency not in ("A", "Q"):
        raise HTTPException(status_code=400, detail="frequency must be A or Q")

    tables = GDPBYINDUSTRY_TABLE_GROUPS.get(request.category)

    # Use start_gdpbyindustry_update (creates run_type='update') instead of start_gdpbyindustry_backfill
    run_id = task_runner.start_gdpbyindustry_update(
        frequency=request.frequency,
        year=request.year,
        tables=tables,
    )

    if run_id is None:
        return TaskResponse(
            success=False,
            message="GDP by Industry task already running. Please wait for it to complete.",
        )

    return TaskResponse(
        success=True,
        message=f"GDP by Industry update started (category={request.category}, freq={request.frequency})",
        run_id=run_id,
    )
