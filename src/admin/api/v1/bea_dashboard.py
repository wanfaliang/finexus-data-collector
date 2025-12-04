"""
BEA Dashboard API Endpoints

Endpoints for BEA data freshness, API usage, and collection status.
"""
from typing import List, Optional
from datetime import datetime, date, timedelta
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, desc

from src.admin.core.database import get_db
from src.admin.schemas.bea import (
    BEADatasetFreshnessResponse,
    BEAFreshnessOverviewResponse,
    BEAAPIUsageResponse,
    BEAAPIUsageDetailResponse,
    BEACollectionRunResponse,
)
from src.database.bea_tracking_models import (
    BEADatasetFreshness,
    BEAAPIUsageLog,
    BEACollectionRun,
    BEATableUpdateStatus,
)
from src.database.bea_models import (
    BEADataset,
    NIPATable, NIPASeries, NIPAData,
    RegionalTable, RegionalLineCode, RegionalData,
    GDPByIndustryTable, GDPByIndustryIndustry, GDPByIndustryData,
)

router = APIRouter()

# Dataset descriptions
DATASET_INFO = {
    'NIPA': 'National Income and Product Accounts - GDP, income, consumption',
    'Regional': 'Regional Economic Accounts - State/county GDP, personal income',
    'GDPbyIndustry': 'GDP by Industry - Value added, contributions by industry sector',
    'ITA': 'International Transactions - Trade balance, exports, imports by country',
}


def _get_dataset_status(freshness: Optional[BEADatasetFreshness]) -> str:
    """Determine dataset status from freshness record"""
    if not freshness:
        return "unknown"
    if freshness.update_in_progress:
        return "updating"
    if freshness.needs_update:
        return "needs_update"
    return "current"


# ===================== Freshness Endpoints ===================== #

@router.get("/freshness/overview", response_model=BEAFreshnessOverviewResponse)
async def get_bea_freshness_overview(db: Session = Depends(get_db)):
    """
    Get overview of BEA dataset freshness status

    Returns summary counts and detailed status for each dataset.
    """
    # Get all freshness records
    freshness_records = db.query(BEADatasetFreshness).all()

    # Build dataset responses
    datasets = []
    total_data_points = 0

    for dataset_name in ['NIPA', 'Regional', 'GDPbyIndustry', 'ITA', 'FixedAssets']:
        freshness = next(
            (f for f in freshness_records if f.dataset_name == dataset_name),
            None
        )

        if freshness:
            total_data_points += freshness.data_points_count or 0

        datasets.append(BEADatasetFreshnessResponse(
            dataset_name=dataset_name,
            latest_data_year=freshness.latest_data_year if freshness else None,
            latest_data_period=freshness.latest_data_period if freshness else None,
            last_checked_at=freshness.last_checked_at if freshness else None,
            last_bea_update_detected=freshness.last_bea_update_detected if freshness else None,
            needs_update=freshness.needs_update if freshness else False,
            update_in_progress=freshness.update_in_progress if freshness else False,
            last_update_completed=freshness.last_update_completed if freshness else None,
            tables_count=freshness.tables_count if freshness else 0,
            series_count=freshness.series_count if freshness else 0,
            data_points_count=freshness.data_points_count if freshness else 0,
            total_checks=freshness.total_checks if freshness else 0,
            total_updates_detected=freshness.total_updates_detected if freshness else 0,
        ))

    # Calculate summary
    datasets_current = sum(1 for d in datasets if _get_dataset_status(
        next((f for f in freshness_records if f.dataset_name == d.dataset_name), None)
    ) == "current")
    datasets_need_update = sum(1 for d in datasets if d.needs_update)
    datasets_updating = sum(1 for d in datasets if d.update_in_progress)

    return BEAFreshnessOverviewResponse(
        total_datasets=len(datasets),
        datasets_current=datasets_current,
        datasets_need_update=datasets_need_update,
        datasets_updating=datasets_updating,
        total_data_points=total_data_points,
        datasets=datasets,
    )


@router.get("/freshness/{dataset_name}", response_model=BEADatasetFreshnessResponse)
async def get_dataset_freshness(
    dataset_name: str,
    db: Session = Depends(get_db)
):
    """
    Get detailed freshness status for a specific BEA dataset

    Args:
        dataset_name: Dataset name (NIPA or Regional)
    """
    dataset_name = dataset_name.upper()

    if dataset_name not in DATASET_INFO:
        raise HTTPException(status_code=404, detail=f"Dataset {dataset_name} not found")

    freshness = db.query(BEADatasetFreshness).filter(
        BEADatasetFreshness.dataset_name == dataset_name
    ).first()

    return BEADatasetFreshnessResponse(
        dataset_name=dataset_name,
        latest_data_year=freshness.latest_data_year if freshness else None,
        latest_data_period=freshness.latest_data_period if freshness else None,
        last_checked_at=freshness.last_checked_at if freshness else None,
        last_bea_update_detected=freshness.last_bea_update_detected if freshness else None,
        needs_update=freshness.needs_update if freshness else False,
        update_in_progress=freshness.update_in_progress if freshness else False,
        last_update_completed=freshness.last_update_completed if freshness else None,
        tables_count=freshness.tables_count if freshness else 0,
        series_count=freshness.series_count if freshness else 0,
        data_points_count=freshness.data_points_count if freshness else 0,
        total_checks=freshness.total_checks if freshness else 0,
        total_updates_detected=freshness.total_updates_detected if freshness else 0,
    )


# ===================== API Usage Endpoints ===================== #

@router.get("/usage/today", response_model=BEAAPIUsageResponse)
async def get_bea_usage_today(db: Session = Depends(get_db)):
    """
    Get BEA API usage statistics for today

    Shows requests, data volume, and errors.
    """
    today = date.today()

    # Aggregate today's usage
    result = db.query(
        func.sum(BEAAPIUsageLog.requests_count).label('total_requests'),
        func.sum(BEAAPIUsageLog.data_bytes).label('total_bytes'),
        func.sum(BEAAPIUsageLog.error_count).label('total_errors'),
    ).filter(
        BEAAPIUsageLog.usage_date == today
    ).first()

    total_requests = result.total_requests or 0
    total_bytes = result.total_bytes or 0
    total_errors = result.total_errors or 0

    # Check current minute usage for rate limit info
    now = datetime.utcnow()
    minute_ago = now - timedelta(minutes=1)

    minute_result = db.query(
        func.sum(BEAAPIUsageLog.requests_count).label('minute_requests'),
        func.sum(BEAAPIUsageLog.data_bytes).label('minute_bytes'),
    ).filter(
        BEAAPIUsageLog.usage_minute >= minute_ago
    ).first()

    minute_requests = minute_result.minute_requests or 0
    minute_bytes = minute_result.minute_bytes or 0

    return BEAAPIUsageResponse(
        date=str(today),
        total_requests=total_requests,
        total_data_mb=total_bytes / (1024 * 1024),
        total_errors=total_errors,
        requests_remaining=max(0, 100 - minute_requests),
        data_mb_remaining=max(0, 100 - (minute_bytes / (1024 * 1024))),
    )


@router.get("/usage/history", response_model=List[BEAAPIUsageResponse])
async def get_bea_usage_history(
    days: int = Query(7, ge=1, le=30),
    db: Session = Depends(get_db)
):
    """
    Get BEA API usage history for the past N days

    Args:
        days: Number of days of history (1-30)
    """
    start_date = date.today() - timedelta(days=days)

    # Aggregate by date
    results = db.query(
        BEAAPIUsageLog.usage_date,
        func.sum(BEAAPIUsageLog.requests_count).label('total_requests'),
        func.sum(BEAAPIUsageLog.data_bytes).label('total_bytes'),
        func.sum(BEAAPIUsageLog.error_count).label('total_errors'),
    ).filter(
        BEAAPIUsageLog.usage_date >= start_date
    ).group_by(
        BEAAPIUsageLog.usage_date
    ).order_by(
        BEAAPIUsageLog.usage_date.desc()
    ).all()

    return [
        BEAAPIUsageResponse(
            date=str(r.usage_date),
            total_requests=r.total_requests or 0,
            total_data_mb=(r.total_bytes or 0) / (1024 * 1024),
            total_errors=r.total_errors or 0,
            requests_remaining=100,  # Historical, so full quota
            data_mb_remaining=100.0,
        )
        for r in results
    ]


# ===================== Collection Runs Endpoints ===================== #

@router.get("/runs/recent", response_model=List[BEACollectionRunResponse])
async def get_recent_collection_runs(
    limit: int = Query(10, ge=1, le=50),
    dataset: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """
    Get recent BEA collection runs

    Args:
        limit: Number of runs to return (1-50)
        dataset: Optional filter by dataset name
    """
    query = db.query(BEACollectionRun).order_by(
        desc(BEACollectionRun.started_at)
    )

    if dataset:
        query = query.filter(BEACollectionRun.dataset_name == dataset.upper())

    runs = query.limit(limit).all()

    return [
        BEACollectionRunResponse(
            run_id=r.run_id,
            dataset_name=r.dataset_name,
            run_type=r.run_type,
            frequency=r.frequency,
            geo_scope=r.geo_scope,
            year_spec=r.year_spec,
            started_at=r.started_at,
            completed_at=r.completed_at,
            status=r.status,
            error_message=r.error_message,
            tables_processed=r.tables_processed,
            series_processed=r.series_processed,
            data_points_inserted=r.data_points_inserted,
            data_points_updated=r.data_points_updated,
            api_requests_made=r.api_requests_made,
            start_year=r.start_year,
            end_year=r.end_year,
            duration_seconds=(
                (r.completed_at - r.started_at).total_seconds()
                if r.completed_at else None
            ),
        )
        for r in runs
    ]


@router.get("/runs/{run_id}", response_model=BEACollectionRunResponse)
async def get_collection_run(
    run_id: int,
    db: Session = Depends(get_db)
):
    """
    Get details of a specific collection run

    Args:
        run_id: Collection run ID
    """
    run = db.query(BEACollectionRun).filter(
        BEACollectionRun.run_id == run_id
    ).first()

    if not run:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    return BEACollectionRunResponse(
        run_id=run.run_id,
        dataset_name=run.dataset_name,
        run_type=run.run_type,
        frequency=run.frequency,
        geo_scope=run.geo_scope,
        year_spec=run.year_spec,
        started_at=run.started_at,
        completed_at=run.completed_at,
        status=run.status,
        error_message=run.error_message,
        tables_processed=run.tables_processed,
        series_processed=run.series_processed,
        data_points_inserted=run.data_points_inserted,
        data_points_updated=run.data_points_updated,
        api_requests_made=run.api_requests_made,
        start_year=run.start_year,
        end_year=run.end_year,
        duration_seconds=(
            (run.completed_at - run.started_at).total_seconds()
            if run.completed_at else None
        ),
    )


# ===================== Statistics Endpoints ===================== #

@router.get("/stats/summary")
async def get_bea_stats_summary(db: Session = Depends(get_db)):
    """
    Get summary statistics for BEA data

    Returns counts of tables, series, and data points.
    """
    # NIPA stats
    nipa_tables = db.query(func.count(NIPATable.table_name)).scalar() or 0
    nipa_series = db.query(func.count(NIPASeries.series_code)).scalar() or 0
    nipa_data = db.query(func.count()).select_from(NIPAData).scalar() or 0

    # Regional stats
    regional_tables = db.query(func.count(RegionalTable.table_name)).scalar() or 0
    regional_line_codes = db.query(func.count()).select_from(RegionalLineCode).scalar() or 0
    regional_data = db.query(func.count()).select_from(RegionalData).scalar() or 0

    # GDP by Industry stats
    gdpbyindustry_tables = db.query(func.count(GDPByIndustryTable.table_id)).scalar() or 0
    gdpbyindustry_industries = db.query(func.count(GDPByIndustryIndustry.industry_code)).scalar() or 0
    gdpbyindustry_data = db.query(func.count()).select_from(GDPByIndustryData).scalar() or 0

    return {
        "nipa": {
            "tables": nipa_tables,
            "series": nipa_series,
            "data_points": nipa_data,
        },
        "regional": {
            "tables": regional_tables,
            "line_codes": regional_line_codes,
            "data_points": regional_data,
        },
        "gdpbyindustry": {
            "tables": gdpbyindustry_tables,
            "industries": gdpbyindustry_industries,
            "data_points": gdpbyindustry_data,
        },
        "total_data_points": nipa_data + regional_data + gdpbyindustry_data,
    }
