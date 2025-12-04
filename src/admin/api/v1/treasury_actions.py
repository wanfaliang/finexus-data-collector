"""
Treasury Action Endpoints

API endpoints for triggering Treasury data collection tasks.

Author: FinExus Data Collector
Created: 2025-12-03
"""
from typing import Optional, List
from datetime import datetime, UTC
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from src.admin.core.database import get_db
from src.database.treasury_tracking_models import TreasuryCollectionRun, TreasuryDataFreshness
from src.treasury import TreasuryClient, TreasuryCollector

router = APIRouter()


# ==================== Request/Response Models ==================== #

class AuctionBackfillRequest(BaseModel):
    """Request to backfill Treasury auction data"""
    years: int = Field(5, ge=1, le=20, description="Number of years to backfill")
    security_term: Optional[str] = Field(None, description="Specific term to backfill (e.g., '10-Year')")


class AuctionUpdateRequest(BaseModel):
    """Request to update recent auction data"""
    days: int = Field(30, ge=1, le=365, description="Number of days to look back")
    security_term: Optional[str] = Field(None, description="Specific term to update")


class UpcomingAuctionsRequest(BaseModel):
    """Request to refresh upcoming auctions"""
    pass


class TaskResponse(BaseModel):
    """Response for task start requests"""
    success: bool
    message: str
    run_id: Optional[int] = None


class TaskStatusResponse(BaseModel):
    """Response for task status check"""
    auctions_running: bool
    upcoming_running: bool
    rates_running: bool


# ==================== Task Status Tracking ==================== #

# Simple in-memory tracking (for single-server deployment)
_task_status = {
    'auctions_running': False,
    'upcoming_running': False,
    'rates_running': False,
}


# ==================== Background Task Functions ==================== #

def _run_auction_backfill(
    db_url: str,
    years: int,
    security_term: Optional[str],
    run_id: int,
):
    """Background task to backfill auction data"""
    import logging
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    log = logging.getLogger("TreasuryBackfill")
    log.info(f"Starting auction backfill: years={years}, run_id={run_id}")

    global _task_status
    _task_status['auctions_running'] = True

    run = None
    try:
        engine = create_engine(db_url)
        Session = sessionmaker(bind=engine)
        session = Session()

        # Get the run record
        run = session.query(TreasuryCollectionRun).filter(
            TreasuryCollectionRun.run_id == run_id
        ).first()
        if not run:
            log.error(f"Run {run_id} not found")
            return

        log.info(f"Found run record, starting collector...")

        # Create collector and run backfill
        collector = TreasuryCollector(db_session=session)
        inserted, updated = collector.backfill_auctions(years=years, security_term=security_term)

        log.info(f"Backfill complete: inserted={inserted}, updated={updated}")

        # Update run record
        run.status = 'completed'
        run.completed_at = datetime.now(UTC)
        run.records_fetched = collector.stats['auctions_fetched']
        run.records_inserted = inserted
        run.records_updated = updated
        run.api_requests_made = collector.stats['api_requests']
        # Handle timezone-naive started_at from database
        if run.started_at.tzinfo is None:
            run.duration_seconds = (run.completed_at.replace(tzinfo=None) - run.started_at).total_seconds()
        else:
            run.duration_seconds = (run.completed_at - run.started_at).total_seconds()
        session.commit()

        # Update freshness
        _update_freshness(session, 'auctions')
        log.info(f"Run {run_id} completed successfully")

    except Exception as e:
        log.error(f"Backfill failed: {e}", exc_info=True)
        if run:
            try:
                run.status = 'failed'
                run.error_message = str(e)
                run.completed_at = datetime.now(UTC)
                session.commit()
            except:
                pass
    finally:
        _task_status['auctions_running'] = False
        try:
            session.close()
        except:
            pass


def _run_auction_update(
    db_url: str,
    days: int,
    security_term: Optional[str],
    run_id: int,
):
    """Background task to update recent auction data"""
    import logging
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    log = logging.getLogger("TreasuryUpdate")
    log.info(f"Starting auction update: days={days}, run_id={run_id}")

    global _task_status
    _task_status['auctions_running'] = True

    run = None
    try:
        engine = create_engine(db_url)
        Session = sessionmaker(bind=engine)
        session = Session()

        run = session.query(TreasuryCollectionRun).filter(
            TreasuryCollectionRun.run_id == run_id
        ).first()
        if not run:
            log.error(f"Run {run_id} not found")
            return

        collector = TreasuryCollector(db_session=session)
        inserted, updated = collector.collect_recent_auctions(days=days)

        run.status = 'completed'
        run.completed_at = datetime.now(UTC)
        run.records_fetched = collector.stats['auctions_fetched']
        run.records_inserted = inserted
        run.records_updated = updated
        run.api_requests_made = collector.stats['api_requests']
        # Handle timezone-naive started_at from database
        if run.started_at.tzinfo is None:
            run.duration_seconds = (run.completed_at.replace(tzinfo=None) - run.started_at).total_seconds()
        else:
            run.duration_seconds = (run.completed_at - run.started_at).total_seconds()
        session.commit()

        _update_freshness(session, 'auctions')
        log.info(f"Run {run_id} completed successfully")

    except Exception as e:
        log.error(f"Update failed: {e}", exc_info=True)
        if run:
            try:
                run.status = 'failed'
                run.error_message = str(e)
                run.completed_at = datetime.now(UTC)
                session.commit()
            except:
                pass
    finally:
        _task_status['auctions_running'] = False
        try:
            session.close()
        except:
            pass


def _run_upcoming_refresh(db_url: str, run_id: int):
    """Background task to refresh upcoming auctions"""
    import logging
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    log = logging.getLogger("TreasuryUpcoming")
    log.info(f"Starting upcoming refresh: run_id={run_id}")

    global _task_status
    _task_status['upcoming_running'] = True

    run = None
    try:
        engine = create_engine(db_url)
        Session = sessionmaker(bind=engine)
        session = Session()

        run = session.query(TreasuryCollectionRun).filter(
            TreasuryCollectionRun.run_id == run_id
        ).first()
        if not run:
            log.error(f"Run {run_id} not found")
            return

        collector = TreasuryCollector(db_session=session)
        inserted = collector.collect_upcoming_auctions()

        run.status = 'completed'
        run.completed_at = datetime.now(UTC)
        run.records_fetched = collector.stats['upcoming_fetched']
        run.records_inserted = inserted
        run.api_requests_made = collector.stats['api_requests']
        # Handle timezone-naive started_at from database
        if run.started_at.tzinfo is None:
            run.duration_seconds = (run.completed_at.replace(tzinfo=None) - run.started_at).total_seconds()
        else:
            run.duration_seconds = (run.completed_at - run.started_at).total_seconds()
        session.commit()

        _update_freshness(session, 'upcoming')
        log.info(f"Run {run_id} completed successfully")

    except Exception as e:
        log.error(f"Upcoming refresh failed: {e}", exc_info=True)
        if run:
            try:
                run.status = 'failed'
                run.error_message = str(e)
                run.completed_at = datetime.now(UTC)
                session.commit()
            except:
                pass
    finally:
        _task_status['upcoming_running'] = False
        try:
            session.close()
        except:
            pass


def _update_freshness(session: Session, data_type: str):
    """Update freshness record after collection"""
    from sqlalchemy import func
    from src.database.treasury_models import TreasuryAuction, TreasuryUpcomingAuction, TreasuryDailyRate

    freshness = session.query(TreasuryDataFreshness).filter(
        TreasuryDataFreshness.data_type == data_type
    ).first()

    if not freshness:
        freshness = TreasuryDataFreshness(data_type=data_type)
        session.add(freshness)

    freshness.last_update_completed = datetime.now(UTC)
    freshness.needs_update = False
    freshness.update_in_progress = False
    freshness.total_updates = (freshness.total_updates or 0) + 1

    # Update record counts and latest date
    if data_type == 'auctions':
        freshness.total_records = session.query(func.count(TreasuryAuction.auction_id)).scalar() or 0
        freshness.latest_data_date = session.query(func.max(TreasuryAuction.auction_date)).scalar()
    elif data_type == 'upcoming':
        freshness.total_records = session.query(func.count(TreasuryUpcomingAuction.upcoming_id)).scalar() or 0
        freshness.latest_data_date = session.query(func.max(TreasuryUpcomingAuction.auction_date)).scalar()
    elif data_type == 'daily_rates':
        freshness.total_records = session.query(func.count(TreasuryDailyRate.rate_id)).scalar() or 0
        freshness.latest_data_date = session.query(func.max(TreasuryDailyRate.rate_date)).scalar()

    session.commit()


# ==================== API Endpoints ==================== #

@router.get("/task-status", response_model=TaskStatusResponse)
async def get_task_status():
    """
    Get status of running Treasury collection tasks.
    """
    return TaskStatusResponse(
        auctions_running=_task_status['auctions_running'],
        upcoming_running=_task_status['upcoming_running'],
        rates_running=_task_status['rates_running'],
    )


@router.post("/backfill/auctions", response_model=TaskResponse)
async def start_auction_backfill(
    request: AuctionBackfillRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Start Treasury auction data backfill.

    Fetches historical auction results for Notes and Bonds (2Y, 5Y, 7Y, 10Y, 20Y, 30Y).
    """
    if _task_status['auctions_running']:
        return TaskResponse(
            success=False,
            message="Auction collection task is already running"
        )

    # Create collection run record
    run = TreasuryCollectionRun(
        collection_type='auctions',
        run_type='backfill',
        security_term=request.security_term,
        year_spec=f'LAST{request.years}',
        status='running',
    )
    db.add(run)
    db.commit()
    db.refresh(run)

    # Get database URL from settings (not session, which masks password)
    from src.config import settings
    db_url = settings.database.url

    # Start background task
    background_tasks.add_task(
        _run_auction_backfill,
        db_url,
        request.years,
        request.security_term,
        run.run_id,
    )

    return TaskResponse(
        success=True,
        message=f"Started auction backfill for {request.years} years" +
                (f" ({request.security_term})" if request.security_term else ""),
        run_id=run.run_id,
    )


@router.post("/update/auctions", response_model=TaskResponse)
async def start_auction_update(
    request: AuctionUpdateRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Update recent Treasury auction data.

    Fetches auction results from the last N days.
    """
    if _task_status['auctions_running']:
        return TaskResponse(
            success=False,
            message="Auction collection task is already running"
        )

    run = TreasuryCollectionRun(
        collection_type='auctions',
        run_type='update',
        security_term=request.security_term,
        year_spec=f'LAST{request.days}D',
        status='running',
    )
    db.add(run)
    db.commit()
    db.refresh(run)

    from src.config import settings
    db_url = settings.database.url

    background_tasks.add_task(
        _run_auction_update,
        db_url,
        request.days,
        request.security_term,
        run.run_id,
    )

    return TaskResponse(
        success=True,
        message=f"Started auction update for last {request.days} days",
        run_id=run.run_id,
    )


@router.post("/refresh/upcoming", response_model=TaskResponse)
async def refresh_upcoming_auctions(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Refresh upcoming Treasury auctions calendar.

    Fetches the latest upcoming auction schedule from Treasury.
    """
    if _task_status['upcoming_running']:
        return TaskResponse(
            success=False,
            message="Upcoming auctions refresh is already running"
        )

    run = TreasuryCollectionRun(
        collection_type='upcoming',
        run_type='refresh',
        status='running',
    )
    db.add(run)
    db.commit()
    db.refresh(run)

    from src.config import settings
    db_url = settings.database.url

    background_tasks.add_task(
        _run_upcoming_refresh,
        db_url,
        run.run_id,
    )

    return TaskResponse(
        success=True,
        message="Started upcoming auctions refresh",
        run_id=run.run_id,
    )


@router.post("/test-api", response_model=TaskResponse)
async def test_treasury_api():
    """
    Test connectivity to Treasury Fiscal Data API.

    Makes a simple API call to verify connectivity.
    """
    try:
        client = TreasuryClient()
        # Try to get upcoming auctions (lightweight call)
        auctions = client.get_upcoming_auctions()
        return TaskResponse(
            success=True,
            message=f"API connection successful. Found {len(auctions)} upcoming auctions."
        )
    except Exception as e:
        return TaskResponse(
            success=False,
            message=f"API connection failed: {str(e)}"
        )
