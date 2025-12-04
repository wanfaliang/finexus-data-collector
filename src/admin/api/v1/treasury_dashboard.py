"""
Treasury Dashboard API Endpoints

Endpoints for Treasury auction data, freshness status, and collection runs.
"""
from typing import List, Optional
from datetime import datetime, date, timedelta
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, desc, and_

from src.admin.core.database import get_db
from src.admin.schemas.treasury import (
    TreasuryFreshnessResponse,
    TreasuryFreshnessOverviewResponse,
    TreasuryCollectionRunResponse,
    TreasuryAuctionResponse,
    TreasuryAuctionSummaryResponse,
    TreasuryUpcomingAuctionResponse,
    TreasuryDailyRateResponse,
    TreasuryStatsResponse,
)
from src.database.treasury_models import (
    TreasuryAuction,
    TreasuryUpcomingAuction,
    TreasuryDailyRate,
    TreasuryAuctionReaction,
)
from src.database.treasury_tracking_models import (
    TreasuryDataFreshness,
    TreasuryCollectionRun,
    TreasuryAPIUsageLog,
)

router = APIRouter()


# ===================== Freshness Endpoints ===================== #

@router.get("/freshness/overview", response_model=TreasuryFreshnessOverviewResponse)
async def get_treasury_freshness_overview(db: Session = Depends(get_db)):
    """
    Get overview of Treasury data freshness status.
    """
    # Get all freshness records
    freshness_records = db.query(TreasuryDataFreshness).all()

    # Build responses
    data_types = []
    total_records = 0

    for data_type in ['auctions', 'upcoming', 'daily_rates']:
        freshness = next(
            (f for f in freshness_records if f.data_type == data_type),
            None
        )

        if freshness:
            total_records += freshness.total_records or 0

        data_types.append(TreasuryFreshnessResponse(
            data_type=data_type,
            latest_data_date=freshness.latest_data_date if freshness else None,
            last_checked_at=freshness.last_checked_at if freshness else None,
            last_update_detected=freshness.last_update_detected if freshness else None,
            needs_update=freshness.needs_update if freshness else False,
            update_in_progress=freshness.update_in_progress if freshness else False,
            last_update_completed=freshness.last_update_completed if freshness else None,
            total_records=freshness.total_records if freshness else 0,
            total_checks=freshness.total_checks if freshness else 0,
            total_updates=freshness.total_updates if freshness else 0,
        ))

    # Calculate summary
    types_current = sum(1 for d in data_types if not d.needs_update and not d.update_in_progress)
    types_need_update = sum(1 for d in data_types if d.needs_update)
    types_updating = sum(1 for d in data_types if d.update_in_progress)

    return TreasuryFreshnessOverviewResponse(
        total_data_types=len(data_types),
        types_current=types_current,
        types_need_update=types_need_update,
        types_updating=types_updating,
        total_records=total_records,
        data_types=data_types,
    )


# ===================== Collection Runs ===================== #

@router.get("/runs/recent", response_model=List[TreasuryCollectionRunResponse])
async def get_recent_collection_runs(
    limit: int = Query(20, ge=1, le=100),
    collection_type: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """
    Get recent Treasury collection runs.
    """
    query = db.query(TreasuryCollectionRun).order_by(desc(TreasuryCollectionRun.started_at))

    if collection_type:
        query = query.filter(TreasuryCollectionRun.collection_type == collection_type)

    runs = query.limit(limit).all()

    return [
        TreasuryCollectionRunResponse(
            run_id=run.run_id,
            collection_type=run.collection_type,
            run_type=run.run_type,
            security_term=run.security_term,
            started_at=run.started_at,
            completed_at=run.completed_at,
            status=run.status,
            error_message=run.error_message,
            records_fetched=run.records_fetched,
            records_inserted=run.records_inserted,
            records_updated=run.records_updated,
            api_requests_made=run.api_requests_made,
            duration_seconds=float(run.duration_seconds) if run.duration_seconds else None,
        )
        for run in runs
    ]


# ===================== Auction Data ===================== #

@router.get("/auctions/recent", response_model=List[TreasuryAuctionResponse])
async def get_recent_auctions(
    limit: int = Query(20, ge=1, le=100),
    security_term: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """
    Get recent Treasury auction results.
    """
    query = db.query(TreasuryAuction).order_by(desc(TreasuryAuction.auction_date))

    if security_term:
        query = query.filter(TreasuryAuction.security_term == security_term)

    auctions = query.limit(limit).all()

    return [
        TreasuryAuctionResponse(
            auction_id=a.auction_id,
            cusip=a.cusip,
            auction_date=a.auction_date,
            security_type=a.security_type,
            security_term=a.security_term,
            offering_amount=float(a.offering_amount) if a.offering_amount else None,
            total_tendered=float(a.total_tendered) if a.total_tendered else None,
            total_accepted=float(a.total_accepted) if a.total_accepted else None,
            bid_to_cover_ratio=float(a.bid_to_cover_ratio) if a.bid_to_cover_ratio else None,
            high_yield=float(a.high_yield) if a.high_yield else None,
            coupon_rate=float(a.coupon_rate) if a.coupon_rate else None,
            tail_bps=float(a.tail_bps) if a.tail_bps else None,
            auction_result=a.auction_result,
        )
        for a in auctions
    ]


@router.get("/auctions/summary", response_model=TreasuryAuctionSummaryResponse)
async def get_auction_summary(db: Session = Depends(get_db)):
    """
    Get summary statistics for Treasury auctions.
    """
    # Total auctions
    total_auctions = db.query(func.count(TreasuryAuction.auction_id)).scalar() or 0

    # Auctions by term
    by_term = dict(
        db.query(
            TreasuryAuction.security_term,
            func.count(TreasuryAuction.auction_id)
        ).group_by(TreasuryAuction.security_term).all()
    )

    # Date range
    min_date = db.query(func.min(TreasuryAuction.auction_date)).scalar()
    max_date = db.query(func.max(TreasuryAuction.auction_date)).scalar()

    # Recent auctions by result
    thirty_days_ago = date.today() - timedelta(days=30)
    recent_results = dict(
        db.query(
            TreasuryAuction.auction_result,
            func.count(TreasuryAuction.auction_id)
        ).filter(
            TreasuryAuction.auction_date >= thirty_days_ago,
            TreasuryAuction.auction_result.isnot(None)
        ).group_by(TreasuryAuction.auction_result).all()
    )

    # Average metrics
    avg_btc = db.query(func.avg(TreasuryAuction.bid_to_cover_ratio)).filter(
        TreasuryAuction.auction_date >= thirty_days_ago
    ).scalar()

    return TreasuryAuctionSummaryResponse(
        total_auctions=total_auctions,
        auctions_by_term=by_term,
        earliest_auction=min_date,
        latest_auction=max_date,
        recent_results=recent_results,
        avg_bid_to_cover_30d=float(avg_btc) if avg_btc else None,
    )


@router.get("/auctions/{security_term}/history", response_model=List[TreasuryAuctionResponse])
async def get_auction_history_by_term(
    security_term: str,
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db)
):
    """
    Get auction history for a specific security term.
    """
    auctions = db.query(TreasuryAuction).filter(
        TreasuryAuction.security_term == security_term
    ).order_by(desc(TreasuryAuction.auction_date)).limit(limit).all()

    return [
        TreasuryAuctionResponse(
            auction_id=a.auction_id,
            cusip=a.cusip,
            auction_date=a.auction_date,
            security_type=a.security_type,
            security_term=a.security_term,
            offering_amount=float(a.offering_amount) if a.offering_amount else None,
            total_tendered=float(a.total_tendered) if a.total_tendered else None,
            total_accepted=float(a.total_accepted) if a.total_accepted else None,
            bid_to_cover_ratio=float(a.bid_to_cover_ratio) if a.bid_to_cover_ratio else None,
            high_yield=float(a.high_yield) if a.high_yield else None,
            coupon_rate=float(a.coupon_rate) if a.coupon_rate else None,
            tail_bps=float(a.tail_bps) if a.tail_bps else None,
            auction_result=a.auction_result,
        )
        for a in auctions
    ]


# ===================== Upcoming Auctions ===================== #

@router.get("/upcoming", response_model=List[TreasuryUpcomingAuctionResponse])
async def get_upcoming_auctions(
    include_processed: bool = False,
    db: Session = Depends(get_db)
):
    """
    Get upcoming Treasury auctions.
    """
    query = db.query(TreasuryUpcomingAuction).order_by(TreasuryUpcomingAuction.auction_date)

    if not include_processed:
        query = query.filter(TreasuryUpcomingAuction.is_processed == False)

    auctions = query.all()

    return [
        TreasuryUpcomingAuctionResponse(
            upcoming_id=a.upcoming_id,
            cusip=a.cusip,
            security_type=a.security_type,
            security_term=a.security_term,
            auction_date=a.auction_date,
            issue_date=a.issue_date,
            maturity_date=a.maturity_date,
            offering_amount=float(a.offering_amount) if a.offering_amount else None,
            is_processed=a.is_processed,
        )
        for a in auctions
    ]


# ===================== Daily Rates ===================== #

@router.get("/rates/latest", response_model=TreasuryDailyRateResponse)
async def get_latest_rates(db: Session = Depends(get_db)):
    """
    Get the latest Treasury yield curve rates.
    """
    rate = db.query(TreasuryDailyRate).order_by(desc(TreasuryDailyRate.rate_date)).first()

    if not rate:
        raise HTTPException(status_code=404, detail="No daily rate data available")

    return TreasuryDailyRateResponse(
        rate_date=rate.rate_date,
        yield_1m=float(rate.yield_1m) if rate.yield_1m else None,
        yield_3m=float(rate.yield_3m) if rate.yield_3m else None,
        yield_6m=float(rate.yield_6m) if rate.yield_6m else None,
        yield_1y=float(rate.yield_1y) if rate.yield_1y else None,
        yield_2y=float(rate.yield_2y) if rate.yield_2y else None,
        yield_5y=float(rate.yield_5y) if rate.yield_5y else None,
        yield_7y=float(rate.yield_7y) if rate.yield_7y else None,
        yield_10y=float(rate.yield_10y) if rate.yield_10y else None,
        yield_20y=float(rate.yield_20y) if rate.yield_20y else None,
        yield_30y=float(rate.yield_30y) if rate.yield_30y else None,
        spread_2s10s=float(rate.spread_2s10s) if rate.spread_2s10s else None,
        spread_2s30s=float(rate.spread_2s30s) if rate.spread_2s30s else None,
    )


@router.get("/rates/history", response_model=List[TreasuryDailyRateResponse])
async def get_rates_history(
    days: int = Query(30, ge=1, le=365),
    db: Session = Depends(get_db)
):
    """
    Get Treasury yield curve history for the last N days.
    """
    start_date = date.today() - timedelta(days=days)

    rates = db.query(TreasuryDailyRate).filter(
        TreasuryDailyRate.rate_date >= start_date
    ).order_by(TreasuryDailyRate.rate_date).all()

    return [
        TreasuryDailyRateResponse(
            rate_date=r.rate_date,
            yield_1m=float(r.yield_1m) if r.yield_1m else None,
            yield_3m=float(r.yield_3m) if r.yield_3m else None,
            yield_6m=float(r.yield_6m) if r.yield_6m else None,
            yield_1y=float(r.yield_1y) if r.yield_1y else None,
            yield_2y=float(r.yield_2y) if r.yield_2y else None,
            yield_5y=float(r.yield_5y) if r.yield_5y else None,
            yield_7y=float(r.yield_7y) if r.yield_7y else None,
            yield_10y=float(r.yield_10y) if r.yield_10y else None,
            yield_20y=float(r.yield_20y) if r.yield_20y else None,
            yield_30y=float(r.yield_30y) if r.yield_30y else None,
            spread_2s10s=float(r.spread_2s10s) if r.spread_2s10s else None,
            spread_2s30s=float(r.spread_2s30s) if r.spread_2s30s else None,
        )
        for r in rates
    ]


# ===================== Stats ===================== #

@router.get("/stats", response_model=TreasuryStatsResponse)
async def get_treasury_stats(db: Session = Depends(get_db)):
    """
    Get overall Treasury data statistics.
    """
    total_auctions = db.query(func.count(TreasuryAuction.auction_id)).scalar() or 0
    total_upcoming = db.query(func.count(TreasuryUpcomingAuction.upcoming_id)).filter(
        TreasuryUpcomingAuction.is_processed == False
    ).scalar() or 0
    total_daily_rates = db.query(func.count(TreasuryDailyRate.rate_id)).scalar() or 0

    # Date ranges
    earliest_auction = db.query(func.min(TreasuryAuction.auction_date)).scalar()
    latest_auction = db.query(func.max(TreasuryAuction.auction_date)).scalar()

    # Recent collection activity
    recent_runs = db.query(func.count(TreasuryCollectionRun.run_id)).filter(
        TreasuryCollectionRun.started_at >= datetime.utcnow() - timedelta(days=7)
    ).scalar() or 0

    return TreasuryStatsResponse(
        total_auctions=total_auctions,
        total_upcoming_auctions=total_upcoming,
        total_daily_rates=total_daily_rates,
        earliest_auction=earliest_auction,
        latest_auction=latest_auction,
        collection_runs_last_7d=recent_runs,
    )
