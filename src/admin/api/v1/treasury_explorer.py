"""
Treasury Explorer API Endpoints

Endpoints for exploring Treasury auction data.
"""
from typing import List, Optional
from datetime import datetime, date
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import func, desc

from src.admin.core.database import get_db
from src.database.treasury_models import (
    TreasuryAuction, TreasuryUpcomingAuction, TreasuryDailyRate
)

router = APIRouter()


# ===================== Response Models ===================== #

class AuctionResponse(BaseModel):
    auction_id: int
    cusip: str
    auction_date: date
    security_type: str
    security_term: str
    offering_amount: Optional[float] = None
    bid_to_cover_ratio: Optional[float] = None
    high_yield: Optional[float] = None
    coupon_rate: Optional[float] = None
    tail_bps: Optional[float] = None
    auction_result: Optional[str] = None


class AuctionDetailResponse(BaseModel):
    auction_id: int
    cusip: str
    auction_date: date
    security_type: str
    security_term: str
    term_months: Optional[int] = None
    issue_date: Optional[date] = None
    maturity_date: Optional[date] = None
    offering_amount: Optional[float] = None
    total_tendered: Optional[float] = None
    total_accepted: Optional[float] = None
    bid_to_cover_ratio: Optional[float] = None
    competitive_tendered: Optional[float] = None
    competitive_accepted: Optional[float] = None
    non_competitive_tendered: Optional[float] = None
    non_competitive_accepted: Optional[float] = None
    primary_dealer_tendered: Optional[float] = None
    primary_dealer_accepted: Optional[float] = None
    direct_bidder_tendered: Optional[float] = None
    direct_bidder_accepted: Optional[float] = None
    indirect_bidder_accepted: Optional[float] = None
    high_yield: Optional[float] = None
    high_discount_rate: Optional[float] = None
    low_yield: Optional[float] = None
    median_yield: Optional[float] = None
    coupon_rate: Optional[float] = None
    price_per_100: Optional[float] = None
    wi_yield: Optional[float] = None
    tail_bps: Optional[float] = None
    auction_result: Optional[str] = None


class TermSummaryResponse(BaseModel):
    security_term: str
    auction_count: int
    avg_bid_to_cover: Optional[float] = None
    avg_yield: Optional[float] = None
    latest_auction_date: Optional[date] = None
    latest_yield: Optional[float] = None


class YieldHistoryPoint(BaseModel):
    auction_date: date
    high_yield: Optional[float] = None
    bid_to_cover_ratio: Optional[float] = None
    offering_amount: Optional[float] = None


class YieldHistoryResponse(BaseModel):
    security_term: str
    data: List[YieldHistoryPoint]


class UpcomingAuctionResponse(BaseModel):
    upcoming_id: int
    cusip: Optional[str] = None
    security_type: str
    security_term: str
    auction_date: date
    issue_date: Optional[date] = None
    maturity_date: Optional[date] = None
    offering_amount: Optional[float] = None
    announcement_date: Optional[date] = None


# ===================== API Endpoints ===================== #

@router.get("/terms", response_model=List[TermSummaryResponse])
async def get_term_summaries(
    db: Session = Depends(get_db)
):
    """
    Get summary statistics for each security term (2Y, 5Y, 7Y, 10Y, 20Y, 30Y)
    """
    # Define standard terms
    terms = ['2-Year', '5-Year', '7-Year', '10-Year', '20-Year', '30-Year']

    results = []
    for term in terms:
        # Get auction count and averages
        stats = db.query(
            func.count(TreasuryAuction.auction_id),
            func.avg(TreasuryAuction.bid_to_cover_ratio),
            func.avg(TreasuryAuction.high_yield),
            func.max(TreasuryAuction.auction_date),
        ).filter(
            TreasuryAuction.security_term == term
        ).first()

        # Get latest yield
        latest = db.query(TreasuryAuction).filter(
            TreasuryAuction.security_term == term
        ).order_by(desc(TreasuryAuction.auction_date)).first()

        results.append(TermSummaryResponse(
            security_term=term,
            auction_count=stats[0] or 0,
            avg_bid_to_cover=float(stats[1]) if stats[1] else None,
            avg_yield=float(stats[2]) if stats[2] else None,
            latest_auction_date=stats[3],
            latest_yield=float(latest.high_yield) if latest and latest.high_yield else None,
        ))

    return results


@router.get("/auctions", response_model=List[AuctionResponse])
async def get_auctions(
    security_term: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db)
):
    """
    Get list of Treasury auctions with optional filters
    """
    query = db.query(TreasuryAuction)

    if security_term:
        query = query.filter(TreasuryAuction.security_term == security_term)
    if start_date:
        query = query.filter(TreasuryAuction.auction_date >= start_date)
    if end_date:
        query = query.filter(TreasuryAuction.auction_date <= end_date)

    auctions = query.order_by(desc(TreasuryAuction.auction_date)).offset(offset).limit(limit).all()

    return [
        AuctionResponse(
            auction_id=a.auction_id,
            cusip=a.cusip,
            auction_date=a.auction_date,
            security_type=a.security_type,
            security_term=a.security_term,
            offering_amount=float(a.offering_amount) if a.offering_amount else None,
            bid_to_cover_ratio=float(a.bid_to_cover_ratio) if a.bid_to_cover_ratio else None,
            high_yield=float(a.high_yield) if a.high_yield else None,
            coupon_rate=float(a.coupon_rate) if a.coupon_rate else None,
            tail_bps=float(a.tail_bps) if a.tail_bps else None,
            auction_result=a.auction_result,
        )
        for a in auctions
    ]


@router.get("/auctions/{auction_id}", response_model=AuctionDetailResponse)
async def get_auction_detail(
    auction_id: int,
    db: Session = Depends(get_db)
):
    """
    Get detailed information for a specific auction
    """
    auction = db.query(TreasuryAuction).filter(
        TreasuryAuction.auction_id == auction_id
    ).first()

    if not auction:
        raise HTTPException(status_code=404, detail=f"Auction {auction_id} not found")

    return AuctionDetailResponse(
        auction_id=auction.auction_id,
        cusip=auction.cusip,
        auction_date=auction.auction_date,
        security_type=auction.security_type,
        security_term=auction.security_term,
        term_months=auction.term_months,
        issue_date=auction.issue_date,
        maturity_date=auction.maturity_date,
        offering_amount=float(auction.offering_amount) if auction.offering_amount else None,
        total_tendered=float(auction.total_tendered) if auction.total_tendered else None,
        total_accepted=float(auction.total_accepted) if auction.total_accepted else None,
        bid_to_cover_ratio=float(auction.bid_to_cover_ratio) if auction.bid_to_cover_ratio else None,
        competitive_tendered=float(auction.competitive_tendered) if auction.competitive_tendered else None,
        competitive_accepted=float(auction.competitive_accepted) if auction.competitive_accepted else None,
        non_competitive_tendered=float(auction.non_competitive_tendered) if auction.non_competitive_tendered else None,
        non_competitive_accepted=float(auction.non_competitive_accepted) if auction.non_competitive_accepted else None,
        primary_dealer_tendered=float(auction.primary_dealer_tendered) if auction.primary_dealer_tendered else None,
        primary_dealer_accepted=float(auction.primary_dealer_accepted) if auction.primary_dealer_accepted else None,
        direct_bidder_tendered=float(auction.direct_bidder_tendered) if auction.direct_bidder_tendered else None,
        direct_bidder_accepted=float(auction.direct_bidder_accepted) if auction.direct_bidder_accepted else None,
        indirect_bidder_accepted=float(auction.indirect_bidder_accepted) if auction.indirect_bidder_accepted else None,
        high_yield=float(auction.high_yield) if auction.high_yield else None,
        high_discount_rate=float(auction.high_discount_rate) if auction.high_discount_rate else None,
        low_yield=float(auction.low_yield) if auction.low_yield else None,
        median_yield=float(auction.median_yield) if auction.median_yield else None,
        coupon_rate=float(auction.coupon_rate) if auction.coupon_rate else None,
        price_per_100=float(auction.price_per_100) if auction.price_per_100 else None,
        wi_yield=float(auction.wi_yield) if auction.wi_yield else None,
        tail_bps=float(auction.tail_bps) if auction.tail_bps else None,
        auction_result=auction.auction_result,
    )


@router.get("/history/{security_term}", response_model=YieldHistoryResponse)
async def get_yield_history(
    security_term: str,
    years: int = Query(5, ge=1, le=20),
    db: Session = Depends(get_db)
):
    """
    Get yield history for a specific security term
    """
    from datetime import timedelta

    cutoff_date = date.today() - timedelta(days=years * 365)

    auctions = db.query(TreasuryAuction).filter(
        TreasuryAuction.security_term == security_term,
        TreasuryAuction.auction_date >= cutoff_date
    ).order_by(TreasuryAuction.auction_date).all()

    return YieldHistoryResponse(
        security_term=security_term,
        data=[
            YieldHistoryPoint(
                auction_date=a.auction_date,
                high_yield=float(a.high_yield) if a.high_yield else None,
                bid_to_cover_ratio=float(a.bid_to_cover_ratio) if a.bid_to_cover_ratio else None,
                offering_amount=float(a.offering_amount) if a.offering_amount else None,
            )
            for a in auctions
        ]
    )


@router.get("/upcoming", response_model=List[UpcomingAuctionResponse])
async def get_upcoming_auctions(
    db: Session = Depends(get_db)
):
    """
    Get list of upcoming Treasury auctions
    """
    auctions = db.query(TreasuryUpcomingAuction).filter(
        TreasuryUpcomingAuction.auction_date >= date.today()
    ).order_by(TreasuryUpcomingAuction.auction_date).all()

    return [
        UpcomingAuctionResponse(
            upcoming_id=a.upcoming_id,
            cusip=a.cusip,
            security_type=a.security_type,
            security_term=a.security_term,
            auction_date=a.auction_date,
            issue_date=a.issue_date,
            maturity_date=a.maturity_date,
            offering_amount=float(a.offering_amount) if a.offering_amount else None,
            announcement_date=a.announcement_date,
        )
        for a in auctions
    ]


@router.get("/compare")
async def compare_terms(
    terms: str = Query("10-Year,30-Year", description="Comma-separated list of terms"),
    years: int = Query(5, ge=1, le=20),
    db: Session = Depends(get_db)
):
    """
    Compare yield history across multiple terms
    """
    from datetime import timedelta

    term_list = [t.strip() for t in terms.split(',')]
    cutoff_date = date.today() - timedelta(days=years * 365)

    result = {}
    for term in term_list:
        auctions = db.query(TreasuryAuction).filter(
            TreasuryAuction.security_term == term,
            TreasuryAuction.auction_date >= cutoff_date
        ).order_by(TreasuryAuction.auction_date).all()

        result[term] = [
            {
                "auction_date": a.auction_date.isoformat(),
                "high_yield": float(a.high_yield) if a.high_yield else None,
                "bid_to_cover_ratio": float(a.bid_to_cover_ratio) if a.bid_to_cover_ratio else None,
            }
            for a in auctions
        ]

    return {"terms": result, "years": years}


@router.get("/snapshot")
async def get_auction_snapshot(
    db: Session = Depends(get_db)
):
    """
    Get snapshot of latest auction for each term (for dashboard cards)
    """
    terms = ['2-Year', '5-Year', '7-Year', '10-Year', '20-Year', '30-Year']

    result = []
    for term in terms:
        # Get latest auction
        latest = db.query(TreasuryAuction).filter(
            TreasuryAuction.security_term == term
        ).order_by(desc(TreasuryAuction.auction_date)).first()

        # Get previous auction for comparison
        previous = db.query(TreasuryAuction).filter(
            TreasuryAuction.security_term == term
        ).order_by(desc(TreasuryAuction.auction_date)).offset(1).first()

        if latest:
            yield_change = None
            if latest.high_yield and previous and previous.high_yield:
                yield_change = float(latest.high_yield) - float(previous.high_yield)

            result.append({
                "security_term": term,
                "auction_date": latest.auction_date.isoformat(),
                "high_yield": float(latest.high_yield) if latest.high_yield else None,
                "bid_to_cover_ratio": float(latest.bid_to_cover_ratio) if latest.bid_to_cover_ratio else None,
                "offering_amount": float(latest.offering_amount) if latest.offering_amount else None,
                "yield_change": yield_change,
                "coupon_rate": float(latest.coupon_rate) if latest.coupon_rate else None,
            })

    return {"data": result}
