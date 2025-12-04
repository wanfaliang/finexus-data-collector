"""
Treasury API Schemas

Pydantic models for Treasury API endpoints.
"""
from datetime import datetime, date
from typing import Optional, List, Dict, Any
from decimal import Decimal
from pydantic import BaseModel, Field


# ===================== Freshness Schemas ===================== #

class TreasuryFreshnessResponse(BaseModel):
    """Response model for Treasury data freshness status"""

    data_type: str  # 'auctions', 'upcoming', 'daily_rates'
    latest_data_date: Optional[date] = None
    last_checked_at: Optional[datetime] = None
    last_update_detected: Optional[datetime] = None
    needs_update: bool = False
    update_in_progress: bool = False
    last_update_completed: Optional[datetime] = None
    total_records: int = 0
    total_checks: int = 0
    total_updates: int = 0

    class Config:
        from_attributes = True


class TreasuryFreshnessOverviewResponse(BaseModel):
    """Overview of Treasury data freshness"""

    total_data_types: int
    types_current: int
    types_need_update: int
    types_updating: int
    total_records: int
    data_types: List[TreasuryFreshnessResponse]


# ===================== Collection Run Schemas ===================== #

class TreasuryCollectionRunResponse(BaseModel):
    """Response model for a collection run"""

    run_id: int
    collection_type: str  # 'auctions', 'upcoming', 'daily_rates'
    run_type: str  # 'backfill', 'update', 'refresh'
    security_term: Optional[str] = None
    started_at: datetime
    completed_at: Optional[datetime] = None
    status: str  # 'running', 'completed', 'failed'
    error_message: Optional[str] = None
    records_fetched: int = 0
    records_inserted: int = 0
    records_updated: int = 0
    api_requests_made: int = 0
    duration_seconds: Optional[float] = None

    class Config:
        from_attributes = True


# ===================== Auction Schemas ===================== #

class TreasuryAuctionResponse(BaseModel):
    """Response model for a Treasury auction"""

    auction_id: int
    cusip: str
    auction_date: date
    security_type: str
    security_term: str
    offering_amount: Optional[float] = None
    total_tendered: Optional[float] = None
    total_accepted: Optional[float] = None
    bid_to_cover_ratio: Optional[float] = None
    high_yield: Optional[float] = None
    coupon_rate: Optional[float] = None
    tail_bps: Optional[float] = None
    auction_result: Optional[str] = None  # 'strong', 'neutral', 'weak', 'tailed'

    class Config:
        from_attributes = True


class TreasuryAuctionDetailResponse(TreasuryAuctionResponse):
    """Detailed auction response with all fields"""

    issue_date: Optional[date] = None
    maturity_date: Optional[date] = None
    term_months: Optional[int] = None
    competitive_tendered: Optional[float] = None
    competitive_accepted: Optional[float] = None
    non_competitive_tendered: Optional[float] = None
    non_competitive_accepted: Optional[float] = None
    primary_dealer_accepted: Optional[float] = None
    direct_bidder_accepted: Optional[float] = None
    indirect_bidder_accepted: Optional[float] = None
    high_discount_rate: Optional[float] = None
    low_yield: Optional[float] = None
    median_yield: Optional[float] = None
    wi_yield: Optional[float] = None
    price_per_100: Optional[float] = None
    auction_score: Optional[float] = None


class TreasuryAuctionSummaryResponse(BaseModel):
    """Summary statistics for Treasury auctions"""

    total_auctions: int
    auctions_by_term: Dict[str, int]
    earliest_auction: Optional[date] = None
    latest_auction: Optional[date] = None
    recent_results: Dict[str, int] = {}  # Count by result type
    avg_bid_to_cover_30d: Optional[float] = None


# ===================== Upcoming Auction Schemas ===================== #

class TreasuryUpcomingAuctionResponse(BaseModel):
    """Response model for an upcoming auction"""

    upcoming_id: int
    cusip: Optional[str] = None
    security_type: str
    security_term: str
    auction_date: date
    issue_date: Optional[date] = None
    maturity_date: Optional[date] = None
    offering_amount: Optional[float] = None
    is_processed: bool = False

    class Config:
        from_attributes = True


# ===================== Daily Rate Schemas ===================== #

class TreasuryDailyRateResponse(BaseModel):
    """Response model for daily Treasury rates"""

    rate_date: date
    yield_1m: Optional[float] = None
    yield_3m: Optional[float] = None
    yield_6m: Optional[float] = None
    yield_1y: Optional[float] = None
    yield_2y: Optional[float] = None
    yield_5y: Optional[float] = None
    yield_7y: Optional[float] = None
    yield_10y: Optional[float] = None
    yield_20y: Optional[float] = None
    yield_30y: Optional[float] = None
    spread_2s10s: Optional[float] = None
    spread_2s30s: Optional[float] = None

    class Config:
        from_attributes = True


class TreasuryYieldCurveResponse(BaseModel):
    """Response model for yield curve snapshot"""

    rate_date: date
    maturities: List[str]  # ['1M', '3M', '6M', '1Y', '2Y', '5Y', '7Y', '10Y', '20Y', '30Y']
    yields: List[Optional[float]]


# ===================== Stats Schemas ===================== #

class TreasuryStatsResponse(BaseModel):
    """Overall Treasury data statistics"""

    total_auctions: int
    total_upcoming_auctions: int
    total_daily_rates: int
    earliest_auction: Optional[date] = None
    latest_auction: Optional[date] = None
    collection_runs_last_7d: int = 0


# ===================== Auction Analysis Schemas ===================== #

class AuctionAnalysisResponse(BaseModel):
    """Analysis of a single auction"""

    auction_id: int
    security_term: str
    auction_date: date

    # Key metrics
    bid_to_cover_ratio: Optional[float] = None
    high_yield: Optional[float] = None
    tail_bps: Optional[float] = None
    auction_result: Optional[str] = None

    # Comparison to historical
    btc_percentile: Optional[float] = None  # Where this BTC falls vs history
    yield_vs_prior: Optional[float] = None  # Change from prior auction

    # Investor breakdown
    primary_dealer_pct: Optional[float] = None
    direct_bidder_pct: Optional[float] = None
    indirect_bidder_pct: Optional[float] = None


class AuctionTrendResponse(BaseModel):
    """Trend data for auction metrics over time"""

    security_term: str
    auctions: List[Dict[str, Any]]  # Time series data
    avg_bid_to_cover: float
    avg_tail_bps: Optional[float] = None
    strong_auction_pct: float
    weak_auction_pct: float
