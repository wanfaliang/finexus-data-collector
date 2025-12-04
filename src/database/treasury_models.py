"""
Treasury Database Models

Models for U.S. Treasury auction data and related market reactions.
Data source: Fiscal Data API (https://fiscaldata.treasury.gov/api-documentation/)

Key datasets:
- Treasury Securities Auctions (2Y, 5Y, 7Y, 10Y, 20Y, 30Y Notes/Bonds)
- Auction results with bid-to-cover, high yield, tails
- Market reaction tracking (yield moves, equity moves)

Author: FinExus Data Collector
Created: 2025-12-03
"""
from datetime import datetime, UTC
from sqlalchemy import (
    Column, Integer, String, Numeric, Date, DateTime,
    Boolean, Text, ForeignKey, Index, UniqueConstraint, SmallInteger
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import JSONB

Base = declarative_base()


# ==================== AUCTION TABLES ====================

class TreasurySecurityType(Base):
    """Reference table for Treasury security types"""
    __tablename__ = 'treasury_security_types'

    security_type = Column(String(20), primary_key=True)  # 'Note', 'Bond', 'Bill', 'TIPS', 'FRN'
    description = Column(String(200), nullable=False)
    is_active = Column(Boolean, default=True)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<TreasurySecurityType(type='{self.security_type}')>"


class TreasuryAuction(Base):
    """
    Treasury auction master table

    Stores auction metadata and results for Notes/Bonds (2Y, 5Y, 7Y, 10Y, 20Y, 30Y).
    Primary data source: Fiscal Data API auctions_query endpoint
    """
    __tablename__ = 'treasury_auctions'

    # Primary key - auto-increment for simplicity
    auction_id = Column(Integer, primary_key=True, autoincrement=True)

    # Auction identification
    cusip = Column(String(9), nullable=False, index=True)  # 9-character CUSIP
    auction_date = Column(Date, nullable=False, index=True)

    # Security details
    security_type = Column(String(20), nullable=False, index=True)  # 'Note', 'Bond'
    security_term = Column(String(20), nullable=False, index=True)  # '2-Year', '5-Year', etc.
    term_months = Column(SmallInteger)  # 24, 60, 84, 120, 240, 360

    # Dates
    issue_date = Column(Date)
    maturity_date = Column(Date)

    # Size and demand metrics
    offering_amount = Column(Numeric(18, 2))  # Amount offered (in millions)
    total_tendered = Column(Numeric(18, 2))  # Total bids received
    total_accepted = Column(Numeric(18, 2))  # Amount accepted/awarded
    bid_to_cover_ratio = Column(Numeric(6, 3))  # total_tendered / total_accepted

    # Competitive bidding breakdown
    competitive_tendered = Column(Numeric(18, 2))
    competitive_accepted = Column(Numeric(18, 2))
    non_competitive_tendered = Column(Numeric(18, 2))
    non_competitive_accepted = Column(Numeric(18, 2))

    # Investor categories (if available)
    primary_dealer_tendered = Column(Numeric(18, 2))
    primary_dealer_accepted = Column(Numeric(18, 2))
    direct_bidder_tendered = Column(Numeric(18, 2))
    direct_bidder_accepted = Column(Numeric(18, 2))
    indirect_bidder_accepted = Column(Numeric(18, 2))

    # Yield results
    high_yield = Column(Numeric(8, 5))  # Stop-out yield (auction clearing yield)
    high_discount_rate = Column(Numeric(8, 5))
    low_yield = Column(Numeric(8, 5))
    median_yield = Column(Numeric(8, 5))

    # Coupon and price
    coupon_rate = Column(Numeric(8, 5))  # Coupon rate for the security
    price_per_100 = Column(Numeric(12, 6))  # Price per $100 face value

    # When-Issued (WI) data - from market data (may be populated separately)
    wi_yield = Column(Numeric(8, 5))  # When-issued yield before auction

    # Computed metrics
    tail_bps = Column(Numeric(8, 3))  # (high_yield - wi_yield) * 100
    auction_score = Column(Numeric(8, 3))  # Composite strength score

    # Classification
    auction_result = Column(String(20))  # 'strong', 'neutral', 'weak', 'tailed'

    # Data source tracking
    source_endpoint = Column(String(100))  # API endpoint used
    raw_json = Column(JSONB)  # Original API response

    # Timestamps
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        UniqueConstraint('cusip', 'auction_date', name='uq_treasury_auction_cusip_date'),
        Index('ix_treasury_auctions_date_term', 'auction_date', 'security_term'),
        Index('ix_treasury_auctions_term', 'security_term'),
        Index('ix_treasury_auctions_result', 'auction_result'),
    )

    def __repr__(self):
        return f"<TreasuryAuction(date={self.auction_date}, term='{self.security_term}', yield={self.high_yield})>"


class TreasuryAuctionReaction(Base):
    """
    Market reaction around Treasury auctions

    Tracks yield moves, equity moves, and other market indicators
    at different time buckets relative to the auction.
    """
    __tablename__ = 'treasury_auction_reactions'

    reaction_id = Column(Integer, primary_key=True, autoincrement=True)
    auction_id = Column(Integer, ForeignKey('treasury_auctions.auction_id'), nullable=False, index=True)

    # Time bucket relative to auction
    # 'D-1' = day before, 'D0' = auction day, 'D+1' = day after
    # Or intraday: '0m', '5m', '30m', 'EOD'
    time_bucket = Column(String(10), nullable=False)

    # Treasury yields at this time
    yield_2y = Column(Numeric(8, 5))
    yield_5y = Column(Numeric(8, 5))
    yield_7y = Column(Numeric(8, 5))
    yield_10y = Column(Numeric(8, 5))
    yield_20y = Column(Numeric(8, 5))
    yield_30y = Column(Numeric(8, 5))

    # Yield changes from previous bucket or reference point
    delta_yield_2y = Column(Numeric(8, 5))
    delta_yield_5y = Column(Numeric(8, 5))
    delta_yield_7y = Column(Numeric(8, 5))
    delta_yield_10y = Column(Numeric(8, 5))
    delta_yield_20y = Column(Numeric(8, 5))
    delta_yield_30y = Column(Numeric(8, 5))

    # Equity market
    spx_price = Column(Numeric(12, 4))
    spx_return = Column(Numeric(8, 5))  # Percent return
    nasdaq_price = Column(Numeric(12, 4))
    nasdaq_return = Column(Numeric(8, 5))

    # Volatility
    vix_level = Column(Numeric(8, 4))
    vix_change = Column(Numeric(8, 4))

    # Mortgage rates (for longer-term auctions)
    mortgage_30y = Column(Numeric(8, 5))

    # Dollar index
    dxy_level = Column(Numeric(10, 4))
    dxy_change = Column(Numeric(8, 5))

    # Composite risk score
    risk_score = Column(Numeric(8, 4))

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        UniqueConstraint('auction_id', 'time_bucket', name='uq_treasury_reaction_auction_bucket'),
        Index('ix_treasury_reactions_time', 'time_bucket'),
    )

    def __repr__(self):
        return f"<TreasuryAuctionReaction(auction_id={self.auction_id}, bucket='{self.time_bucket}')>"


class TreasuryUpcomingAuction(Base):
    """
    Upcoming Treasury auctions (from upcoming_auctions endpoint)

    Used for calendar/scheduling and pre-auction analysis.
    Records are cleared/updated when auctions complete.
    """
    __tablename__ = 'treasury_upcoming_auctions'

    upcoming_id = Column(Integer, primary_key=True, autoincrement=True)

    # Auction details
    cusip = Column(String(9), index=True)
    security_type = Column(String(20), nullable=False)
    security_term = Column(String(20), nullable=False, index=True)

    # Dates
    auction_date = Column(Date, nullable=False, index=True)
    issue_date = Column(Date)
    maturity_date = Column(Date)

    # Expected size
    offering_amount = Column(Numeric(18, 2))

    # Announcement details
    announcement_date = Column(Date)

    # Status
    is_processed = Column(Boolean, default=False, index=True)  # True once auction results are captured

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_treasury_upcoming_date_term', 'auction_date', 'security_term'),
    )

    def __repr__(self):
        return f"<TreasuryUpcomingAuction(date={self.auction_date}, term='{self.security_term}')>"


# ==================== DAILY TREASURY RATES ====================

class TreasuryDailyRate(Base):
    """
    Daily Treasury yield curve rates

    Constant maturity Treasury (CMT) rates from Treasury or FRED.
    Used for tracking yield moves around auctions and general market monitoring.
    """
    __tablename__ = 'treasury_daily_rates'

    rate_id = Column(Integer, primary_key=True, autoincrement=True)
    rate_date = Column(Date, nullable=False, index=True)

    # Treasury yields by maturity
    yield_1m = Column(Numeric(8, 5))
    yield_3m = Column(Numeric(8, 5))
    yield_6m = Column(Numeric(8, 5))
    yield_1y = Column(Numeric(8, 5))
    yield_2y = Column(Numeric(8, 5))
    yield_3y = Column(Numeric(8, 5))
    yield_5y = Column(Numeric(8, 5))
    yield_7y = Column(Numeric(8, 5))
    yield_10y = Column(Numeric(8, 5))
    yield_20y = Column(Numeric(8, 5))
    yield_30y = Column(Numeric(8, 5))

    # Spread calculations
    spread_2s10s = Column(Numeric(8, 5))  # 10Y - 2Y spread
    spread_2s30s = Column(Numeric(8, 5))  # 30Y - 2Y spread
    spread_5s30s = Column(Numeric(8, 5))  # 30Y - 5Y spread

    # Real yields (TIPS)
    real_yield_5y = Column(Numeric(8, 5))
    real_yield_10y = Column(Numeric(8, 5))
    real_yield_30y = Column(Numeric(8, 5))

    # Breakeven inflation
    breakeven_5y = Column(Numeric(8, 5))
    breakeven_10y = Column(Numeric(8, 5))

    # Data source
    source = Column(String(50))  # 'treasury_gov', 'fred', etc.

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        UniqueConstraint('rate_date', name='uq_treasury_daily_rates_date'),
    )

    def __repr__(self):
        return f"<TreasuryDailyRate(date={self.rate_date}, 10y={self.yield_10y})>"


# ==================== AUCTION CALENDAR ====================

class TreasuryAuctionSchedule(Base):
    """
    Treasury auction schedule/calendar

    Regular auction schedule for different security types.
    Used for planning and forecasting.
    """
    __tablename__ = 'treasury_auction_schedule'

    schedule_id = Column(Integer, primary_key=True, autoincrement=True)

    security_term = Column(String(20), nullable=False, index=True)

    # Typical auction schedule
    auction_frequency = Column(String(50))  # 'Monthly', 'Weekly', 'Quarterly'
    typical_auction_day = Column(String(50))  # 'Second Tuesday', 'Every Monday'
    typical_settlement = Column(String(50))  # 'T+1', 'Same day'

    # Size information
    typical_size_min = Column(Numeric(18, 2))  # Typical minimum offering
    typical_size_max = Column(Numeric(18, 2))  # Typical maximum offering

    # Notes
    notes = Column(Text)

    is_active = Column(Boolean, default=True)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<TreasuryAuctionSchedule(term='{self.security_term}', freq='{self.auction_frequency}')>"
