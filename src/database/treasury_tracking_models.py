"""
Treasury Update Tracking and Freshness Models

Similar to BEA/BLS tracking, provides:
- API usage tracking
- Collection run history
- Freshness monitoring

Author: FinExus Data Collector
Created: 2025-12-03
"""
from datetime import datetime, UTC
from sqlalchemy import (
    Column, Integer, String, SmallInteger, Date, DateTime,
    Boolean, Numeric, Text, Index
)
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class TreasuryAPIUsageLog(Base):
    """Track Treasury Fiscal Data API usage"""
    __tablename__ = 'treasury_api_usage_log'

    log_id = Column(Integer, primary_key=True, autoincrement=True)
    usage_date = Column(Date, nullable=False, index=True)
    usage_hour = Column(DateTime, nullable=False, index=True)

    # Usage counts
    requests_count = Column(Integer, nullable=False, default=0)
    data_bytes = Column(Integer, nullable=False, default=0)
    error_count = Column(Integer, nullable=False, default=0)

    # Request details
    endpoint_name = Column(String(100), index=True)  # 'auctions_query', 'upcoming_auctions', etc.

    # Response info
    http_status = Column(SmallInteger)
    response_time_ms = Column(Integer)
    records_returned = Column(Integer)

    # Tracking
    script_name = Column(String(100))
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_treasury_api_usage_date_hour', 'usage_date', 'usage_hour'),
    )

    def __repr__(self):
        return f"<TreasuryAPIUsageLog(date={self.usage_date}, endpoint={self.endpoint_name})>"


class TreasuryDataFreshness(Base):
    """Track Treasury data freshness and update status"""
    __tablename__ = 'treasury_data_freshness'

    data_type = Column(String(50), primary_key=True)  # 'auctions', 'daily_rates', 'upcoming'

    # Latest data in our database
    latest_data_date = Column(Date)
    latest_auction_term = Column(String(20))  # For auctions: '10-Year', etc.

    # Freshness tracking
    last_checked_at = Column(DateTime, index=True)
    last_update_detected = Column(DateTime)

    # Update status
    needs_update = Column(Boolean, nullable=False, default=False, index=True)
    update_in_progress = Column(Boolean, nullable=False, default=False)
    last_update_started = Column(DateTime)
    last_update_completed = Column(DateTime)

    # Statistics
    total_records = Column(Integer, nullable=False, default=0)

    # Check statistics
    total_checks = Column(Integer, nullable=False, default=0)
    total_updates = Column(Integer, nullable=False, default=0)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<TreasuryDataFreshness(type={self.data_type}, needs_update={self.needs_update})>"


class TreasuryCollectionRun(Base):
    """Track individual Treasury data collection runs"""
    __tablename__ = 'treasury_collection_runs'

    run_id = Column(Integer, primary_key=True, autoincrement=True)

    # What was collected
    collection_type = Column(String(50), nullable=False, index=True)  # 'auctions', 'upcoming', 'daily_rates'
    run_type = Column(String(50), nullable=False)  # 'backfill', 'update', 'scheduled'

    # Parameters
    security_term = Column(String(20))  # '2-Year', '10-Year', etc.
    start_date = Column(Date)
    end_date = Column(Date)
    year_spec = Column(String(20))  # 'ALL', 'LAST5', 'LAST10'

    # Timing
    started_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
    completed_at = Column(DateTime)
    duration_seconds = Column(Numeric(10, 2))

    # Status
    status = Column(String(20), nullable=False, default='running', index=True)  # 'running', 'completed', 'failed', 'partial'
    error_message = Column(Text)

    # Results
    records_fetched = Column(Integer, nullable=False, default=0)
    records_inserted = Column(Integer, nullable=False, default=0)
    records_updated = Column(Integer, nullable=False, default=0)
    api_requests_made = Column(Integer, nullable=False, default=0)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_treasury_runs_type_status', 'collection_type', 'status'),
        Index('ix_treasury_runs_started', 'started_at'),
    )

    def __repr__(self):
        return f"<TreasuryCollectionRun(id={self.run_id}, type={self.collection_type}, status={self.status})>"
