"""
SQLAlchemy models for BEA update tracking and freshness detection system

Similar to BLS tracking, this provides:
- API usage tracking to stay within rate limits
- Dataset freshness monitoring
- Update status tracking for incremental updates

Author: FinExus Data Collector
Created: 2025-11-26
"""
from datetime import datetime, UTC
from sqlalchemy import (
    Column, Integer, String, SmallInteger, Date, DateTime,
    Boolean, Numeric, Text, Index
)
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class BEAAPIUsageLog(Base):
    """Track BEA API usage for rate limit management

    BEA limits: 100 requests/min, 100MB data/min, 30 errors/min
    Violation results in 1-hour lockout
    """
    __tablename__ = 'bea_api_usage_log'

    log_id = Column(Integer, primary_key=True, autoincrement=True)
    usage_date = Column(Date, nullable=False, index=True)
    usage_minute = Column(DateTime, nullable=False, index=True)  # Track per-minute for rate limiting

    # Usage counts
    requests_count = Column(Integer, nullable=False, default=0)
    data_bytes = Column(Integer, nullable=False, default=0)  # Bytes retrieved
    error_count = Column(Integer, nullable=False, default=0)

    # Request details
    dataset_name = Column(String(50), index=True)  # 'NIPA', 'Regional', etc.
    method_name = Column(String(50))  # 'GetData', 'GetParameterList', etc.

    # Response info
    http_status = Column(SmallInteger)
    response_time_ms = Column(Integer)

    # Tracking
    script_name = Column(String(100))
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bea_api_usage_date_minute', 'usage_date', 'usage_minute'),
        Index('ix_bea_api_usage_dataset', 'dataset_name'),
    )

    def __repr__(self):
        return f"<BEAAPIUsageLog(date={self.usage_date}, dataset={self.dataset_name}, requests={self.requests_count})>"


class BEADatasetFreshness(Base):
    """Track BEA dataset freshness and update status

    BEA releases data on schedules (monthly GDP estimates, quarterly updates, etc.)
    This tracks when we last checked and when updates were detected.
    """
    __tablename__ = 'bea_dataset_freshness'

    dataset_name = Column(String(50), primary_key=True)  # 'NIPA', 'Regional'

    # Latest data period in our database
    latest_data_year = Column(SmallInteger)
    latest_data_period = Column(String(10))  # 'Q1', 'Q2', etc. or 'A' for annual

    # Freshness tracking
    last_checked_at = Column(DateTime, index=True)
    last_bea_update_detected = Column(DateTime)  # When BEA released new data

    # Update status
    needs_update = Column(Boolean, nullable=False, default=False, index=True)
    update_in_progress = Column(Boolean, nullable=False, default=False)
    last_update_started = Column(DateTime)
    last_update_completed = Column(DateTime)

    # Statistics
    tables_count = Column(Integer, nullable=False, default=0)
    series_count = Column(Integer, nullable=False, default=0)
    data_points_count = Column(Integer, nullable=False, default=0)

    # Check statistics
    total_checks = Column(Integer, nullable=False, default=0)
    total_updates_detected = Column(Integer, nullable=False, default=0)

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<BEADatasetFreshness(dataset={self.dataset_name}, needs_update={self.needs_update})>"


class BEATableUpdateStatus(Base):
    """Track update status for individual BEA tables

    For NIPA: tracks tables like T10101, T20600
    For Regional: tracks tables like CAINC1, SAGDP1
    """
    __tablename__ = 'bea_table_update_status'

    dataset_name = Column(String(50), primary_key=True)
    table_name = Column(String(20), primary_key=True)

    # Latest data in our database
    last_data_year = Column(SmallInteger)
    last_data_period = Column(String(10))

    # BEA's reported data range
    bea_first_year = Column(SmallInteger)
    bea_last_year = Column(SmallInteger)

    # Tracking timestamps
    last_checked_at = Column(DateTime, nullable=False)
    last_updated_at = Column(DateTime)

    # Status
    is_current = Column(Boolean, nullable=False, default=False, index=True)
    rows_in_table = Column(Integer, nullable=False, default=0)

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bea_table_status_dataset', 'dataset_name'),
        Index('ix_bea_table_status_current', 'is_current'),
    )

    def __repr__(self):
        return f"<BEATableUpdateStatus(dataset={self.dataset_name}, table={self.table_name}, current={self.is_current})>"


class BEASentinelSeries(Base):
    """Sentinel series for detecting BEA data freshness

    Key series that update regularly and indicate new data releases.
    For NIPA: GDP headline numbers
    For Regional: State GDP totals, Personal Income totals
    """
    __tablename__ = 'bea_sentinel_series'

    dataset_name = Column(String(50), primary_key=True)
    sentinel_id = Column(String(100), primary_key=True)  # Composite key for the data point
    sentinel_order = Column(Integer, nullable=False)

    # Identification
    table_name = Column(String(20), nullable=False)
    line_code = Column(SmallInteger)  # For Regional
    geo_fips = Column(String(10))  # For Regional (national level = '00000')
    series_code = Column(String(50))  # For NIPA
    industry_code = Column(String(20))  # For GDPbyIndustry
    frequency = Column(String(1))  # 'A', 'Q', 'M' - for datasets with multiple frequencies

    # Selection reason
    selection_reason = Column(String(100))  # 'GDP headline', 'State total', etc.

    # Stored values for comparison
    last_value = Column(Numeric(20, 6))
    last_year = Column(SmallInteger)
    last_period = Column(String(10))

    # Tracking
    last_checked_at = Column(DateTime)
    has_changed = Column(Boolean, nullable=False, default=False)  # True if last check found a difference

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bea_sentinel_dataset', 'dataset_name'),
        Index('ix_bea_sentinel_order', 'dataset_name', 'sentinel_order'),
    )

    def __repr__(self):
        return f"<BEASentinelSeries(dataset={self.dataset_name}, id={self.sentinel_id}, order={self.sentinel_order})>"


class BEACollectionRun(Base):
    """Track collection/update runs for BEA data

    Logs each collection attempt with status and statistics.
    """
    __tablename__ = 'bea_collection_runs'

    run_id = Column(Integer, primary_key=True, autoincrement=True)

    # Run identification
    dataset_name = Column(String(50), nullable=False, index=True)
    run_type = Column(String(20), nullable=False)  # 'backfill', 'update', 'refresh'

    # Run parameters (dataset-specific)
    frequency = Column(String(1))  # 'A' (annual), 'Q' (quarterly), 'M' (monthly) - for NIPA/GDPbyIndustry
    geo_scope = Column(String(20))  # 'STATE', 'COUNTY', 'MSA' - for Regional
    year_spec = Column(String(50))  # 'ALL', 'LAST5', 'LAST10', or specific years like '2020,2021,2022'

    # Timing
    started_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
    completed_at = Column(DateTime)

    # Status
    status = Column(String(20), nullable=False, default='running')  # 'running', 'completed', 'failed', 'partial'
    error_message = Column(Text)

    # Statistics
    tables_processed = Column(Integer, nullable=False, default=0)
    series_processed = Column(Integer, nullable=False, default=0)
    data_points_inserted = Column(Integer, nullable=False, default=0)
    data_points_updated = Column(Integer, nullable=False, default=0)
    api_requests_made = Column(Integer, nullable=False, default=0)

    # Parameters used (legacy - kept for backward compatibility)
    start_year = Column(SmallInteger)
    end_year = Column(SmallInteger)
    tables_filter = Column(Text)  # JSON list of specific tables if filtered

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bea_runs_dataset_status', 'dataset_name', 'status'),
        Index('ix_bea_runs_started', 'started_at'),
    )

    def __repr__(self):
        return f"<BEACollectionRun(id={self.run_id}, dataset={self.dataset_name}, status={self.status})>"


class BEAReleaseSchedule(Base):
    """Track BEA data release schedule

    BEA has regular release schedules for different data:
    - GDP: Monthly advance, second, third estimates
    - Regional: Annual releases with periodic updates
    """
    __tablename__ = 'bea_release_schedule'

    schedule_id = Column(Integer, primary_key=True, autoincrement=True)

    dataset_name = Column(String(50), nullable=False, index=True)
    release_name = Column(String(100), nullable=False)  # 'GDP Advance Estimate', 'State Personal Income'

    # Schedule info
    release_date = Column(Date, nullable=False, index=True)
    release_time = Column(String(10))  # '08:30 ET' typically

    # What's being released
    data_period = Column(String(20))  # 'Q3 2025', '2024 Annual'
    tables_affected = Column(Text)  # JSON list of table names

    # Status
    is_processed = Column(Boolean, nullable=False, default=False)
    processed_at = Column(DateTime)

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bea_schedule_date', 'release_date'),
        Index('ix_bea_schedule_dataset_date', 'dataset_name', 'release_date'),
    )

    def __repr__(self):
        return f"<BEAReleaseSchedule(dataset={self.dataset_name}, release={self.release_name}, date={self.release_date})>"
