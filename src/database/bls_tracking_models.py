"""
SQLAlchemy models for BLS update tracking

Update Cycle System:
- BLSUpdateCycle: Tracks each update operation for a survey
- BLSUpdateCycleSeries: Tracks which series have been updated in a cycle
- BLSAPIUsageLog: Tracks daily API quota usage

Freshness checking is done on-the-fly by comparing API data with database,
no persistent sentinel tracking needed.
"""
from datetime import datetime
from sqlalchemy import Column, String, Integer, Date, DateTime, Boolean, ForeignKey, text
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class BLSUpdateCycle(Base):
    """
    Track update cycles for each survey.

    An update cycle represents a single update operation that may span multiple days
    due to API quota limits. Only one cycle per survey can be current at a time.

    - Soft Update: Uses existing current cycle, continues where left off
    - Force Update: Creates new cycle, marks old one not current, starts fresh
    """
    __tablename__ = 'bls_update_cycles'

    id = Column(Integer, primary_key=True, autoincrement=True)
    survey_code = Column(String(5), nullable=False, index=True)
    is_current = Column(Boolean, nullable=False, default=True, index=True)
    is_running = Column(Boolean, nullable=False, default=False, server_default=text('false'), index=True)  # True while update is actively running

    # Timing
    started_at = Column(DateTime, nullable=False, default=datetime.now)
    completed_at = Column(DateTime, nullable=True)  # NULL if in progress

    # Progress tracking
    total_series = Column(Integer, nullable=False, default=0)
    series_updated = Column(Integer, nullable=False, default=0)
    requests_used = Column(Integer, nullable=False, default=0)

    # Relationship to series
    series = relationship("BLSUpdateCycleSeries", back_populates="cycle", cascade="all, delete-orphan")

    def __repr__(self):
        status = "current" if self.is_current else "completed" if self.completed_at else "abandoned"
        return f"<BLSUpdateCycle(id={self.id}, survey={self.survey_code}, status={status}, progress={self.series_updated}/{self.total_series})>"


class BLSUpdateCycleSeries(Base):
    """
    Track which series have been updated in a cycle.

    A series is considered "done" for a cycle if it has a record here.
    To find series needing update: active series NOT IN this table for current cycle.
    """
    __tablename__ = 'bls_update_cycle_series'

    cycle_id = Column(Integer, ForeignKey('bls_update_cycles.id', ondelete='CASCADE'), primary_key=True)
    series_id = Column(String(30), primary_key=True)
    updated_at = Column(DateTime, nullable=False, default=datetime.now)

    # Relationship to cycle
    cycle = relationship("BLSUpdateCycle", back_populates="series")

    def __repr__(self):
        return f"<BLSUpdateCycleSeries(cycle={self.cycle_id}, series={self.series_id})>"


class BLSAPIUsageLog(Base):
    """Track daily BLS API usage for quota management"""
    __tablename__ = 'bls_api_usage_log'

    log_id = Column(Integer, primary_key=True, autoincrement=True)
    usage_date = Column(Date, nullable=False, index=True)
    requests_used = Column(Integer, nullable=False)
    series_count = Column(Integer, nullable=False)
    survey_code = Column(String(5))
    execution_time = Column(DateTime, nullable=False, default=datetime.now)
    script_name = Column(String(100))

    def __repr__(self):
        return f"<BLSAPIUsageLog(date={self.usage_date}, requests={self.requests_used}, survey={self.survey_code})>"
