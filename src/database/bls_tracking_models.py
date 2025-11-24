"""
SQLAlchemy models for BLS update tracking and sentinel system
"""
from datetime import datetime
from sqlalchemy import Column, String, SmallInteger, Integer, Date, DateTime, Boolean, Numeric
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class BLSSeriesUpdateStatus(Base):
    """Track update status for individual BLS series"""
    __tablename__ = 'bls_series_update_status'

    series_id = Column(String(30), primary_key=True)
    survey_code = Column(String(5), nullable=False, index=True)
    last_data_year = Column(SmallInteger)
    last_data_period = Column(String(5))
    last_checked_at = Column(DateTime, nullable=False)
    last_updated_at = Column(DateTime)
    is_current = Column(Boolean, nullable=False, default=False, index=True)


class BLSAPIUsageLog(Base):
    """Track daily BLS API usage"""
    __tablename__ = 'bls_api_usage_log'

    log_id = Column(Integer, primary_key=True, autoincrement=True)
    usage_date = Column(Date, nullable=False, index=True)
    requests_used = Column(Integer, nullable=False)
    series_count = Column(Integer, nullable=False)
    survey_code = Column(String(5))
    execution_time = Column(DateTime, nullable=False, default=datetime.now)
    script_name = Column(String(100))


class BLSSurveySentinel(Base):
    """Sentinel series for detecting BLS data freshness"""
    __tablename__ = 'bls_survey_sentinels'

    survey_code = Column(String(5), primary_key=True, nullable=False)
    series_id = Column(String(30), primary_key=True, nullable=False)
    sentinel_order = Column(Integer, nullable=False)
    selection_reason = Column(String(50))

    # Stored values for comparison
    last_value = Column(Numeric(20, 6))
    last_year = Column(SmallInteger)
    last_period = Column(String(5))
    last_footnotes = Column(String(500))

    # Tracking
    last_checked_at = Column(DateTime)
    last_changed_at = Column(DateTime)
    check_count = Column(Integer, nullable=False, default=0)
    change_count = Column(Integer, nullable=False, default=0)

    # Metadata
    created_at = Column(DateTime, nullable=False, default=datetime.now)
    updated_at = Column(DateTime, nullable=False, default=datetime.now, onupdate=datetime.now)

    def __repr__(self):
        return f"<BLSSurveySentinel(survey={self.survey_code}, series={self.series_id}, order={self.sentinel_order})>"


class BLSSurveyFreshness(Base):
    """Track BLS survey data freshness status"""
    __tablename__ = 'bls_survey_freshness'

    survey_code = Column(String(5), primary_key=True, nullable=False)

    # Freshness detection
    last_bls_update_detected = Column(DateTime)
    last_sentinel_check = Column(DateTime, index=True)
    sentinels_changed = Column(Integer, nullable=False, default=0)
    sentinels_total = Column(Integer, nullable=False, default=50)

    # Update status
    needs_full_update = Column(Boolean, nullable=False, default=False, index=True)
    last_full_update_started = Column(DateTime)
    last_full_update_completed = Column(DateTime)
    full_update_in_progress = Column(Boolean, nullable=False, default=False)
    series_updated_count = Column(Integer, nullable=False, default=0)
    series_total_count = Column(Integer, nullable=False, default=0)

    # Statistics
    bls_update_frequency_days = Column(Numeric(5, 2))
    total_checks = Column(Integer, nullable=False, default=0)
    total_updates_detected = Column(Integer, nullable=False, default=0)

    # Metadata
    created_at = Column(DateTime, nullable=False, default=datetime.now)
    updated_at = Column(DateTime, nullable=False, default=datetime.now, onupdate=datetime.now)

    def __repr__(self):
        return f"<BLSSurveyFreshness(survey={self.survey_code}, needs_update={self.needs_full_update})>"
