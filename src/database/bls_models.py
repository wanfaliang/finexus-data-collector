"""
BLS (Bureau of Labor Statistics) Database Models

Separate models file for BLS data to keep it isolated and maintainable.
Covers all BLS surveys: AP, CPI, CES, LA, PPI, etc.

Author: FinExus Data Collector
Created: 2025-11-15
"""
from datetime import datetime, UTC
from sqlalchemy import (
    Column, Integer, String, Numeric, Date, DateTime,
    Boolean, Text, ForeignKey, Index, UniqueConstraint, SmallInteger
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


# ==================== REFERENCE TABLES ====================

class BLSSurvey(Base):
    """BLS Survey catalog - master list of all BLS surveys"""
    __tablename__ = 'bls_surveys'

    survey_code = Column(String(10), primary_key=True)  # 'AP', 'CU', 'LA', 'CE', etc.
    survey_name = Column(String(255), nullable=False)
    description = Column(Text)
    has_seasonal_adjustment = Column(Boolean, default=True)
    has_annual_averages = Column(Boolean, default=False)
    has_calculations = Column(Boolean, default=False)

    # URLs for documentation
    doc_url = Column(String(500))
    series_id_format_url = Column(String(500))

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<BLSSurvey(code='{self.survey_code}', name='{self.survey_name}')>"


class BLSArea(Base):
    """Geographic area codes for BLS data"""
    __tablename__ = 'bls_areas'

    area_code = Column(String(20), primary_key=True)  # Extended for LA survey codes like 'ST0100000000000'
    area_name = Column(String(255), nullable=False)
    area_type = Column(String(50))  # 'National', 'Region', 'City', 'State'
    parent_area_code = Column(String(10))  # For hierarchical relationships

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        Index('ix_bls_areas_type', 'area_type'),
    )

    def __repr__(self):
        return f"<BLSArea(code='{self.area_code}', name='{self.area_name}')>"


class BLSPeriod(Base):
    """Period codes for BLS data"""
    __tablename__ = 'bls_periods'

    period_code = Column(String(5), primary_key=True)  # 'M01'-'M13', 'Q01'-'Q05', 'A01', etc.
    period_abbr = Column(String(10))  # 'JAN', 'Q1', etc.
    period_name = Column(String(50))  # 'January', 'Quarter 1', 'Annual'
    period_type = Column(String(20))  # 'MONTHLY', 'QUARTERLY', 'ANNUAL'
    sort_order = Column(SmallInteger)  # For ordering

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<BLSPeriod(code='{self.period_code}', name='{self.period_name}')>"


# ==================== AP (AVERAGE PRICE) SPECIFIC TABLES ====================

class APItem(Base):
    """Item catalog for Average Price survey"""
    __tablename__ = 'bls_ap_items'

    item_code = Column(String(10), primary_key=True)  # '701111', '703111', etc.
    item_name = Column(String(500), nullable=False)
    category = Column(String(100))  # 'Food', 'Fuel', 'Gasoline', etc.
    unit = Column(String(50))  # 'per lb.', 'per gallon', etc.

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<APItem(code='{self.item_code}', name='{self.item_name[:50]}')>"


class APSeries(Base):
    """AP Series catalog - metadata for each time series"""
    __tablename__ = 'bls_ap_series'

    series_id = Column(String(20), primary_key=True)  # 'APU0000701111', etc.

    # Decomposed series ID components
    seasonal_code = Column(String(1))  # 'S' or 'U'
    area_code = Column(String(10), ForeignKey('bls_areas.area_code'))
    item_code = Column(String(10), ForeignKey('bls_ap_items.item_code'))

    # Metadata
    series_title = Column(Text, nullable=False)
    begin_year = Column(SmallInteger)
    begin_period = Column(String(5))
    end_year = Column(SmallInteger)
    end_period = Column(String(5))

    # Status
    is_active = Column(Boolean, default=True)  # False if series discontinued

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_ap_series_area', 'area_code'),
        Index('ix_bls_ap_series_item', 'item_code'),
        Index('ix_bls_ap_series_active', 'is_active'),
    )

    def __repr__(self):
        return f"<APSeries(id='{self.series_id}', title='{self.series_title[:50]}')>"


class APData(Base):
    """AP Time series data - actual price observations"""
    __tablename__ = 'bls_ap_data'

    # Composite primary key
    series_id = Column(String(20), ForeignKey('bls_ap_series.series_id'), primary_key=True)
    year = Column(SmallInteger, primary_key=True, nullable=False)
    period = Column(String(5), primary_key=True, nullable=False)  # 'M01'-'M13'

    # Data
    value = Column(Numeric(20, 6))  # Price value (can be NULL for missing data)
    footnote_codes = Column(String(50))  # Comma-separated footnote codes

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_ap_data_year_period', 'year', 'period'),
        Index('ix_bls_ap_data_series_year', 'series_id', 'year'),
    )

    def __repr__(self):
        return f"<APData(series='{self.series_id}', date={self.year}-{self.period}, value={self.value})>"


# ==================== CU (CONSUMER PRICE INDEX) SPECIFIC TABLES ====================

class BLSPeriodicity(Base):
    """Periodicity codes for BLS series (monthly, semi-annual, etc.)"""
    __tablename__ = 'bls_periodicity'

    periodicity_code = Column(String(5), primary_key=True)  # 'R', 'S', etc.
    periodicity_name = Column(String(100))  # 'Regular', 'Semi-annual'
    description = Column(Text)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<BLSPeriodicity(code='{self.periodicity_code}', name='{self.periodicity_name}')>"


class CUArea(Base):
    """Geographic area codes for Consumer Price Index - All Urban Consumers"""
    __tablename__ = 'bls_cu_areas'

    area_code = Column(String(10), primary_key=True)  # '0000', '0100', etc.
    area_name = Column(String(255), nullable=False)
    display_level = Column(SmallInteger)  # Hierarchy depth (0 = top level)
    selectable = Column(String(1))  # 'T' or 'F'
    sort_sequence = Column(SmallInteger)  # Display ordering

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_cu_areas_display_level', 'display_level'),
        Index('ix_bls_cu_areas_selectable', 'selectable'),
    )

    def __repr__(self):
        return f"<CUArea(code='{self.area_code}', name='{self.area_name[:50]}')>"


class CUItem(Base):
    """Item catalog for Consumer Price Index survey"""
    __tablename__ = 'bls_cu_items'

    item_code = Column(String(20), primary_key=True)  # 'SA0', 'SAH', etc.
    item_name = Column(String(500), nullable=False)
    display_level = Column(SmallInteger)  # Hierarchy depth (0 = top level)
    selectable = Column(String(1))  # 'T' or 'F'
    sort_sequence = Column(SmallInteger)  # Display ordering

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_cu_items_display_level', 'display_level'),
        Index('ix_bls_cu_items_selectable', 'selectable'),
    )

    def __repr__(self):
        return f"<CUItem(code='{self.item_code}', name='{self.item_name[:50]}')>"


class CUSeries(Base):
    """CU Series catalog - metadata for each CPI time series"""
    __tablename__ = 'bls_cu_series'

    series_id = Column(String(20), primary_key=True)  # 'CUSR0000SA0', etc.

    # Decomposed series ID components
    area_code = Column(String(10), ForeignKey('bls_cu_areas.area_code'))
    item_code = Column(String(20), ForeignKey('bls_cu_items.item_code'))
    seasonal_code = Column(String(1))  # 'S' (seasonally adjusted) or 'U' (not adjusted)
    periodicity_code = Column(String(5), ForeignKey('bls_periodicity.periodicity_code'))

    # Base period information (unique to CPI)
    base_code = Column(String(10))  # 'S', 'U', etc.
    base_period = Column(String(50))  # '1982-84=100', '1967=100', etc.

    # Metadata
    series_title = Column(Text, nullable=False)
    footnote_codes = Column(String(50))
    begin_year = Column(SmallInteger)
    begin_period = Column(String(5))
    end_year = Column(SmallInteger)
    end_period = Column(String(5))

    # Status
    is_active = Column(Boolean, default=True)  # False if series discontinued

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_cu_series_area', 'area_code'),
        Index('ix_bls_cu_series_item', 'item_code'),
        Index('ix_bls_cu_series_seasonal', 'seasonal_code'),
        Index('ix_bls_cu_series_active', 'is_active'),
    )

    def __repr__(self):
        return f"<CUSeries(id='{self.series_id}', title='{self.series_title[:50]}')>"


class CUData(Base):
    """CU Time series data - actual CPI index values"""
    __tablename__ = 'bls_cu_data'

    # Composite primary key
    series_id = Column(String(20), ForeignKey('bls_cu_series.series_id'), primary_key=True)
    year = Column(SmallInteger, primary_key=True, nullable=False)
    period = Column(String(5), primary_key=True, nullable=False)  # 'M01'-'M13', 'S01', etc.

    # Data
    value = Column(Numeric(20, 6))  # Index value (can be NULL for missing data)
    footnote_codes = Column(String(50))  # Comma-separated footnote codes

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_cu_data_year_period', 'year', 'period'),
        Index('ix_bls_cu_data_series_year', 'series_id', 'year'),
    )

    def __repr__(self):
        return f"<CUData(series='{self.series_id}', date={self.year}-{self.period}, value={self.value})>"


class CUAspect(Base):
    """CU Aspect data - additional metrics per observation (M1, H1, V1)"""
    __tablename__ = 'bls_cu_aspects'

    # Composite primary key
    series_id = Column(String(20), ForeignKey('bls_cu_series.series_id'), primary_key=True)
    year = Column(SmallInteger, primary_key=True, nullable=False)
    period = Column(String(5), primary_key=True, nullable=False)
    aspect_type = Column(String(5), primary_key=True, nullable=False)  # 'M1', 'H1', 'V1', etc.

    # Data
    value = Column(String(100))  # Can be numeric or text (e.g., "S-Jan. 2012")
    footnote_codes = Column(String(50))

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_cu_aspects_series_year', 'series_id', 'year'),
        Index('ix_bls_cu_aspects_type', 'aspect_type'),
    )

    def __repr__(self):
        return f"<CUAspect(series='{self.series_id}', {self.year}-{self.period}, type={self.aspect_type})>"


# ==================== CW (CONSUMER PRICE INDEX - WAGE EARNERS) SPECIFIC TABLES ====================

class CWArea(Base):
    """Area codes for Consumer Price Index - Urban Wage Earners survey"""
    __tablename__ = 'bls_cw_areas'

    area_code = Column(String(10), primary_key=True)  # '0000', '0100', etc.
    area_name = Column(String(255), nullable=False)
    display_level = Column(SmallInteger)  # Hierarchy depth (0 = top level)
    selectable = Column(String(1))  # 'T' or 'F'
    sort_sequence = Column(SmallInteger)  # Display ordering

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_cw_areas_display_level', 'display_level'),
        Index('ix_bls_cw_areas_selectable', 'selectable'),
    )

    def __repr__(self):
        return f"<CWArea(code='{self.area_code}', name='{self.area_name[:50]}')>"


class CWItem(Base):
    """Item catalog for Consumer Price Index - Urban Wage Earners survey"""
    __tablename__ = 'bls_cw_items'

    item_code = Column(String(20), primary_key=True)  # 'SA0', 'SAH', etc.
    item_name = Column(String(500), nullable=False)
    display_level = Column(SmallInteger)  # Hierarchy depth (0 = top level)
    selectable = Column(String(1))  # 'T' or 'F'
    sort_sequence = Column(SmallInteger)  # Display ordering

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_cw_items_display_level', 'display_level'),
        Index('ix_bls_cw_items_selectable', 'selectable'),
    )

    def __repr__(self):
        return f"<CWItem(code='{self.item_code}', name='{self.item_name[:50]}')>"


class CWSeries(Base):
    """CW Series catalog - metadata for each CPI-W time series"""
    __tablename__ = 'bls_cw_series'

    series_id = Column(String(20), primary_key=True)  # 'CWSR0000SA0', etc.

    # Decomposed series ID components
    area_code = Column(String(10), ForeignKey('bls_cw_areas.area_code'))
    item_code = Column(String(20), ForeignKey('bls_cw_items.item_code'))
    seasonal_code = Column(String(1))  # 'S' (seasonally adjusted) or 'U' (not adjusted)
    periodicity_code = Column(String(5), ForeignKey('bls_periodicity.periodicity_code'))

    # Base period information (unique to CPI)
    base_code = Column(String(10))  # 'S', 'U', etc.
    base_period = Column(String(50))  # '1982-84=100', '1967=100', etc.

    # Metadata
    series_title = Column(Text, nullable=False)
    footnote_codes = Column(String(50))
    begin_year = Column(SmallInteger)
    begin_period = Column(String(5))
    end_year = Column(SmallInteger)
    end_period = Column(String(5))

    # Status
    is_active = Column(Boolean, default=True)  # False if series discontinued

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_cw_series_area', 'area_code'),
        Index('ix_bls_cw_series_item', 'item_code'),
        Index('ix_bls_cw_series_seasonal', 'seasonal_code'),
        Index('ix_bls_cw_series_active', 'is_active'),
    )

    def __repr__(self):
        return f"<CWSeries(id='{self.series_id}', title='{self.series_title[:50]}')>"


class CWData(Base):
    """CW Time series data - actual CPI-W index values"""
    __tablename__ = 'bls_cw_data'

    # Composite primary key
    series_id = Column(String(20), ForeignKey('bls_cw_series.series_id'), primary_key=True)
    year = Column(SmallInteger, primary_key=True, nullable=False)
    period = Column(String(5), primary_key=True, nullable=False)  # 'M01'-'M13', 'S01', etc.

    # Data
    value = Column(Numeric(20, 6))  # Index value (can be NULL for missing data)
    footnote_codes = Column(String(50))  # Comma-separated footnote codes

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_cw_data_year_period', 'year', 'period'),
        Index('ix_bls_cw_data_series_year', 'series_id', 'year'),
    )

    def __repr__(self):
        return f"<CWData(series='{self.series_id}', date={self.year}-{self.period}, value={self.value})>"


class CWAspect(Base):
    """CW Aspect data - additional metrics per observation (M1, H1, V1, F)"""
    __tablename__ = 'bls_cw_aspects'

    # Composite primary key
    series_id = Column(String(20), ForeignKey('bls_cw_series.series_id'), primary_key=True)
    year = Column(SmallInteger, primary_key=True, nullable=False)
    period = Column(String(5), primary_key=True, nullable=False)
    aspect_type = Column(String(5), primary_key=True, nullable=False)  # 'M1', 'H1', 'V1', 'F', etc.

    # Data
    value = Column(String(100))  # Can be numeric or text (e.g., "S-Jan. 2012")
    footnote_codes = Column(String(50))

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_cw_aspects_series_year', 'series_id', 'year'),
        Index('ix_bls_cw_aspects_type', 'aspect_type'),
    )

    def __repr__(self):
        return f"<CWAspect(series='{self.series_id}', {self.year}-{self.period}, type={self.aspect_type})>"


# ==================== SU (CHAINED CPI-U) SPECIFIC TABLES ====================

class SUArea(Base):
    """Area codes for Chained Consumer Price Index - All Urban Consumers survey"""
    __tablename__ = 'bls_su_areas'

    area_code = Column(String(10), primary_key=True)  # '0000' (U.S. city average only)
    area_name = Column(String(255), nullable=False)
    display_level = Column(SmallInteger)  # Hierarchy depth (0 = top level)
    selectable = Column(String(1))  # 'T' or 'F'
    sort_sequence = Column(SmallInteger)  # Display ordering

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_su_areas_display_level', 'display_level'),
        Index('ix_bls_su_areas_selectable', 'selectable'),
    )

    def __repr__(self):
        return f"<SUArea(code='{self.area_code}', name='{self.area_name[:50]}')>"


class SUItem(Base):
    """Item catalog for Chained Consumer Price Index - All Urban Consumers survey"""
    __tablename__ = 'bls_su_items'

    item_code = Column(String(20), primary_key=True)  # 'SA0', 'SAH', etc.
    item_name = Column(String(500), nullable=False)
    display_level = Column(SmallInteger)  # Hierarchy depth (0 = top level)
    selectable = Column(String(1))  # 'T' or 'F'
    sort_sequence = Column(SmallInteger)  # Display ordering

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_su_items_display_level', 'display_level'),
        Index('ix_bls_su_items_selectable', 'selectable'),
    )

    def __repr__(self):
        return f"<SUItem(code='{self.item_code}', name='{self.item_name[:50]}')>"


class SUSeries(Base):
    """SU Series catalog - metadata for each Chained CPI-U time series"""
    __tablename__ = 'bls_su_series'

    series_id = Column(String(20), primary_key=True)  # 'SUUR0000SA0', etc.

    # Decomposed series ID components
    area_code = Column(String(10), ForeignKey('bls_su_areas.area_code'))
    item_code = Column(String(20), ForeignKey('bls_su_items.item_code'))
    seasonal_code = Column(String(1))  # 'S' (seasonally adjusted) or 'U' (not adjusted)
    periodicity_code = Column(String(5), ForeignKey('bls_periodicity.periodicity_code'))

    # Base period information (unique to CPI)
    base_code = Column(String(10))  # 'S' = Superlative chaining
    base_period = Column(String(50))  # 'DECEMBER 1999=100', etc.

    # Metadata
    series_title = Column(Text, nullable=False)
    footnote_codes = Column(String(50))
    begin_year = Column(SmallInteger)
    begin_period = Column(String(5))
    end_year = Column(SmallInteger)
    end_period = Column(String(5))

    # Status
    is_active = Column(Boolean, default=True)  # False if series discontinued

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_su_series_area', 'area_code'),
        Index('ix_bls_su_series_item', 'item_code'),
        Index('ix_bls_su_series_seasonal', 'seasonal_code'),
        Index('ix_bls_su_series_active', 'is_active'),
    )

    def __repr__(self):
        return f"<SUSeries(id='{self.series_id}', title='{self.series_title[:50]}')>"


class SUData(Base):
    """SU Time series data - actual Chained CPI-U index values"""
    __tablename__ = 'bls_su_data'

    # Composite primary key
    series_id = Column(String(20), ForeignKey('bls_su_series.series_id'), primary_key=True)
    year = Column(SmallInteger, primary_key=True, nullable=False)
    period = Column(String(5), primary_key=True, nullable=False)  # 'M01'-'M13', 'S01', etc.

    # Data
    value = Column(Numeric(20, 6))  # Index value (can be NULL for missing data)
    footnote_codes = Column(String(50))  # Comma-separated footnote codes

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_su_data_year_period', 'year', 'period'),
        Index('ix_bls_su_data_series_year', 'series_id', 'year'),
    )

    def __repr__(self):
        return f"<SUData(series='{self.series_id}', date={self.year}-{self.period}, value={self.value})>"


# ==================== LA (LOCAL AREA UNEMPLOYMENT) SPECIFIC TABLES ====================

class LAArea(Base):
    """Area codes for Local Area Unemployment Statistics"""
    __tablename__ = 'bls_la_areas'

    area_code = Column(String(20), primary_key=True)  # 'ST0100000000000', 'MT1234500000000', etc.
    area_type_code = Column(String(1))  # 'A' (state), 'B' (metro), 'C' (county), etc.
    area_text = Column(String(255), nullable=False)  # Geographic area name
    display_level = Column(SmallInteger)  # Hierarchy depth (0 = top level)
    selectable = Column(String(1))  # 'T' or 'F'
    sort_sequence = Column(Integer)  # Display ordering (can be large for 8K+ areas)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_la_areas_type', 'area_type_code'),
        Index('ix_bls_la_areas_display_level', 'display_level'),
        Index('ix_bls_la_areas_selectable', 'selectable'),
    )

    def __repr__(self):
        return f"<LAArea(code='{self.area_code}', text='{self.area_text[:50]}')>"


class LAMeasure(Base):
    """Measure codes for Local Area Unemployment Statistics"""
    __tablename__ = 'bls_la_measures'

    measure_code = Column(String(5), primary_key=True)  # '03', '04', '05', etc.
    measure_text = Column(String(100), nullable=False)  # 'unemployment rate', 'unemployment', etc.

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<LAMeasure(code='{self.measure_code}', text='{self.measure_text}')>"


class LASeries(Base):
    """LA Series catalog - metadata for each unemployment time series"""
    __tablename__ = 'bls_la_series'

    series_id = Column(String(30), primary_key=True)  # 'LASBS060000000000003', etc.

    # Decomposed series ID components
    area_type_code = Column(String(1))  # 'A' (state), 'B' (metro), 'F' (county), etc.
    area_code = Column(String(20), ForeignKey('bls_la_areas.area_code'))
    measure_code = Column(String(5), ForeignKey('bls_la_measures.measure_code'))
    seasonal_code = Column(String(1))  # 'S' (seasonally adjusted) or 'U' (not adjusted)
    srd_code = Column(String(5))  # State/Region/Division code

    # Metadata
    series_title = Column(Text, nullable=False)
    footnote_codes = Column(String(50))
    begin_year = Column(SmallInteger)
    begin_period = Column(String(5))
    end_year = Column(SmallInteger)
    end_period = Column(String(5))

    # Status
    is_active = Column(Boolean, default=True)  # False if series discontinued

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_la_series_area_type', 'area_type_code'),
        Index('ix_bls_la_series_area', 'area_code'),
        Index('ix_bls_la_series_measure', 'measure_code'),
        Index('ix_bls_la_series_seasonal', 'seasonal_code'),
        Index('ix_bls_la_series_active', 'is_active'),
    )

    def __repr__(self):
        return f"<LASeries(id='{self.series_id}', title='{self.series_title[:50]}')>"


class LAData(Base):
    """LA Time series data - unemployment statistics observations"""
    __tablename__ = 'bls_la_data'

    # Composite primary key
    series_id = Column(String(30), ForeignKey('bls_la_series.series_id'), primary_key=True)
    year = Column(SmallInteger, primary_key=True, nullable=False)
    period = Column(String(5), primary_key=True, nullable=False)  # 'M01'-'M13'

    # Data
    value = Column(Numeric(20, 1))  # Unemployment rate (%), count, or ratio
    footnote_codes = Column(String(50))  # Comma-separated footnote codes

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_la_data_year_period', 'year', 'period'),
        Index('ix_bls_la_data_series_year', 'series_id', 'year'),
    )

    def __repr__(self):
        return f"<LAData(series='{self.series_id}', date={self.year}-{self.period}, value={self.value})>"


# ==================== CE (CURRENT EMPLOYMENT STATISTICS) SPECIFIC TABLES ====================

class CEIndustry(Base):
    """Industry codes for Current Employment Statistics survey"""
    __tablename__ = 'bls_ce_industries'

    industry_code = Column(String(10), primary_key=True)  # '00000000', '10000000', etc.
    naics_code = Column(String(50))  # NAICS classification code (can be compound like '332200;991,9')
    industry_name = Column(String(500), nullable=False)
    publishing_status = Column(String(5))  # 'A', 'B', 'AO', 'AT', 'ET', etc. (publication priority)
    display_level = Column(SmallInteger)  # Hierarchy level (0-7+)
    selectable = Column(String(5))  # 'T' or 'F'
    sort_sequence = Column(Integer)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_ce_industries_naics', 'naics_code'),
        Index('ix_bls_ce_industries_level', 'display_level'),
    )

    def __repr__(self):
        return f"<CEIndustry(code='{self.industry_code}', name='{self.industry_name[:50]}')>"


class CEDataType(Base):
    """Data type codes for Current Employment Statistics"""
    __tablename__ = 'bls_ce_data_types'

    data_type_code = Column(String(5), primary_key=True)  # '01', '02', '03', etc.
    data_type_text = Column(String(500), nullable=False)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<CEDataType(code='{self.data_type_code}', text='{self.data_type_text[:50]}')>"


class CESupersector(Base):
    """Supersector codes for Current Employment Statistics"""
    __tablename__ = 'bls_ce_supersectors'

    supersector_code = Column(String(5), primary_key=True)  # '00', '05', '10', etc.
    supersector_name = Column(String(200), nullable=False)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<CESupersector(code='{self.supersector_code}', name='{self.supersector_name}')>"


class CESeries(Base):
    """CE Series catalog - metadata for each employment time series"""
    __tablename__ = 'bls_ce_series'

    series_id = Column(String(20), primary_key=True)  # 'CES0000000001', etc.

    # Decomposed series ID components
    supersector_code = Column(String(5), ForeignKey('bls_ce_supersectors.supersector_code'))
    industry_code = Column(String(10), ForeignKey('bls_ce_industries.industry_code'))
    data_type_code = Column(String(5), ForeignKey('bls_ce_data_types.data_type_code'))
    seasonal_code = Column(String(1))  # 'S' (seasonally adjusted) or 'U' (not adjusted)

    # Metadata
    series_title = Column(Text, nullable=False)
    footnote_codes = Column(String(50))
    begin_year = Column(SmallInteger)
    begin_period = Column(String(5))
    end_year = Column(SmallInteger)
    end_period = Column(String(5))

    # Status
    is_active = Column(Boolean, default=True)  # False if series discontinued

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_ce_series_supersector', 'supersector_code'),
        Index('ix_bls_ce_series_industry', 'industry_code'),
        Index('ix_bls_ce_series_data_type', 'data_type_code'),
        Index('ix_bls_ce_series_seasonal', 'seasonal_code'),
        Index('ix_bls_ce_series_active', 'is_active'),
    )

    def __repr__(self):
        return f"<CESeries(id='{self.series_id}', title='{self.series_title[:50]}')>"


class CEData(Base):
    """CE Time series data - employment statistics observations"""
    __tablename__ = 'bls_ce_data'

    # Composite primary key
    series_id = Column(String(20), ForeignKey('bls_ce_series.series_id'), primary_key=True)
    year = Column(SmallInteger, primary_key=True, nullable=False)
    period = Column(String(5), primary_key=True, nullable=False)  # 'M01'-'M12'

    # Data
    value = Column(Numeric(20, 1))  # Employment (thousands), hours, earnings, or index
    footnote_codes = Column(String(50))  # Comma-separated footnote codes

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_ce_data_year_period', 'year', 'period'),
        Index('ix_bls_ce_data_series_year', 'series_id', 'year'),
    )

    def __repr__(self):
        return f"<CEData(series='{self.series_id}', date={self.year}-{self.period}, value={self.value})>"


# ==================== PC (PRODUCER PRICE INDEX - INDUSTRY) SPECIFIC TABLES ====================

class PCIndustry(Base):
    """Industry codes for Producer Price Index (Industry) survey"""
    __tablename__ = 'bls_pc_industries'

    industry_code = Column(String(10), primary_key=True)  # NAICS-based code (e.g., '113310')
    industry_name = Column(String(500), nullable=False)

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<PCIndustry(code='{self.industry_code}', name='{self.industry_name[:50]}')>"


class PCProduct(Base):
    """Product codes for Producer Price Index (Industry) survey"""
    __tablename__ = 'bls_pc_products'

    # Composite primary key
    industry_code = Column(String(10), ForeignKey('bls_pc_industries.industry_code'), primary_key=True)
    product_code = Column(String(20), primary_key=True)  # Product identifier within industry

    product_name = Column(String(500), nullable=False)

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<PCProduct(code='{self.product_code}', name='{self.product_name[:50]}')>"


class PCSeries(Base):
    """PC Series catalog - metadata for each producer price time series"""
    __tablename__ = 'bls_pc_series'

    series_id = Column(String(30), primary_key=True)  # Format: PCU + industry + product
    industry_code = Column(String(10), ForeignKey('bls_pc_industries.industry_code'))
    product_code = Column(String(20))  # Not a FK due to composite key in PCProduct
    seasonal_code = Column(String(1))  # S=Seasonally Adjusted, U=Not Adjusted
    base_date = Column(String(10))  # Base year/month for index (YYYYMM format)
    series_title = Column(Text, nullable=False)
    footnote_codes = Column(String(50))
    begin_year = Column(SmallInteger)
    begin_period = Column(String(5))
    end_year = Column(SmallInteger)
    end_period = Column(String(5))
    is_active = Column(Boolean, default=True)  # Derived from end_year/period

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    # Indexes
    __table_args__ = (
        Index('ix_bls_pc_series_industry', 'industry_code'),
        Index('ix_bls_pc_series_seasonal', 'seasonal_code'),
        Index('ix_bls_pc_series_active', 'is_active'),
    )

    def __repr__(self):
        return f"<PCSeries(id='{self.series_id}', title='{self.series_title[:50]}')>"


class PCData(Base):
    """PC Time series data - producer price index observations"""
    __tablename__ = 'bls_pc_data'

    # Composite primary key
    series_id = Column(String(30), ForeignKey('bls_pc_series.series_id'), primary_key=True)
    year = Column(SmallInteger, primary_key=True, nullable=False)
    period = Column(String(5), primary_key=True, nullable=False)  # 'M01'-'M13'

    # Data
    value = Column(Numeric(20, 3))  # Price index (3 decimal places after June 2021)
    footnote_codes = Column(String(500))  # Comma-separated footnote codes or text

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_pc_data_year_period', 'year', 'period'),
        Index('ix_bls_pc_data_series_year', 'series_id', 'year'),
    )

    def __repr__(self):
        return f"<PCData(series='{self.series_id}', date={self.year}-{self.period}, value={self.value})>"


# ==================== WP (PRODUCER PRICE INDEX - COMMODITIES) SPECIFIC TABLES ====================

class WPGroup(Base):
    """Commodity group codes for Producer Price Index (Commodities) survey"""
    __tablename__ = 'bls_wp_groups'

    group_code = Column(String(10), primary_key=True)  # Commodity group code (e.g., '01', '05', 'FD-ID')
    group_name = Column(String(500), nullable=False)

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<WPGroup(code='{self.group_code}', name='{self.group_name[:50]}')>"


class WPItem(Base):
    """Item codes for Producer Price Index (Commodities) survey"""
    __tablename__ = 'bls_wp_items'

    # Composite primary key
    group_code = Column(String(10), ForeignKey('bls_wp_groups.group_code'), primary_key=True)
    item_code = Column(String(20), primary_key=True)  # Item identifier within group

    item_name = Column(String(500), nullable=False)

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<WPItem(code='{self.item_code}', name='{self.item_name[:50]}')>"


class WPSeries(Base):
    """WP Series catalog - metadata for each commodity price time series"""
    __tablename__ = 'bls_wp_series'

    series_id = Column(String(30), primary_key=True)  # Format: WP + S/U + group + item
    group_code = Column(String(10), ForeignKey('bls_wp_groups.group_code'))
    item_code = Column(String(20))  # Not a FK due to composite key in WPItem
    seasonal_code = Column(String(1))  # S=Seasonally Adjusted, U=Not Adjusted
    base_date = Column(String(10))  # Base year/month for index (YYYYMM format)
    series_title = Column(Text, nullable=False)
    footnote_codes = Column(String(500))
    begin_year = Column(SmallInteger)
    begin_period = Column(String(5))
    end_year = Column(SmallInteger)
    end_period = Column(String(5))
    is_active = Column(Boolean, default=True)  # Derived from end_year/period

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    # Indexes
    __table_args__ = (
        Index('ix_bls_wp_series_group', 'group_code'),
        Index('ix_bls_wp_series_seasonal', 'seasonal_code'),
        Index('ix_bls_wp_series_active', 'is_active'),
    )

    def __repr__(self):
        return f"<WPSeries(id='{self.series_id}', title='{self.series_title[:50]}')>"


class WPData(Base):
    """WP Time series data - commodity price index observations"""
    __tablename__ = 'bls_wp_data'

    # Composite primary key
    series_id = Column(String(30), ForeignKey('bls_wp_series.series_id'), primary_key=True)
    year = Column(SmallInteger, primary_key=True, nullable=False)
    period = Column(String(5), primary_key=True, nullable=False)  # 'M01'-'M13'

    # Data
    value = Column(Numeric(20, 3))  # Price index (3 decimal places after June 2021)
    footnote_codes = Column(String(500))  # Comma-separated footnote codes or text

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_wp_data_year_period', 'year', 'period'),
        Index('ix_bls_wp_data_series_year', 'series_id', 'year'),
    )

    def __repr__(self):
        return f"<WPData(series='{self.series_id}', date={self.year}-{self.period}, value={self.value})>"


# ==================== SM (STATE AND METRO AREA EMPLOYMENT) SPECIFIC TABLES ====================

class SMState(Base):
    """State codes for State and Metro Area Employment survey"""
    __tablename__ = 'bls_sm_states'

    state_code = Column(String(5), primary_key=True)  # State code (e.g., '01'=Alabama, '36'=New York)
    state_name = Column(String(100), nullable=False)

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<SMState(code='{self.state_code}', name='{self.state_name}')>"


class SMArea(Base):
    """Area codes for State and Metro Area Employment survey"""
    __tablename__ = 'bls_sm_areas'

    area_code = Column(String(10), primary_key=True)  # Area code (e.g., '00000'=Statewide, '35620'=NYC metro)
    area_name = Column(String(200), nullable=False)

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<SMArea(code='{self.area_code}', name='{self.area_name[:50]}')>"


class SMSupersector(Base):
    """Supersector codes for State and Metro Area Employment survey"""
    __tablename__ = 'bls_sm_supersectors'

    supersector_code = Column(String(5), primary_key=True)  # Supersector code (e.g., '00'=Total, '05'=Trade)
    supersector_name = Column(String(200), nullable=False)

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<SMSupersector(code='{self.supersector_code}', name='{self.supersector_name[:50]}')>"


class SMIndustry(Base):
    """Industry codes for State and Metro Area Employment survey"""
    __tablename__ = 'bls_sm_industries'

    industry_code = Column(String(10), primary_key=True)  # NAICS-based industry code
    industry_name = Column(String(500), nullable=False)

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<SMIndustry(code='{self.industry_code}', name='{self.industry_name[:50]}')>"


class SMSeries(Base):
    """SM Series catalog - metadata for each state/metro employment time series"""
    __tablename__ = 'bls_sm_series'

    series_id = Column(String(30), primary_key=True)  # Format: SMS + state + area + supersector + industry + datatype
    state_code = Column(String(5), ForeignKey('bls_sm_states.state_code'))
    area_code = Column(String(10), ForeignKey('bls_sm_areas.area_code'))
    supersector_code = Column(String(5), ForeignKey('bls_sm_supersectors.supersector_code'))
    industry_code = Column(String(10), ForeignKey('bls_sm_industries.industry_code'))
    data_type_code = Column(String(5))  # Data type (01=employment, 02=hours, 03=earnings)
    seasonal_code = Column(String(1))  # S=Seasonally Adjusted, U=Not Adjusted
    benchmark_year = Column(SmallInteger)  # Benchmark year for data
    footnote_codes = Column(String(500))
    begin_year = Column(SmallInteger)
    begin_period = Column(String(5))
    end_year = Column(SmallInteger)
    end_period = Column(String(5))
    is_active = Column(Boolean, default=True)  # Derived from end_year/period

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    # Indexes
    __table_args__ = (
        Index('ix_bls_sm_series_state', 'state_code'),
        Index('ix_bls_sm_series_area', 'area_code'),
        Index('ix_bls_sm_series_industry', 'industry_code'),
        Index('ix_bls_sm_series_seasonal', 'seasonal_code'),
        Index('ix_bls_sm_series_active', 'is_active'),
    )

    def __repr__(self):
        return f"<SMSeries(id='{self.series_id}', state='{self.state_code}', area='{self.area_code}')>"


class SMData(Base):
    """SM Time series data - state/metro employment statistics observations"""
    __tablename__ = 'bls_sm_data'

    # Composite primary key
    series_id = Column(String(30), ForeignKey('bls_sm_series.series_id'), primary_key=True)
    year = Column(SmallInteger, primary_key=True, nullable=False)
    period = Column(String(5), primary_key=True, nullable=False)  # 'M01'-'M13'

    # Data
    value = Column(Numeric(20, 1))  # Employment (thousands), hours, or earnings
    footnote_codes = Column(String(500))  # Comma-separated footnote codes or text

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_sm_data_year_period', 'year', 'period'),
        Index('ix_bls_sm_data_series_year', 'series_id', 'year'),
    )

    def __repr__(self):
        return f"<SMData(series='{self.series_id}', date={self.year}-{self.period}, value={self.value})>"


# ==================== JT (JOLTS - JOB OPENINGS AND LABOR TURNOVER) SPECIFIC TABLES ====================

class JTDataElement(Base):
    """JT Data element types - what is being measured"""
    __tablename__ = 'bls_jt_dataelements'

    dataelement_code = Column(String(5), primary_key=True)  # 'JO', 'HI', 'TS', 'QU', 'LD', 'OS', 'UO'
    dataelement_text = Column(String(200), nullable=False)
    display_level = Column(SmallInteger)
    selectable = Column(String(1))  # 'T' or 'F'
    sort_sequence = Column(SmallInteger)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<JTDataElement(code='{self.dataelement_code}', text='{self.dataelement_text}')>"


class JTIndustry(Base):
    """JT Industry classifications"""
    __tablename__ = 'bls_jt_industries'

    industry_code = Column(String(10), primary_key=True)  # '000000', '100000', etc.
    industry_text = Column(String(500), nullable=False)
    display_level = Column(SmallInteger)
    selectable = Column(String(1))  # 'T' or 'F'
    sort_sequence = Column(SmallInteger)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<JTIndustry(code='{self.industry_code}', text='{self.industry_text}')>"


class JTState(Base):
    """JT State codes"""
    __tablename__ = 'bls_jt_states'

    state_code = Column(String(5), primary_key=True)  # '00', '01', etc.
    state_text = Column(String(200), nullable=False)
    display_level = Column(SmallInteger)
    selectable = Column(String(1))  # 'T' or 'F'
    sort_sequence = Column(SmallInteger)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<JTState(code='{self.state_code}', text='{self.state_text}')>"


class JTArea(Base):
    """JT Area codes (geographic regions)"""
    __tablename__ = 'bls_jt_areas'

    area_code = Column(String(10), primary_key=True)  # '00000', etc.
    area_text = Column(String(200), nullable=False)
    display_level = Column(SmallInteger)
    selectable = Column(String(1))  # 'T' or 'F'
    sort_sequence = Column(SmallInteger)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<JTArea(code='{self.area_code}', text='{self.area_text}')>"


class JTSizeClass(Base):
    """JT Establishment size classes"""
    __tablename__ = 'bls_jt_sizeclasses'

    sizeclass_code = Column(String(5), primary_key=True)  # '00', '01', etc.
    sizeclass_text = Column(String(200), nullable=False)
    display_level = Column(SmallInteger)
    selectable = Column(String(1))  # 'T' or 'F'
    sort_sequence = Column(SmallInteger)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<JTSizeClass(code='{self.sizeclass_code}', text='{self.sizeclass_text}')>"


class JTRateLevel(Base):
    """JT Rate vs Level indicator"""
    __tablename__ = 'bls_jt_ratelevels'

    ratelevel_code = Column(String(1), primary_key=True)  # 'R' or 'L'
    ratelevel_text = Column(String(200), nullable=False)
    display_level = Column(SmallInteger)
    selectable = Column(String(1))  # 'T' or 'F'
    sort_sequence = Column(SmallInteger)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<JTRateLevel(code='{self.ratelevel_code}', text='{self.ratelevel_text}')>"


class JTSeries(Base):
    """JT Series catalog - metadata for each JOLTS time series"""
    __tablename__ = 'bls_jt_series'

    # Primary key
    series_id = Column(String(30), primary_key=True)  # 'JTS000000000000000HIL', etc.

    # Foreign keys to reference tables
    seasonal = Column(String(1))  # 'S' or 'U'
    industry_code = Column(String(10), ForeignKey('bls_jt_industries.industry_code'))
    state_code = Column(String(5), ForeignKey('bls_jt_states.state_code'))
    area_code = Column(String(10), ForeignKey('bls_jt_areas.area_code'))
    sizeclass_code = Column(String(5), ForeignKey('bls_jt_sizeclasses.sizeclass_code'))
    dataelement_code = Column(String(5), ForeignKey('bls_jt_dataelements.dataelement_code'))
    ratelevel_code = Column(String(1), ForeignKey('bls_jt_ratelevels.ratelevel_code'))

    # Series metadata
    footnote_codes = Column(String(500))
    begin_year = Column(SmallInteger)
    begin_period = Column(String(5))
    end_year = Column(SmallInteger)
    end_period = Column(String(5))

    # Helper field for active series
    is_active = Column(Boolean, default=True)

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_jt_series_industry', 'industry_code'),
        Index('ix_bls_jt_series_state', 'state_code'),
        Index('ix_bls_jt_series_dataelement', 'dataelement_code'),
        Index('ix_bls_jt_series_seasonal', 'seasonal'),
        Index('ix_bls_jt_series_active', 'is_active'),
    )

    def __repr__(self):
        return f"<JTSeries(id='{self.series_id}', industry='{self.industry_code}', element='{self.dataelement_code}')>"


class JTData(Base):
    """JT Time series data - JOLTS observations"""
    __tablename__ = 'bls_jt_data'

    # Composite primary key
    series_id = Column(String(30), ForeignKey('bls_jt_series.series_id'), primary_key=True)
    year = Column(SmallInteger, primary_key=True, nullable=False)
    period = Column(String(5), primary_key=True, nullable=False)  # 'M01'-'M12'

    # Data
    value = Column(Numeric(20, 1))  # Job openings, hires, etc. (in thousands or rate)
    footnote_codes = Column(String(500))

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_jt_data_year_period', 'year', 'period'),
        Index('ix_bls_jt_data_series_year', 'series_id', 'year'),
    )

    def __repr__(self):
        return f"<JTData(series='{self.series_id}', date={self.year}-{self.period}, value={self.value})>"


# ==================== EC (EMPLOYMENT COST INDEX) SPECIFIC TABLES ====================

class ECCompensation(Base):
    """EC Compensation types - what costs are being measured"""
    __tablename__ = 'bls_ec_compensations'

    comp_code = Column(String(5), primary_key=True)  # '1', '2', etc.
    comp_text = Column(String(200), nullable=False)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<ECCompensation(code='{self.comp_code}', text='{self.comp_text}')>"


class ECGroup(Base):
    """EC Industry/occupation groups"""
    __tablename__ = 'bls_ec_groups'

    group_code = Column(String(10), primary_key=True)  # '000', '101', etc.
    group_name = Column(String(500), nullable=False)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<ECGroup(code='{self.group_code}', name='{self.group_name}')>"


class ECOwnership(Base):
    """EC Ownership types"""
    __tablename__ = 'bls_ec_ownerships'

    ownership_code = Column(String(5), primary_key=True)  # '1', '2', '3'
    ownership_name = Column(String(200), nullable=False)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<ECOwnership(code='{self.ownership_code}', name='{self.ownership_name}')>"


class ECPeriodicity(Base):
    """EC Periodicity - type of measurement"""
    __tablename__ = 'bls_ec_periodicities'

    periodicity_code = Column(String(5), primary_key=True)  # 'I', 'Q', 'A'
    periodicity_text = Column(String(200), nullable=False)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<ECPeriodicity(code='{self.periodicity_code}', text='{self.periodicity_text}')>"


class ECSeries(Base):
    """EC Series catalog - metadata for each Employment Cost Index time series"""
    __tablename__ = 'bls_ec_series'

    # Primary key
    series_id = Column(String(30), primary_key=True)  # 'ECS10001I', etc.

    # Foreign keys to reference tables
    comp_code = Column(String(5), ForeignKey('bls_ec_compensations.comp_code'))
    group_code = Column(String(10), ForeignKey('bls_ec_groups.group_code'))
    ownership_code = Column(String(5), ForeignKey('bls_ec_ownerships.ownership_code'))
    periodicity_code = Column(String(5), ForeignKey('bls_ec_periodicities.periodicity_code'))
    seasonal = Column(String(1))  # 'S' or 'U'

    # Series metadata
    begin_year = Column(SmallInteger)
    begin_period = Column(String(5))
    end_year = Column(SmallInteger)
    end_period = Column(String(5))

    # Helper field for active series
    is_active = Column(Boolean, default=True)

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_ec_series_comp', 'comp_code'),
        Index('ix_bls_ec_series_group', 'group_code'),
        Index('ix_bls_ec_series_ownership', 'ownership_code'),
        Index('ix_bls_ec_series_seasonal', 'seasonal'),
        Index('ix_bls_ec_series_active', 'is_active'),
    )

    def __repr__(self):
        return f"<ECSeries(id='{self.series_id}', group='{self.group_code}', comp='{self.comp_code}')>"


class ECData(Base):
    """EC Time series data - Employment Cost Index observations"""
    __tablename__ = 'bls_ec_data'

    # Composite primary key
    series_id = Column(String(30), ForeignKey('bls_ec_series.series_id'), primary_key=True)
    year = Column(SmallInteger, primary_key=True, nullable=False)
    period = Column(String(5), primary_key=True, nullable=False)  # 'Q01'-'Q04'

    # Data
    value = Column(Numeric(20, 3))  # Index or percent change
    footnote_codes = Column(String(500))

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_ec_data_year_period', 'year', 'period'),
        Index('ix_bls_ec_data_series_year', 'series_id', 'year'),
    )

    def __repr__(self):
        return f"<ECData(series='{self.series_id}', date={self.year}-{self.period}, value={self.value})>"


# ==================== OE (OCCUPATIONAL EMPLOYMENT AND WAGE STATISTICS) SPECIFIC TABLES ====================

class OEAreaType(Base):
    """OE Area type codes"""
    __tablename__ = 'bls_oe_areatypes'

    areatype_code = Column(String(5), primary_key=True)  # 'M', 'N', 'S'
    areatype_name = Column(String(200), nullable=False)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<OEAreaType(code='{self.areatype_code}', name='{self.areatype_name}')>"


class OEDataType(Base):
    """OE Data type codes - what is being measured"""
    __tablename__ = 'bls_oe_datatypes'

    datatype_code = Column(String(5), primary_key=True)  # '01', '02', etc.
    datatype_name = Column(String(200), nullable=False)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<OEDataType(code='{self.datatype_code}', name='{self.datatype_name}')>"


class OEIndustry(Base):
    """OE Industry classifications"""
    __tablename__ = 'bls_oe_industries'

    industry_code = Column(String(10), primary_key=True)  # '000000', etc.
    industry_name = Column(String(500), nullable=False)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<OEIndustry(code='{self.industry_code}', name='{self.industry_name}')>"


class OEOccupation(Base):
    """OE Occupation codes (SOC - Standard Occupational Classification)"""
    __tablename__ = 'bls_oe_occupations'

    occupation_code = Column(String(10), primary_key=True)  # '000000', '110000', etc.
    occupation_name = Column(String(500), nullable=False)
    occupation_description = Column(Text)
    display_level = Column(SmallInteger)
    selectable = Column(String(1))  # 'T' or 'F'
    sort_sequence = Column(Integer)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<OEOccupation(code='{self.occupation_code}', name='{self.occupation_name}')>"


class OESector(Base):
    """OE Sector codes"""
    __tablename__ = 'bls_oe_sectors'

    sector_code = Column(String(10), primary_key=True)  # '00--01', '11--12', etc.
    sector_name = Column(String(500), nullable=False)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<OESector(code='{self.sector_code}', name='{self.sector_name}')>"


class OEArea(Base):
    """OE Area codes (geographic areas)"""
    __tablename__ = 'bls_oe_areas'

    area_code = Column(String(10), primary_key=True)  # '0010180', etc.
    area_name = Column(String(500), nullable=False)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<OEArea(code='{self.area_code}', name='{self.area_name}')>"


class OESeries(Base):
    """OE Series catalog - metadata for each OEWS time series"""
    __tablename__ = 'bls_oe_series'

    # Primary key
    series_id = Column(String(30), primary_key=True)  # 'OEUM001018000000000000001', etc.

    # Foreign keys to reference tables
    seasonal = Column(String(1))  # 'U' (typically only U for OE)
    areatype_code = Column(String(5), ForeignKey('bls_oe_areatypes.areatype_code'))
    industry_code = Column(String(10), ForeignKey('bls_oe_industries.industry_code'))
    occupation_code = Column(String(10), ForeignKey('bls_oe_occupations.occupation_code'))
    datatype_code = Column(String(5), ForeignKey('bls_oe_datatypes.datatype_code'))
    state_code = Column(String(5))  # State FIPS code
    area_code = Column(String(10), ForeignKey('bls_oe_areas.area_code'))
    sector_code = Column(String(10), ForeignKey('bls_oe_sectors.sector_code'))

    # Series metadata
    series_title = Column(Text)  # Pre-built descriptive title
    footnote_codes = Column(String(500))
    begin_year = Column(SmallInteger)
    begin_period = Column(String(5))
    end_year = Column(SmallInteger)
    end_period = Column(String(5))

    # Helper field for active series
    is_active = Column(Boolean, default=True)

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_oe_series_occupation', 'occupation_code'),
        Index('ix_bls_oe_series_industry', 'industry_code'),
        Index('ix_bls_oe_series_area', 'area_code'),
        Index('ix_bls_oe_series_datatype', 'datatype_code'),
        Index('ix_bls_oe_series_active', 'is_active'),
    )

    def __repr__(self):
        return f"<OESeries(id='{self.series_id}', occupation='{self.occupation_code}', area='{self.area_code}')>"


class OEData(Base):
    """OE Time series data - OEWS observations"""
    __tablename__ = 'bls_oe_data'

    # Composite primary key
    series_id = Column(String(30), ForeignKey('bls_oe_series.series_id'), primary_key=True)
    year = Column(SmallInteger, primary_key=True, nullable=False)
    period = Column(String(5), primary_key=True, nullable=False)  # 'A01' for annual

    # Data
    value = Column(Numeric(20, 2))  # Employment counts, wages, etc.
    footnote_codes = Column(String(500))

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_oe_data_year_period', 'year', 'period'),
        Index('ix_bls_oe_data_series_year', 'series_id', 'year'),
    )

    def __repr__(self):
        return f"<OEData(series='{self.series_id}', date={self.year}-{self.period}, value={self.value})>"


# ==================== PR (MAJOR SECTOR PRODUCTIVITY AND COSTS) ====================

class PRClass(Base):
    """PR Worker class types - employees vs all workers"""
    __tablename__ = 'bls_pr_classes'

    class_code = Column(String(5), primary_key=True)
    class_text = Column(String(200), nullable=False)
    display_level = Column(SmallInteger)
    selectable = Column(String(1))
    sort_sequence = Column(Integer)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<PRClass(code='{self.class_code}', text='{self.class_text}')>"


class PRMeasure(Base):
    """PR Measure types - productivity, costs, output, hours, etc."""
    __tablename__ = 'bls_pr_measures'

    measure_code = Column(String(5), primary_key=True)
    measure_text = Column(String(500), nullable=False)
    display_level = Column(SmallInteger)
    selectable = Column(String(1))
    sort_sequence = Column(Integer)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<PRMeasure(code='{self.measure_code}', text='{self.measure_text}')>"


class PRDuration(Base):
    """PR Duration types - percent change types and index"""
    __tablename__ = 'bls_pr_durations'

    duration_code = Column(String(5), primary_key=True)
    duration_text = Column(String(200), nullable=False)
    display_level = Column(SmallInteger)
    selectable = Column(String(1))
    sort_sequence = Column(Integer)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<PRDuration(code='{self.duration_code}', text='{self.duration_text}')>"


class PRSector(Base):
    """PR Sector classifications - business, manufacturing, etc."""
    __tablename__ = 'bls_pr_sectors'

    sector_code = Column(String(10), primary_key=True)
    sector_name = Column(String(500), nullable=False)
    display_level = Column(SmallInteger)
    selectable = Column(String(1))
    sort_sequence = Column(Integer)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<PRSector(code='{self.sector_code}', name='{self.sector_name}')>"


class PRSeries(Base):
    """PR Series catalog - metadata for each productivity time series"""
    __tablename__ = 'bls_pr_series'

    series_id = Column(String(30), primary_key=True)
    sector_code = Column(String(10), ForeignKey('bls_pr_sectors.sector_code'))
    class_code = Column(String(5), ForeignKey('bls_pr_classes.class_code'))
    measure_code = Column(String(5), ForeignKey('bls_pr_measures.measure_code'))
    duration_code = Column(String(5), ForeignKey('bls_pr_durations.duration_code'))
    seasonal = Column(String(1))  # S = Seasonally adjusted
    base_year = Column(String(10))  # e.g., "2017" for index base
    footnote_codes = Column(String(500))
    begin_year = Column(SmallInteger)
    begin_period = Column(String(5))
    end_year = Column(SmallInteger)
    end_period = Column(String(5))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_pr_series_sector', 'sector_code'),
        Index('ix_bls_pr_series_measure', 'measure_code'),
        Index('ix_bls_pr_series_active', 'is_active'),
    )

    def __repr__(self):
        return f"<PRSeries(id='{self.series_id}', sector={self.sector_code}, measure={self.measure_code})>"


class PRData(Base):
    """PR Time series data - productivity and cost observations"""
    __tablename__ = 'bls_pr_data'

    # Composite primary key
    series_id = Column(String(30), ForeignKey('bls_pr_series.series_id'), primary_key=True)
    year = Column(SmallInteger, primary_key=True, nullable=False)
    period = Column(String(5), primary_key=True, nullable=False)  # Q01-Q04, A01

    # Data
    value = Column(Numeric(20, 1))  # Index values, percent changes
    footnote_codes = Column(String(500))

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_pr_data_year_period', 'year', 'period'),
        Index('ix_bls_pr_data_series_year', 'series_id', 'year'),
    )

    def __repr__(self):
        return f"<PRData(series='{self.series_id}', date={self.year}-{self.period}, value={self.value})>"


# ==================== IP (INDUSTRY PRODUCTIVITY) ====================

class IPSector(Base):
    """IP Economic sector classifications"""
    __tablename__ = 'bls_ip_sectors'

    sector_code = Column(String(5), primary_key=True)
    sector_text = Column(String(500), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<IPSector(code='{self.sector_code}', text='{self.sector_text}')>"


class IPIndustry(Base):
    """IP Industry classifications (NAICS-based)"""
    __tablename__ = 'bls_ip_industries'

    industry_code = Column(String(15), primary_key=True)
    naics_code = Column(String(20))
    industry_text = Column(String(500), nullable=False)
    display_level = Column(SmallInteger)
    selectable = Column(String(1))
    sort_sequence = Column(Integer)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<IPIndustry(code='{self.industry_code}', naics='{self.naics_code}')>"


class IPMeasure(Base):
    """IP Measure types - productivity, costs, output, hours, etc."""
    __tablename__ = 'bls_ip_measures'

    measure_code = Column(String(5), primary_key=True)
    measure_text = Column(String(500), nullable=False)
    display_level = Column(SmallInteger)
    selectable = Column(String(1))
    sort_sequence = Column(Integer)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<IPMeasure(code='{self.measure_code}', text='{self.measure_text}')>"


class IPDuration(Base):
    """IP Duration types - index/level vs percent change"""
    __tablename__ = 'bls_ip_durations'

    duration_code = Column(String(5), primary_key=True)
    duration_text = Column(String(200), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<IPDuration(code='{self.duration_code}', text='{self.duration_text}')>"


class IPType(Base):
    """IP Data types - Index, Percent, Hours, Currency, etc."""
    __tablename__ = 'bls_ip_types'

    type_code = Column(String(5), primary_key=True)
    type_text = Column(String(200), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<IPType(code='{self.type_code}', text='{self.type_text}')>"


class IPArea(Base):
    """IP Geographic areas (states and U.S. total)"""
    __tablename__ = 'bls_ip_areas'

    area_code = Column(String(10), primary_key=True)
    area_text = Column(String(500), nullable=False)
    display_level = Column(SmallInteger)
    selectable = Column(String(1))
    sort_sequence = Column(Integer)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<IPArea(code='{self.area_code}', text='{self.area_text}')>"


class IPSeries(Base):
    """IP Series catalog - metadata for each industry productivity time series"""
    __tablename__ = 'bls_ip_series'

    series_id = Column(String(30), primary_key=True)
    seasonal = Column(String(1))  # U = Not seasonally adjusted
    sector_code = Column(String(5), ForeignKey('bls_ip_sectors.sector_code'))
    industry_code = Column(String(15), ForeignKey('bls_ip_industries.industry_code'))
    measure_code = Column(String(5), ForeignKey('bls_ip_measures.measure_code'))
    duration_code = Column(String(5), ForeignKey('bls_ip_durations.duration_code'))
    base_year = Column(String(10))  # e.g., "2017" for index base, or "-"
    type_code = Column(String(5), ForeignKey('bls_ip_types.type_code'))
    area_code = Column(String(10), ForeignKey('bls_ip_areas.area_code'))
    series_title = Column(Text)
    footnote_codes = Column(String(500))
    begin_year = Column(SmallInteger)
    begin_period = Column(String(5))
    end_year = Column(SmallInteger)
    end_period = Column(String(5))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_ip_series_sector', 'sector_code'),
        Index('ix_bls_ip_series_industry', 'industry_code'),
        Index('ix_bls_ip_series_measure', 'measure_code'),
        Index('ix_bls_ip_series_active', 'is_active'),
    )

    def __repr__(self):
        return f"<IPSeries(id='{self.series_id}', industry={self.industry_code}, measure={self.measure_code})>"


class IPData(Base):
    """IP Time series data - industry productivity observations"""
    __tablename__ = 'bls_ip_data'

    # Composite primary key
    series_id = Column(String(30), ForeignKey('bls_ip_series.series_id'), primary_key=True)
    year = Column(SmallInteger, primary_key=True, nullable=False)
    period = Column(String(5), primary_key=True, nullable=False)  # A01 for annual

    # Data
    value = Column(Numeric(20, 3))  # Index values, percent changes, hours, etc.
    footnote_codes = Column(String(500))

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_ip_data_year_period', 'year', 'period'),
        Index('ix_bls_ip_data_series_year', 'series_id', 'year'),
    )

    def __repr__(self):
        return f"<IPData(series='{self.series_id}', date={self.year}-{self.period}, value={self.value})>"


# ==================== TU (AMERICAN TIME USE SURVEY) MODELS ====================

class TUStatType(Base):
    """TU Statistic types - number of persons, hours per day, participants, etc."""
    __tablename__ = 'bls_tu_stattypes'

    stattype_code = Column(String(10), primary_key=True)
    stattype_text = Column(String(500), nullable=False)
    display_level = Column(SmallInteger)
    selectable = Column(String(1))
    sort_sequence = Column(Integer)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<TUStatType(code='{self.stattype_code}', text='{self.stattype_text}')>"


class TUActivityCode(Base):
    """TU Activity codes - sleeping, working, eating, leisure, etc."""
    __tablename__ = 'bls_tu_actcodes'

    actcode_code = Column(String(10), primary_key=True)
    actcode_text = Column(String(500), nullable=False)
    display_level = Column(SmallInteger)
    selectable = Column(String(1))
    sort_sequence = Column(Integer)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<TUActivityCode(code='{self.actcode_code}', text='{self.actcode_text}')>"


class TUSex(Base):
    """TU Sex categories"""
    __tablename__ = 'bls_tu_sex'

    sex_code = Column(String(5), primary_key=True)
    sex_text = Column(String(100), nullable=False)
    display_level = Column(SmallInteger)
    selectable = Column(String(1))
    sort_sequence = Column(Integer)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<TUSex(code='{self.sex_code}', text='{self.sex_text}')>"


class TUAge(Base):
    """TU Age groups"""
    __tablename__ = 'bls_tu_ages'

    age_code = Column(String(10), primary_key=True)
    age_text = Column(String(200), nullable=False)
    display_level = Column(SmallInteger)
    selectable = Column(String(1))
    sort_sequence = Column(Integer)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<TUAge(code='{self.age_code}', text='{self.age_text}')>"


class TURace(Base):
    """TU Race categories"""
    __tablename__ = 'bls_tu_races'

    race_code = Column(String(5), primary_key=True)
    race_text = Column(String(200), nullable=False)
    display_level = Column(SmallInteger)
    selectable = Column(String(1))
    sort_sequence = Column(Integer)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<TURace(code='{self.race_code}', text='{self.race_text}')>"


class TUEducation(Base):
    """TU Education levels"""
    __tablename__ = 'bls_tu_education'

    educ_code = Column(String(5), primary_key=True)
    educ_text = Column(String(300), nullable=False)
    display_level = Column(SmallInteger)
    selectable = Column(String(1))
    sort_sequence = Column(Integer)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<TUEducation(code='{self.educ_code}', text='{self.educ_text}')>"


class TUMaritalStatus(Base):
    """TU Marital status categories"""
    __tablename__ = 'bls_tu_marital_status'

    maritlstat_code = Column(String(5), primary_key=True)
    maritlstat_text = Column(String(300), nullable=False)
    display_level = Column(SmallInteger)
    selectable = Column(String(1))
    sort_sequence = Column(Integer)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<TUMaritalStatus(code='{self.maritlstat_code}', text='{self.maritlstat_text}')>"


class TULaborForceStatus(Base):
    """TU Labor force status categories"""
    __tablename__ = 'bls_tu_labor_force_status'

    lfstat_code = Column(String(5), primary_key=True)
    lfstat_text = Column(String(200), nullable=False)
    display_level = Column(SmallInteger)
    selectable = Column(String(1))
    sort_sequence = Column(Integer)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<TULaborForceStatus(code='{self.lfstat_code}', text='{self.lfstat_text}')>"


class TUOrigin(Base):
    """TU Hispanic or Latino origin"""
    __tablename__ = 'bls_tu_origin'

    orig_code = Column(String(5), primary_key=True)
    orig_text = Column(String(200), nullable=False)
    display_level = Column(SmallInteger)
    selectable = Column(String(1))
    sort_sequence = Column(Integer)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<TUOrigin(code='{self.orig_code}', text='{self.orig_text}')>"


class TURegion(Base):
    """TU Geographic regions"""
    __tablename__ = 'bls_tu_regions'

    region_code = Column(String(5), primary_key=True)
    region_text = Column(String(200), nullable=False)
    display_level = Column(SmallInteger)
    selectable = Column(String(1))
    sort_sequence = Column(Integer)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<TURegion(code='{self.region_code}', text='{self.region_text}')>"


class TUWhere(Base):
    """TU Where activity took place"""
    __tablename__ = 'bls_tu_where'

    where_code = Column(String(5), primary_key=True)
    where_text = Column(String(200), nullable=False)
    display_level = Column(SmallInteger)
    selectable = Column(String(1))
    sort_sequence = Column(Integer)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<TUWhere(code='{self.where_code}', text='{self.where_text}')>"


class TUWho(Base):
    """TU Who was with the respondent"""
    __tablename__ = 'bls_tu_who'

    who_code = Column(String(5), primary_key=True)
    who_text = Column(String(200), nullable=False)
    display_level = Column(SmallInteger)
    selectable = Column(String(1))
    sort_sequence = Column(Integer)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<TUWho(code='{self.who_code}', text='{self.who_text}')>"


class TUTimeOfDay(Base):
    """TU Time of day categories"""
    __tablename__ = 'bls_tu_timeofday'

    timeday_code = Column(String(5), primary_key=True)
    timeday_text = Column(String(200), nullable=False)
    display_level = Column(SmallInteger)
    selectable = Column(String(1))
    sort_sequence = Column(Integer)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<TUTimeOfDay(code='{self.timeday_code}', text='{self.timeday_text}')>"


class TUSeries(Base):
    """TU Series catalog - metadata for each time use time series"""
    __tablename__ = 'bls_tu_series'

    series_id = Column(String(30), primary_key=True)
    seasonal = Column(String(1))  # S = Seasonally adjusted, U = Not seasonally adjusted
    stattype_code = Column(String(10), ForeignKey('bls_tu_stattypes.stattype_code'))
    datays_code = Column(String(5))  # Data years code
    sex_code = Column(String(5), ForeignKey('bls_tu_sex.sex_code'))
    region_code = Column(String(5), ForeignKey('bls_tu_regions.region_code'))
    lfstat_code = Column(String(5), ForeignKey('bls_tu_labor_force_status.lfstat_code'))
    educ_code = Column(String(5), ForeignKey('bls_tu_education.educ_code'))
    maritlstat_code = Column(String(5), ForeignKey('bls_tu_marital_status.maritlstat_code'))
    age_code = Column(String(10), ForeignKey('bls_tu_ages.age_code'))
    orig_code = Column(String(5), ForeignKey('bls_tu_origin.orig_code'))
    race_code = Column(String(5), ForeignKey('bls_tu_races.race_code'))
    mjcow_code = Column(String(5))  # Class of worker
    nmet_code = Column(String(5))  # Metropolitan status
    where_code = Column(String(5), ForeignKey('bls_tu_where.where_code'))
    sjmj_code = Column(String(5))  # Single/multiple jobholder
    timeday_code = Column(String(5), ForeignKey('bls_tu_timeofday.timeday_code'))
    actcode_code = Column(String(10), ForeignKey('bls_tu_actcodes.actcode_code'))
    industry_code = Column(String(10))  # Industry code
    occ_code = Column(String(10))  # Occupation code
    prhhchild_code = Column(String(5))  # Presence of household children
    earn_code = Column(String(5))  # Earnings category
    disability_code = Column(String(5))  # Disability status
    who_code = Column(String(5), ForeignKey('bls_tu_who.who_code'))
    hhnscc03_code = Column(String(5))  # Household structure
    schenr_code = Column(String(5))  # School enrollment
    prownhhchild_code = Column(String(5))  # Presence of own household children
    work_code = Column(String(5))  # Work status
    elnum_code = Column(String(5))  # Eldercare number
    ecage_code = Column(String(10))  # Eldercare age
    elfreq_code = Column(String(5))  # Eldercare frequency
    eldur_code = Column(String(5))  # Eldercare duration
    elwho_code = Column(String(5))  # Eldercare who
    ecytd_code = Column(String(5))  # Eldercare year-to-date
    elder_code = Column(String(5))  # Eldercare
    lfstatw_code = Column(String(5))  # Labor force status for women
    pertype_code = Column(String(5))  # Person type
    series_title = Column(Text)
    footnote_codes = Column(String(500))
    begin_year = Column(SmallInteger)
    begin_period = Column(String(5))
    end_year = Column(SmallInteger)
    end_period = Column(String(5))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_tu_series_actcode', 'actcode_code'),
        Index('ix_bls_tu_series_stattype', 'stattype_code'),
        Index('ix_bls_tu_series_sex', 'sex_code'),
        Index('ix_bls_tu_series_age', 'age_code'),
        Index('ix_bls_tu_series_active', 'is_active'),
    )

    def __repr__(self):
        return f"<TUSeries(id='{self.series_id}', activity={self.actcode_code}, stat={self.stattype_code})>"


class TUData(Base):
    """TU Time series data - time use observations"""
    __tablename__ = 'bls_tu_data'

    # Composite primary key
    series_id = Column(String(30), ForeignKey('bls_tu_series.series_id'), primary_key=True)
    year = Column(SmallInteger, primary_key=True, nullable=False)
    period = Column(String(5), primary_key=True, nullable=False)  # A01 for annual

    # Data
    value = Column(Numeric(20, 3))
    footnote_codes = Column(String(500))

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_tu_data_year_period', 'year', 'period'),
        Index('ix_bls_tu_data_series_year', 'series_id', 'year'),
    )

    def __repr__(self):
        return f"<TUData(series='{self.series_id}', date={self.year}-{self.period}, value={self.value})>"


class TUAspect(Base):
    """TU Aspect data - standard errors and other statistical aspects"""
    __tablename__ = 'bls_tu_aspect'

    # Composite primary key
    series_id = Column(String(30), ForeignKey('bls_tu_series.series_id'), primary_key=True)
    year = Column(SmallInteger, primary_key=True, nullable=False)
    period = Column(String(5), primary_key=True, nullable=False)
    aspect_type = Column(String(5), primary_key=True, nullable=False)  # E = Standard error

    # Data
    value = Column(Numeric(20, 3))
    footnote_codes = Column(String(500))

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_tu_aspect_series_year', 'series_id', 'year'),
        Index('ix_bls_tu_aspect_year_period', 'year', 'period'),
    )

    def __repr__(self):
        return f"<TUAspect(series='{self.series_id}', date={self.year}-{self.period}, type={self.aspect_type}, value={self.value})>"


# ==================== LN (LABOR FORCE STATISTICS FROM CPS) MODELS ====================

class LNLaborForceStatus(Base):
    """LN Labor force status codes"""
    __tablename__ = 'bls_ln_lfst'

    lfst_code = Column(String(5), primary_key=True)
    lfst_text = Column(String(500), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<LNLaborForceStatus(code='{self.lfst_code}', text='{self.lfst_text}')>"


class LNPeriodicity(Base):
    """LN Periodicity codes"""
    __tablename__ = 'bls_ln_periodicity'

    periodicity_code = Column(String(5), primary_key=True)
    periodicity_text = Column(String(100), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<LNPeriodicity(code='{self.periodicity_code}', text='{self.periodicity_text}')>"


class LNAbsence(Base):
    """LN Absence codes"""
    __tablename__ = 'bls_ln_absn'

    absn_code = Column(String(5), primary_key=True)
    absn_text = Column(String(500), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<LNAbsence(code='{self.absn_code}', text='{self.absn_text}')>"


class LNActivity(Base):
    """LN Activity codes"""
    __tablename__ = 'bls_ln_activity'

    activity_code = Column(String(5), primary_key=True)
    activity_text = Column(String(500), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<LNActivity(code='{self.activity_code}', text='{self.activity_text}')>"


class LNAge(Base):
    """LN Age group codes"""
    __tablename__ = 'bls_ln_ages'

    ages_code = Column(String(5), primary_key=True)
    ages_text = Column(String(300), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<LNAge(code='{self.ages_code}', text='{self.ages_text}')>"


class LNCertification(Base):
    """LN Certification codes"""
    __tablename__ = 'bls_ln_cert'

    cert_code = Column(String(5), primary_key=True)
    cert_text = Column(String(300), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<LNCertification(code='{self.cert_code}', text='{self.cert_text}')>"


class LNClass(Base):
    """LN Class of worker codes"""
    __tablename__ = 'bls_ln_class'

    class_code = Column(String(5), primary_key=True)
    class_text = Column(String(500), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<LNClass(code='{self.class_code}', text='{self.class_text}')>"


class LNDuration(Base):
    """LN Duration codes"""
    __tablename__ = 'bls_ln_duration'

    duration_code = Column(String(5), primary_key=True)
    duration_text = Column(String(500), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<LNDuration(code='{self.duration_code}', text='{self.duration_text}')>"


class LNEducation(Base):
    """LN Education codes"""
    __tablename__ = 'bls_ln_education'

    education_code = Column(String(5), primary_key=True)
    education_text = Column(String(500), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<LNEducation(code='{self.education_code}', text='{self.education_text}')>"


class LNEntrance(Base):
    """LN Entrance to labor force codes"""
    __tablename__ = 'bls_ln_entr'

    entr_code = Column(String(5), primary_key=True)
    entr_text = Column(String(300), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<LNEntrance(code='{self.entr_code}', text='{self.entr_text}')>"


class LNExperience(Base):
    """LN Work experience codes"""
    __tablename__ = 'bls_ln_expr'

    expr_code = Column(String(5), primary_key=True)
    expr_text = Column(String(300), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<LNExperience(code='{self.expr_code}', text='{self.expr_text}')>"


class LNHeadOfHousehold(Base):
    """LN Head of household codes"""
    __tablename__ = 'bls_ln_hheader'

    hheader_code = Column(String(5), primary_key=True)
    hheader_text = Column(String(300), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<LNHeadOfHousehold(code='{self.hheader_code}', text='{self.hheader_text}')>"


class LNHour(Base):
    """LN Hours worked codes"""
    __tablename__ = 'bls_ln_hour'

    hour_code = Column(String(5), primary_key=True)
    hour_text = Column(String(500), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<LNHour(code='{self.hour_code}', text='{self.hour_text}')>"


class LNIndustry(Base):
    """LN Industry codes"""
    __tablename__ = 'bls_ln_indy'

    indy_code = Column(String(10), primary_key=True)
    indy_text = Column(String(500), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<LNIndustry(code='{self.indy_code}', text='{self.indy_text}')>"


class LNJobDesire(Base):
    """LN Want a job codes"""
    __tablename__ = 'bls_ln_jdes'

    jdes_code = Column(String(5), primary_key=True)
    jdes_text = Column(String(300), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<LNJobDesire(code='{self.jdes_code}', text='{self.jdes_text}')>"


class LNLook(Base):
    """LN Job seeker codes"""
    __tablename__ = 'bls_ln_look'

    look_code = Column(String(5), primary_key=True)
    look_text = Column(String(500), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<LNLook(code='{self.look_code}', text='{self.look_text}')>"


class LNMaritalStatus(Base):
    """LN Marital status codes"""
    __tablename__ = 'bls_ln_mari'

    mari_code = Column(String(5), primary_key=True)
    mari_text = Column(String(500), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<LNMaritalStatus(code='{self.mari_code}', text='{self.mari_text}')>"


class LNMultipleJobholder(Base):
    """LN Multiple jobholder codes"""
    __tablename__ = 'bls_ln_mjhs'

    mjhs_code = Column(String(5), primary_key=True)
    mjhs_text = Column(String(500), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<LNMultipleJobholder(code='{self.mjhs_code}', text='{self.mjhs_text}')>"


class LNOccupation(Base):
    """LN Occupation codes"""
    __tablename__ = 'bls_ln_occupation'

    occupation_code = Column(String(10), primary_key=True)
    occupation_text = Column(String(500), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<LNOccupation(code='{self.occupation_code}', text='{self.occupation_text}')>"


class LNOrigin(Base):
    """LN Hispanic or Latino origin codes"""
    __tablename__ = 'bls_ln_orig'

    orig_code = Column(String(5), primary_key=True)
    orig_text = Column(String(300), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<LNOrigin(code='{self.orig_code}', text='{self.orig_text}')>"


class LNPercentage(Base):
    """LN Percentage codes"""
    __tablename__ = 'bls_ln_pcts'

    pcts_code = Column(String(5), primary_key=True)
    pcts_text = Column(String(500), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<LNPercentage(code='{self.pcts_code}', text='{self.pcts_text}')>"


class LNRace(Base):
    """LN Race codes"""
    __tablename__ = 'bls_ln_race'

    race_code = Column(String(5), primary_key=True)
    race_text = Column(String(300), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<LNRace(code='{self.race_code}', text='{self.race_text}')>"


class LNAbsenceReason(Base):
    """LN Absence reason codes"""
    __tablename__ = 'bls_ln_rjnw'

    rjnw_code = Column(String(5), primary_key=True)
    rjnw_text = Column(String(500), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<LNAbsenceReason(code='{self.rjnw_code}', text='{self.rjnw_text}')>"


class LNJobSearch(Base):
    """LN Job search codes"""
    __tablename__ = 'bls_ln_rnlf'

    rnlf_code = Column(String(5), primary_key=True)
    rnlf_text = Column(String(500), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<LNJobSearch(code='{self.rnlf_code}', text='{self.rnlf_text}')>"


class LNPartTimeReason(Base):
    """LN Part time reason codes"""
    __tablename__ = 'bls_ln_rwns'

    rwns_code = Column(String(5), primary_key=True)
    rwns_text = Column(String(500), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<LNPartTimeReason(code='{self.rwns_code}', text='{self.rwns_text}')>"


class LNSeek(Base):
    """LN Job seeker codes"""
    __tablename__ = 'bls_ln_seek'

    seek_code = Column(String(5), primary_key=True)
    seek_text = Column(String(300), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<LNSeek(code='{self.seek_code}', text='{self.seek_text}')>"


class LNSex(Base):
    """LN Sex codes"""
    __tablename__ = 'bls_ln_sexs'

    sexs_code = Column(String(5), primary_key=True)
    sexs_text = Column(String(100), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<LNSex(code='{self.sexs_code}', text='{self.sexs_text}')>"


class LNDataType(Base):
    """LN Data type codes"""
    __tablename__ = 'bls_ln_tdat'

    tdat_code = Column(String(5), primary_key=True)
    tdat_text = Column(String(500), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<LNDataType(code='{self.tdat_code}', text='{self.tdat_text}')>"


class LNVeteran(Base):
    """LN Veteran status codes"""
    __tablename__ = 'bls_ln_vets'

    vets_code = Column(String(5), primary_key=True)
    vets_text = Column(String(300), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<LNVeteran(code='{self.vets_code}', text='{self.vets_text}')>"


class LNWorkStatus(Base):
    """LN Work status codes"""
    __tablename__ = 'bls_ln_wkst'

    wkst_code = Column(String(5), primary_key=True)
    wkst_text = Column(String(500), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<LNWorkStatus(code='{self.wkst_code}', text='{self.wkst_text}')>"


class LNBorn(Base):
    """LN Nativity/Citizenship codes"""
    __tablename__ = 'bls_ln_born'

    born_code = Column(String(5), primary_key=True)
    born_text = Column(String(300), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<LNBorn(code='{self.born_code}', text='{self.born_text}')>"


class LNChild(Base):
    """LN Presence of children codes"""
    __tablename__ = 'bls_ln_chld'

    chld_code = Column(String(5), primary_key=True)
    chld_text = Column(String(300), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<LNChild(code='{self.chld_code}', text='{self.chld_text}')>"


class LNDisability(Base):
    """LN Disability codes"""
    __tablename__ = 'bls_ln_disa'

    disa_code = Column(String(5), primary_key=True)
    disa_text = Column(String(300), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<LNDisability(code='{self.disa_code}', text='{self.disa_text}')>"


class LNTelework(Base):
    """LN Telework codes"""
    __tablename__ = 'bls_ln_tlwk'

    tlwk_code = Column(String(5), primary_key=True)
    tlwk_text = Column(String(300), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<LNTelework(code='{self.tlwk_code}', text='{self.tlwk_text}')>"


class LNSeries(Base):
    """LN Series catalog - metadata for each CPS labor force time series"""
    __tablename__ = 'bls_ln_series'

    series_id = Column(String(20), primary_key=True)

    # Foreign keys to reference tables
    lfst_code = Column(String(5), ForeignKey('bls_ln_lfst.lfst_code'))
    periodicity_code = Column(String(5), ForeignKey('bls_ln_periodicity.periodicity_code'))
    absn_code = Column(String(5), ForeignKey('bls_ln_absn.absn_code'))
    activity_code = Column(String(5), ForeignKey('bls_ln_activity.activity_code'))
    ages_code = Column(String(5), ForeignKey('bls_ln_ages.ages_code'))
    cert_code = Column(String(5), ForeignKey('bls_ln_cert.cert_code'))
    class_code = Column(String(5), ForeignKey('bls_ln_class.class_code'))
    duration_code = Column(String(5), ForeignKey('bls_ln_duration.duration_code'))
    education_code = Column(String(5), ForeignKey('bls_ln_education.education_code'))
    entr_code = Column(String(5), ForeignKey('bls_ln_entr.entr_code'))
    expr_code = Column(String(5), ForeignKey('bls_ln_expr.expr_code'))
    hheader_code = Column(String(5), ForeignKey('bls_ln_hheader.hheader_code'))
    hour_code = Column(String(5), ForeignKey('bls_ln_hour.hour_code'))
    indy_code = Column(String(10), ForeignKey('bls_ln_indy.indy_code'))
    jdes_code = Column(String(5), ForeignKey('bls_ln_jdes.jdes_code'))
    look_code = Column(String(5), ForeignKey('bls_ln_look.look_code'))
    mari_code = Column(String(5), ForeignKey('bls_ln_mari.mari_code'))
    mjhs_code = Column(String(5), ForeignKey('bls_ln_mjhs.mjhs_code'))
    occupation_code = Column(String(10), ForeignKey('bls_ln_occupation.occupation_code'))
    orig_code = Column(String(5), ForeignKey('bls_ln_orig.orig_code'))
    pcts_code = Column(String(5), ForeignKey('bls_ln_pcts.pcts_code'))
    race_code = Column(String(5), ForeignKey('bls_ln_race.race_code'))
    rjnw_code = Column(String(5), ForeignKey('bls_ln_rjnw.rjnw_code'))
    rnlf_code = Column(String(5), ForeignKey('bls_ln_rnlf.rnlf_code'))
    rwns_code = Column(String(5), ForeignKey('bls_ln_rwns.rwns_code'))
    seek_code = Column(String(5), ForeignKey('bls_ln_seek.seek_code'))
    sexs_code = Column(String(5), ForeignKey('bls_ln_sexs.sexs_code'))
    tdat_code = Column(String(5), ForeignKey('bls_ln_tdat.tdat_code'))
    vets_code = Column(String(5), ForeignKey('bls_ln_vets.vets_code'))
    wkst_code = Column(String(5), ForeignKey('bls_ln_wkst.wkst_code'))
    born_code = Column(String(5), ForeignKey('bls_ln_born.born_code'))
    chld_code = Column(String(5), ForeignKey('bls_ln_chld.chld_code'))
    disa_code = Column(String(5), ForeignKey('bls_ln_disa.disa_code'))
    tlwk_code = Column(String(5), ForeignKey('bls_ln_tlwk.tlwk_code'))

    # Seasonal adjustment
    seasonal = Column(String(1))  # S = Seasonally adjusted, U = Not seasonally adjusted

    # Series metadata
    series_title = Column(Text, nullable=False)
    footnote_codes = Column(String(500))
    begin_year = Column(SmallInteger)
    begin_period = Column(String(5))
    end_year = Column(SmallInteger)
    end_period = Column(String(5))

    # Helper field for active series
    is_active = Column(Boolean, default=True)

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_ln_series_lfst', 'lfst_code'),
        Index('ix_bls_ln_series_ages', 'ages_code'),
        Index('ix_bls_ln_series_sexs', 'sexs_code'),
        Index('ix_bls_ln_series_race', 'race_code'),
        Index('ix_bls_ln_series_education', 'education_code'),
        Index('ix_bls_ln_series_occupation', 'occupation_code'),
        Index('ix_bls_ln_series_industry', 'indy_code'),
        Index('ix_bls_ln_series_seasonal', 'seasonal'),
        Index('ix_bls_ln_series_active', 'is_active'),
    )

    def __repr__(self):
        return f"<LNSeries(id='{self.series_id}', title='{self.series_title[:50]}')>"


class LNData(Base):
    """LN Time series data - CPS labor force observations"""
    __tablename__ = 'bls_ln_data'

    # Composite primary key
    series_id = Column(String(20), ForeignKey('bls_ln_series.series_id'), primary_key=True)
    year = Column(SmallInteger, primary_key=True, nullable=False)
    period = Column(String(5), primary_key=True, nullable=False)  # M01-M13, Q01-Q04, A01

    # Data
    value = Column(Numeric(20, 1))  # Labor force counts (thousands) or rates (%)
    footnote_codes = Column(String(500))

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_ln_data_year_period', 'year', 'period'),
        Index('ix_bls_ln_data_series_year', 'series_id', 'year'),
    )

    def __repr__(self):
        return f"<LNData(series='{self.series_id}', date={self.year}-{self.period}, value={self.value})>"


# ==================== EI (IMPORT/EXPORT PRICE INDEXES) MODELS ====================

class EIIndex(Base):
    """Index types for Import/Export Price Indexes"""
    __tablename__ = 'bls_ei_indexes'

    index_code = Column(String(5), primary_key=True)  # 'CD', 'CO', 'CT', 'IC', etc.
    index_name = Column(String(255), nullable=False)  # 'Locality of Destination Price Indexes', etc.

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<EIIndex(code='{self.index_code}', name='{self.index_name}')>"


class EISeries(Base):
    """EI Series catalog - metadata for each import/export price index series"""
    __tablename__ = 'bls_ei_series'

    series_id = Column(String(30), primary_key=True)  # 'EIUCDCANMANU', etc.

    # Series classification
    seasonal_code = Column(String(1))  # 'S' (seasonally adjusted) or 'U' (not adjusted)
    index_code = Column(String(5), ForeignKey('bls_ei_indexes.index_code'))
    series_name = Column(String(255))  # 'Canada-Manufacturing', etc.
    base_period = Column(String(50))  # 'December 2020=100', etc.

    # Series metadata
    series_title = Column(Text, nullable=False)
    footnote_codes = Column(String(500))
    begin_year = Column(SmallInteger)
    begin_period = Column(String(5))
    end_year = Column(SmallInteger)
    end_period = Column(String(5))

    # Helper field for active series
    is_active = Column(Boolean, default=True)

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_ei_series_index', 'index_code'),
        Index('ix_bls_ei_series_seasonal', 'seasonal_code'),
        Index('ix_bls_ei_series_active', 'is_active'),
    )

    def __repr__(self):
        return f"<EISeries(id='{self.series_id}', name='{self.series_name}')>"


class EIData(Base):
    """EI Time series data - import/export price index observations"""
    __tablename__ = 'bls_ei_data'

    # Composite primary key
    series_id = Column(String(30), ForeignKey('bls_ei_series.series_id'), primary_key=True)
    year = Column(SmallInteger, primary_key=True, nullable=False)
    period = Column(String(5), primary_key=True, nullable=False)  # M01-M12

    # Data
    value = Column(Numeric(20, 3))  # Index values
    footnote_codes = Column(String(500))

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_ei_data_year_period', 'year', 'period'),
        Index('ix_bls_ei_data_series_year', 'series_id', 'year'),
    )

    def __repr__(self):
        return f"<EIData(series='{self.series_id}', date={self.year}-{self.period}, value={self.value})>"


# ==================== BD (BUSINESS EMPLOYMENT DYNAMICS) MODELS ====================

class BDState(Base):
    """State codes for Business Employment Dynamics"""
    __tablename__ = 'bls_bd_states'

    state_code = Column(String(5), primary_key=True)  # '00', '01', '02', etc.
    state_name = Column(String(100), nullable=False)  # 'U.S. totals', 'Alabama', etc.

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<BDState(code='{self.state_code}', name='{self.state_name}')>"


class BDIndustry(Base):
    """Industry codes for Business Employment Dynamics"""
    __tablename__ = 'bls_bd_industries'

    industry_code = Column(String(10), primary_key=True)  # '000000', '100000', etc.
    industry_name = Column(String(255), nullable=False)
    display_level = Column(SmallInteger)
    selectable = Column(String(1))
    sort_sequence = Column(SmallInteger)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<BDIndustry(code='{self.industry_code}', name='{self.industry_name}')>"


class BDDataClass(Base):
    """Data class codes for Business Employment Dynamics"""
    __tablename__ = 'bls_bd_dataclasses'

    dataclass_code = Column(String(5), primary_key=True)  # '01', '02', etc.
    dataclass_name = Column(String(255), nullable=False)  # 'Gross Job Gains', 'Expansions', etc.
    display_level = Column(SmallInteger)
    selectable = Column(String(1))
    sort_sequence = Column(SmallInteger)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<BDDataClass(code='{self.dataclass_code}', name='{self.dataclass_name}')>"


class BDDataElement(Base):
    """Data element codes for Business Employment Dynamics"""
    __tablename__ = 'bls_bd_dataelements'

    dataelement_code = Column(String(5), primary_key=True)  # '1', '2'
    dataelement_name = Column(String(100), nullable=False)  # 'Employment', 'Number of Establishments'

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<BDDataElement(code='{self.dataelement_code}', name='{self.dataelement_name}')>"


class BDSizeClass(Base):
    """Size class codes for Business Employment Dynamics"""
    __tablename__ = 'bls_bd_sizeclasses'

    sizeclass_code = Column(String(5), primary_key=True)  # '00', '01', etc.
    sizeclass_name = Column(String(255), nullable=False)  # 'All size classes', '1 to 4 employees', etc.

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<BDSizeClass(code='{self.sizeclass_code}', name='{self.sizeclass_name}')>"


class BDRateLevel(Base):
    """Rate/level codes for Business Employment Dynamics"""
    __tablename__ = 'bls_bd_ratelevels'

    ratelevel_code = Column(String(5), primary_key=True)  # 'L', 'R'
    ratelevel_name = Column(String(50), nullable=False)  # 'Level', 'Rate'

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<BDRateLevel(code='{self.ratelevel_code}', name='{self.ratelevel_name}')>"


class BDUnitAnalysis(Base):
    """Unit of analysis codes for Business Employment Dynamics"""
    __tablename__ = 'bls_bd_unitanalysis'

    unitanalysis_code = Column(String(5), primary_key=True)  # '1'
    unitanalysis_name = Column(String(100), nullable=False)  # 'Establishment'

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<BDUnitAnalysis(code='{self.unitanalysis_code}', name='{self.unitanalysis_name}')>"


class BDOwnership(Base):
    """Ownership codes for Business Employment Dynamics"""
    __tablename__ = 'bls_bd_ownership'

    ownership_code = Column(String(5), primary_key=True)  # '5'
    ownership_name = Column(String(100), nullable=False)  # 'Private Sector'

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<BDOwnership(code='{self.ownership_code}', name='{self.ownership_name}')>"


class BDSeries(Base):
    """BD Series catalog - metadata for each business dynamics time series"""
    __tablename__ = 'bls_bd_series'

    series_id = Column(String(30), primary_key=True)  # 'BDS0000000000000000110001LQ5', etc.

    # Series classification (foreign keys to reference tables)
    seasonal_code = Column(String(1))  # 'S' (seasonally adjusted) or 'U' (not adjusted)
    msa_code = Column(String(10))  # MSA code
    state_code = Column(String(5), ForeignKey('bls_bd_states.state_code'))
    county_code = Column(String(10))  # County code
    industry_code = Column(String(10), ForeignKey('bls_bd_industries.industry_code'))
    unitanalysis_code = Column(String(5), ForeignKey('bls_bd_unitanalysis.unitanalysis_code'))
    dataelement_code = Column(String(5), ForeignKey('bls_bd_dataelements.dataelement_code'))
    sizeclass_code = Column(String(5), ForeignKey('bls_bd_sizeclasses.sizeclass_code'))
    dataclass_code = Column(String(5), ForeignKey('bls_bd_dataclasses.dataclass_code'))
    ratelevel_code = Column(String(5), ForeignKey('bls_bd_ratelevels.ratelevel_code'))
    periodicity_code = Column(String(5), ForeignKey('bls_periodicity.periodicity_code'))
    ownership_code = Column(String(5), ForeignKey('bls_bd_ownership.ownership_code'))

    # Series metadata
    series_title = Column(Text, nullable=False)
    footnote_codes = Column(String(500))
    begin_year = Column(SmallInteger)
    begin_period = Column(String(5))
    end_year = Column(SmallInteger)
    end_period = Column(String(5))

    # Helper field for active series
    is_active = Column(Boolean, default=True)

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_bd_series_state', 'state_code'),
        Index('ix_bls_bd_series_industry', 'industry_code'),
        Index('ix_bls_bd_series_dataclass', 'dataclass_code'),
        Index('ix_bls_bd_series_dataelement', 'dataelement_code'),
        Index('ix_bls_bd_series_sizeclass', 'sizeclass_code'),
        Index('ix_bls_bd_series_active', 'is_active'),
    )

    def __repr__(self):
        return f"<BDSeries(id='{self.series_id}', dataclass={self.dataclass_code})>"


class BDData(Base):
    """BD Time series data - business employment dynamics observations"""
    __tablename__ = 'bls_bd_data'

    # Composite primary key
    series_id = Column(String(30), ForeignKey('bls_bd_series.series_id'), primary_key=True)
    year = Column(SmallInteger, primary_key=True, nullable=False)
    period = Column(String(5), primary_key=True, nullable=False)  # Q01-Q04

    # Data
    value = Column(Numeric(20, 1))  # Employment counts (thousands) or rates (%)
    footnote_codes = Column(String(500))

    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bls_bd_data_year_period', 'year', 'period'),
        Index('ix_bls_bd_data_series_year', 'series_id', 'year'),
    )

    def __repr__(self):
        return f"<BDData(series='{self.series_id}', date={self.year}-{self.period}, value={self.value})>"


# ==================== HELPER FUNCTIONS ====================

def create_bls_tables(engine):
    """Create all BLS tables in the database"""
    Base.metadata.create_all(engine)
    print("BLS tables created successfully!")


def drop_bls_tables(engine):
    """Drop all BLS tables (use with caution!)"""
    Base.metadata.drop_all(engine)
    print("BLS tables dropped!")


if __name__ == "__main__":
    # Example usage
    from sqlalchemy import create_engine
    import os

    # Get database URL from environment
    DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://user:password@localhost:5432/finexus_db')

    engine = create_engine(DATABASE_URL, echo=True)
    create_bls_tables(engine)

    print("\nBLS Tables created:")
    print("  - bls_surveys (Survey catalog)")
    print("  - bls_areas (Geographic areas)")
    print("  - bls_periods (Period definitions)")
    print("  - bls_ap_items (AP item catalog)")
    print("  - bls_ap_series (AP series metadata)")
    print("  - bls_ap_data (AP time series data)")
