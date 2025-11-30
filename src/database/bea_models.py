"""
BEA (Bureau of Economic Analysis) Database Models

Separate models file for BEA data to keep it isolated and maintainable.
Covers NIPA (National Income and Product Accounts) and Regional datasets.

Author: FinExus Data Collector
Created: 2025-11-26
"""
from datetime import datetime, UTC
from sqlalchemy import (
    Column, Integer, String, Numeric, Date, DateTime,
    Boolean, Text, ForeignKey, ForeignKeyConstraint, Index, UniqueConstraint, SmallInteger
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


# ==================== REFERENCE TABLES ====================

class BEADataset(Base):
    """BEA Dataset catalog - master list of all BEA datasets"""
    __tablename__ = 'bea_datasets'

    dataset_name = Column(String(50), primary_key=True)  # 'NIPA', 'Regional', etc.
    dataset_description = Column(String(500), nullable=False)
    is_active = Column(Boolean, default=True)

    # URLs for documentation
    doc_url = Column(String(500))

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    def __repr__(self):
        return f"<BEADataset(name='{self.dataset_name}')>"


# ==================== NIPA (National Income and Product Accounts) TABLES ====================

class NIPATable(Base):
    """NIPA Table catalog - metadata for available NIPA tables"""
    __tablename__ = 'bea_nipa_tables'

    table_name = Column(String(20), primary_key=True)  # 'T10101', 'T20600', etc.
    table_description = Column(Text, nullable=False)

    # Available frequencies for this table
    has_annual = Column(Boolean, default=True)
    has_quarterly = Column(Boolean, default=False)
    has_monthly = Column(Boolean, default=False)

    # Year range
    first_year = Column(SmallInteger)
    last_year = Column(SmallInteger)

    # Status
    is_active = Column(Boolean, default=True)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bea_nipa_tables_active', 'is_active'),
    )

    def __repr__(self):
        return f"<NIPATable(name='{self.table_name}')>"


class NIPASeries(Base):
    """NIPA Series catalog - metadata for each NIPA time series"""
    __tablename__ = 'bea_nipa_series'

    series_code = Column(String(50), primary_key=True)  # Unique series identifier
    table_name = Column(String(20), ForeignKey('bea_nipa_tables.table_name'), nullable=False, index=True)

    line_number = Column(SmallInteger, nullable=False)
    line_description = Column(Text, nullable=False)

    # Metric information
    metric_name = Column(String(100))  # 'Current Dollars', 'Fisher Price Index', etc.
    cl_unit = Column(String(50))  # Calculation unit type
    unit_mult = Column(SmallInteger)  # Base-10 exponent (6 = millions, 9 = billions)

    # Status
    is_active = Column(Boolean, default=True)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bea_nipa_series_table_line', 'table_name', 'line_number'),
        Index('ix_bea_nipa_series_active', 'is_active'),
    )

    def __repr__(self):
        return f"<NIPASeries(code='{self.series_code}', line={self.line_number})>"


class NIPAData(Base):
    """NIPA Time series data - actual NIPA values"""
    __tablename__ = 'bea_nipa_data'

    # Composite primary key
    series_code = Column(String(50), ForeignKey('bea_nipa_series.series_code'), primary_key=True)
    time_period = Column(String(10), primary_key=True)  # 'YYYY' for annual, 'YYYYQn' for quarterly, 'YYYYMn' for monthly

    # Data
    value = Column(Numeric(20, 6))  # Data value (can be NULL for missing data)

    # Metadata
    note_ref = Column(String(100))  # Reference to footnotes

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bea_nipa_data_series', 'series_code'),
        Index('ix_bea_nipa_data_period', 'time_period'),
    )

    def __repr__(self):
        return f"<NIPAData(series='{self.series_code}', period={self.time_period}, value={self.value})>"


# ==================== REGIONAL TABLES ====================

class RegionalTable(Base):
    """Regional Table catalog - metadata for available Regional tables"""
    __tablename__ = 'bea_regional_tables'

    table_name = Column(String(20), primary_key=True)  # 'CAINC1', 'SAGDP1', etc.
    table_description = Column(Text, nullable=False)

    # Geographic scope
    geo_scope = Column(String(50))  # 'States', 'Counties', 'Metros', etc.

    # Year range
    first_year = Column(SmallInteger)
    last_year = Column(SmallInteger)

    # Status
    is_active = Column(Boolean, default=True)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bea_regional_tables_active', 'is_active'),
        Index('ix_bea_regional_tables_scope', 'geo_scope'),
    )

    def __repr__(self):
        return f"<RegionalTable(name='{self.table_name}')>"


class RegionalLineCode(Base):
    """Regional Line Code catalog - available statistics per table"""
    __tablename__ = 'bea_regional_line_codes'

    table_name = Column(String(20), ForeignKey('bea_regional_tables.table_name'), primary_key=True)
    line_code = Column(SmallInteger, primary_key=True)

    line_description = Column(Text, nullable=False)

    # Unit information
    cl_unit = Column(String(50))  # 'dollars', 'percent', 'number', etc.
    unit_mult = Column(SmallInteger)  # Base-10 exponent

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bea_regional_line_codes_table', 'table_name'),
    )

    def __repr__(self):
        return f"<RegionalLineCode(table='{self.table_name}', line={self.line_code})>"


class RegionalGeoFips(Base):
    """Regional Geographic FIPS codes"""
    __tablename__ = 'bea_regional_geo_fips'

    geo_fips = Column(String(10), primary_key=True)  # '00000' for US, '01000' for Alabama, etc.
    geo_name = Column(String(255), nullable=False)
    geo_type = Column(String(50))  # 'Nation', 'State', 'County', 'MSA', 'Region', etc.

    # Parent relationship for hierarchy
    parent_fips = Column(String(10))  # State FIPS for counties, etc.

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bea_regional_geo_fips_type', 'geo_type'),
        Index('ix_bea_regional_geo_fips_parent', 'parent_fips'),
    )

    def __repr__(self):
        return f"<RegionalGeoFips(fips='{self.geo_fips}', name='{self.geo_name}')>"


class RegionalData(Base):
    """Regional Time series data - actual Regional values"""
    __tablename__ = 'bea_regional_data'

    # Composite primary key
    table_name = Column(String(20), primary_key=True)
    line_code = Column(SmallInteger, primary_key=True)
    geo_fips = Column(String(10), primary_key=True)
    time_period = Column(String(10), primary_key=True)  # 'YYYY' for annual, 'YYYYQn' for quarterly

    # Data
    value = Column(Numeric(20, 6))  # Data value (can be NULL for missing data)

    # Metadata from API response
    cl_unit = Column(String(50))
    unit_mult = Column(SmallInteger)

    # Footnote reference
    note_ref = Column(String(100))

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ['table_name', 'line_code'],
            ['bea_regional_line_codes.table_name', 'bea_regional_line_codes.line_code']
        ),
        ForeignKeyConstraint(
            ['geo_fips'],
            ['bea_regional_geo_fips.geo_fips']
        ),
        Index('ix_bea_regional_data_table', 'table_name'),
        Index('ix_bea_regional_data_geo', 'geo_fips'),
        Index('ix_bea_regional_data_period', 'time_period'),
        Index('ix_bea_regional_data_table_line', 'table_name', 'line_code'),
        Index('ix_bea_regional_data_geo_period', 'geo_fips', 'time_period'),
    )

    def __repr__(self):
        return f"<RegionalData(table='{self.table_name}', geo='{self.geo_fips}', period={self.time_period})>"


# ==================== SUMMARY/AGGREGATION TABLES ====================

class GDPSummary(Base):
    """GDP Summary data for quick access to key GDP metrics"""
    __tablename__ = 'bea_gdp_summary'

    # Composite primary key
    geo_fips = Column(String(10), primary_key=True)  # State or national FIPS
    year = Column(SmallInteger, primary_key=True)
    quarter = Column(String(5))  # NULL for annual, 'Q1'-'Q4' for quarterly

    # Key GDP metrics (in millions of dollars unless otherwise noted)
    gdp_current = Column(Numeric(20, 2))  # Current dollar GDP
    gdp_real = Column(Numeric(20, 2))  # Real GDP (chained dollars)
    gdp_percent_change = Column(Numeric(10, 4))  # Percent change from prior period

    # Per capita
    gdp_per_capita = Column(Numeric(15, 2))  # Per capita GDP

    # Population (for reference)
    population = Column(Integer)

    # Source tracking
    source_table = Column(String(20))  # Which BEA table this came from

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bea_gdp_summary_geo', 'geo_fips'),
        Index('ix_bea_gdp_summary_year', 'year'),
        Index('ix_bea_gdp_summary_geo_year', 'geo_fips', 'year'),
    )

    def __repr__(self):
        period = f"{self.year}{self.quarter}" if self.quarter else str(self.year)
        return f"<GDPSummary(geo='{self.geo_fips}', period={period}, gdp={self.gdp_current})>"


class PersonalIncomeSummary(Base):
    """Personal Income Summary data for quick access to key income metrics"""
    __tablename__ = 'bea_personal_income_summary'

    # Composite primary key
    geo_fips = Column(String(10), primary_key=True)  # State, county, or national FIPS
    year = Column(SmallInteger, primary_key=True)
    quarter = Column(String(5))  # NULL for annual, 'Q1'-'Q4' for quarterly

    # Key income metrics (in thousands of dollars unless otherwise noted)
    personal_income = Column(Numeric(20, 2))  # Total personal income
    per_capita_income = Column(Numeric(15, 2))  # Per capita personal income (dollars)

    # Components
    wages_salaries = Column(Numeric(20, 2))
    supplements_to_wages = Column(Numeric(20, 2))
    proprietors_income = Column(Numeric(20, 2))
    dividends_interest_rent = Column(Numeric(20, 2))
    personal_current_transfer_receipts = Column(Numeric(20, 2))

    # Population (for reference)
    population = Column(Integer)

    # Source tracking
    source_table = Column(String(20))  # Which BEA table this came from

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bea_pi_summary_geo', 'geo_fips'),
        Index('ix_bea_pi_summary_year', 'year'),
        Index('ix_bea_pi_summary_geo_year', 'geo_fips', 'year'),
    )

    def __repr__(self):
        period = f"{self.year}{self.quarter}" if self.quarter else str(self.year)
        return f"<PersonalIncomeSummary(geo='{self.geo_fips}', period={period}, pi={self.personal_income})>"


# ==================== GDP BY INDUSTRY TABLES ====================

class GDPByIndustryTable(Base):
    """GDP by Industry Table catalog - metadata for available tables"""
    __tablename__ = 'bea_gdpbyindustry_tables'

    table_id = Column(Integer, primary_key=True)  # 1, 2, 3, etc.
    table_description = Column(Text, nullable=False)

    # Available frequencies for this table
    has_annual = Column(Boolean, default=True)
    has_quarterly = Column(Boolean, default=False)

    # Year range (annual from 1997, quarterly from 2005)
    first_annual_year = Column(SmallInteger)
    last_annual_year = Column(SmallInteger)
    first_quarterly_year = Column(SmallInteger)
    last_quarterly_year = Column(SmallInteger)

    # Status
    is_active = Column(Boolean, default=True)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bea_gdpbyindustry_tables_active', 'is_active'),
    )

    def __repr__(self):
        return f"<GDPByIndustryTable(id={self.table_id})>"


class GDPByIndustryIndustry(Base):
    """GDP by Industry - Industry codes catalog"""
    __tablename__ = 'bea_gdpbyindustry_industries'

    industry_code = Column(String(20), primary_key=True)  # 'ALL', '11', '21', 'FIRE', etc.
    industry_description = Column(Text, nullable=False)

    # Industry hierarchy
    parent_code = Column(String(20))  # Parent industry for roll-ups
    industry_level = Column(SmallInteger)  # 1=sector, 2=subsector, 3=industry group

    # Status
    is_active = Column(Boolean, default=True)

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bea_gdpbyindustry_industries_active', 'is_active'),
        Index('ix_bea_gdpbyindustry_industries_parent', 'parent_code'),
    )

    def __repr__(self):
        return f"<GDPByIndustryIndustry(code='{self.industry_code}')>"


class GDPByIndustryData(Base):
    """GDP by Industry time series data"""
    __tablename__ = 'bea_gdpbyindustry_data'

    # Composite primary key
    # Note: row_type is needed for tables 6, 7 which have multiple rows per industry
    # (total, compensation, taxes, surplus)
    table_id = Column(Integer, ForeignKey('bea_gdpbyindustry_tables.table_id'), primary_key=True)
    industry_code = Column(String(20), ForeignKey('bea_gdpbyindustry_industries.industry_code'), primary_key=True)
    frequency = Column(String(1), primary_key=True)  # 'A' or 'Q'
    time_period = Column(String(10), primary_key=True)  # 'YYYY' for annual, 'YYYYQn' for quarterly
    row_type = Column(String(20), primary_key=True, default='total')  # 'total', 'compensation', 'taxes', 'surplus'

    # Data
    value = Column(Numeric(20, 6))  # Data value (can be NULL for missing data)

    # Metadata from API response
    table_description = Column(Text)  # Cached for convenience
    industry_description = Column(Text)  # Cached for convenience
    cl_unit = Column(String(100))  # Unit description
    unit_mult = Column(SmallInteger)  # Base-10 exponent

    # Footnote reference
    note_ref = Column(String(100))

    created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)

    __table_args__ = (
        Index('ix_bea_gdpbyindustry_data_table', 'table_id'),
        Index('ix_bea_gdpbyindustry_data_industry', 'industry_code'),
        Index('ix_bea_gdpbyindustry_data_period', 'time_period'),
        Index('ix_bea_gdpbyindustry_data_freq', 'frequency'),
        Index('ix_bea_gdpbyindustry_data_row_type', 'row_type'),
        Index('ix_bea_gdpbyindustry_data_table_industry', 'table_id', 'industry_code'),
        Index('ix_bea_gdpbyindustry_data_table_period', 'table_id', 'time_period'),
    )

    def __repr__(self):
        return f"<GDPByIndustryData(table={self.table_id}, industry='{self.industry_code}', period={self.time_period}, row_type='{self.row_type}')>"
