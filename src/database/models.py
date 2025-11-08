"""
SQLAlchemy Models for FinExus Data Collection System
Defines all database tables with proper relationships and constraints
"""
from datetime import datetime, date
from typing import Optional
from decimal import Decimal

from sqlalchemy import (
    Column, Integer, String, Numeric, Date, DateTime, 
    Boolean, Text, ForeignKey, Index, CheckConstraint,
    UniqueConstraint, BigInteger, Float
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

Base = declarative_base()


class Company(Base):
    """Master company/ticker table with profile information"""
    __tablename__ = 'companies'
    
    company_id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20), unique=True, nullable=False, index=True)
    company_name = Column(String(200), nullable=False)
    
    # Profile data from FMP
    price = Column(Numeric(20, 4))
    market_cap = Column(BigInteger)
    beta = Column(Numeric(20, 4))
    last_dividend = Column(Numeric(20, 4))
    range = Column(String(50))
    change = Column(Numeric(20, 4))
    change_percentage = Column(Numeric(20, 4))
    volume = Column(BigInteger)
    average_volume = Column(BigInteger)
    currency = Column(String(10))
    cik = Column(String(20), index=True)
    isin = Column(String(20))
    cusip = Column(String(20))
    exchange = Column(String(20))
    exchange_full_name = Column(String(100))
    industry = Column(String(100), index=True)
    sector = Column(String(100), index=True)
    country = Column(String(100))
    website = Column(String(200))
    description = Column(Text)
    ceo = Column(String(100))
    full_time_employees = Column(String(20))  # API returns as string
    phone = Column(String(50))
    address = Column(String(200))
    city = Column(String(100))
    state = Column(String(50))
    zip = Column(String(20))
    image = Column(String(200))
    ipo_date = Column(Date)
    default_image = Column(Boolean)
    is_etf = Column(Boolean)
    is_actively_trading = Column(Boolean)
    is_adr = Column(Boolean)
    is_fund = Column(Boolean)
    
    # Metadata
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    
    def __repr__(self):
        return f"<Company(symbol='{self.symbol}', name='{self.company_name}')>"


class IncomeStatement(Base):
    """Income statement data for all companies"""
    __tablename__ = 'income_statements'

    # Composite primary key
    symbol = Column(String(20), ForeignKey('companies.symbol'), primary_key=True)
    date = Column(Date, primary_key=True)
    period = Column(String(10), primary_key=True)  # 'Q1', 'Q2', 'Q3', 'Q4', 'FY'

    # Income statement fields
    reported_currency = Column(String(10))
    cik = Column(String(20))
    filling_date = Column(Date)  # Note: API uses 'fillingDate' (typo in their API)
    filing_date = Column(Date)  # API also returns properly spelled version
    accepted_date = Column(DateTime)
    calendar_year = Column(String(10))
    fiscal_year = Column(Integer)  # API returns this too

    revenue = Column(Numeric(20, 2))
    cost_of_revenue = Column(Numeric(20, 2))
    gross_profit = Column(Numeric(20, 2))
    gross_profit_ratio = Column(Numeric(20, 6))

    research_and_development_expenses = Column(Numeric(20, 2))
    general_and_administrative_expenses = Column(Numeric(20, 2))
    selling_and_marketing_expenses = Column(Numeric(20, 2))
    selling_general_and_administrative_expenses = Column(Numeric(20, 2))
    other_expenses = Column(Numeric(20, 2))
    operating_expenses = Column(Numeric(20, 2))
    cost_and_expenses = Column(Numeric(20, 2))

    interest_income = Column(Numeric(20, 2))
    interest_expense = Column(Numeric(20, 2))
    net_interest_income = Column(Numeric(20, 2))

    depreciation_and_amortization = Column(Numeric(20, 2))
    ebitda = Column(Numeric(20, 2))
    ebitda_ratio = Column(Numeric(20, 6))
    ebit = Column(Numeric(20, 2))

    non_operating_income_excluding_interest = Column(Numeric(20, 2))
    operating_income = Column(Numeric(20, 2))
    operating_income_ratio = Column(Numeric(20, 6))
    total_other_income_expenses_net = Column(Numeric(20, 2))
    income_before_tax = Column(Numeric(20, 2))
    income_before_tax_ratio = Column(Numeric(20, 6))
    income_tax_expense = Column(Numeric(20, 2))

    net_income = Column(Numeric(20, 2))
    net_income_ratio = Column(Numeric(20, 6))
    net_income_from_continuing_operations = Column(Numeric(20, 2))
    net_income_from_discontinued_operations = Column(Numeric(20, 2))
    other_adjustments_to_net_income = Column(Numeric(20, 2))
    net_income_deductions = Column(Numeric(20, 2))
    bottom_line_net_income = Column(Numeric(20, 2))

    eps = Column(Numeric(20, 4))
    eps_diluted = Column(Numeric(20, 4))
    weighted_average_shs_out = Column(BigInteger)
    weighted_average_shs_out_dil = Column(BigInteger)

    # Metadata
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (
        Index('ix_income_statements_symbol_date', 'symbol', 'date'),
        Index('ix_income_statements_fiscal_year', 'fiscal_year'),
    )


class BalanceSheet(Base):
    """Balance sheet data for all companies"""
    __tablename__ = 'balance_sheets'

    symbol = Column(String(20), ForeignKey('companies.symbol'), primary_key=True)
    date = Column(Date, primary_key=True)
    period = Column(String(10), primary_key=True)

    reported_currency = Column(String(10))
    cik = Column(String(20))
    filling_date = Column(Date)
    filing_date = Column(Date)
    accepted_date = Column(DateTime)
    calendar_year = Column(String(10))
    fiscal_year = Column(Integer)

    # Assets
    cash_and_cash_equivalents = Column(Numeric(20, 2))
    short_term_investments = Column(Numeric(20, 2))
    cash_and_short_term_investments = Column(Numeric(20, 2))
    net_receivables = Column(Numeric(20, 2))
    accounts_receivables = Column(Numeric(20, 2))
    other_receivables = Column(Numeric(20, 2))
    inventory = Column(Numeric(20, 2))
    prepaids = Column(Numeric(20, 2))
    other_current_assets = Column(Numeric(20, 2))
    total_current_assets = Column(Numeric(20, 2))

    property_plant_equipment_net = Column(Numeric(20, 2))
    goodwill = Column(Numeric(20, 2))
    intangible_assets = Column(Numeric(20, 2))
    goodwill_and_intangible_assets = Column(Numeric(20, 2))
    long_term_investments = Column(Numeric(20, 2))
    tax_assets = Column(Numeric(20, 2))
    other_non_current_assets = Column(Numeric(20, 2))
    total_non_current_assets = Column(Numeric(20, 2))
    other_assets = Column(Numeric(20, 2))
    total_assets = Column(Numeric(20, 2))

    # Liabilities
    total_payables = Column(Numeric(20, 2))
    account_payables = Column(Numeric(20, 2))
    other_payables = Column(Numeric(20, 2))
    accrued_expenses = Column(Numeric(20, 2))
    short_term_debt = Column(Numeric(20, 2))
    capital_lease_obligations_current = Column(Numeric(20, 2))
    capital_lease_obligations_non_current = Column(Numeric(20, 2))
    tax_payables = Column(Numeric(20, 2))
    deferred_revenue = Column(Numeric(20, 2))
    other_current_liabilities = Column(Numeric(20, 2))
    total_current_liabilities = Column(Numeric(20, 2))

    long_term_debt = Column(Numeric(20, 2))
    deferred_revenue_non_current = Column(Numeric(20, 2))
    deferred_tax_liabilities_non_current = Column(Numeric(20, 2))
    other_non_current_liabilities = Column(Numeric(20, 2))
    total_non_current_liabilities = Column(Numeric(20, 2))
    other_liabilities = Column(Numeric(20, 2))
    capital_lease_obligations = Column(Numeric(20, 2))
    total_liabilities = Column(Numeric(20, 2))

    # Equity
    treasury_stock = Column(Numeric(20, 2))
    preferred_stock = Column(Numeric(20, 2))
    common_stock = Column(Numeric(20, 2))
    retained_earnings = Column(Numeric(20, 2))
    additional_paid_in_capital = Column(Numeric(20, 2))
    accumulated_other_comprehensive_income_loss = Column(Numeric(20, 2))
    other_total_stockholders_equity = Column(Numeric(20, 2))
    total_stockholders_equity = Column(Numeric(20, 2))
    total_equity = Column(Numeric(20, 2))
    minority_interest = Column(Numeric(20, 2))
    total_liabilities_and_total_equity = Column(Numeric(20, 2))

    total_investments = Column(Numeric(20, 2))
    total_debt = Column(Numeric(20, 2))
    net_debt = Column(Numeric(20, 2))

    # Metadata
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (
        Index('ix_balance_sheets_symbol_date', 'symbol', 'date'),
        Index('ix_balance_sheets_fiscal_year', 'fiscal_year'),
    )


class CashFlow(Base):
    """Cash flow statement data for all companies"""
    __tablename__ = 'cash_flows'

    symbol = Column(String(20), ForeignKey('companies.symbol'), primary_key=True)
    date = Column(Date, primary_key=True)
    period = Column(String(10), primary_key=True)

    reported_currency = Column(String(10))
    cik = Column(String(20))
    filing_date = Column(Date)
    accepted_date = Column(DateTime)
    fiscal_year = Column(Integer)

    # Operating activities
    net_income = Column(Numeric(20, 2))
    depreciation_and_amortization = Column(Numeric(20, 2))
    deferred_income_tax = Column(Numeric(20, 2))
    stock_based_compensation = Column(Numeric(20, 2))
    change_in_working_capital = Column(Numeric(20, 2))
    accounts_receivables = Column(Numeric(20, 2))
    inventory = Column(Numeric(20, 2))
    accounts_payables = Column(Numeric(20, 2))
    other_working_capital = Column(Numeric(20, 2))
    other_non_cash_items = Column(Numeric(20, 2))
    net_cash_provided_by_operating_activities = Column(Numeric(20, 2))

    # Investing activities
    investments_in_property_plant_and_equipment = Column(Numeric(20, 2))
    acquisitions_net = Column(Numeric(20, 2))
    purchases_of_investments = Column(Numeric(20, 2))
    sales_maturities_of_investments = Column(Numeric(20, 2))
    other_investing_activities = Column(Numeric(20, 2))
    net_cash_provided_by_investing_activities = Column(Numeric(20, 2))

    # Financing activities
    net_debt_issuance = Column(Numeric(20, 2))
    long_term_net_debt_issuance = Column(Numeric(20, 2))
    short_term_net_debt_issuance = Column(Numeric(20, 2))
    net_stock_issuance = Column(Numeric(20, 2))
    net_common_stock_issuance = Column(Numeric(20, 2))
    common_stock_issuance = Column(Numeric(20, 2))
    common_stock_repurchased = Column(Numeric(20, 2))
    net_preferred_stock_issuance = Column(Numeric(20, 2))
    net_dividends_paid = Column(Numeric(20, 2))
    common_dividends_paid = Column(Numeric(20, 2))
    preferred_dividends_paid = Column(Numeric(20, 2))
    other_financing_activities = Column(Numeric(20, 2))
    net_cash_provided_by_financing_activities = Column(Numeric(20, 2))

    # Summary
    effect_of_forex_changes_on_cash = Column(Numeric(20, 2))
    net_change_in_cash = Column(Numeric(20, 2))
    cash_at_end_of_period = Column(Numeric(20, 2))
    cash_at_beginning_of_period = Column(Numeric(20, 2))

    operating_cash_flow = Column(Numeric(20, 2))
    capital_expenditure = Column(Numeric(20, 2))
    free_cash_flow = Column(Numeric(20, 2))
    income_taxes_paid = Column(Numeric(20, 2))
    interest_paid = Column(Numeric(20, 2))

    # Metadata
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (
        Index('ix_cash_flows_symbol_date', 'symbol', 'date'),
        Index('ix_cash_flows_fiscal_year', 'fiscal_year'),
    )


class FinancialRatio(Base):
    """Financial ratios for all companies"""
    __tablename__ = 'financial_ratios'

    symbol = Column(String(20), ForeignKey('companies.symbol'), primary_key=True)
    date = Column(Date, primary_key=True)
    period = Column(String(10), primary_key=True)

    fiscal_year = Column(Integer)
    reported_currency = Column(String(10))

    # Profitability ratios
    gross_profit_margin = Column(Numeric(20, 6))
    ebit_margin = Column(Numeric(20, 6))
    ebitda_margin = Column(Numeric(20, 6))
    operating_profit_margin = Column(Numeric(20, 6))
    pretax_profit_margin = Column(Numeric(20, 6))
    continuous_operations_profit_margin = Column(Numeric(20, 6))
    net_profit_margin = Column(Numeric(20, 6))
    bottom_line_profit_margin = Column(Numeric(20, 6))

    # Activity ratios
    receivables_turnover = Column(Numeric(20, 4))
    payables_turnover = Column(Numeric(20, 4))
    inventory_turnover = Column(Numeric(20, 4))
    fixed_asset_turnover = Column(Numeric(20, 4))
    asset_turnover = Column(Numeric(20, 4))

    # Liquidity ratios
    current_ratio = Column(Numeric(20, 4))
    quick_ratio = Column(Numeric(20, 4))
    solvency_ratio = Column(Numeric(20, 4))
    cash_ratio = Column(Numeric(20, 4))

    # Valuation ratios
    price_to_earnings_ratio = Column(Numeric(20, 4))
    price_to_earnings_growth_ratio = Column(Numeric(20, 4))
    forward_price_to_earnings_growth_ratio = Column(Numeric(20, 4))
    price_to_book_ratio = Column(Numeric(20, 4))
    price_to_sales_ratio = Column(Numeric(20, 4))
    price_to_free_cash_flow_ratio = Column(Numeric(20, 4))
    price_to_operating_cash_flow_ratio = Column(Numeric(20, 4))

    # Leverage ratios
    debt_to_assets_ratio = Column(Numeric(20, 6))
    debt_to_equity_ratio = Column(Numeric(20, 4))
    debt_to_capital_ratio = Column(Numeric(20, 6))
    long_term_debt_to_capital_ratio = Column(Numeric(20, 6))
    financial_leverage_ratio = Column(Numeric(20, 4))

    # Other ratios
    working_capital_turnover_ratio = Column(Numeric(20, 4))
    operating_cash_flow_ratio = Column(Numeric(20, 4))
    operating_cash_flow_sales_ratio = Column(Numeric(20, 6))
    free_cash_flow_operating_cash_flow_ratio = Column(Numeric(20, 6))

    # Coverage ratios
    debt_service_coverage_ratio = Column(Numeric(20, 4))
    interest_coverage_ratio = Column(Numeric(20, 4))
    short_term_operating_cash_flow_coverage_ratio = Column(Numeric(20, 4))
    operating_cash_flow_coverage_ratio = Column(Numeric(20, 4))
    capital_expenditure_coverage_ratio = Column(Numeric(20, 4))
    dividend_paid_and_capex_coverage_ratio = Column(Numeric(20, 4))

    # Dividend ratios
    dividend_payout_ratio = Column(Numeric(20, 6))
    dividend_yield = Column(Numeric(20, 6))
    dividend_yield_percentage = Column(Numeric(20, 6))

    # Per share metrics
    revenue_per_share = Column(Numeric(20, 4))
    net_income_per_share = Column(Numeric(20, 4))
    dividend_per_share = Column(Numeric(20, 4))
    interest_debt_per_share = Column(Numeric(20, 4))
    cash_per_share = Column(Numeric(20, 4))
    book_value_per_share = Column(Numeric(20, 4))
    tangible_book_value_per_share = Column(Numeric(20, 4))
    shareholders_equity_per_share = Column(Numeric(20, 4))
    operating_cash_flow_per_share = Column(Numeric(20, 4))
    capex_per_share = Column(Numeric(20, 4))
    free_cash_flow_per_share = Column(Numeric(20, 4))

    # Other metrics
    net_income_per_ebt = Column(Numeric(20, 6))
    ebt_per_ebit = Column(Numeric(20, 6))
    price_to_fair_value = Column(Numeric(20, 4))
    debt_to_market_cap = Column(Numeric(20, 6))
    effective_tax_rate = Column(Numeric(20, 6))
    enterprise_value_multiple = Column(Numeric(20, 4))

    # Metadata
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (
        Index('ix_financial_ratios_symbol_date', 'symbol', 'date'),
        Index('ix_financial_ratios_fiscal_year', 'fiscal_year'),
    )


class KeyMetric(Base):
    """Key financial metrics for all companies"""
    __tablename__ = 'key_metrics'

    symbol = Column(String(20), ForeignKey('companies.symbol'), primary_key=True)
    date = Column(Date, primary_key=True)
    period = Column(String(10), primary_key=True)

    fiscal_year = Column(Integer)
    reported_currency = Column(String(10))

    # Valuation metrics
    market_cap = Column(BigInteger)
    enterprise_value = Column(BigInteger)
    ev_to_sales = Column(Numeric(20, 4))
    ev_to_operating_cash_flow = Column(Numeric(20, 4))
    ev_to_free_cash_flow = Column(Numeric(20, 4))
    ev_to_ebitda = Column(Numeric(20, 4))
    net_debt_to_ebitda = Column(Numeric(20, 4))

    # Liquidity metrics
    current_ratio = Column(Numeric(20, 4))

    # Quality metrics
    income_quality = Column(Numeric(20, 6))
    graham_number = Column(Numeric(20, 4))
    graham_net_net = Column(Numeric(20, 4))

    # Burden metrics
    tax_burden = Column(Numeric(20, 6))
    interest_burden = Column(Numeric(20, 6))

    # Working capital metrics
    working_capital = Column(Numeric(20, 2))
    invested_capital = Column(Numeric(20, 2))

    # Return metrics
    return_on_assets = Column(Numeric(20, 6))
    operating_return_on_assets = Column(Numeric(20, 6))
    return_on_tangible_assets = Column(Numeric(20, 6))
    return_on_equity = Column(Numeric(20, 6))
    return_on_invested_capital = Column(Numeric(20, 6))
    return_on_capital_employed = Column(Numeric(20, 6))

    # Yield metrics
    earnings_yield = Column(Numeric(20, 6))
    free_cash_flow_yield = Column(Numeric(20, 6))

    # Capital allocation metrics
    capex_to_operating_cash_flow = Column(Numeric(20, 6))
    capex_to_depreciation = Column(Numeric(20, 4))
    capex_to_revenue = Column(Numeric(20, 6))

    # Expense ratios
    sales_general_and_administrative_to_revenue = Column(Numeric(20, 6))
    research_and_developement_to_revenue = Column(Numeric(20, 6))  # Note: API has typo "Developement"
    stock_based_compensation_to_revenue = Column(Numeric(20, 6))
    intangibles_to_total_assets = Column(Numeric(20, 6))

    # Average metrics
    average_receivables = Column(Numeric(20, 2))
    average_payables = Column(Numeric(20, 2))
    average_inventory = Column(Numeric(20, 2))

    # Working capital days
    days_of_sales_outstanding = Column(Numeric(20, 2))
    days_of_payables_outstanding = Column(Numeric(20, 2))
    days_of_inventory_on_hand = Column(Numeric(20, 2))
    days_of_inventory_outstanding = Column(Numeric(20, 2))
    operating_cycle = Column(Numeric(20, 2))
    cash_conversion_cycle = Column(Numeric(20, 2))

    # Free cash flow metrics
    free_cash_flow_to_equity = Column(Numeric(20, 2))
    free_cash_flow_to_firm = Column(Numeric(20, 2))

    # Asset value metrics
    tangible_asset_value = Column(Numeric(20, 2))
    net_current_asset_value = Column(Numeric(20, 2))

    # Metadata
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (
        Index('ix_key_metrics_symbol_date', 'symbol', 'date'),
    )


class PriceDaily(Base):
    """Daily price data for companies and indices"""
    __tablename__ = 'prices_daily'

    symbol = Column(String(20), ForeignKey('companies.symbol'), primary_key=True)
    date = Column(Date, primary_key=True)

    open = Column(Numeric(20, 4))
    high = Column(Numeric(20, 4))
    low = Column(Numeric(20, 4))
    close = Column(Numeric(20, 4))
    volume = Column(BigInteger)
    change = Column(Numeric(20, 4))
    change_percent = Column(Numeric(20, 6))
    vwap = Column(Numeric(20, 4))

    # Metadata
    created_at = Column(DateTime, default=func.now(), nullable=False)

    __table_args__ = (
        Index('ix_prices_daily_date_symbol', 'date'),
    )


class PriceMonthly(Base):
    """Monthly (month-end) price data"""
    __tablename__ = 'prices_monthly'

    symbol = Column(String(20), ForeignKey('companies.symbol'), primary_key=True)
    date = Column(Date, primary_key=True, index=True)  # Month-end date

    open = Column(Numeric(20, 4))
    high = Column(Numeric(20, 4))
    low = Column(Numeric(20, 4))
    close = Column(Numeric(20, 4))
    volume = Column(BigInteger)
    change = Column(Numeric(20, 4))
    change_percent = Column(Numeric(20, 6))
    vwap = Column(Numeric(20, 4))

    created_at = Column(DateTime, default=func.now(), nullable=False)

    __table_args__ = (
        Index('ix_prices_monthly_date_symbol', 'date', 'symbol'),
    )


class PriceDailyBulk(Base):
    """
    Bulk EOD prices - unvalidated data lake
    No foreign key to companies - stores all global symbols from bulk API
    Use as fallback/safety net when regular prices_daily is missing data
    """
    __tablename__ = 'prices_daily_bulk'

    # Composite primary key - NO foreign key to companies
    symbol = Column(String(20), primary_key=True, nullable=False)
    date = Column(Date, primary_key=True, nullable=False)

    # OHLCV data
    open = Column(Numeric(20, 4))
    high = Column(Numeric(20, 4))
    low = Column(Numeric(20, 4))
    close = Column(Numeric(20, 4))
    adj_close = Column(Numeric(20, 4))  # Adjusted close (from bulk API)
    volume = Column(BigInteger)

    # Metadata
    collected_at = Column(DateTime, default=func.now(), nullable=False)

    __table_args__ = (
        Index('ix_prices_daily_bulk_symbol', 'symbol'),
        Index('ix_prices_daily_bulk_date', 'date'),
        Index('ix_prices_daily_bulk_symbol_date', 'symbol', 'date'),
    )


class PeersBulk(Base):
    """
    Stock peers bulk data - unvalidated peer relationships
    No foreign key to companies - stores all global symbols from bulk API
    Peers list stored as comma-separated text for simplicity
    Override approach: latest peer relationships only (no history)
    """
    __tablename__ = 'peers_bulk'

    # Primary key - NO foreign key to companies
    symbol = Column(String(20), primary_key=True, nullable=False)

    # Comma-separated peer list (e.g., "LTH,LEA,LNW,KMX,URBN")
    peers_list = Column(Text, nullable=True)

    # Metadata
    collected_at = Column(DateTime, default=func.now(), nullable=False)

    __table_args__ = (
        Index('ix_peers_bulk_symbol', 'symbol'),
    )


class EnterpriseValue(Base):
    """Enterprise value calculations"""
    __tablename__ = 'enterprise_values'

    symbol = Column(String(20), ForeignKey('companies.symbol'), primary_key=True)
    date = Column(Date, primary_key=True)
    
    stock_price = Column(Numeric(20, 4))
    number_of_shares = Column(BigInteger)
    market_capitalization = Column(BigInteger)
    minus_cash_and_cash_equivalents = Column(Numeric(20, 2))
    add_total_debt = Column(Numeric(20, 2))
    enterprise_value = Column(BigInteger)
    
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    
    __table_args__ = (
        Index('ix_enterprise_values_symbol_date', 'symbol', 'date'),
    )


class EmployeeHistory(Base):
    """Historical employee counts"""
    __tablename__ = 'employee_history'
    
    symbol = Column(String(20), ForeignKey('companies.symbol'), primary_key=True)
    period_of_report = Column(Date, primary_key=True)
    
    employee_count = Column(Integer)
    filing_date = Column(Date)
    acceptance_time = Column(DateTime)
    source = Column(String(50))
    form_type = Column(String(20))
    cik = Column(String(20))
    company_name = Column(String(200))
    
    created_at = Column(DateTime, default=func.now(), nullable=False)
    
    __table_args__ = (
        Index('ix_employee_history_symbol_period', 'symbol', 'period_of_report'),
    )


class AnalystEstimate(Base):
    """Analyst revenue and earnings estimates"""
    __tablename__ = 'analyst_estimates'

    symbol = Column(String(20), ForeignKey('companies.symbol'), primary_key=True)
    date = Column(Date, primary_key=True)

    # Revenue estimates
    revenue_low = Column(Numeric(20, 2))
    revenue_high = Column(Numeric(20, 2))
    revenue_avg = Column(Numeric(20, 2))
    num_analysts_revenue = Column(Integer)

    # Net Income estimates
    net_income_low = Column(Numeric(20, 2))
    net_income_high = Column(Numeric(20, 2))
    net_income_avg = Column(Numeric(20, 2)) 

    # EBIT estimates
    ebit_low = Column(Numeric(20, 2))
    ebit_high = Column(Numeric(20, 2))
    ebit_avg = Column(Numeric(20, 2))

    # EBITDA estimates
    ebitda_low = Column(Numeric(20, 2))
    ebitda_high = Column(Numeric(20, 2))
    ebitda_avg = Column(Numeric(20, 2))

    # EPS estimates
    eps_avg = Column(Numeric(20, 4))
    eps_high = Column(Numeric(20, 4))
    eps_low = Column(Numeric(20, 4))
    num_analysts_eps = Column(Integer)

    # sga expenses estimates
    sga_expense_low = Column(Numeric(20, 2))
    sga_expense_high = Column(Numeric(20, 2))
    sga_expense_avg = Column(Numeric(20, 2))

    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (
        Index('ix_analyst_estimates_symbol_date', 'date'),
    )


class PriceTarget(Base):
    """Analyst price target consensus"""
    __tablename__ = 'price_targets'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20), ForeignKey('companies.symbol'), nullable=False, index=True)
    published_date = Column(Date, nullable=False, index=True)
    
    target_high = Column(Numeric(20, 4))
    target_low = Column(Numeric(20, 4))
    target_consensus = Column(Numeric(20, 4))
    target_median = Column(Numeric(20, 4))
    
    created_at = Column(DateTime, default=func.now(), nullable=False)
    
    __table_args__ = (
        Index('ix_price_targets_symbol_date', 'symbol', 'published_date'),
        UniqueConstraint('symbol', 'published_date', name='uq_price_targets_symbol_date'),
    )


class InsiderTrading(Base):
    """Individual insider trading transactions"""
    __tablename__ = 'insider_trading'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    symbol = Column(String(20), ForeignKey('companies.symbol'), nullable=False, index=True)
    filing_date = Column(Date, nullable=False, index=True)
    transaction_date = Column(Date, nullable=False, index=True)
    
    reporting_cik = Column(String(20))
    company_cik = Column(String(20))
    transaction_type = Column(String(50))
    securities_owned = Column(Numeric(20, 2))
    reporting_name = Column(String(200))
    type_of_owner = Column(String(100))
    acquisition_or_disposition = Column(String(1))  # 'A' or 'D'
    direct_or_indirect = Column(String(1))  # 'D' or 'I'
    form_type = Column(String(20))
    securities_transacted = Column(Numeric(20, 2))
    price = Column(Numeric(20, 4))
    security_name = Column(String(200))
    url = Column(String(500))
    
    created_at = Column(DateTime, default=func.now(), nullable=False)
    
    __table_args__ = (
        Index('ix_insider_trading_symbol_transaction_date', 'symbol', 'transaction_date'),
        Index('ix_insider_trading_symbol_filing_date', 'symbol', 'filing_date'),
    )


class InstitutionalOwnership(Base):
    """Quarterly institutional ownership summaries"""
    __tablename__ = 'institutional_ownership'
    
    symbol = Column(String(20), ForeignKey('companies.symbol'), primary_key=True)
    date = Column(Date, primary_key=True)
    
    cik = Column(String(20))
    
    investors_holding = Column(Integer)
    last_investors_holding = Column(Integer)
    investors_holding_change = Column(Integer)
    
    number_of_13_fshares = Column(BigInteger)
    last_number_of_13_fshares = Column(BigInteger)
    number_of_13_fshares_change = Column(BigInteger)
    
    total_invested = Column(Numeric(20, 2))
    last_total_invested = Column(Numeric(20, 2))
    total_invested_change = Column(Numeric(20, 2))
    
    ownership_percent = Column(Numeric(20, 6))
    last_ownership_percent = Column(Numeric(20, 6))
    ownership_percent_change = Column(Numeric(20, 6))
    
    new_positions = Column(Integer)
    last_new_positions = Column(Integer)
    new_positions_change = Column(Integer)
    
    increased_positions = Column(Integer)
    last_increased_positions = Column(Integer)
    increased_positions_change = Column(Integer)

    closed_positions = Column(Integer)
    last_closed_positions = Column(Integer)
    closed_positions_change = Column(Integer)

    reduced_positions = Column(Integer)
    last_reduced_positions = Column(Integer)
    reduced_positions_change = Column(Integer)

    total_calls = Column(Integer)
    last_total_calls = Column(Integer)
    total_calls_change = Column(Integer)

    total_puts = Column(Integer)
    last_total_puts = Column(Integer)
    total_puts_change = Column(Integer)

    put_call_ratio = Column(Numeric(20, 6))
    last_put_call_ratio = Column(Numeric(20, 6))
    put_call_ratio_change = Column(Numeric(20, 6))

    collected_year = Column(Integer)
    collected_quarter = Column(Integer)
        
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    
    __table_args__ = (
        Index('ix_institutional_ownership_year_quarter', 'collected_year', 'collected_quarter'),
        Index('ix_institutional_ownership_date', 'date'),
    )


class InsiderStatistics(Base):
    """Aggregated insider trading statistics"""
    __tablename__ = 'insider_statistics'

    symbol = Column(String(20), ForeignKey('companies.symbol'), primary_key=True)
    cik = Column(String(20), index=True)
    year = Column(Integer, primary_key=True)
    quarter = Column(Integer, primary_key=True)  # 1 to 4
    
    acquired_transactions = Column(Integer)
    disposed_transactions = Column(Integer)
    acquired_disposed_ratio = Column(Numeric(20, 6))
    total_acquired = Column(Numeric(20, 2))
    total_disposed = Column(Numeric(20, 2))
    total_sales = Column(Numeric(20, 2))    
    total_purchases = Column(Numeric(20, 2))
    average_acquired = Column(Numeric(20, 2))
    average_disposed = Column(Numeric(20, 2))
      
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (
        Index('ix_insider_statistics_symbol_year_quarter', 'year', 'quarter'),
    )


class DataCollectionLog(Base):
    """Log of data collection runs"""
    __tablename__ = 'data_collection_log'
    
    run_id = Column(Integer, primary_key=True, autoincrement=True)
    job_name = Column(String(100), nullable=False, index=True)
    start_time = Column(DateTime, nullable=False, default=func.now())
    end_time = Column(DateTime)
    status = Column(String(20), nullable=False)  # 'running', 'success', 'failed', 'partial'
    
    companies_requested = Column(Integer)
    companies_processed = Column(Integer)
    companies_failed = Column(Integer)
    
    records_inserted = Column(Integer)
    records_updated = Column(Integer)
    records_failed = Column(Integer)
    
    error_message = Column(Text)
    error_details = Column(Text)  # JSON string with detailed errors
    
    created_at = Column(DateTime, default=func.now(), nullable=False)
    
    __table_args__ = (
        Index('ix_collection_log_job_start', 'job_name', 'start_time'),
        Index('ix_collection_log_status', 'status'),
    )


class TableUpdateTracking(Base):
    """Track last update for each table and symbol"""
    __tablename__ = 'table_update_tracking'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    table_name = Column(String(100), nullable=False, index=True)
    symbol = Column(String(20), index=True)  # NULL for global tables like economic_indicators
    
    last_update_timestamp = Column(DateTime, nullable=False)
    last_api_date = Column(Date)  # Last date received from API
    record_count = Column(Integer)
    
    next_update_due = Column(DateTime)  # When next update should occur
    update_frequency = Column(String(20))  # 'daily', 'weekly', 'monthly', 'quarterly'
    
    last_error = Column(Text)
    consecutive_errors = Column(Integer, default=0)
    
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    
    __table_args__ = (
        UniqueConstraint('table_name', 'symbol', name='uq_table_symbol'),
        Index('ix_update_tracking_table_symbol', 'table_name', 'symbol'),
        Index('ix_update_tracking_next_due', 'next_update_due'),
    )


class EconomicIndicator(Base):
    """Metadata for economic indicators (FRED and FMP)"""
    __tablename__ = 'economic_indicators'

    indicator_code = Column(String(100), primary_key=True)
    indicator_name = Column(String(255))
    source = Column(String(20))  # 'FRED' or 'FMP'
    source_series_id = Column(String(100))  # Original API series ID
    native_frequency = Column(String(20))  # 'DAILY', 'MONTHLY', 'QUARTERLY', 'ANNUAL'
    units = Column(String(100))  # 'Billions', 'Index', 'Percent', etc.
    description = Column(Text)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())


class EconomicDataRaw(Base):
    """Raw/daily economic data time series"""
    __tablename__ = 'economic_data_raw'

    indicator_code = Column(String(100), ForeignKey('economic_indicators.indicator_code'), primary_key=True)
    date = Column(Date, primary_key=True, nullable=False)
    value = Column(Numeric(20, 6))
    created_at = Column(DateTime, default=func.now())

    __table_args__ = (
        Index('ix_economic_data_raw_date', 'date'),
    )


class EconomicDataMonthly(Base):
    """Monthly aggregated economic data (month-end values)"""
    __tablename__ = 'economic_data_monthly'

    indicator_code = Column(String(100), ForeignKey('economic_indicators.indicator_code'), primary_key=True)
    date = Column(Date, primary_key=True, nullable=False)  # Always month-end date
    value = Column(Numeric(20, 6))
    created_at = Column(DateTime, default=func.now())

    __table_args__ = (
        Index('ix_economic_data_monthly_date', 'date'),
    )


class EconomicDataQuarterly(Base):
    """Quarterly aggregated economic data (quarter-end values)"""
    __tablename__ = 'economic_data_quarterly'

    indicator_code = Column(String(100), ForeignKey('economic_indicators.indicator_code'), primary_key=True)
    date = Column(Date, primary_key=True, nullable=False)  # Always quarter-end date
    value = Column(Numeric(20, 6))
    created_at = Column(DateTime, default=func.now())

    __table_args__ = (
        Index('ix_economic_data_quarterly_date', 'date'),
    )


class EconomicCalendar(Base):
    """Economic data releases calendar - upcoming and historical economic events"""
    __tablename__ = 'economic_calendar'

    # Composite primary key: date + country + event (unique event at specific time)
    date = Column(DateTime, primary_key=True, nullable=False)  # Includes time component
    country = Column(String(10), primary_key=True, nullable=False)  # Country code (US, JP, etc.)
    event = Column(String(200), primary_key=True, nullable=False)  # Event name

    # Event details
    currency = Column(String(10))  # Currency code (USD, JPY, etc.)
    previous = Column(Numeric(20, 4))  # Previous value
    estimate = Column(Numeric(20, 4))  # Estimated value (can be NULL)
    actual = Column(Numeric(20, 4))  # Actual value (can be NULL before release)
    change = Column(Numeric(20, 4))  # Change from previous
    change_percentage = Column(Numeric(20, 4))  # Percentage change
    impact = Column(String(20))  # Impact level: "Low", "Medium", "High"
    unit = Column(String(10))  # Unit of measurement (B for Billion, etc.)

    # Metadata
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (
        Index('ix_economic_calendar_date', 'date'),
        Index('ix_economic_calendar_country', 'country'),
        Index('ix_economic_calendar_impact', 'impact'),
    )


class EarningsCalendar(Base):
    """Earnings announcements calendar - upcoming and historical earnings releases"""
    __tablename__ = 'earnings_calendar'

    # Composite primary key: symbol + date (one earnings announcement per company per date)
    # NO foreign key constraint - allows symbols not yet in companies table
    symbol = Column(String(20), primary_key=True, nullable=False, index=True)
    date = Column(Date, primary_key=True, nullable=False)  # Earnings announcement date

    # EPS data
    eps_actual = Column(Numeric(20, 4))  # Actual EPS (NULL before release)
    eps_estimated = Column(Numeric(20, 4))  # Estimated EPS

    # Revenue data
    revenue_actual = Column(Numeric(20, 2))  # Actual revenue (NULL before release)
    revenue_estimated = Column(Numeric(20, 2))  # Estimated revenue

    # Tracking
    last_updated = Column(Date)  # When this record was last updated by FMP

    # Metadata
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (
        Index('ix_earnings_calendar_date', 'date'),
        Index('ix_earnings_calendar_symbol_date', 'symbol', 'date'),
    )


# Helper function to create all tables
def create_all_tables(engine):
    """Create all tables in the database"""
    Base.metadata.create_all(engine)
    print("All tables created successfully!")


def drop_all_tables(engine):
    """Drop all tables in the database (use with caution!)"""
    Base.metadata.drop_all(engine)
    print("All tables dropped!")


if __name__ == "__main__":
    # Example usage
    from sqlalchemy import create_engine
    
    # Replace with your actual database URL
    DATABASE_URL = "postgresql://user:password@localhost:5432/finexus"
    
    engine = create_engine(DATABASE_URL, echo=True)
    create_all_tables(engine)
