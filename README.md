# FinExus Data Collector - Phase 1

PostgreSQL-based financial data collection system that replaces Excel exports with centralized database storage.

## Quick Start

### 1. Setup
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your API keys and database URL
```

### 2. Initialize Database
```bash
# Create PostgreSQL database
createdb finexus_db

# Initialize tables
python scripts/init_database.py
```

### 3. Add Companies
```bash
# Edit scripts/add_companies.py with your tickers
python scripts/add_companies.py
```

### 4. Backfill Data
```bash
python scripts/backfill_data.py --years 10
```

### 5. Run Collectors
```bash
# Test run (once)
python src/jobs/update_all_data.py --run-once

# Production (scheduled)
python src/jobs/update_all_data.py --schedule
```

## Features

- **20 Database Tables**: All financial data types plus bulk data lakes (prices, peers)
- **Incremental Updates**: Only fetch new data
- **Bulk Data Collection**: EOD prices (100K+ symbols), peer relationships (75K+ symbols)
- **Fallback Queries**: Automatic failover from regular to bulk prices
- **Peer Network Analysis**: Company relationships and competitive analysis
- **Vertical Economic Indicators**: Handle different update frequencies
- **Comprehensive Tracking**: Audit trail of all operations
- **Automatic Scheduling**: Daily/weekly/monthly updates

## Project Structure
```
finexus-data-collector/
├── src/
│   ├── config.py              # Configuration management
│   ├── database/
│   │   ├── models.py          # All 18 tables
│   │   └── connection.py      # DB pooling
│   ├── collectors/
│   │   ├── base_collector.py  # Base class
│   │   ├── financial_collector.py
│   │   ├── price_collector.py
│   │   └── economic_collector.py
│   └── jobs/
│       └── update_all_data.py # Main orchestrator
├── scripts/
│   ├── init_database.py
│   └── backfill_data.py
└── requirements.txt
```

## Collector Architecture

### Base Collector (`base_collector.py`)

All collectors inherit from `BaseCollector`, which provides:

**Core Functionality:**
- ✅ API request handling with retry logic and exponential backoff
- ✅ Incremental update logic (tracks last update per symbol)
- ✅ Force refill mode (`force_refill` flag bypasses tracking)
- ✅ Error handling and logging to database
- ✅ Data sanitization (prevents BigInteger/Numeric overflow)
- ✅ Index symbol detection (skips inappropriate data for indices)

**Key Methods:**
- `collect_for_symbol(symbol)` - Main entry point (override in subclasses)
- `should_update_symbol(table, symbol, max_age_days)` - Smart incremental logic
- `update_tracking(table, symbol, ...)` - Records last update time
- `sanitize_record(record, model, symbol)` - Prevents database constraint violations
- `start_collection_run(job_name, symbols)` - Begins tracked job
- `end_collection_run(status)` - Finalizes tracked job

**Properties:**
- `force_refill` - Set to `True` to ignore tracking and fetch full history
- `records_inserted` - Counter for successful inserts
- `records_updated` - Counter for updates
- `errors` - List of error dictionaries

### Individual Collectors

#### 1. **CompanyCollector** (`company_collector.py`)
- **Tables**: `companies`
- **Data**: Company profiles, metadata, exchange info, CEO, industry, sector
- **Update Frequency**: Every 15 days
- **Special Features**:
  - `add_new_company(symbol, name)` - Add new companies to database
  - Upserts (updates existing records)
- **Usage**:
  ```python
  collector = CompanyCollector(session)
  collector.collect_for_symbol('AAPL')
  collector.add_new_company('NVDA', 'NVIDIA Corporation')
  ```

#### 2. **FinancialCollector** (`financial_collector.py`)
- **Tables**: `income_statements`, `balance_sheets`, `cash_flows`, `financial_ratios`, `key_metrics`
- **Data**: Financial statements (annual & quarterly), ratios, key metrics
- **Update Frequency**: Quarterly (90 days) - aligns with earnings cycles
- **Historical Depth**:
  - Annual: 50 years initial, 10 years on update
  - Quarterly: 200 quarters (50 years) initial, 40 quarters on update
- **Special Features**:
  - Dual period tracking (annual vs quarterly)
  - Handles both FY and Q1-Q4 periods
  - Sanitizes extreme financial values
- **Force Refill**: Fetches full 50 years of data

#### 3. **PriceCollector** (`price_collector.py`)
- **Tables**: `prices_daily`, `prices_monthly`
- **Data**: Daily OHLCV data, auto-generated month-end prices
- **Update Frequency**: Daily
- **Historical Depth**: 10+ years (configurable via `default_years_history`)
- **Special Features**:
  - Automatically generates monthly prices from daily data
  - Handles market indices (^GSPC, ^DJI, ^IXIC, ^RUT)
  - `collect_all_indices()` - Fetch S&P 500, Dow, NASDAQ, Russell 2000
- **Usage**:
  ```python
  collector = PriceCollector(session)
  collector.collect_for_symbol('AAPL')
  collector.collect_all_indices()  # Get market indices
  ```

#### 4. **AnalystCollector** (`analyst_collector.py`)
- **Tables**: `analyst_estimates`, `price_targets`
- **Data**: Revenue/EPS estimates, price target consensus
- **Update Frequency**: Every 15 days
- **Historical Depth**: 10 years of estimates
- **Special Features**:
  - Deduplicates on (symbol, date) to prevent conflicts
  - Tracks analyst consensus changes over time
- **Note**: Estimates don't change daily, 15-day frequency balances freshness with API efficiency

#### 5. **InsiderCollector** (`insider_collector.py`)
- **Tables**: `insider_trading`, `institutional_ownership`, `insider_statistics`
- **Data**: Insider transactions, institutional positions, aggregated statistics
- **Update Frequency**:
  - Insider trading: Every 7 days
  - Institutional/statistics: Every 90 days
- **Special Features**:
  - Pagination (fetches all pages from API)
  - Handles invalid dates gracefully
  - Transaction isolation (rollback on error to prevent cascades)
- **Force Refill**: Fetches complete insider trading history

#### 6. **EmployeeCollector** (`employee_collector.py`)
- **Tables**: `employee_history`
- **Data**: Historical employee counts by year
- **Update Frequency**: Every 90 days
- **Special Features**:
  - Deduplicates on (symbol, year)
  - Tracks workforce changes over time

#### 7. **EnterpriseCollector** (`enterprise_collector.py`)
- **Tables**: `enterprise_values`
- **Data**: Enterprise value calculations, market cap components
- **Update Frequency**: Every 90 days
- **Special Features**:
  - Quarterly data tracking
  - Deduplicates on (symbol, date)

#### 8. **EconomicCollector** (`economic_collector.py`)
- **Tables**: `economic_indicators`, `economic_data_raw`, `economic_data_monthly`, `economic_data_quarterly`
- **Data**: ~48 economic indicators from FRED and FMP
- **Update Frequency**: Daily
- **Historical Depth**: Full history (back to 1900s for some indicators)
- **Special Features**:
  - Wraps `FREDCollector` for data fetching
  - Four-table design: metadata + raw + monthly + quarterly
  - Automatic frequency inference
  - Batch inserts (10K records per batch)
- **Indicators Include**:
  - GDP, CPI, unemployment, Fed Funds rate
  - Full Treasury yield curve (1M to 30Y)
  - Housing data, sentiment indices
  - Recession probabilities, retail sales
- **Usage**:
  ```python
  collector = EconomicCollector(session)
  collector.collect_all()  # Collects all ~48 indicators
  ```

#### 9. **BulkPriceCollector** (`bulk_price_collector.py`)
- **Tables**: `prices_daily_bulk`
- **Data**: All global EOD prices from FMP bulk API (100K+ symbols)
- **Special Features**:
  - **No foreign keys** - Stores all global symbols without validation
  - **CSV parsing** - Handles bulk CSV endpoint responses
  - **Batch processing** - 1,000 records per batch for efficiency and error isolation
  - **Acts as data lake** - Unvalidated fallback/safety net for missing prices
  - **Separate from regular prices** - No conflicts with PriceCollector
- **Use Cases**:
  - Daily bulk collection (1 API call vs 100+ individual calls)
  - Gap filling when regular collection fails
  - Symbol discovery (what's new in market)
  - Data validation (cross-check prices)
  - Retroactive portfolio expansion (copy historical data for new companies)
- **Usage**: Via `scripts/collect_bulk_eod.py`

#### 10. **BulkPeersCollector** (`bulk_peers_collector.py`)
- **Tables**: `peers_bulk`
- **Data**: Peer relationships for all global symbols from FMP bulk API (75K+ symbols)
- **Special Features**:
  - **No foreign keys** - Stores all global symbols without validation
  - **CSV parsing** - Handles bulk CSV endpoint responses
  - **Override approach** - Replaces old peer data with latest (no history)
  - **Batch processing** - 1,000 records per batch for efficiency and error isolation
  - **Comma-separated storage** - Simple text format matching API response
- **Use Cases**:
  - Competitive analysis (who are symbol's competitors?)
  - Sector/industry grouping validation
  - Peer network analysis (find related companies)
  - Portfolio diversification (avoid overlapping peers)
  - Market structure research
- **Usage**: Via `scripts/collect_bulk_peers.py`

#### 11. **BulkProfileCollector** (`bulk_profile_collector.py`)
- **Tables**: `companies`
- **Data**: Bulk company profiles from CSV files
- **Special Features**:
  - Filters for US exchanges only
  - Transforms camelCase to snake_case
  - Deduplicates symbols
  - Batch processing (1000 records per batch)
- **Usage**: Via `scripts/load_bulk_profiles.py`

#### 12. **FREDCollector** (`fred_collector.py`) ⚠️ *Not a BaseCollector*
- **Purpose**: Standalone FRED/FMP data fetcher (Excel export)
- **Data**: Economic indicators from FRED and FMP APIs
- **Output**: Excel workbooks with 4 sheets (Raw_Long, Monthly_Panel, Quarterly_Panel, Meta)
- **Note**: Used internally by `EconomicCollector` for database integration

#### 13. ⚠️ **BulkFinancialCollector** (`bulk_financial_collector.py`) - *UNUSED*
- **Purpose**: Load financial statements from bulk CSV files
- **Status**: Not actively used in current workflow
- **Why Unused**: The `FinancialCollector` (API-based) is more practical and automated
- **Note**: May be deprecated in future - prefer API-based collectors

### Collector Design Patterns

**Incremental Updates:**
```python
# Normal mode: Only fetches new data
collector.collect_for_symbol('AAPL')

# Force refill: Fetches ALL historical data
collector.force_refill = True
collector.collect_for_symbol('AAPL')
```

**Index Symbol Handling:**
```python
# Most collectors automatically skip indices
if self.is_index_symbol(symbol):
    logger.info(f"Skipping financial data for index {symbol}")
    return True
```

**Error Isolation:**
```python
try:
    # Collect data
    success = self._collect_data(symbol)
except Exception as e:
    logger.error(f"Error: {e}")
    self.session.rollback()  # Prevent cascade failures
    return False
```

**Data Sanitization:**
```python
# Automatically caps values to prevent overflow
record = self.sanitize_record(record, Model, symbol)
# BigInteger max: 9.2 quintillion
# Numeric(20,x) max: 10^20
```

## Database Schema

### Core Tables (6)
- companies, income_statements, balance_sheets, cash_flows, financial_ratios, key_metrics

### Market Data (6)
- prices_daily, prices_daily_bulk, prices_monthly, enterprise_values, employee_history, peers_bulk

### Analyst & Ownership (5)
- analyst_estimates, price_targets, insider_trading, institutional_ownership, insider_statistics

### Economic (4)
- economic_indicators, economic_data_raw, economic_data_monthly, economic_data_quarterly

### Metadata (2)
- data_collection_log, table_update_tracking

## Usage Examples

### Query Data
```python
from src.database.connection import get_session
from src.database.models import IncomeStatement

with get_session() as session:
    stmt = session.query(IncomeStatement)\
        .filter(IncomeStatement.symbol == 'AAPL')\
        .order_by(IncomeStatement.date.desc())\
        .first()
    
    print(f"Revenue: ${stmt.revenue:,.0f}")
```

### Manual Update
```python
from src.collectors.financial_collector import FinancialCollector
from src.database.connection import get_session

with get_session() as session:
    collector = FinancialCollector(session)
    collector.collect_for_symbol('AAPL')
```

## Configuration

Edit `.env`:
```env
DATABASE_URL=postgresql://user:password@localhost:5432/finexus_db
FMP_API_KEY=your_key
FRED_API_KEY=your_key
```

## Database Migrations (Alembic)

### Creating New Migrations
```bash
# Auto-generate migration from model changes
alembic revision --autogenerate -m "description of changes"

# Apply migrations to database
alembic upgrade head

# Rollback last migration
alembic downgrade -1

# View migration history
alembic history
```

### Common Migration Scenarios
```bash
# Initial database setup (from scratch)
alembic upgrade head

# Update schema after pulling new code
alembic upgrade head

# Check current database version
alembic current
```

## Bulk Data Loading

### 1. Load Company Profiles from CSV

Bulk load thousands of company profiles at once:

```bash
# Place CSV files in data/bulk_csv/ directory
# CSV should have columns: symbol, companyName, exchange, industry, sector, etc.

python scripts/load_bulk_profiles.py
```

**What it does:**
- Reads all CSV files from `data/bulk_csv/` directory
- Filters for US exchanges only (NYSE, NASDAQ, AMEX, etc.)
- Transforms camelCase column names to snake_case
- Upserts companies (updates existing, inserts new)
- Reports: files processed, records inserted/updated

**CSV Format Requirements:**
- Must have `symbol` and `exchange` columns
- Supports FMP bulk export format (camelCase columns)
- Automatically converts data types

### 1b. Load ALL Global Company Profiles

To load companies from ALL exchanges globally (not just US):

```bash
python scripts/load_all_profiles.py
```

**What it does:**
- Same as `load_bulk_profiles.py` but **no exchange filtering**
- Loads companies from all global exchanges (LSE, TSX, etc.)
- Useful for global portfolio or international analysis
- Typically loads 50K-70K companies vs 35K-40K US-only

**When to use:**
- US companies only: Use `load_bulk_profiles.py` (default)
- Global companies: Use `load_all_profiles.py`

### 2. Alternative: Load from FMP List
```bash
# Load specific exchanges
python scripts/load_us_profiles.py
```

## Backfilling Historical Data

### Basic Usage

The `backfill_priority_data.py` script is the main tool for loading historical data:

```bash
# Backfill first 10 companies from priority list
python scripts/backfill_priority_data.py data/priority_lists/priority1_active_in_db.txt --limit 10

# Backfill all companies in the list
python scripts/backfill_priority_data.py data/priority_lists/priority1_active_in_db.txt
```

### Command-Line Arguments

**Required:**
- `priority_file` - Path to .txt or .csv file with stock symbols

**Optional:**
- `--limit N` - Process only first N companies (useful for testing)
- `--collectors LIST` - Select which data types to collect (default: all)
- `--resume` - Continue from where previous run stopped
- `--progress-file PATH` - Custom location for progress tracking (default: `data/progress/backfill_progress.txt`)
- `--force` - **Force full historical refill** (ignores tracking, fetches all data)

### Understanding `--collectors`

Choose which data types to collect:

```bash
Collect from Russell 3000 list
python scripts/backfill_priority_data.py data/priority_lists/Russell_3000.csv --limit 120
# Only company profiles and prices (fast)
python scripts/backfill_priority_data.py priority.txt --collectors company,price

# Financial statements and ratios only
python scripts/backfill_priority_data.py priority.txt --collectors financial

# Everything except insider data
python scripts/backfill_priority_data.py priority.txt --collectors company,financial,price,analyst,employee,enterprise
```

Available collectors:
- `company` - Company profile updates
- `financial` - Income statements, balance sheets, cash flows, ratios, metrics
- `price` - Daily and monthly price data
- `analyst` - Analyst estimates and price targets
- `insider` - Insider trading, institutional ownership, statistics
- `employee` - Employee count history
- `enterprise` - Enterprise value calculations

### Understanding `--force` Flag

**Normal mode (incremental):**
- Checks `table_update_tracking` for last update
- Only fetches new data since last run
- Fast, efficient for daily updates

**Force mode (`--force`):**
- Ignores tracking table completely
- Fetches **full historical data** (e.g., 50 years annual, 200 quarters)
- Use when:
  - Initial data load for new companies
  - Recovering from data corruption
  - Backfilling historical data after API fixes
  - Testing with fresh data

```bash
# Force refill: Get ALL historical data
python scripts/backfill_priority_data.py priority.txt --limit 5 --force

# Normal incremental update
python scripts/backfill_priority_data.py priority.txt --limit 5
```

### Resume Capability

If backfilling fails or is interrupted:

```bash
# First run (processes 100 companies, fails at #47)
python scripts/backfill_priority_data.py priority.txt --limit 100

# Resume from where it stopped
python scripts/backfill_priority_data.py priority.txt --limit 100 --resume
```

Progress is automatically saved every 10 companies to `data/progress/backfill_progress.txt`

### Typical Workflow Examples

```bash
# 1. Test with small sample (5 companies, all data types)
python scripts/backfill_priority_data.py priority.txt --limit 5

# 2. Initial historical load (100 companies, force full history)
python scripts/backfill_priority_data.py priority.txt --limit 100 --force

# 3. Daily price updates (incremental, fast)
python scripts/backfill_priority_data.py priority.txt --collectors price

# 4. Large batch with resume protection
python scripts/backfill_priority_data.py priority.txt --limit 1000 --resume

# 5. Force refresh specific data type
python scripts/backfill_priority_data.py priority.txt --collectors financial --force
```

## Economic Data Collection

### Initial Collection
```bash
# Collect all economic indicators (FRED + FMP)
python scripts/test_economic_collector.py
```

Collects ~48 indicators including:
- GDP, CPI, unemployment, interest rates
- Full Treasury yield curve
- Housing data, sentiment indices
- Recession probabilities

**Data volume:** ~200K raw data points, ~30K monthly, ~10K quarterly

### Scheduled Updates

```bash
# Manual update (checks tracking, updates if needed)
python scripts/update_economic_data.py

# Schedule daily at 8:00 AM (Windows Task Scheduler)
# Use: run_economic_update.bat

# OR use Python scheduler
pip install schedule
python scripts/scheduler.py
```

Economic data updates daily with new values from FRED and FMP APIs.

## Economic Calendar Collection

The Economic Calendar tracks upcoming and historical economic data releases with estimates and actual values. Essential for anticipating market-moving events.

### Quick Start

**Collect upcoming 90 days (default):**
```bash
python scripts/collect_economic_calendar.py
```

**Backfill last year:**
```bash
python scripts/collect_economic_calendar.py --backfill-days 365
```

### Features

- **Upcoming events**: Track estimates for future economic releases
- **Historical data**: Full history of past releases with actual values
- **Auto-updates**: Updates existing events when actuals are released
- **Global coverage**: Events from all countries (US, JP, EU, etc.)
- **Impact ratings**: "Low", "Medium", "High" impact classification

### Collection Modes

**1. Upcoming Events (Default)**
```bash
# Next 90 days
python scripts/collect_economic_calendar.py

# Next 30 days
python scripts/collect_economic_calendar.py --upcoming 30
```

**2. Specific Date Range**
```bash
# Collect Q1 2024
python scripts/collect_economic_calendar.py --from 2024-01-01 --to 2024-03-31
```

**3. Historical Backfill**
```bash
# Last 365 days
python scripts/collect_economic_calendar.py --backfill-days 365

# From specific date to today
python scripts/collect_economic_calendar.py --backfill-from 2020-01-01
```

### Data Structure

Each event includes:
- **date**: Event timestamp (includes time component)
- **country**: Country code (US, JP, GB, etc.)
- **event**: Event name (e.g., "Non-Farm Payrolls", "CPI YoY")
- **currency**: Currency code (USD, JPY, EUR, etc.)
- **previous**: Previous value
- **estimate**: Analyst estimate (NULL if not available)
- **actual**: Actual released value (NULL before release)
- **change**: Change from previous value
- **change_percentage**: Percentage change
- **impact**: Impact level ("Low", "Medium", "High")
- **unit**: Unit of measurement (B = Billion, etc.)

### Scheduled Daily Collection

For daily updates, schedule this command:

```bash
# Collect next 90 days every day
# Updates estimates and fills in actual values as they're released
python scripts/collect_economic_calendar.py
```

**Recommended schedule:** Daily at 6:00 AM (after most global releases)

### Query Examples

```sql
-- Upcoming high-impact US events
SELECT date, event, estimate, impact
FROM economic_calendar
WHERE country = 'US'
  AND impact = 'High'
  AND date > NOW()
ORDER BY date;

-- Events with biggest surprises (actual vs estimate)
SELECT date, event, country, estimate, actual,
       ABS(change_percentage) as surprise
FROM economic_calendar
WHERE estimate IS NOT NULL
  AND actual IS NOT NULL
ORDER BY surprise DESC
LIMIT 10;

-- All events for a specific date
SELECT * FROM economic_calendar
WHERE date::date = '2024-01-15'
ORDER BY impact DESC, date;
```

### Notes

- **API Limit**: 90-day maximum per request (script handles chunking automatically)
- **Updates**: Run daily to get latest estimates and actual values
- **History**: All historical events are preserved for backtesting
- **UPSERT**: Automatically updates events when actual values are released

## Earnings Calendar Collection

The Earnings Calendar tracks upcoming and historical earnings announcements with estimated and actual EPS/revenue. Essential for earnings season planning and trade timing.

### Quick Start

**Collect upcoming 90 days (default):**
```bash
python scripts/collect_earnings_calendar.py
```

**Backfill last 2 years:**
```bash
python scripts/collect_earnings_calendar.py --backfill-days 730
```

### Features

- **Company-specific**: Earnings dates for individual stocks
- **Estimates vs Actuals**: Track analyst estimates and actual results
- **Historical data**: Full history for backtesting earnings strategies
- **Auto-updates**: Updates when actual results are released
- **Revenue tracking**: Both EPS and revenue estimates/actuals

### Collection Modes

**1. Upcoming Earnings (Default)**
```bash
# Next 90 days
python scripts/collect_earnings_calendar.py

# Next 30 days
python scripts/collect_earnings_calendar.py --upcoming 30
```

**2. Specific Date Range**
```bash
# Collect Q4 2024 earnings season
python scripts/collect_earnings_calendar.py --from 2024-10-01 --to 2024-12-31
```

**3. Historical Backfill**
```bash
# Last 2 years
python scripts/collect_earnings_calendar.py --backfill-days 730

# From specific date to today
python scripts/collect_earnings_calendar.py --backfill-from 2020-01-01
```

### Data Structure

Each announcement includes:
- **symbol**: Company ticker
- **date**: Earnings announcement date
- **eps_estimated**: Analyst EPS estimate (NULL if not available)
- **eps_actual**: Actual EPS (NULL before release)
- **revenue_estimated**: Analyst revenue estimate (NULL if not available)
- **revenue_actual**: Actual revenue (NULL before release)
- **last_updated**: When FMP last updated this record

### Scheduled Daily Collection

For daily updates, schedule this command:

```bash
# Collect next 90 days every day
# Updates estimates and fills in actuals as they're released
python scripts/collect_earnings_calendar.py
```

**Recommended schedule:** Daily at 7:00 AM (after most after-hours earnings)

### Query Examples

```sql
-- Upcoming earnings for your portfolio
SELECT e.symbol, c.company_name, e.date, e.eps_estimated, e.revenue_estimated
FROM earnings_calendar e
JOIN companies c ON e.symbol = c.symbol
WHERE e.date >= CURRENT_DATE
  AND e.symbol IN ('AAPL', 'MSFT', 'GOOGL', 'AMZN')
ORDER BY e.date;

-- Biggest earnings surprises this quarter
SELECT symbol, date, eps_estimated, eps_actual,
       ((eps_actual - eps_estimated) / ABS(eps_estimated) * 100) as surprise_pct
FROM earnings_calendar
WHERE date >= '2024-10-01'
  AND eps_estimated IS NOT NULL
  AND eps_actual IS NOT NULL
ORDER BY ABS((eps_actual - eps_estimated) / eps_estimated) DESC
LIMIT 10;

-- Earnings calendar for next week
SELECT symbol, date, eps_estimated, revenue_estimated
FROM earnings_calendar
WHERE date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '7 days'
ORDER BY date, symbol;

-- Companies that beat estimates
SELECT symbol, date, eps_estimated, eps_actual
FROM earnings_calendar
WHERE eps_actual > eps_estimated
  AND date >= '2024-01-01'
ORDER BY date DESC;
```

### Notes

- **API Limit**: 90-day maximum per request, 4000 records max (script handles chunking)
- **Updates**: Run daily to get latest estimates and actual results
- **History**: All historical announcements are preserved for backtesting
- **UPSERT**: Automatically updates when actual values are released
- **Foreign Key**: Linked to `companies` table via symbol

## Bulk EOD Price Collection

The bulk price system collects **all global EOD prices** (100K+ symbols) in a single API call and stores them in a separate data lake table (`prices_daily_bulk`) without validation or foreign key constraints.

### Why Use Bulk Prices?

**Benefits:**
- **Efficient**: 1 API call vs 100+ individual calls for portfolio companies
- **Complete**: Captures entire market, not just your portfolio
- **Resilient**: Acts as fallback when regular price collection fails
- **Discoverable**: See what symbols exist in the market
- **Flexible**: Retroactively add historical data when expanding portfolio

**Architecture:**
- **Separate table** (`prices_daily_bulk`) - No conflicts with regular `prices_daily`
- **No foreign keys** - Stores all symbols without company profile validation
- **No overhead** - Regular price collection continues independently
- **Fallback queries** - Helper functions automatically check bulk table when regular data missing

### Collecting Bulk Prices

```bash
# Collect yesterday's prices (default)
python scripts/collect_bulk_eod.py

# Collect specific date
python scripts/collect_bulk_eod.py --date 2024-11-01

# Collect date range (backfill)
python scripts/collect_bulk_eod.py --start-date 2024-11-01 --end-date 2024-11-05

# Collect last N days
python scripts/collect_bulk_eod.py --last-days 7
```

**Performance:**
- Processes 70K+ symbols in ~30 seconds
- Batch inserts (1,000 per batch)
- Progress logging for large batches

### Using Bulk Prices in Queries

The `price_helpers.py` module provides fallback query functions:

```python
from src.utils.price_helpers import get_price, get_close_price, get_price_range
from src.database.connection import get_session
from datetime import date

with get_session() as session:
    # Get price with automatic fallback to bulk
    price = get_price(session, 'AAPL', date(2024, 11, 1), fallback_to_bulk=True)
    if price:
        print(f"Close: ${price['close']}")
        print(f"Source: {price['source']}")  # 'regular' or 'bulk'

    # Get just the close price
    close = get_close_price(session, 'AAPL', date(2024, 11, 1))

    # Get price range (combines both tables)
    prices = get_price_range(session, 'AAPL',
                            date(2024, 11, 1),
                            date(2024, 11, 5))
    for p in prices:
        print(f"{p['date']}: ${p['close']} (from {p['source']})")
```

### Helper Functions Available

**Query Functions:**
- `get_price(session, symbol, date, fallback_to_bulk)` - Single date with fallback
- `get_close_price(session, symbol, date)` - Quick close price lookup
- `get_price_range(session, symbol, start_date, end_date)` - Date range with fallback

**Analysis Functions:**
- `check_price_availability(session, symbol, date)` - Check which tables have data
- `find_missing_dates(session, symbol, start_date, end_date)` - Gap detection
- `compare_prices(session, symbol, date)` - Validate regular vs bulk data

**Data Management:**
- `copy_from_bulk_to_regular(session, symbol, start_date, end_date)` - Populate regular table from bulk (useful when adding new companies to portfolio)

### Typical Workflows

**Daily Collection (Recommended):**
```bash
# Run after market close (e.g., 6:00 PM EST)
python scripts/collect_bulk_eod.py

# Schedule with Windows Task Scheduler or cron
# This gives you a complete market snapshot every day
```

**Backfilling Historical Data:**
```bash
# Fill last 30 days for validation/gap filling
python scripts/collect_bulk_eod.py --last-days 30

# Fill specific date range for new portfolio additions
python scripts/collect_bulk_eod.py --start-date 2024-01-01 --end-date 2024-11-01
```

**Adding New Company to Portfolio:**
```python
# After adding a new company profile, copy historical prices from bulk
from src.utils.price_helpers import copy_from_bulk_to_regular
from src.database.connection import get_session
from datetime import date

with get_session() as session:
    # Copy 1 year of historical data
    records_copied = copy_from_bulk_to_regular(
        session,
        'TSLA',
        date(2023, 11, 1),
        date(2024, 11, 1)
    )
    print(f"Copied {records_copied} records from bulk to regular table")
```

**Gap Detection and Filling:**
```python
from src.utils.price_helpers import find_missing_dates, get_price
from src.database.connection import get_session
from datetime import date

with get_session() as session:
    # Find gaps in regular price data
    missing = find_missing_dates(session, 'AAPL',
                                date(2024, 1, 1),
                                date(2024, 11, 1))

    # Check if bulk has these dates
    for missing_date in missing:
        bulk_price = get_price(session, 'AAPL', missing_date,
                              fallback_to_bulk=True)
        if bulk_price and bulk_price['source'] == 'bulk':
            print(f"Bulk has data for {missing_date}: ${bulk_price['close']}")
```

### When to Use Bulk vs Regular

**Use Bulk Collection:**
- Daily market-wide snapshots
- Initial historical backfill (faster than individual calls)
- Discovering new symbols in the market
- Validation and gap detection

**Use Regular Price Collection:**
- Portfolio-specific updates with full OHLCV validation
- When you need only specific symbols
- Integration with company profiles (foreign key relationships)

**Best Practice:**
- Run bulk collection daily for complete market coverage
- Run regular price collection for portfolio companies with validation
- Use fallback queries in downstream applications for resilience

## Bulk Stock Peers Collection

The bulk peers system collects **peer relationships for all global symbols** (75K+ symbols) in a single API call and stores them in a simple table (`peers_bulk`) without validation or foreign key constraints.

### Why Collect Peer Data?

**Benefits:**
- **Competitive Analysis**: Quickly identify competitors for any symbol
- **Sector Validation**: Cross-check company classifications
- **Portfolio Diversification**: Avoid overlapping peer exposure
- **Network Analysis**: Find related companies through peer-of-peer relationships
- **Market Structure**: Understand industry groupings

**Architecture:**
- **Simple text storage** - Comma-separated peer lists (matches API format)
- **No foreign keys** - Stores all symbols without company profile validation
- **Override approach** - Latest peer data only (peers change infrequently)
- **Efficient** - 1 API call for entire market vs individual lookups

### Collecting Bulk Peers

```bash
# Collect all peer relationships (no arguments needed)
python scripts/collect_bulk_peers.py
```

**What it does:**
- Fetches CSV from FMP bulk peers API
- Parses ~75,000 symbol relationships
- Upserts into `peers_bulk` table (replaces old data)
- Completes in ~10-15 seconds

**Update frequency:** Monthly or quarterly (peers don't change often)

### Using Peers Data in Analysis

The `peers_helpers.py` module provides powerful query functions:

```python
from src.utils.peers_helpers import (
    get_peers, find_common_peers, get_peer_network,
    are_peers, search_by_peer
)
from src.database.connection import get_session

with get_session() as session:
    # Get AAPL's peers
    peers = get_peers(session, 'AAPL')
    print(f"AAPL has {len(peers)} peers: {peers}")

    # Find common competitors between AAPL and MSFT
    common = find_common_peers(session, 'AAPL', 'MSFT')
    print(f"Common peers: {common}")

    # Check if they're mutual peers
    if are_peers(session, 'AAPL', 'MSFT'):
        print("AAPL and MSFT list each other as peers")

    # Get 2-level peer network (peers of peers)
    network = get_peer_network(session, 'AAPL', depth=2)
    print(f"Level 1: {len(network[1])} direct peers")
    print(f"Level 2: {len(network[2])} peers of peers")

    # Reverse lookup: who lists AAPL as a peer?
    listings = search_by_peer(session, 'AAPL')
    print(f"{len(listings)} symbols list AAPL as peer")
```

### Helper Functions Available

**Basic Queries:**
- `get_peers(session, symbol)` - Get list of peer symbols
- `get_peers_raw(session, symbol)` - Get raw comma-separated string
- `get_peer_counts(session, symbols)` - Count peers for multiple symbols

**Comparative Analysis:**
- `find_common_peers(session, symbol1, symbol2)` - Find shared peers
- `are_peers(session, symbol1, symbol2)` - Check if mutual peers

**Network Analysis:**
- `get_peer_network(session, symbol, depth)` - Multi-level peer traversal
- `search_by_peer(session, peer_symbol)` - Reverse lookup (who lists this symbol?)
- `find_most_connected(session, limit)` - Find symbols with most peers

### Typical Use Cases

**Competitive Analysis:**
```python
from src.utils.peers_helpers import get_peers
from src.database.connection import get_session

with get_session() as session:
    # Analyze TSLA's competitive landscape
    peers = get_peers(session, 'TSLA')

    print(f"Tesla's competitors: {peers}")
    # Might show: ['F', 'GM', 'TM', 'HMC', 'RIVN', 'LCID', ...]

    # Now fetch financial metrics for these peers for comparison
```

**Portfolio Diversification Check:**
```python
from src.utils.peers_helpers import find_common_peers

with get_session() as session:
    # Check overlap between two holdings
    holdings = ['AAPL', 'MSFT', 'GOOGL', 'AMZN']

    for i, stock1 in enumerate(holdings):
        for stock2 in holdings[i+1:]:
            common = find_common_peers(session, stock1, stock2)
            if len(common) > 5:  # Significant overlap
                print(f"Warning: {stock1} and {stock2} share {len(common)} peers")
                print(f"  Common: {common[:5]}")
```

**Sector Discovery:**
```python
from src.utils.peers_helpers import get_peer_network

with get_session() as session:
    # Discover related companies in fintech
    network = get_peer_network(session, 'SQ', depth=2)

    # All level 1 and 2 peers are likely in similar space
    all_related = set()
    for level, peers_set in network.items():
        all_related.update(peers_set)

    print(f"Found {len(all_related)} companies in SQ's peer network")
```

**Market Structure Analysis:**
```python
from src.utils.peers_helpers import find_most_connected, search_by_peer

with get_session() as session:
    # Find hub companies (most connected)
    hubs = find_most_connected(session, limit=10)

    for item in hubs:
        print(f"{item['symbol']}: {item['peer_count']} peers")

    # Check who considers AAPL a competitor
    competitors_of_aapl = search_by_peer(session, 'AAPL')
    print(f"{len(competitors_of_aapl)} companies list AAPL as peer")
```

### Testing Your Peers Data

Run the comprehensive test suite:

```bash
python scripts/test_peers_collector.py
```

This tests:
- Basic peer queries for major symbols
- Common peer finding
- Multi-level network traversal
- Peer counts and statistics
- Most connected symbols
- Reverse lookup functionality
- Database statistics

### Update Schedule

**Recommended:**
- **Monthly** - Run on 1st of each month
- **Quarterly** - For less critical applications
- **On-demand** - When adding new companies to your analysis

```bash
# Schedule with Windows Task Scheduler or cron
# Example: First day of each month
python scripts/collect_bulk_peers.py
```

**Why infrequent updates?**
- Peer relationships change slowly (companies don't switch industries daily)
- FMP likely updates their peer data monthly/quarterly
- Override approach means each collection replaces all data
- Saves API quota for more dynamic data (prices, financials)

### Architecture Decisions

**Why override instead of incremental?**
- Peers change infrequently - tracking history adds complexity with little value
- Most queries need current state ("who are AAPL's peers today?")
- Simpler schema and faster queries
- Can always add history tracking later if needed

**Why text storage instead of array?**
- Matches API format exactly (comma-separated)
- Simple to update and query
- Easy to split in application when needed: `peers.split(',')`
- Less storage overhead
- Still searchable with SQL `CONTAINS` operator

**Why no foreign keys?**
- Same benefits as bulk prices - no validation overhead
- Can store peer relationships for symbols not in your portfolio
- Resilient to missing company profiles
- Faster inserts during bulk collection

## Price Data Optimization Scripts

The following scripts optimize price data collection and maintenance by leveraging bulk price data and eliminating redundant API calls.

### 1. Backfill Daily Prices from Bulk (`backfill_prices_from_bulk.py`)

Copies price data from `prices_daily_bulk` to `prices_daily` without making any API calls. This is essential for keeping individual symbol prices current when you've already collected bulk market data.

**Use Cases:**
- After running bulk EOD collection, sync to individual symbol tables
- Fill gaps in `prices_daily` from the bulk data lake
- Save API quota by reusing already-collected bulk data
- Backfill new symbols added to your portfolio

**How It Works:**
1. Finds symbols where `prices_daily_bulk` has newer dates than `prices_daily`
2. Copies missing dates from bulk to daily table
3. Calculates `change` and `change_percent` fields (bulk doesn't have these)
4. Sets `vwap` to NULL (bulk doesn't have VWAP data)
5. UPSERTs to prevent duplicates

**Usage:**
```bash
# Preview what would be backfilled
python scripts/backfill_prices_from_bulk.py --dry-run

# Backfill all symbols where bulk is ahead
python scripts/backfill_prices_from_bulk.py

# Test with first 10 symbols
python scripts/backfill_prices_from_bulk.py --limit 10
```

**Typical Workflow:**
```bash
# Step 1: Collect bulk prices (1 API call for all 100K+ symbols)
python scripts/collect_bulk_eod.py

# Step 2: Sync to prices_daily (0 API calls!)
python scripts/backfill_prices_from_bulk.py

# Result: All portfolio symbols updated without burning API quota
```

### 2. Fill Bulk EOD Gaps (`backfill_bulk_eod_gaps.py`)

Detects and fills missing dates in the `prices_daily_bulk` table using a two-phase approach: first update to current, then fix historical gaps.

**Two-Phase Strategy:**
- **Phase 1**: Fill from latest bulk date up to today (get current first)
- **Phase 2**: Scan historical date range for missing weekdays and backfill

**Use Cases:**
- Daily bulk EOD collection failed for specific dates
- Network issues caused gaps in bulk price data
- Ensure bulk table is complete before syncing to daily table
- Systematic gap detection and repair

**Usage:**
```bash
# Preview gaps (dry run)
python scripts/backfill_bulk_eod_gaps.py --dry-run

# Fill recent dates + up to 10 historical gaps (default)
python scripts/backfill_bulk_eod_gaps.py

# Fill recent + first 5 historical gaps, search last 180 days
python scripts/backfill_bulk_eod_gaps.py --max-days 180 --max-fills 5

# Fill recent + all gaps found in last year
python scripts/backfill_bulk_eod_gaps.py --max-days 365 --max-fills 100
```

**Features:**
- **Smart detection**: Only checks weekdays (Mon-Fri) to avoid API waste on sparse weekend data
- **Two-phase approach**: Always gets current first, then fixes historical issues
- **Controlled backfill**: Limit number of dates filled per run to manage API usage
- **Comprehensive logging**: Shows exactly what dates were found and filled

**Example Output:**
```
PHASE 1: Checking for recent missing dates...
📅 Latest bulk date: 2025-11-05
📅 Today: 2025-11-07
📅 Missing recent dates: 2 weekdays
    - 2025-11-06
    - 2025-11-07

[1/2] Filling 2025-11-06...
  ✓ 2025-11-06: 67,234 symbols inserted

PHASE 2: Checking for historical gaps...
Found 3 missing weekdays
    1. 2025-10-15
    2. 2025-10-22
    3. 2025-10-29
```

### 3. Regenerate Monthly Prices (`regenerate_monthly_prices.py`)

Regenerates `prices_monthly` from `prices_daily` data. Essential for maintaining monthly price consistency when daily prices are backfilled via bulk operations or other methods that bypass the normal collector.

**Use Cases:**
- Fix monthly gaps after bulk daily backfills
- One-time cleanup when monthly data is inconsistent
- Periodic maintenance to ensure data integrity
- After manually importing daily price data

**How It Works:**
1. Reads all daily prices for each symbol
2. Resamples to month-end (last trading day of each month)
3. UPSERTs into `prices_monthly` table
4. Uses existing `PriceCollector._generate_monthly_prices()` logic

**Usage:**
```bash
# Test with first 10 symbols
python scripts/regenerate_monthly_prices.py --limit 10

# Regenerate all monthly prices (full cleanup)
python scripts/regenerate_monthly_prices.py

# Regenerate specific symbols
python scripts/regenerate_monthly_prices.py --symbols AAPL,MSFT,GOOGL

# Regenerate from priority list
python scripts/regenerate_monthly_prices.py --symbols-file data/priority_lists/priority1_active_in_db.txt
```

**When to Use:**
- After running `backfill_prices_from_bulk.py` (bulk doesn't regenerate monthly)
- After importing price data from external sources
- Periodic maintenance (monthly or quarterly)
- When you notice monthly price gaps

**Best Practice Workflow:**
```bash
# 1. Fill bulk table gaps
python scripts/backfill_bulk_eod_gaps.py

# 2. Sync bulk → daily
python scripts/backfill_prices_from_bulk.py

# 3. Regenerate monthly prices
python scripts/regenerate_monthly_prices.py

# Result: Complete data integrity across all three price tables
```

### Optimized Daily Price Update Workflow

**Recommended daily schedule:**

```bash
# 6:00 PM EST - After market close
# Collect bulk EOD (1 API call for 100K+ symbols)
python scripts/collect_bulk_eod.py

# Sync to individual symbol table (0 API calls)
python scripts/backfill_prices_from_bulk.py

# Regenerate monthly prices (0 API calls)
python scripts/regenerate_monthly_prices.py
```

**Benefits:**
- **3 scripts, 1 API call** - Maximum efficiency
- **Complete coverage** - All symbols, all price tables updated
- **Data integrity** - Monthly prices stay consistent
- **API savings** - Would normally require 1 API call per symbol

**Comparison:**

| Method | API Calls | Time | Coverage |
|--------|-----------|------|----------|
| Individual symbol updates (2000 symbols) | 2,000 | ~60 min | Portfolio only |
| Bulk + sync (this approach) | 1 | ~2 min | Entire market (100K+ symbols) |

 ## Nasdaq Stock Screener Collector

  Collects daily snapshots of all stocks listed on Nasdaq's screener with comprehensive market data.

  **Data Collected:**
  - Company profile (symbol, name, sector, industry, country)
  - Market metrics (last sale, net change, percent change, market cap, volume)
  - IPO year
  - Historical tracking with daily snapshots

  **Scripts:**
  - `scripts/download_nasdaq_screener_selenium.py` - Download CSV using Selenium (recommended)
  - `scripts/collect_nasdaq_screener.py` - Full collection pipeline (download + process + database)

  **Usage:**
  ```bash
  # Full collection (auto-downloads and processes)
  python scripts/collect_nasdaq_screener.py

  # Download only
  python scripts/download_nasdaq_screener_selenium.py

  # Process existing CSV
  python scripts/collect_nasdaq_screener.py --csv-file data/nasdaq_screener/file.csv

  # Collect only if data is stale (> 1 day)
  python scripts/collect_nasdaq_screener.py --if-needed

  Note: Uses Selenium for reliable downloads. Playwright version (download_nasdaq_screener.py) is available but less reliable due to Nasdaq's anti-bot protection.   
```

## Priority Lists

Create priority list files for backfilling:

**Format 1: Plain text (.txt)**
```
AAPL
MSFT
GOOGL
```

**Format 2: CSV (.csv)**
```csv
symbol,name,sector
AAPL,Apple Inc,Technology
MSFT,Microsoft,Technology
```

Place in `data/priority_lists/` directory.

### Generate Priority Lists
```bash
# Creates prioritized company lists based on market cap, volume
python scripts/prioritize_companies.py
```

## Useful Utilities

### Check Company Status
```bash
# Verify which companies are active and in database
python scripts/check_active_companies.py
```

### Validate Bulk CSV Files
```bash
# Inspect CSV format before loading
python scripts/inspect_bulk_csv.py data/bulk_csv/your_file.csv

# Validate data quality
python scripts/validate_bulk_data.py
```

### Database Diagnostics
```bash
# Check for data issues
python scripts/diagnose_bigint_overflow.py
```

## Tracking & Monitoring

All collection runs are logged to database tables:

**`data_collection_log`** - Run history, success/failure rates, timing
**`table_update_tracking`** - Per-symbol tracking, next update times

Query examples:
```python
from src.database.models import DataCollectionLog, TableUpdateTracking

# Recent collection runs
recent_runs = session.query(DataCollectionLog)\
    .order_by(DataCollectionLog.start_time.desc())\
    .limit(10).all()

# Check when AAPL was last updated
tracking = session.query(TableUpdateTracking)\
    .filter(TableUpdateTracking.symbol == 'AAPL')\
    .all()
```

## Best Practices

### Initial Setup
1. Load company profiles via bulk CSV
2. Run small test: `--limit 5`
3. Backfill with `--force` for full history
4. Set up scheduled updates (incremental)

### Daily Operations
1. Update prices: `--collectors price` (fast)
2. Update economic data: `update_economic_data.py`
3. Weekly: financial statements (incremental)
4. Monthly: insider/analyst data

### Error Recovery
1. Check `data_collection_log` for errors
2. Use `--resume` to continue failed runs
3. Use `--force` for specific symbols if data is corrupted
4. Review tracking table for stale updates

## Performance Tips

- Use `--limit` for testing before full runs
- Use specific `--collectors` to target data types
- Enable `--resume` for large batches (>100 companies)
- Incremental mode is 10-20x faster than `--force`
- Bulk CSV loading is fastest for initial company setup

## Documentation

See `CONVERSATION_CONTEXT.md` for:
- Complete architecture details
- Recent fixes and updates
- Known issues and solutions

## Support

Phase 2 Status: ✅ Production Ready
- All collectors operational
- Incremental updates working
- Economic data integrated
- Error handling robust
