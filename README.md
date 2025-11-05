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

- **18 Database Tables**: All financial data types
- **Incremental Updates**: Only fetch new data
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
- **Update Frequency**: Every 15 days
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

#### 9. **BulkProfileCollector** (`bulk_profile_collector.py`)
- **Tables**: `companies`
- **Data**: Bulk company profiles from CSV files
- **Special Features**:
  - Filters for US exchanges only
  - Transforms camelCase to snake_case
  - Deduplicates symbols
  - Batch processing (1000 records per batch)
- **Usage**: Via `scripts/load_bulk_profiles.py`

#### 10. **FREDCollector** (`fred_collector.py`) ⚠️ *Not a BaseCollector*
- **Purpose**: Standalone FRED/FMP data fetcher (Excel export)
- **Data**: Economic indicators from FRED and FMP APIs
- **Output**: Excel workbooks with 4 sheets (Raw_Long, Monthly_Panel, Quarterly_Panel, Meta)
- **Note**: Used internally by `EconomicCollector` for database integration

#### ⚠️ **BulkFinancialCollector** (`bulk_financial_collector.py`) - *UNUSED*
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

### Market Data (4)
- prices_daily, prices_monthly, enterprise_values, employee_history

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
