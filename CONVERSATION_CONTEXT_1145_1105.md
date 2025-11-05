# FinExus Data Collector - Project Context

## Project Overview
FinExus Data Collector is a Python-based financial data collection system that aggregates stock market data from the Financial Modeling Prep (FMP) API into a PostgreSQL database. The system handles comprehensive financial data including:
- Company profiles and metadata
- Daily and monthly price data
- Financial statements (income, balance sheet, cash flow) - both annual and quarterly
- Financial ratios and key metrics
- Analyst estimates and price targets
- Insider trading and institutional ownership
- Employee counts and enterprise values

## Current Phase: Phase 2 - Bulk Loading & Backfilling

### Completed:
- **Phase 1**: Manual company addition + backfill system (complete)
- **Database schema**: All tables defined with proper constraints and relationships
- **Base collector infrastructure**: Smart incremental updates with tracking tables
- **Quarterly financial data**: Modified FinancialCollector to collect both annual (50 years) and quarterly (200 quarters) data
- **Force refill capability**: Added `--force` flag to bypass incremental logic and fetch full historical data
- **Update frequency optimization**: Adjusted max_age_days across collectors (daily: 1, weekly: 7, bi-weekly: 15, quarterly: 90)
- **Numeric field overflow fix**: Migrated all Numeric(10,x) columns to Numeric(20,x) via Alembic to handle extreme financial values
- **Data sanitization layer**: Implemented model-aware sanitization in base_collector.py to prevent database constraint violations
- **Transaction rollback handling**: Added rollback logic to prevent cascade failures when one collector method fails

### Recent Fixes:
1. **Duplicate record errors** - Added `df.drop_duplicates()` to collectors (analyst, employee, enterprise)
2. **Transaction cascade errors** - Added `session.rollback()` in exception handlers across multiple collectors
3. **Numeric field overflow** - Two-pronged approach: schema migration + sanitization
4. **BigInteger overflow** - Added sanitization to PriceCollector for volume fields
5. **Invalid date handling** - Added `errors='coerce'` and `df.dropna()` for date parsing in InsiderCollector
6. **Transaction isolation** - Added try-except-rollback wrappers to all three methods in InsiderCollector

## Key Architecture Decisions

### Data Collection Strategy:
- **Incremental updates**: Uses tracking tables to fetch only new data after initial backfill
- **Force refill mode**: `--force` flag bypasses incremental logic, fetches all historical data
- **Smart limits**: Different limits for initial vs update (e.g., 50/10 for annual, 200/40 for quarterly)
- **Period-based tracking**: Financial statements tracked separately for annual vs quarterly updates

### Error Handling Strategy:
- **Schema constraints** (primary): Database columns sized appropriately (Numeric 20,x, BigInteger)
- **Data sanitization** (secondary): Safety net that caps/nulls values exceeding 90% of database limits
- **Transaction isolation**: Each collector method has try-except-rollback to prevent cascade failures
- **Graceful degradation**: Failed data collection logs errors but continues with other symbols/data types

### Economic Data Architecture:
- **Two-table design**: Metadata (`economic_indicators`) + Time series data (separate tables)
- **Four tables total**:
  - `economic_indicators` - Indicator metadata (code, name, source, frequency, units)
  - `economic_data_raw` - Raw/daily time series data
  - `economic_data_monthly` - Month-end aggregations
  - `economic_data_quarterly` - Quarter-end aggregations
- **Data sources**: FRED (primary, ~27 indicators) + FMP (economic indicators, Treasury curve)
- **Integration approach**: EconomicCollector wraps FREDCollector for database storage

## File Structure

### Core Collectors (`src/collectors/`):
- `base_collector.py` - Base class with shared functionality (sanitization, tracking, error handling)
- `company_collector.py` - Company profiles (max_age_days=15)
- `price_collector.py` - Daily/monthly prices (max_age_days=1)
- `financial_collector.py` - Financial statements, ratios, metrics (max_age_days=15, dual period)
- `analyst_collector.py` - Analyst estimates and price targets (max_age_days=15)
- `insider_collector.py` - Insider trading, institutional ownership, insider statistics (max_age_days=7/90)
- `employee_collector.py` - Employee count history (max_age_days=90)
- `enterprise_collector.py` - Enterprise value calculations (max_age_days=90)
- `fred_collector.py` - FRED/FMP economic data fetcher (Excel export, used by EconomicCollector)
- `economic_collector.py` - Economic data database integration (wraps FREDCollector, saves to 4 tables)

### Scripts (`scripts/`):
- `backfill_priority_data.py` - Main backfill script with `--limit` and `--force` flags
- `check_active_companies.py` - Validates company list before backfill
- `check_data_quality.py` - Data quality checks and validation
- `bulk_profile_collector.py` - Bulk loading from CSV files

### Database (`src/database/`):
- `models.py` - SQLAlchemy ORM models for all tables
- `connection.py` - Database connection management
- `migrations/` - Alembic migration history

## Known Issues

### Resolved:
- ✅ Duplicate key violations (WMT analyst_estimates)
- ✅ Transaction cascade failures (BLK, AMD)
- ✅ Numeric field overflow (AMD financial_ratios)
- ✅ BigInteger overflow (WELL prices)

### Monitoring:
- 🔄 Invalid date handling (NOC insider_trading) - Recent fix added, needs testing
- 🔄 Transaction isolation in InsiderCollector - Recent try-except wrappers added, needs verification

### Recently Completed:
- ✅ Economic data integration - EconomicCollector now saves to database (4-table design)
- ✅ Economic database schema - Metadata + Raw/Monthly/Quarterly tables created
- ✅ FREDCollector integration - Wraps existing fred_collector.py for database storage

### Pending:
- ⏳ Test economic data collection end-to-end
- ⏳ Large-scale backfill of Russell 3000 companies

## Usage Examples

### Test single company:
```bash
python scripts/backfill_priority_data.py --limit 1
```

### Backfill with force refill (fetch all historical data):
```bash
python scripts/backfill_priority_data.py --limit 10 --force
```

### Backfill large batch:
```bash
python scripts/backfill_priority_data.py --limit 100
```

### Test economic data collection:
```bash
python scripts/test_economic_collector.py
```

Or run the collector directly:
```python
from src.database.connection import get_session
from src.collectors.economic_collector import EconomicCollector

with get_session() as session:
    collector = EconomicCollector(session)
    success = collector.collect_all()
```

## Next Steps

1. **Test NOC insider trading fix** - Verify that try-except-rollback wrappers resolve transaction cascade errors
2. **Verify all collectors work end-to-end** - Run backfill on subset of companies to confirm all error fixes work
3. **Complete large-scale backfill** - Backfill Russell 3000 list once stability confirmed
4. **Integrate economic data collection** - Fix EconomicCollector and configure FRED_API_KEY
5. **Production deployment** - Schedule incremental updates for daily/weekly/quarterly data

## Important Notes

- Database connection requires `.env` file with `DATABASE_URL` and `FMP_API_KEY`
- FMP API has rate limits - collectors include sleep delays
- Incremental updates check `last_api_date` in tracking table
- Force refill mode ignores tracking and fetches full history
- Financial data now includes both annual (FY) and quarterly (Q1-Q4) periods
- All Numeric columns migrated from precision 10 to 20 to handle extreme values
- Sanitization layer provides safety net for truly corrupt/overflow data

● Summary of Options

  Manual (Development)

  python scripts/update_economic_data.py
  - Run whenever you want
  - Good for testing

  Windows Task Scheduler (Production - Windows)

  run_economic_update.bat
  - Built into Windows
  - Most reliable for Windows servers
  - Runs even when not logged in

  Python Scheduler (Production - Any OS)

  pip install schedule
  python scripts/scheduler.py
  - Cross-platform
  - Run as background service
  - More flexible (can add more jobs easily)

  Linux/Mac Cron (Production - Linux/Mac)

  # Add to crontab -e
  0 8 * * * cd /path/to/finexus-data-collector && venv/bin/python scripts/update_economic_data.py

  Recommended Setup

  For your Windows environment:

  1. Development: Run python scripts/update_economic_data.py manually when needed
  2. Production: Use Windows Task Scheduler with the .bat file

  Smart Update Logic

  The script already checks the tracking table - it won't waste API calls if data was updated recently (within 23 hours). The economic data updates daily with new values, so daily at 8:00 AM is ideal (before    
   market open).

  Want me to help set up any of these options?

> What do you mean by saying set up any of these options? 

● Good question! Let me clarify what I can help with for each option:

  What I Mean by "Set Up"

  Option 1: Manual Script ✅ Already Done

  The script is ready to use. Just run:
  python scripts/update_economic_data.py
  Nothing else needed!

  Option 2: Windows Task Scheduler

  I cannot directly configure Windows Task Scheduler, but I can:
  - ✅ Walk you through the steps (which buttons to click)
  - ✅ Help troubleshoot if it doesn't work
  - ✅ Explain settings (run whether user is logged on, wake computer, etc.)
  - ✅ Test the batch file before scheduling

  Option 3: Python Scheduler

  I can help you:
  - ✅ Install the schedule package: pip install schedule
  - ✅ Test the scheduler script before running it continuously
  - ✅ Create a Windows service wrapper (so it runs in background)
  - ✅ Add more scheduled jobs (daily prices, weekly financials, etc.)

  So "Set Up" Means:

  I'm asking if you need help with:
  1. Testing the scripts I created
  2. Walking through Windows Task Scheduler configuration
  3. Installing dependencies for Python scheduler
  4. Creating additional scheduled jobs
  5. Troubleshooting if something doesn't work

  Or are you good to go and will handle scheduling yourself? The scripts are ready to use as-is - you can just run them manually for now and add scheduling later when you're ready for production!