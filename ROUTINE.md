THIS IS THE ROUTINE

## The `backfill_priority_data.py` script is the main tool for loading historical data:

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



## Summary

### Backfill priority list

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

### Manual update eonomic data (checks tracking, updates if needed)
python scripts/update_economic_data.py

### Economic Calendar Collection

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

### Economic Calendar Collection

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

**Backfill last 2 years:**
```bash
python scripts/collect_earnings_calendar.py --backfill-days 730
```

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
```

### Nasdaq Stock Screener Collector


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

### Nasdaq EFT Screener Collector

``` bash
python scripts/collect_nasdaq_etf_screener.py
```
### Manual Download & Import for Nasdaq Screeners

  If the automated scrapers fail, you can manually download and import the CSV files:

  Stock Screener

  1. Download CSV:
    - Visit: https://www.nasdaq.com/market-activity/stocks/screener
    - Click "Download CSV" button
    - Save to: data/nasdaq_screener/
  2. Import to database:
  python scripts/collect_nasdaq_screener.py --csv-file data/nasdaq_screener/your_file.csv

  ETF Screener

  1. Download CSV:
    - Visit: https://www.nasdaq.com/market-activity/etf/screener
    - Click "Download CSV" button
    - Save to: data/nasdaq_etf_screener/
  2. Import to database:
  python scripts/collect_nasdaq_etf_screener.py --csv-file data/nasdaq_etf_screener/your_file.csv

  Optional: Specify snapshot date:
  python scripts/collect_nasdaq_screener.py --csv-file path/to/file.csv --snapshot-date 2025-11-11
  python scripts/collect_nasdaq_etf_screener.py --csv-file path/to/file.csv --snapshot-date 2025-11-11