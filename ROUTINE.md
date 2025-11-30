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
or
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



## Summary of Financial Collector

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

### Earning Calendar Collection

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

## BLS AP Data Collection

### Overview

The BLS (Bureau of Labor Statistics) integration collects economic data including:
- **AP (Average Prices)**: Food, fuel, and gasoline prices (1980-present)
- Future: CPI, unemployment, employment, wages, PPI, productivity

**Current Status:** AP survey integrated with 351K+ observations

### Initial Setup

**1. Create Database Tables**

```bash
# Generate Alembic migration for BLS tables
alembic revision --autogenerate -m "Add BLS AP tables"

# Apply migration
alembic upgrade head
```

**Tables created:**
- `bls_surveys` - Survey catalog
- `bls_areas` - Geographic areas (74 locations)
- `bls_periods` - Period codes (M01-M13, Q01-Q04)
- `bls_ap_items` - Item catalog (160 food/fuel items)
- `bls_ap_series` - Series metadata (1,482 series)
- `bls_ap_data` - Time series observations

**2. Load Historical Data from Flat Files**

Download flat files from: https://download.bls.gov/pub/time.series/ap/

Required files in `data/bls/ap/`:
- `ap.area`, `ap.item`, `ap.period`, `ap.series` (metadata)
- `ap.data.0.Current` (recent data)
- `ap.data.1.HouseholdFuels` (fuel prices)
- `ap.data.2.Gasoline` (gasoline prices)
- `ap.data.3.Food` (food prices)

```bash
# Load all data (~351K unique observations)
python scripts/bls/load_ap_flat_files.py

# Options:
python scripts/bls/load_ap_flat_files.py --batch-size 5000
python scripts/bls/load_ap_flat_files.py --skip-reference  # Skip metadata
python scripts/bls/load_ap_flat_files.py --skip-data       # Skip observations
```

### Regular Updates

**Update with Latest Data from API**

```bash
# RECOMMENDED: Update last 2 years (catches revisions)
python scripts/bls/update_ap_latest.py --start-year 2024 --end-year 2025

# Alternative: Update current year only (faster but may miss revisions)
python scripts/bls/update_ap_latest.py

# Update specific series
python scripts/bls/update_ap_latest.py --series-ids APU0000701111,APU0000703111

# Test with limited series
python scripts/bls/update_ap_latest.py --limit 10
```

**Recommended Schedule:**
- **Monthly updates** for AP data (new data released monthly)
- **Run on the 15th** of each month to capture previous month's data
- **Use 2-year window** (`--start-year 2024`) to catch BLS revisions

**API Usage:**
- 544 active series ÷ 50 per request = ~11 API calls
- 2-year window = ~20-30 observations per series
- Well within 500 requests/day limit for registered users

**Why 2 years?**
- BLS sometimes revises previous months' data
- Captures corrections and late-arriving data
- Minimal overhead (~11 API calls vs ~11 for 1 year)

### Data Structure

**Series ID Format:** `APU0000701111`
- `AP` = Survey code (Average Prices)
- `U` = Not seasonally adjusted (S = seasonally adjusted)
- `0000` = Area code (U.S. city average)
- `701111` = Item code (flour, white, all purpose)

**Period Codes:**
- `M01`-`M12` = January-December
- `M13` = Annual average

### Example Queries

```sql
-- Latest ground chuck prices
SELECT year, period, value
FROM bls_ap_data
WHERE series_id = 'APU0000703111'
ORDER BY year DESC, period DESC
LIMIT 12;

-- Active vs discontinued series
SELECT is_active, COUNT(*)
FROM bls_ap_series
GROUP BY is_active;

-- All flour prices in 2024
SELECT s.series_title, d.year, d.period, d.value
FROM bls_ap_data d
JOIN bls_ap_series s ON d.series_id = s.series_id
WHERE s.item_code = '701111'
  AND d.year = 2024
ORDER BY s.area_code, d.period;

-- Price inflation for specific item
SELECT year, period, value,
       LAG(value) OVER (ORDER BY year, period) as prev_value,
       ROUND(((value / LAG(value) OVER (ORDER BY year, period) - 1) * 100)::numeric, 2) as pct_change
FROM bls_ap_data
WHERE series_id = 'APU0000703111'
  AND year >= 2023
ORDER BY year, period;
```

### Data Coverage

**Geographic Coverage:**
- National: U.S. city average
- Regions: Northeast, Midwest, South, West (and subregions)
- Cities: Major metro areas (Pittsburgh, Cleveland, Milwaukee, etc.)

**Item Coverage:**
- **Food**: Flour, bread, rice, pasta, beef, chicken, eggs, milk, vegetables, etc.
- **Household Fuels**: Electricity, natural gas, fuel oil
- **Gasoline**: Regular, midgrade, premium unleaded

**Time Coverage:** 1980 - Present (monthly)
- 544 active series (continuing)
- 938 discontinued series (historical)

### Configuration

BLS API key configured in `.env`:
```
BLS_API_KEY=
```

Access via code:
```python
from config import settings
api_key = settings.api.bls_api_key
```

### Future Surveys

Framework ready for additional BLS surveys:
- **LA** - Local Area Unemployment Statistics
- **CE** - Current Employment Statistics
- **WP** - Producer Price Index (PPI)
- **PR** - Productivity and Costs

## BLS CU Data Collection (Consumer Price Index)

### Overview

The CU (Consumer Price Index) survey is the **most important inflation indicator** tracked by the Federal Reserve and financial markets. CPI measures the average change in prices paid by urban consumers for a basket of goods and services.

**Current Status:** CU survey integrated with 1.7M+ observations (1913-2025)

**Key CPI Series:**
- **CUSR0000SA0** - CPI-U All items (headline CPI)
- **CUSR0000SA0L1E** - All items less food & energy (Core CPI)
- **CUSR0000SA0E** - Energy
- **CUSR0000SAH** - Housing (largest component ~40%)
- **CUSR0000SAM** - Medical care
- **CUSR0000SAT** - Transportation

### Initial Setup

**1. Create Database Tables**

```bash
# Generate Alembic migration for CU tables
alembic revision --autogenerate -m "Add BLS CU (CPI) tables"

# Apply migration
alembic upgrade head
```

**Tables created:**
- `bls_periodicity` - Periodicity codes (monthly, semi-annual)
- `bls_cu_items` - Item catalog (~600 items)
- `bls_cu_series` - Series metadata (8,104 series)
- `bls_cu_data` - Time series observations (index values)
- `bls_cu_aspects` - Additional metrics (M1, H1, V1)

**2. Load Historical Data from Flat Files**

Download flat files from: https://download.bls.gov/pub/time.series/cu/

Required files in `data/bls/cu/`:
- `cu.area`, `cu.item`, `cu.period`, `cu.periodicity`, `cu.series` (metadata)
- `cu.data.0.Current` through `cu.data.20.*` (21 data files, 158MB total)
- `cu.aspect` (optional - additional metrics - 29MB)

```bash
# STEP 1: Load metadata + recent data for all series (1997-2025)
python scripts/bls/load_cu_flat_files.py

# STEP 2: Load complete historical data (1913-2025)
# This adds ~604K historical observations for key series
python scripts/bls/load_cu_flat_files.py --data-files "cu.data.1.AllItems,cu.data.2.Summaries,cu.data.3.AsizeNorthEast,cu.data.4.AsizeNorthCentral,cu.data.5.AsizeSouth,cu.data.6.AsizeWest,cu.data.7.OtherNorthEast,cu.data.8.OtherNorthCentral,cu.data.9.OtherSouth,cu.data.10.OtherWest,cu.data.11.USFoodBeverage,cu.data.12.USHousing,cu.data.13.USApparel,cu.data.14.USTransportation,cu.data.15.USMedical,cu.data.16.USRecreation,cu.data.17.USEducationAndCommunication,cu.data.18.USOtherGoodsAndServices,cu.data.19.PopulationSize,cu.data.20.USCommoditiesServicesSpecial" --skip-reference

# STEP 3 (Optional): Load aspect data (M1, H1, V1 metrics)
python scripts/bls/load_cu_flat_files.py --load-aspects --skip-reference --skip-data
```

**Data loaded:**
- **1,726,868 time series observations** (after UPSERT deduplication)
- **Time range: 1913-2025** (112 years of historical CPI data)
- **8,103 unique series** with data
- ~512,000 aspect records (if loaded)

### Regular Updates

**Update with Latest Data from API**

```bash
# RECOMMENDED: Update last 2 years (catches revisions)
python scripts/bls/update_cu_latest.py --start-year 2024 --end-year 2025

# Alternative: Update current year only
python scripts/bls/update_cu_latest.py

# Update specific series
python scripts/bls/update_cu_latest.py --series-ids CUSR0000SA0,CUSR0000SA0L1E

# Test with limited series
python scripts/bls/update_cu_latest.py --limit 10
```

**Recommended Schedule:**
- **Monthly updates** for CPI data (released mid-month)
- **Run on the 15th** of each month to capture previous month's data
- **Use 2-year window** to catch BLS revisions

**API Usage:**
- ~5,000 active series ÷ 50 per request = **~100 API calls**
- 2-year window = ~24 observations per series
- Well within 500 requests/day limit
- Update takes ~3-5 minutes (throttled to 50 req/10s)

**Why 2 years?**
- BLS sometimes revises previous months' CPI data
- Captures corrections and seasonal adjustment updates
- Minimal overhead (~100 requests vs ~100 for 1 year)

### Data Structure

**Series ID Format:** `CUSR0000SA0`
- `CU` = Survey code (Consumer Price Index)
- `S` = Seasonal adjustment (S = seasonally adjusted, U = not adjusted)
- `R` = Population (R = All urban consumers, W = Urban wage earners)
- `0000` = Area code (U.S. city average)
- `SA0` = Item code (All items)

**Base Period:** `1982-84=100`
- CPI is an index, not a price
- 100 = average price level in 1982-1984
- Current values show % change from base period

**Period Codes:**
- `M01`-`M12` = January-December
- `M13` = Annual average
- `S01`-`S03` = Semi-annual (some series)

**Aspect Types** (in cu.aspects table):
- `M1` - Month-to-month change
- `H1` - Historical reference (highest/lowest)
- `V1` - Variance measure

### Example Queries

```sql
-- Latest CPI readings (last 12 months)
SELECT s.series_title, d.year, d.period, d.value
FROM bls_cu_data d
JOIN bls_cu_series s ON d.series_id = s.series_id
WHERE s.series_id IN ('CUSR0000SA0', 'CUSR0000SA0L1E')  -- Headline & Core
ORDER BY s.series_id, d.year DESC, d.period DESC
LIMIT 24;

-- Year-over-year inflation calculation
SELECT
    year,
    period,
    value,
    LAG(value, 12) OVER (ORDER BY year, period) as value_12m_ago,
    ROUND(((value / LAG(value, 12) OVER (ORDER BY year, period) - 1) * 100)::numeric, 2) as yoy_inflation_pct
FROM bls_cu_data
WHERE series_id = 'CUSR0000SA0'  -- Headline CPI
  AND year >= 2023
ORDER BY year, period;

-- CPI components breakdown (latest month)
SELECT
    i.item_name,
    d.value as index_value,
    a.value as monthly_change
FROM bls_cu_data d
JOIN bls_cu_series s ON d.series_id = s.series_id
JOIN bls_cu_items i ON s.item_code = i.item_code
LEFT JOIN bls_cu_aspects a ON (
    a.series_id = d.series_id
    AND a.year = d.year
    AND a.period = d.period
    AND a.aspect_type = 'M1'
)
WHERE s.area_code = '0000'  -- U.S. city average
  AND s.seasonal_code = 'S'  -- Seasonally adjusted
  AND i.display_level = 0    -- Top-level items
  AND d.year = 2025
  AND d.period = 'M09'
ORDER BY i.sort_sequence;

-- Active vs discontinued series
SELECT is_active, COUNT(*) as count
FROM bls_cu_series
GROUP BY is_active;

-- Most recent data availability by series
SELECT
    series_id,
    series_title,
    end_year,
    end_period
FROM bls_cu_series
WHERE is_active = true
ORDER BY end_year DESC, end_period DESC
LIMIT 20;
```

### Data Coverage

**CPI-U vs CPI-W:**
- **CPI-U** (All urban consumers): 93% of U.S. population - Most widely used
- **CPI-W** (Urban wage earners): 29% of population - Used for Social Security

**Geographic Coverage:**
- National: U.S. city average
- Regions: Northeast, Midwest, South, West (and subregions)
- Cities: Major metro areas (by population size)

**Item Coverage (~600 items):**
- **Food & Beverages**: Groceries, dining out
- **Housing**: Rent, owners' equivalent rent, utilities (largest component ~40%)
- **Apparel**: Clothing, footwear
- **Transportation**: Vehicles, gasoline, insurance
- **Medical Care**: Services, commodities
- **Recreation**: Entertainment, sports equipment
- **Education & Communication**: Tuition, phone services
- **Other**: Personal care, tobacco, misc.

**Special Aggregates:**
- All items
- All items less food & energy (Core CPI)
- All items less shelter
- Commodities vs Services
- Energy commodities

**Time Coverage:** 1947 - Present (monthly)
- ~5,000 active series (continuing)
- ~3,000 discontinued series (historical)

### Configuration

BLS API key configured in `.env`:
```
BLS_API_KEY=
```

### Why CPI Matters for Financial Analysis

1. **Fed Policy**: Primary inflation target (2% PCE, but watches CPI closely)
2. **Real Returns**: Adjust nominal returns for inflation
3. **Market Impact**: Surprise CPI readings move stocks, bonds, currencies
4. **Economic Cycle**: Leading/coincident indicator of economic conditions
5. **Sector Analysis**: Component breakdowns show sector-specific inflation
6. **Forecasting**: Input for economic models and projections


### Run on the 15th of each month
  python scripts/bls/update_ap_latest.py --start-year 2024 --end-year 2025  # ~11 requests
  python scripts/bls/update_cu_latest.py --start-year 2024 --end-year 2025  # ~100 requests
  python scripts/bls/update_la_latest.py --area-types A,B --seasonal S     # ~7 requests (monthly: states + metros)

## BLS LA Data Collection (Local Area Unemployment Statistics)

Summary

  Problem identified: LA (Local Area Unemployment Statistics) was using the shared bls_areas
  table, missing critical geographic metadata for 8,325 areas.

  Fix applied:

  1. Created LAArea model with proper columns:
    - area_code (PK)
    - area_type_code (A=state, B=metro, C=county, etc.)
    - area_text (geographic name)
    - display_level, selectable, sort_sequence
  2. Generated migration ed546fe6ba3a:
    - Created bls_la_areas table
    - Copied existing LA area codes from bls_areas to maintain referential integrity
    - Updated LASeries foreign key: bls_areas.area_code → bls_la_areas.area_code
  3. Updated LA parser:
    - Fixed parse_areas() to parse all columns correctly (was missing display_level, selectable,     
  sort_sequence)
    - Changed import from BLSArea to LAArea
    - Updated load_reference_tables() to use LAArea model
  4. Loaded complete area data:
    - 8,325 areas loaded from la.area file
    - 7 measures
    - 33,881 series

  Result: LA survey now has its own complete area table with full geographic hierarchy metadata.     
  This is critical because LA is fundamentally about local area geographic breakdowns!

### Overview

The LA (Local Area Unemployment Statistics) survey provides **unemployment data at granular geographic levels** - from states down to individual cities and counties. Essential for labor market analysis, regional economic forecasting, and understanding geographic employment trends.

**Current Status:** LA survey integrated with 15.2M+ observations (1976-2025)

**Key LA Series:**
- **LASST060000000000003** - California state unemployment rate
- **LASST480000000000003** - Texas state unemployment rate
- **LASBS120000000000003** - Florida balance of state unemployment rate
- **LAUCN040130000000003** - Maricopa County, AZ unemployment rate
- **Metro series** - Unemployment for major metropolitan areas

**Coverage:** 33,881 total series across 8,325 geographic areas

### Initial Setup

**1. Create Database Tables**

```bash
# Generate Alembic migration for LA tables
alembic revision --autogenerate -m "Add BLS LA (Local Area Unemployment) tables"

# Apply migration
alembic upgrade head
```

**Tables created:**
- `bls_la_measures` - Measure catalog (7 measures: rate, employment, labor force, etc.)
- `bls_la_series` - Series metadata (33,881 series)
- `bls_la_data` - Time series observations (unemployment statistics)

**Note:** Extended `bls_areas.area_code` from VARCHAR(10) to VARCHAR(20) to accommodate LA area codes (e.g., 'ST0100000000000')

**2. Load Historical Data from Flat Files**

Download flat files from: https://download.bls.gov/pub/time.series/la/

All files should be placed in `data/bls/la/`:
- `la.area`, `la.measure`, `la.period`, `la.series` (metadata)
- `la.data.*` (71 data files, 2.3GB total)

```bash
# OPTION A: Load essential files only (faster, ~240K observations)
python scripts/bls/load_la_flat_files.py

# OPTION B: Load ALL 71 files for complete coverage (recommended, ~15M observations)
python scripts/bls/load_la_flat_files.py --load-all

# OPTION C: Load current data files only (~16M observations)
python scripts/bls/load_la_flat_files.py --load-current-all

# OPTION D: Load specific file(s)
python scripts/bls/load_la_flat_files.py --data-files "la.data.1.CurrentS,la.data.64.County"

# Additional options:
python scripts/bls/load_la_flat_files.py --load-all --batch-size 5000  # Adjust batch size
python scripts/bls/load_la_flat_files.py --skip-reference  # Skip metadata (for reloads)
```

**Data loaded (with --load-all):**
- **15,190,327 time series observations** (after UPSERT deduplication)
- **Time range: 1976-2025** (49 years of unemployment data)
- **33,881 unique series** across all geographic levels
- **8,325 areas** (states, metros, counties, cities)
- **7 measures** (unemployment rate, employment, labor force, etc.)

**File Organization:**
- Files 0-9: Current data by year ranges (`CurrentU00-04` through `CurrentU95-99`)
- File 1: `CurrentS` - Seasonally adjusted current data
- Files 2-5: State and region aggregates
- Files 10-59: Individual state files (50 states + territories)
- Files 60-65: Area type aggregates (Metro, County, City, etc.)

**Loading Time:** 40-50 minutes for all 71 files with 10K batch size

### Regular Updates

**Tiered Update Strategy (Recommended)**

Due to the large number of series (33,725 active = 675 API requests), use a tiered approach:

**Monthly Updates (States + Major Metros, Seasonally Adjusted):**
```bash
# ~322 series = ~7 API requests
python scripts/bls/update_la_latest.py --area-types A,B --seasonal S --start-year 2024
```

**Quarterly Updates (All Metro/Micro Areas):**
```bash
# ~4,488 series = ~90 API requests
python scripts/bls/update_la_latest.py --area-types B,D,E --start-year 2024
```

**Semi-Annual Updates (Counties and Cities):**
```bash
# ~20,773 series = ~416 API requests
python scripts/bls/update_la_latest.py --area-types F,G --start-year 2024
```

**Alternative: Update All Active Series (Use Caution)**
```bash
# ~33,725 series = ~675 API requests (EXCEEDS 500/day limit!)
# Only use if you have multiple days or special API access
python scripts/bls/update_la_latest.py --start-year 2024
```

**Additional Filtering Options:**

```bash
# Update unemployment rate only (measure code 03)
python scripts/bls/update_la_latest.py --measure-codes 03 --start-year 2024

# Update specific series
python scripts/bls/update_la_latest.py --series-ids LASST060000000000003,LASST480000000000003

# Test with limited series
python scripts/bls/update_la_latest.py --limit 10

# Dry run (preview what would be updated)
python scripts/bls/update_la_latest.py --area-types A,B --seasonal S --dry-run
```

**Recommended Schedule:**
- **Monthly**: States + metros, seasonally adjusted (~7 requests)
- **Quarterly**: All metro/micro areas (~90 requests)
- **Semi-Annual**: Counties and cities (~416 requests)
- **Run mid-month** to capture previous month's data

**API Usage Notes:**
- LA data updated monthly by BLS
- 2-year window catches revisions: `--start-year 2024`
- Stay within 500 requests/day limit with tiered strategy
- Throttled to 50 req/10s automatically

### Data Structure

**Series ID Format:** `LASST060000000000003`
- `LA` = Survey code (Local Area Unemployment Statistics)
- `S` = Seasonal adjustment (S = seasonally adjusted, U = not adjusted)
- `S` = State/Area level indicator
- `T` = Area type code (A-N, see below)
- `06000000000000` = Area code (California)
- `03` = Measure code (unemployment rate)

**Area Type Codes:**
- `A` = Statewide
- `B` = Metropolitan areas
- `C` = Metropolitan divisions
- `D` = Micropolitan areas
- `E` = Combined areas
- `F` = Counties and equivalents
- `G` = Cities and towns above 25,000 population
- `H` = Cities and towns below 25,000 in New England
- `I` = Towns in New England
- `J` = Cities and towns below 25,000, except New England
- `K` = Census divisions
- `L` = Census regions
- `M` = Multi-entity small labor market areas
- `N` = Balance of state areas

**Measure Codes:**
- `03` = Unemployment rate (%)
- `04` = Unemployment (persons)
- `05` = Employment (persons)
- `06` = Labor force (persons)
- `07` = Employment-population ratio (%)
- `08` = Labor force participation rate (%)
- `09` = Civilian noninstitutional population (persons)

**Period Codes:**
- `M01`-`M12` = January-December
- `M13` = Annual average

### Example Queries

```sql
-- Latest unemployment rates by state (seasonally adjusted)
SELECT
    a.area_name,
    d.year,
    d.period,
    d.value as unemployment_rate_pct
FROM bls_la_data d
JOIN bls_la_series s ON d.series_id = s.series_id
JOIN bls_areas a ON s.area_code = a.area_code
WHERE s.area_type_code = 'A'  -- Statewide
  AND s.measure_code = '03'    -- Unemployment rate
  AND s.seasonal_code = 'S'    -- Seasonally adjusted
  AND d.year = 2025
  AND d.period = 'M09'
ORDER BY d.value DESC
LIMIT 10;

-- Year-over-year unemployment change (California)
SELECT
    year,
    period,
    value as current_rate,
    LAG(value, 12) OVER (ORDER BY year, period) as rate_12m_ago,
    ROUND((value - LAG(value, 12) OVER (ORDER BY year, period))::numeric, 2) as yoy_change
FROM bls_la_data
WHERE series_id = 'LASST060000000000003'  -- CA unemployment rate (seasonally adjusted)
  AND year >= 2023
ORDER BY year, period;

-- Top 10 metros by unemployment rate (latest month)
SELECT
    a.area_name,
    d.value as unemployment_rate_pct
FROM bls_la_data d
JOIN bls_la_series s ON d.series_id = s.series_id
JOIN bls_areas a ON s.area_code = a.area_code
WHERE s.area_type_code = 'B'  -- Metropolitan areas
  AND s.measure_code = '03'    -- Unemployment rate
  AND s.seasonal_code = 'S'    -- Seasonally adjusted
  AND d.year = 2025
  AND d.period = 'M09'
ORDER BY d.value DESC
LIMIT 10;

-- Labor market breakdown for specific area (Maricopa County, AZ)
SELECT
    m.measure_text,
    d.year,
    d.period,
    d.value
FROM bls_la_data d
JOIN bls_la_series s ON d.series_id = s.series_id
JOIN bls_la_measures m ON s.measure_code = m.measure_code
WHERE s.area_code = '04013'  -- Maricopa County
  AND d.year = 2025
  AND d.period = 'M09'
ORDER BY s.measure_code;

-- Employment trends across regions
SELECT
    a.area_name,
    d.year,
    d.period,
    d.value as employment_persons
FROM bls_la_data d
JOIN bls_la_series s ON d.series_id = s.series_id
JOIN bls_areas a ON s.area_code = a.area_code
WHERE s.area_type_code IN ('A', 'B')  -- States and metros
  AND s.measure_code = '05'            -- Employment
  AND s.seasonal_code = 'S'
  AND d.year >= 2024
ORDER BY a.area_name, d.year, d.period;

-- Active vs discontinued series
SELECT is_active, COUNT(*) as count
FROM bls_la_series
GROUP BY is_active;

-- Series distribution by area type
SELECT
    area_type_code,
    COUNT(*) as series_count
FROM bls_la_series
WHERE is_active = true
GROUP BY area_type_code
ORDER BY series_count DESC;
```

### Data Coverage

**Geographic Hierarchy:**
- **National**: U.S. totals
- **States**: All 50 states + DC + Puerto Rico
- **Metropolitan Areas**: ~390 metro areas (population > 50,000)
- **Micropolitan Areas**: ~540 micro areas (10,000-50,000 pop)
- **Counties**: ~3,000+ counties and county equivalents
- **Cities**: Major cities and towns (population-based)

**Measure Coverage (7 measures):**
- **Unemployment Rate**: Primary labor market indicator
- **Unemployment**: Number of unemployed persons
- **Employment**: Number of employed persons
- **Labor Force**: Total civilian labor force
- **Employment-Population Ratio**: % of population employed
- **Labor Force Participation Rate**: % of population in labor force
- **Population**: Civilian noninstitutional population

**Seasonal Adjustment:**
- **S** (Seasonally adjusted): Removes seasonal patterns - best for trends
- **U** (Not adjusted): Raw data - useful for year-over-year comparisons

**Time Coverage:** 1976 - Present (monthly)
- ~33,725 active series (continuing)
- ~156 discontinued series (historical)

### Configuration

BLS API key configured in `.env`:
```
BLS_API_KEY=<your_registered_api_key>
```

### Why LA Matters for Financial Analysis

1. **Regional Economy**: Identify strong/weak regional economies for investment
2. **Real Estate**: Unemployment rates predict housing market strength
3. **Consumer Spending**: Employment levels drive retail and consumer discretionary stocks
4. **Federal Reserve Policy**: Regional labor markets influence Fed decisions
5. **Sector Analysis**: Industry-specific employment trends in different regions
6. **Economic Leading Indicators**: Early warning signs of recession/expansion
7. **Municipal Bonds**: Credit risk assessment for state/local governments

### Monthly Update Routine

Recommended monthly schedule (around 15th of each month):

```bash
# Monthly: States + Major Metros (~7 API requests)
python scripts/bls/update_la_latest.py --area-types A,B --seasonal S --start-year 2024

# Quarterly (every 3 months): All Metro/Micro Areas (~90 API requests)
python scripts/bls/update_la_latest.py --area-types B,D,E --start-year 2024

# Semi-Annual (every 6 months): Counties + Cities (~416 API requests)
python scripts/bls/update_la_latest.py --area-types F,G --start-year 2024
```

**Total API Usage per Year:**
- Monthly updates: 7 × 12 = 84 requests/year
- Quarterly updates: 90 × 4 = 360 requests/year
- Semi-annual updates: 416 × 2 = 832 requests/year
- **Total: ~1,276 requests/year** (well within limits when spread across year)

## BLS CE Data Collection (Current Employment Statistics)

### Overview

The CE (Current Employment Statistics) survey is **the most important employment indicator** tracked by policymakers and financial markets. Known as the **"Establishment Survey"** or **"Payroll Survey,"** it provides the famous **monthly jobs report** that moves stocks, bonds, and currencies.

**Current Status:** CE survey integrated with 8.08M+ observations (1939-2025)

**Key CE Series:**
- **CES0000000001** - Total Nonfarm Employment (**THE** monthly jobs report number!)
- **CES0500000001** - Total Private Employment
- **CES0600000001** - Goods-Producing Industries
- **CES0800000001** - Private Service-Providing
- **CES3000000001** - Manufacturing Employment

**Coverage:** 22,049 series across 850 industries

### Initial Setup

**1. Create Database Tables**

```bash
# Generate Alembic migration for CE tables
alembic revision --autogenerate -m "Add BLS CE (Current Employment Statistics) tables"

# Apply migration
alembic upgrade head
```

**Tables created:**
- `bls_ce_industries` - Industry catalog with NAICS codes (850 industries)
- `bls_ce_data_types` - Data type catalog (41 types)
- `bls_ce_supersectors` - Supersector codes (22 high-level groups)
- `bls_ce_series` - Series metadata (22,049 series)
- `bls_ce_data` - Time series observations (employment, hours, earnings)

**2. Load Historical Data from Flat Files**

Download flat files from: https://download.bls.gov/pub/time.series/ce/

All files should be placed in `data/bls/ce/`:
- `ce.industry`, `ce.datatype`, `ce.supersector`, `ce.series` (metadata)
- `ce.data.0.AllCESSeries` (main file - 324MB, all series, all data)
- 60+ industry-specific files (optional subsets)

```bash
# RECOMMENDED: Load main file only (fastest, complete dataset)
python scripts/bls/load_ce_flat_files.py

# Subsequent loads (reference tables already in DB)
python scripts/bls/load_ce_flat_files.py --skip-reference

# Alternative: Load ALL 60+ files (same data, just organized differently)
python scripts/bls/load_ce_flat_files.py --load-all

# Load employment data only (XXa files)
python scripts/bls/load_ce_flat_files.py --load-employment

# Load specific files
python scripts/bls/load_ce_flat_files.py --data-files "ce.data.30a.Manufacturing.Employment,ce.data.50a.Information.Employment"
```

**Data loaded (with main file):**
- **8,084,418 time series observations** (complete dataset!)
- **Time range: 1939-2025** (86 years of employment data!)
- **22,049 unique series** across all industries
- **850 industries** with NAICS classification
- **41 data types** (employment, hours, earnings, indexes)

**Loading Time:** ~10-15 minutes for main file (324MB)

### Regular Updates

**Monthly Update (Recommended - Employment Data)**

The CE survey is released monthly (first Friday of each month). Update employment data for key industries:

```bash
# RECOMMENDED: Update employment data only (data type 01)
# ~440 series = ~9 API requests
python scripts/bls/update_ce_latest.py --data-types 01 --seasonal S --start-year 2024
```

**Alternative Update Strategies**

```bash
# Update specific industries (e.g., manufacturing + information)
python scripts/bls/update_ce_latest.py --industries 30000000,50000000 --start-year 2024

# Update seasonally adjusted employment only
python scripts/bls/update_ce_latest.py --data-types 01 --seasonal S --start-year 2024

# Update the jobs report number specifically
python scripts/bls/update_ce_latest.py --series-ids CES0000000001

# Update multiple key series
python scripts/bls/update_ce_latest.py --series-ids CES0000000001,CES0500000001,CES0600000001

# Test with limited series
python scripts/bls/update_ce_latest.py --limit 10
```

**Recommended Schedule:**
- **Monthly**: Employment data (data type 01) for seasonally adjusted series (~9 requests)
- **Run on first Friday** of each month after jobs report release (8:30 AM ET)
- **Use 2-year window** to catch BLS revisions

**API Usage:**
- All 22,049 active series = ~440 API requests (close to 500 limit!)
- Employment only (data type 01) = ~9 API requests ✅
- Key series only = 1 API request ✅
- Filter by industry, data type, or seasonal to stay within limits

**Why 2 years?**
- BLS revises employment data for previous months
- Benchmark revisions can change historical data
- Minimal overhead for comprehensive updates

### Data Structure

**Series ID Format:** `CES0000000001`
- `CE` = Survey code (Current Employment Statistics)
- `S` = Seasonal adjustment (S = seasonally adjusted, U = not adjusted)
- `00` = Supersector code (00 = Total nonfarm)
- `00000000` = Industry code (8 digits)
- `01` = Data type code (01 = All employees in thousands)

**Data Type Codes (41 types):**
- `01` = All employees, thousands (**THE JOBS NUMBER**)
- `02` = Average weekly hours of all employees
- `03` = Average hourly earnings of all employees
- `04` = Average weekly overtime hours
- `06` = Production/nonsupervisory employees, thousands
- `07` = Average weekly hours (production employees)
- `08` = Average hourly earnings (production employees)
- `10` = Women employees, thousands
- `11` = Average weekly earnings
- `16` = Aggregate weekly hours index
- `17` = Aggregate weekly payrolls index
- `21-24` = Diffusion indexes
- And more...

**Industry Hierarchy:**
- **Level 0**: Total nonfarm (00000000)
- **Level 1**: Major sectors (goods-producing, service-providing)
- **Level 2**: Supersectors (mining, construction, manufacturing, etc.)
- **Levels 3-7**: Detailed industries with NAICS codes

**Period Codes:**
- `M01`-`M12` = January-December
- Quarterly averages available for some series

### Example Queries

```sql
-- Latest jobs report number (Total Nonfarm Employment)
SELECT year, period, value as jobs_thousands
FROM bls_ce_data
WHERE series_id = 'CES0000000001'  -- Total Nonfarm, seasonally adjusted
ORDER BY year DESC, period DESC
LIMIT 12;

-- Month-over-month job change
SELECT
    year,
    period,
    value as current_jobs,
    LAG(value) OVER (ORDER BY year, period) as prev_month_jobs,
    value - LAG(value) OVER (ORDER BY year, period) as monthly_change_thousands
FROM bls_ce_data
WHERE series_id = 'CES0000000001'
  AND year >= 2023
ORDER BY year, period;

-- Employment by major industry (latest month)
SELECT
    i.industry_name,
    d.value as employment_thousands
FROM bls_ce_data d
JOIN bls_ce_series s ON d.series_id = s.series_id
JOIN bls_ce_industries i ON s.industry_code = i.industry_code
WHERE s.data_type_code = '01'  -- Employment
  AND s.seasonal_code = 'S'     -- Seasonally adjusted
  AND i.display_level = 2       -- Major sectors
  AND d.year = 2025
  AND d.period = 'M08'
ORDER BY i.sort_sequence;

-- Average hourly earnings trends
SELECT
    year,
    period,
    value as avg_hourly_earnings
FROM bls_ce_data
WHERE series_id = 'CES0500000003'  -- Total Private, avg hourly earnings, SA
  AND year >= 2023
ORDER BY year, period;

-- Manufacturing employment trend
SELECT
    year,
    period,
    value as manufacturing_jobs_thousands,
    LAG(value, 12) OVER (ORDER BY year, period) as jobs_12m_ago,
    ROUND(((value / LAG(value, 12) OVER (ORDER BY year, period) - 1) * 100)::numeric, 2) as yoy_pct_change
FROM bls_ce_data
WHERE series_id = 'CES3000000001'  -- Manufacturing, all employees, SA
  AND year >= 2023
ORDER BY year, period;

-- Goods vs Services employment comparison
SELECT
    d.year,
    d.period,
    MAX(CASE WHEN s.industry_code = '06000000' THEN d.value END) as goods_producing,
    MAX(CASE WHEN s.industry_code = '08000000' THEN d.value END) as service_providing
FROM bls_ce_data d
JOIN bls_ce_series s ON d.series_id = s.series_id
WHERE s.data_type_code = '01'
  AND s.seasonal_code = 'S'
  AND s.industry_code IN ('06000000', '08000000')
  AND d.year >= 2024
GROUP BY d.year, d.period
ORDER BY d.year, d.period;

-- Active vs discontinued series
SELECT is_active, COUNT(*) as count
FROM bls_ce_series
GROUP BY is_active;

-- Series distribution by data type
SELECT
    dt.data_type_text,
    COUNT(*) as series_count
FROM bls_ce_series s
JOIN bls_ce_data_types dt ON s.data_type_code = dt.data_type_code
WHERE s.is_active = true
GROUP BY dt.data_type_text
ORDER BY series_count DESC
LIMIT 10;
```

### Data Coverage

**Industry Coverage (850 industries):**
- **Mining and Logging**: Oil/gas extraction, coal, metal ore, logging
- **Construction**: Residential, nonresidential, specialty trade contractors
- **Manufacturing**: Durable goods (machinery, computers, transportation equipment)
- **Manufacturing**: Nondurable goods (food, textiles, chemicals, petroleum)
- **Trade, Transportation & Utilities**: Wholesale, retail, warehousing
- **Information**: Publishing, broadcasting, telecommunications, data processing
- **Financial Activities**: Banking, insurance, real estate, leasing
- **Professional & Business Services**: Legal, accounting, consulting, temp help
- **Education & Health Services**: Schools, hospitals, nursing, social assistance
- **Leisure & Hospitality**: Arts, entertainment, accommodation, food services
- **Other Services**: Repair, personal care, religious organizations
- **Government**: Federal, state, local (including education)

**Data Type Coverage (41 types):**
- **Employment**: All employees, production employees, women employees
- **Hours**: Weekly hours, overtime hours, aggregate hours
- **Earnings**: Hourly, weekly, real (inflation-adjusted)
- **Indexes**: Aggregate hours, aggregate payrolls
- **Diffusion Indexes**: 1-month, 3-month, 6-month, 12-month spans
- **Quarterly**: Averages, 3-month changes

**Seasonal Adjustment:**
- **S** (Seasonally adjusted): Most commonly used - removes seasonal patterns
- **U** (Not seasonally adjusted): Raw data - useful for specific analyses

**Time Coverage:** 1939 - Present (monthly)
- ~17,000 active series (continuing - most are seasonally adjusted variants)
- ~5,000 discontinued series (historical)

### Configuration

BLS API key configured in `.env`:
```
BLS_API_KEY=<your_registered_api_key>
```

### Why CE Matters for Financial Analysis

1. **THE Jobs Report**: Total Nonfarm Employment (CES0000000001) is the most-watched economic indicator
2. **Market Impact**: Jobs report releases cause significant stock/bond/FX market movements
3. **Fed Policy**: Primary employment data for Federal Reserve interest rate decisions
4. **Economic Cycle**: Leading indicator of recessions and expansions
5. **Sector Rotation**: Industry employment trends guide sector allocation strategies
6. **Wage Inflation**: Average hourly earnings data critical for inflation forecasting
7. **Consumer Spending**: Employment and wage data predict consumer discretionary strength
8. **Regional Analysis**: Industry-specific employment for geographic investment decisions

### Jobs Report Release Schedule

**Release Date:** First Friday of each month at 8:30 AM ET

**What to Update:**
```bash
# After jobs report release (first Friday morning)
python scripts/bls/update_ce_latest.py --data-types 01 --seasonal S --start-year 2024
```

**Key Series to Watch:**
- **CES0000000001**: Total Nonfarm Employment (headline number)
- **CES0500000001**: Total Private Employment
- **CES0500000003**: Average Hourly Earnings (wage inflation)
- **CES0500000002**: Average Weekly Hours (labor market tightness)

### Monthly Update Routine

```bash
# First Friday after 8:30 AM ET (jobs report release)
python scripts/bls/update_ce_latest.py --data-types 01 --seasonal S --start-year 2024  # ~9 requests
```

**API Usage:**
- Employment data only: ~9 requests/month = 108 requests/year ✅
- Well within 500/day limit

---

## BLS PC Data Collection (Producer Price Index - Industry)

The **Producer Price Index - Industry (PC)** survey measures changes in prices received by domestic producers for their output, organized by industry (NAICS-based). This is one of two PPI surveys:
- **PC (Producer Price Index - Industry)**: Organized by industry/producer
- **WP (Producer Price Index - Commodities)**: Organized by commodity/product

PC is critical for tracking wholesale inflation and is used to adjust many economic statistics for price changes.

### Database Tables

- `bls_pc_industries`: Industry codes and names (1,058 NAICS industries)
- `bls_pc_products`: Product codes within industries (4,746 products)
- `bls_pc_series`: Series metadata - 4,746 price index series
- `bls_pc_data`: Time series observations - 1,185,735 rows (1981-2025)

### Initial Setup

#### 1. Download Files

PC data files are available at: https://download.bls.gov/pub/time.series/pc/

**Automated Download (Recommended):**
```bash
# Download all PC files (79 files, ~149 MB)
python scripts/bls/download_bls_survey.py pc

# Dry run to preview files
python scripts/bls/download_bls_survey.py pc --dry-run
```

**Manual Download:**
Right-click and save:
- Main file: `pc.data.0.Current` (62.2 MB) - **contains all current data**
- Reference files: `pc.industry`, `pc.product`, `pc.series`, `pc.seasonal`, `pc.period`, `pc.footnote`
- Individual files: `pc.data.14.Chemicals`, `pc.data.19.Machinery`, etc. (optional - subsets of main file)

Save to: `data/bls/pc/`

#### 2. Load Initial Data

**Option 1: Load Everything (Recommended)**
```bash
# Loads industries, products, series metadata, and all time series data
python scripts/bls/load_pc_flat_files.py

# Loads 1,185,735 observations from pc.data.0.Current
```

**Option 2: Load Reference Tables Only**
```bash
# Load only metadata (industries, products, series) - for schema setup
python scripts/bls/load_pc_flat_files.py --skip-data
```

**Option 3: Load Specific Industries**
```bash
# Load chemicals and machinery industries only
python scripts/bls/load_pc_flat_files.py \
  --data-files pc.data.14.Chemicals,pc.data.19.Machinery \
  --skip-reference
```

**Loading Process:**
- Parses tab-delimited flat files
- UPSERTs to PostgreSQL using `ON CONFLICT DO UPDATE`
- Batch size: 10,000 rows (configurable with `--batch-size`)
- Creates indexes on industry_code, seasonal_code, year, period
- Sets `is_active` flag based on end_year/period

### Data Structure

**Series ID Format:**
```
PCU + industry_code + product_code
```

Examples:
- `PCU113310113310`: Logging industry (NAICS 113310), primary products, not seasonally adjusted
- `PCS311311`: Food manufacturing (NAICS 311), seasonally adjusted
- `PCU324324`: Petroleum and coal products (NAICS 324)

**Key Industry Codes (NAICS):**
- `113310`: Logging
- `211`: Oil and gas extraction
- `212`: Mining (except oil and gas)
- `311`: Food manufacturing
- `312`: Beverage and tobacco
- `321`: Wood product manufacturing
- `322`: Paper manufacturing
- `324`: Petroleum and coal products (critical for energy prices)
- `325`: Chemical manufacturing
- `331`: Primary metal manufacturing
- `333`: Machinery manufacturing
- `334`: Computer and electronic products
- `336`: Transportation equipment

**Product Codes:**
- Products are identified within each industry
- Example: Industry `113310` (Logging) has products like:
  - `113310`: Primary products
  - `113310M`: Miscellaneous receipts
  - `113310P`: Primary products index

**Base Date:**
- Each series has a base period (e.g., `198112` = December 1981)
- Index is set to 100.0 in the base period
- Values represent relative price changes from base

### Querying the Data

**Get all industries:**
```sql
SELECT * FROM bls_pc_industries ORDER BY industry_code;
```

**Get products for an industry:**
```sql
SELECT * FROM bls_pc_products
WHERE industry_code = '311'  -- Food manufacturing
ORDER BY product_code;
```

**Get recent price indexes for specific industry:**
```sql
SELECT
    s.series_title,
    d.year,
    d.period,
    d.value as price_index
FROM bls_pc_data d
JOIN bls_pc_series s ON d.series_id = s.series_id
WHERE s.industry_code = '324'  -- Petroleum and coal
  AND s.seasonal_code = 'S'    -- Seasonally adjusted
  AND d.year >= 2024
ORDER BY s.series_id, d.year DESC, d.period DESC;
```

**Calculate month-over-month price change:**
```sql
WITH monthly_data AS (
    SELECT
        series_id,
        year,
        period,
        value,
        LAG(value) OVER (PARTITION BY series_id ORDER BY year, period) as prev_value
    FROM bls_pc_data
    WHERE series_id = 'PCU324324'  -- Petroleum and coal
      AND year >= 2024
      AND period LIKE 'M%'
)
SELECT
    year,
    period,
    value as current_index,
    prev_value as previous_index,
    ROUND(((value - prev_value) / prev_value * 100)::numeric, 2) as pct_change
FROM monthly_data
WHERE prev_value IS NOT NULL
ORDER BY year DESC, period DESC;
```

**Top industries by price change:**
```sql
WITH recent_changes AS (
    SELECT
        s.industry_code,
        i.industry_name,
        d.series_id,
        s.series_title,
        d.value as latest_value,
        LAG(d.value, 12) OVER (PARTITION BY d.series_id ORDER BY d.year, d.period) as year_ago_value
    FROM bls_pc_data d
    JOIN bls_pc_series s ON d.series_id = s.series_id
    JOIN bls_pc_industries i ON s.industry_code = i.industry_code
    WHERE s.seasonal_code = 'S'
      AND d.year >= 2023
      AND d.period LIKE 'M%'
      AND s.product_code = s.industry_code  -- Primary product indexes only
)
SELECT
    industry_code,
    industry_name,
    latest_value,
    year_ago_value,
    ROUND(((latest_value - year_ago_value) / year_ago_value * 100)::numeric, 2) as yoy_pct_change
FROM recent_changes
WHERE year_ago_value IS NOT NULL
ORDER BY yoy_pct_change DESC NULLS LAST
LIMIT 20;
```

### Monthly Updates via API

BLS updates PC data around the **15th of each month** for the previous month's data.

**Recommended Update Strategy:**
```bash
# Update specific critical industries (recommended)
python scripts/bls/update_pc_latest.py \
  --industries 311,324,325,331 \
  --start-year 2024

# Update seasonally adjusted series only
python scripts/bls/update_pc_latest.py \
  --seasonal S \
  --start-year 2024

# Update all active PC series (2024 data only)
  python scripts/bls/update_pc_latest.py --start-year 2024

  API Usage:
  - ~95 API requests (4,746 active series ÷ 50 per request)
  - Still well within the 500/day limit (19% usage)
  - Takes approximately 15-20 minutes (respectful delays between requests)

  More Conservative Alternatives:

  # Update only unadjusted series (since most PC series are U, not S)
  python scripts/bls/update_pc_latest.py --seasonal U --start-year 2024
  # ~95 requests (most series are U anyway)

  # Update only key industries (recommended for routine updates)
  python scripts/bls/update_pc_latest.py \
    --industries 311,324,325,331,333,336 \
    --start-year 2024
  # ~10-15 requests

  # Test with limited series first
  python scripts/bls/update_pc_latest.py --limit 50 --start-year 2024
  # Just 1 request, to verify everything works

# Update specific series
python scripts/bls/update_pc_latest.py \
  --series-ids PCU324324,PCU311311,PCU325325

# Test with limited series
python scripts/bls/update_pc_latest.py --limit 10
```

**Update Options:**
- `--industries`: Comma-separated NAICS codes (supports partial matching)
- `--seasonal`: `S` (seasonally adjusted) or `U` (not adjusted)
- `--series-ids`: Specific series to update (bypasses filters)
- `--start-year`: Start year for data fetch (default: current year)
- `--end-year`: End year for data fetch (default: current year)
- `--limit`: Limit number of series (for testing)

**Key Industries to Watch:**
- **324**: Petroleum and coal products (energy prices)
- **311**: Food manufacturing (food inflation)
- **325**: Chemical manufacturing
- **331**: Primary metal manufacturing
- **333**: Machinery manufacturing

### Monthly Update Routine

```bash
# Around 15th of each month (after BLS release)
python scripts/bls/update_pc_latest.py --seasonal S --industries 311,324,325,331 --start-year 2024
```

**API Usage Estimation:**
- All active series: ~95 requests (4,746 series ÷ 50) - NOT recommended
- Seasonally adjusted only: ~48 requests
- 4 key industries: ~5-10 requests ✅
- Well within 500/day limit when filtered properly

**Release Schedule:**
- Published: ~15th of each month
- For: Previous month's data
- Example: August data published around September 15th

### SQL query

The industry codes use dashes for hierarchical grouping:
  - 324--- = Top-level petroleum and coal products
  - 3241-- = Petroleum and coal products mfg
  - 324110 = Specific petroleum refineries

PC petroleum series are NOT seasonally adjusted - they only have U (unadjusted) series,       
  not S (seasonally adjusted).

  Here's the corrected query:

  -- Recent price indexes for petroleum (NOT seasonally adjusted)
  SELECT s.series_title, d.year, d.period, d.value
  FROM bls_pc_data d
  JOIN bls_pc_series s ON d.series_id = s.series_id
  WHERE s.industry_code = '324110'  -- Petroleum refineries (note the format with trailing zeros)
    AND s.seasonal_code = 'U'        -- Changed from 'S' to 'U'
    AND d.year >= 2024
  ORDER BY d.year DESC, d.period DESC
  LIMIT 20;

  Or try these alternatives:

  -- Top-level petroleum and coal products
  SELECT s.series_title, d.year, d.period, d.value
  FROM bls_pc_data d
  JOIN bls_pc_series s ON d.series_id = s.series_id
  WHERE s.industry_code = '324---'  -- Note the dashes for hierarchy
    AND d.year >= 2024
  ORDER BY d.year DESC, d.period DESC;

  -- All petroleum-related series (using LIKE)
  SELECT s.series_title, d.year, d.period, d.value
  FROM bls_pc_data d
  JOIN bls_pc_series s ON d.series_id = s.series_id
  WHERE s.industry_code LIKE '324%'
    AND d.year >= 2024
    AND s.product_code = s.industry_code  -- Primary product indexes only
  ORDER BY s.industry_code, d.year DESC, d.period DESC
  LIMIT 20;

  Key findings:
  1. Industry codes use dashes for hierarchy: 324---, 3241--, 32411-, 324110
  2. PC survey petroleum series are NOT seasonally adjusted (U only, no S)
  3. Use industry_code LIKE '324%' to match all petroleum-related codes

 ---

## BLS WP Data Collection (Producer Price Index - Commodities)

  The **Producer Price Index - Commodities (WP)** survey measures changes in prices received by domestic
  producers, organized by commodity/product type (end use or material composition). This complements PC which    
   is organized by industry.

  **Key Difference:**
  - **WP**: Tracks commodities (what is produced) - e.g., gasoline, chemicals, lumber
  - **PC**: Tracks industries (who produces) - e.g., petroleum refining, chemical manufacturing

  ### Database Tables

  - `bls_wp_groups`: Commodity group codes (56 groups like Fuels, Chemicals, Metals)
  - `bls_wp_items`: Item codes within groups (4,168 commodities organized hierarchically)
  - `bls_wp_series`: Series metadata - 5,498 commodity price index series
  - `bls_wp_data`: Time series observations - 1,306,020 rows (1947-2025)

  ### Initial Setup

  #### 1. Download Files

  WP data files are available at: https://download.bls.gov/pub/time.series/wp/

  **Automated Download (Recommended):**
  ```bash
  # Download all WP files (43 files, ~166 MB)
  python scripts/bls/download_bls_survey.py wp
  
  # Dry run to preview files
  python scripts/bls/download_bls_survey.py wp --dry-run

  Manual Download:
  Right-click and save:
  - Main file: wp.data.0.Current (69 MB) - contains all current data
  - Reference files: wp.group, wp.item, wp.series, wp.seasonal, wp.period, wp.footnote
  - Individual files: wp.data.6.Fuels, wp.data.7.Chemicals, etc. (optional - subsets of main file)

  Save to: data/bls/wp/

  2. Load Initial Data

  Option 1: Load Everything (Recommended)
  # Loads groups, items, series metadata, and all time series data
  python scripts/bls/load_wp_flat_files.py

  # Loads 1,306,020 observations from wp.data.0.Current

  Option 2: Load Reference Tables Only
  # Load only metadata (groups, items, series) - for schema setup
  python scripts/bls/load_wp_flat_files.py --skip-data

  Option 3: Load Specific Commodity Groups
  # Load fuels and chemicals commodity groups only
  python scripts/bls/load_wp_flat_files.py \
    --data-files wp.data.6.Fuels,wp.data.7.Chemicals \
    --skip-reference

  Data Structure

  Series ID Format:
  WPU + group_code + item_code

  Examples:
  - WPU0571: Gasoline (Group 05=Fuels, Item 71=Gasoline)
  - WPS05: Fuels and related products, seasonally adjusted
  - WPU06: Chemicals, not seasonally adjusted

  Key Commodity Groups:
  - 01: Farm products
  - 02: Processed foods and feeds
  - 05: Fuels and related products (gasoline, oil, natural gas) - critical for energy inflation
  - 06: Chemicals and allied products
  - 07: Rubber and plastic products
  - 08: Lumber and wood products
  - 09: Pulp, paper, and allied products
  - 10: Metals and metal products
  - 11: Machinery and equipment
  - 12: Furniture and household durables
  - FD-ID: Final Demand-Intermediate Demand indexes

  Commodity Hierarchy:
  - 2-digit: Major commodity grouping (e.g., 05 = Fuels)
  - 3-digit: Subgroup (e.g., 057 = Gasoline)
  - 4-digit+: Product class, item groupings, individual items

  Querying the Data

  Get all commodity groups:
  SELECT * FROM bls_wp_groups ORDER BY group_code;

  Get items in a commodity group:
  SELECT * FROM bls_wp_items
  WHERE group_code = '05'  -- Fuels
  ORDER BY item_code;

  Corrected query for gasoline prices:
  SELECT
      s.series_title,
      d.year,
      d.period,
      d.value as price_index
  FROM bls_wp_data d
  JOIN bls_wp_series s ON d.series_id = s.series_id
  WHERE s.group_code = '05'  -- Fuels
    AND s.item_code LIKE '71%'  -- Gasoline (changed from 57%)
    AND d.year >= 2024
  ORDER BY s.series_id, d.year DESC, d.period DESC
  LIMIT 20;

  Specific gasoline series:
  -- General gasoline (seasonally adjusted)
  SELECT year, period, value
  FROM bls_wp_data
  WHERE series_id = 'WPS0571'  -- Gasoline, seasonally adjusted
    AND year >= 2024
  ORDER BY year DESC, period DESC;

  -- Unleaded regular gasoline (seasonally adjusted)
  SELECT year, period, value
  FROM bls_wp_data
  WHERE series_id = 'WPS057104'  -- Unleaded regular, seasonally adjusted
    AND year >= 2024
  ORDER BY year DESC, period DESC;

  -- Unleaded premium gasoline (seasonally adjusted)
  SELECT year, period, value
  FROM bls_wp_data
  WHERE series_id = 'WPS057103'  -- Unleaded premium, seasonally adjusted
    AND year >= 2024
  ORDER BY year DESC, period DESC;

  Key WP Series for Energy:
  - WPS0571: Gasoline (general)
  - WPS057104: Unleaded regular gasoline
  - WPS057103: Unleaded premium gasoline
  - WPS057105: Unleaded mid-premium gasoline
  - WPU057: Petroleum products, refined (not seasonally adjusted)

  Item Code Structure:
  - Group 05 = Fuels
  - Item 7 = Petroleum products, refined
  - Item 71 = Gasoline
  - Item 7104 = Unleaded regular gasoline

  Calculate month-over-month commodity price changes:
  WITH monthly_data AS (
      SELECT
          series_id,
          year,
          period,
          value,
          LAG(value) OVER (PARTITION BY series_id ORDER BY year, period) as prev_value
      FROM bls_wp_data
      WHERE series_id = 'WPU0571'  -- Gasoline
        AND year >= 2024
        AND period LIKE 'M%'
  )
  SELECT
      year,
      period,
      value as current_index,
      prev_value as previous_index,
      ROUND(((value - prev_value) / prev_value * 100)::numeric, 2) as pct_change
  FROM monthly_data
  WHERE prev_value IS NOT NULL
  ORDER BY year DESC, period DESC;

  Top commodities by price change:
  WITH recent_changes AS (
      SELECT
          s.group_code,
          g.group_name,
          s.item_code,
          i.item_name,
          d.value as latest_value,
          LAG(d.value, 12) OVER (PARTITION BY d.series_id ORDER BY d.year, d.period) as year_ago_value
      FROM bls_wp_data d
      JOIN bls_wp_series s ON d.series_id = s.series_id
      JOIN bls_wp_groups g ON s.group_code = g.group_code
      JOIN bls_wp_items i ON s.group_code = i.group_code AND s.item_code = i.item_code
      WHERE s.seasonal_code = 'U'
        AND d.year >= 2023
        AND d.period LIKE 'M%'
  )
  SELECT
      group_code,
      group_name,
      item_code,
      item_name,
      latest_value,
      year_ago_value,
      ROUND(((latest_value - year_ago_value) / year_ago_value * 100)::numeric, 2) as yoy_pct_change
  FROM recent_changes
  WHERE year_ago_value IS NOT NULL
  ORDER BY yoy_pct_change DESC NULLS LAST
  LIMIT 20;

  Monthly Updates via API

  BLS updates WP data around the 15th of each month for the previous month's data.

  Recommended Update Strategy:
  # Update specific critical commodity groups (recommended)
  python scripts/bls/update_wp_latest.py --groups 05,06,10  --start-year 2024

  # Update seasonally adjusted series only
  python scripts/bls/update_wp_latest.py \
    --seasonal S \
    --start-year 2024

  # Update specific series
  python scripts/bls/update_wp_latest.py \
    --series-ids WPU0571,WPU05,WPU06

  # Test with limited series
  python scripts/bls/update_wp_latest.py --limit 10

  Update Options:
  - --groups: Comma-separated commodity group codes
  - --seasonal: S (seasonally adjusted) or U (not adjusted)
  - --series-ids: Specific series to update (bypasses filters)
  - --start-year: Start year for data fetch (default: current year)
  - --end-year: End year for data fetch (default: current year)
  - --limit: Limit number of series (for testing)

  Key Commodity Groups to Watch:
  - 05: Fuels and related products (energy inflation, gasoline prices)
  - 02: Processed foods (food inflation)
  - 06: Chemicals
  - 10: Metals (industrial commodities)
  - 11: Machinery and equipment

  Monthly Update Routine

  # Around 15th of each month (after BLS release)
  python scripts/bls/update_wp_latest.py --groups 05,02,06,10 --start-year 2024

  API Usage Estimation:
  - All active series: ~110 requests (5,498 series ÷ 50) - NOT recommended
  - Seasonally adjusted only: 8 requests (360 series)
  - 4 key commodity groups: ~10-15 requests ✅
  - Well within 500/day limit when filtered properly

  Release Schedule:
  - Published: ~15th of each month
  - For: Previous month's data
  - Example: August data published around September 15th
  - Subject to 4-month revisions

  ---

  ## Summary

  **WP Survey Integration Complete!**

  ✅ **1,306,020 observations** loaded (1947-2025)
  ✅ **5,498 commodity price series**
  ✅ **4,168 items** across **56 commodity groups**
  ✅ Database, parser, loader, and API update scripts all created and tested
  ✅ Ready for production use
  '''
 ```
--------

## SM Survey Summary:
  - Survey: State and Metro Area Employment
  - Dataset: 10,026,035 observations (1990-2025)
  - Series: 24,167 active time series
  - Geography: 50 states + DC + 388 metro areas
  - Industries: 807 industry codes across 20 supersectors
  - Data Types: Employment levels (01), employment changes (03)
  - Seasonal Adjustment: Both S (seasonally adjusted) and U (not adjusted)

  Key Scripts:
  Initial load (already done)
  python scripts/bls/load_sm_flat_files.py

  Monthly updates (recommended with filters to avoid hitting 500 request limit)
  python scripts/bls/update_sm_latest.py --states 06,36,48 --start-year 2024
  python scripts/bls/update_sm_latest.py --areas 35620,31080 --start-year 2024
  python scripts/bls/update_sm_latest.py --seasonal S --limit 100

  Example Queries:
  -- California total nonfarm employment (seasonally adjusted)
  SELECT s.series_id, s.state_code, st.state_name, ss.supersector_name,
         d.year, d.period, d.value
  FROM bls_sm_series s
  JOIN bls_sm_states st ON s.state_code = st.state_code
  JOIN bls_sm_supersectors ss ON s.supersector_code = ss.supersector_code
  JOIN bls_sm_data d ON s.series_id = d.series_id
  WHERE s.state_code = '06'
    AND s.supersector_code = '00'
    AND s.data_type_code = '01'
    AND s.seasonal_code = 'S'
    AND d.year >= 2024
  ORDER BY d.year DESC, d.period DESC;

   -- New York metro area manufacturing employment
  SELECT s.series_id, a.area_name, i.industry_name,
         d.year, d.period, d.value
  FROM bls_sm_series s
  JOIN bls_sm_areas a ON s.area_code = a.area_code
  JOIN bls_sm_industries i ON s.industry_code = i.industry_code
  JOIN bls_sm_data d ON s.series_id = d.series_id
  WHERE s.area_code = '35620'
    AND s.supersector_code = '30'
    AND s.seasonal_code = 'U'  -- Changed from 'S' to 'U'
    AND d.year >= 2024
  ORDER BY d.year DESC, d.period DESC;

  State Codes (examples):
  - 06 = California, 36 = New York, 48 = Texas, 12 = Florida, 17 = Illinois

  Metro Area Codes (examples):
  - 35620 = New York-Newark-Jersey City
  - 31080 = Los Angeles-Long Beach-Anaheim
  - 16980 = Chicago-Naperville-Elgin
  - 19100 = Dallas-Fort Worth-Arlington

  Scripts

  Initial Data Load (one-time, already completed):
### Load all historical data (10M observations, ~5-10 minutes)
  python scripts/bls/load_sm_flat_files.py

### Load only reference tables
  python scripts/bls/load_sm_flat_files.py --skip-data

### Load specific data files
  python scripts/bls/load_sm_flat_files.py --data-files sm.data.33a.NewYork,sm.data.5a.California

  Monthly API Updates (recommended approach):
### Update specific states (RECOMMENDED - avoids hitting 500 request limit)
  python scripts/bls/update_sm_latest.py --states 06,36,48 --start-year 2024

### Update specific metro areas
  python scripts/bls/update_sm_latest.py --areas 35620,31080 --start-year 2024

### Update seasonally adjusted series only
  python scripts/bls/update_sm_latest.py --seasonal S --start-year 2024

### Test with limited series
  python scripts/bls/update_sm_latest.py --limit 10

### WARNING: Updating all ~24K active series requires ~480 API requests!
### This exceeds the daily limit of 500. Use filters to narrow scope.

  Key State Codes

  - 06 = California
  - 36 = New York
  - 48 = Texas
  - 12 = Florida
  - 17 = Illinois
  - 42 = Pennsylvania
  - 39 = Ohio
  - 13 = Georgia
  - 37 = North Carolina
  - 26 = Michigan

  Key Metro Area Codes

  - 35620 = New York-Newark-Jersey City, NY-NJ-PA
  - 31080 = Los Angeles-Long Beach-Anaheim, CA
  - 16980 = Chicago-Naperville-Elgin, IL-IN-WI
  - 19100 = Dallas-Fort Worth-Arlington, TX
  - 26420 = Houston-The Woodlands-Sugar Land, TX
  - 47900 = Washington-Arlington-Alexandria, DC-VA-MD-WV
  - 33100 = Miami-Fort Lauderdale-West Palm Beach, FL
  - 37980 = Philadelphia-Camden-Wilmington, PA-NJ-DE-MD
  - 12060 = Atlanta-Sandy Springs-Roswell, GA
  - 14460 = Boston-Cambridge-Newton, MA-NH

  Supersector Codes

  - 00 = Total Nonfarm
  - 05 = Total Private
  - 10 = Mining and Logging
  - 20 = Construction
  - 30 = Manufacturing
  - 31 = Durable Goods
  - 32 = Non-Durable Goods
  - 40 = Trade, Transportation, and Utilities
  - 50 = Information
  - 55 = Financial Activities
  - 60 = Professional and Business Services
  - 65 = Education and Health Services
  - 70 = Leisure and Hospitality
  - 80 = Other Services
  - 90 = Government

  Example SQL Queries

  California total nonfarm employment (seasonally adjusted):
  SELECT s.series_id, s.state_code, st.state_name, ss.supersector_name,
         d.year, d.period, d.value
  FROM bls_sm_series s
  JOIN bls_sm_states st ON s.state_code = st.state_code
  JOIN bls_sm_supersectors ss ON s.supersector_code = ss.supersector_code
  JOIN bls_sm_data d ON s.series_id = d.series_id
  WHERE s.state_code = '06'
    AND s.supersector_code = '00'
    AND s.data_type_code = '01'
    AND s.seasonal_code = 'S'
    AND d.year >= 2024
  ORDER BY d.year DESC, d.period DESC;

  New York metro area manufacturing employment (NOT seasonally adjusted):
  -- NOTE: Most metro area series only available as 'U' (not seasonally adjusted)
  SELECT s.series_id, a.area_name, i.industry_name,
         d.year, d.period, d.value
  FROM bls_sm_series s
  JOIN bls_sm_areas a ON s.area_code = a.area_code
  JOIN bls_sm_industries i ON s.industry_code = i.industry_code
  JOIN bls_sm_data d ON s.series_id = d.series_id
  WHERE s.area_code = '35620'
    AND s.supersector_code = '30'
    AND s.seasonal_code = 'U'
    AND d.year >= 2024
  ORDER BY d.year DESC, d.period DESC;

  Compare employment across major metro areas:
  SELECT a.area_name, ss.supersector_name,
         d.year, d.period, d.value
  FROM bls_sm_series s
  JOIN bls_sm_areas a ON s.area_code = a.area_code
  JOIN bls_sm_supersectors ss ON s.supersector_code = ss.supersector_code
  JOIN bls_sm_data d ON s.series_id = d.series_id
  WHERE s.area_code IN ('35620', '31080', '16980')  -- NYC, LA, Chicago
    AND s.supersector_code = '00'  -- Total nonfarm
    AND s.data_type_code = '01'
    AND s.seasonal_code = 'U'
    AND d.year = 2024
    AND d.period = 'M08'
  ORDER BY a.area_name;

  Important Notes

  - Most metropolitan area series are only available as 'U' (not seasonally adjusted)
  - Statewide series typically have both 'S' and 'U' versions
  - Employment values are in thousands (e.g., 15234.5 = 15,234,500 employees)
  - Series ID format: SMU[state][area][supersector][industry][datatype][seasonal]
  - Example: SMU36356203000000001 = NY state, NYC metro, manufacturing, employment level, not adjusted

  ---
### meta data
  For SM (when we ran load_sm_flat_files.py):
  - ✓ bls_sm_states - 51 states/territories
  - ✓ bls_sm_areas - 388 metro areas
  - ✓ bls_sm_supersectors - 22 supersectors
  - ✓ bls_sm_industries - 807 industries
  - ✓ bls_sm_series - 24,167 series metadata
  - ✓ bls_sm_data - 10,026,035 observations

  For PC (Producer Price Index - Industry):
  - ✓ bls_pc_industries
  - ✓ bls_pc_products
  - ✓ bls_pc_series
  - ✓ bls_pc_data - 1,185,735 observations

  For WP (Producer Price Index - Commodities):
  - ✓ bls_wp_groups
  - ✓ bls_wp_items
  - ✓ bls_wp_series
  - ✓ bls_wp_data - 1,306,020 observations

## JT Survey

 ---
  BLS JT Survey - Job Openings and Labor Turnover (JOLTS)

  Survey: Job Openings and Labor Turnover Survey (JOLTS)
  Dataset: 609,633 observations (2000-2025)
  Series: 1,984 time series
  Industries: 28 industry classifications
  States: 56 states/territories
  Size Classes: 7 establishment size categories

  Database Tables

  - bls_jt_dataelements - Data element types (8: JO, HI, TS, QU, LD, OS, UO, UN)
  - bls_jt_industries - Industry classifications (28 industries)
  - bls_jt_states - State codes (56 states/territories)
  - bls_jt_areas - Geographic areas
  - bls_jt_sizeclasses - Establishment sizes (7 classes)
  - bls_jt_ratelevels - Rate vs Level indicator (R=Rate, L=Level)
  - bls_jt_series - Time series metadata (1,984 series)
  - bls_jt_data - Time series observations (609K+ data points)

  Scripts

  Initial Data Load (one-time, already completed):
  ### Load all historical data (609K observations, ~2-3 minutes)
  python scripts/bls/load_jt_flat_files.py

  ### Load only reference tables
  python scripts/bls/load_jt_flat_files.py --skip-data

  ### Load specific data element files
  python scripts/bls/load_jt_flat_files.py --data-files jt.data.2.JobOpenings,jt.data.3.Hires

  Monthly API Updates (recommended approach):
  ### Update job openings and hires only (RECOMMENDED)
  python scripts/bls/update_jt_latest.py --elements JO,HI --start-year 2024

  ### Update specific industries
  python scripts/bls/update_jt_latest.py --industries 000000,100000 --start-year 2024

  ### Update level series only (not rates)
  python scripts/bls/update_jt_latest.py --ratelevel L --start-year 2024

  ### Test with limited series
  python scripts/bls/update_jt_latest.py --limit 10

  Data Element Codes

  - JO = Job openings (unfilled positions)
  - HI = Hires (new employees)
  - TS = Total separations
  - QU = Quits (voluntary separations)
  - LD = Layoffs and discharges (involuntary)
  - OS = Other separations
  - UO = Unemployed persons per job opening ratio
  - UN = Unemployment rate (not selectable)

  Key Industry Codes

  - 000000 = Total nonfarm
  - 100000 = Total private
  - 300000 = Manufacturing
  - 400000 = Trade, transportation, and utilities
  - 500000 = Information
  - 600000 = Financial activities
  - 700000 = Professional and business services
  - 800000 = Education and health services
  - 900000 = Leisure and hospitality

  Size Class Codes

  - 00 = All size classes
  - 01 = 1 to 9 employees
  - 02 = 10 to 49 employees
  - 03 = 50 to 249 employees
  - 04 = 250 to 999 employees
  - 05 = 1,000 to 4,999 employees
  - 06 = 5,000 or more employees

  Rate vs Level

  - R = Rate (per 100 employees)
  - L = Level (in thousands of employees)

  Example SQL Queries

  Total nonfarm job openings (level, seasonally adjusted):
  SELECT s.series_id, de.dataelement_text, rl.ratelevel_text,
         d.year, d.period, d.value
  FROM bls_jt_series s
  JOIN bls_jt_dataelements de ON s.dataelement_code = de.dataelement_code
  JOIN bls_jt_ratelevels rl ON s.ratelevel_code = rl.ratelevel_code
  JOIN bls_jt_data d ON s.series_id = d.series_id
  WHERE s.industry_code = '000000'
    AND s.dataelement_code = 'JO'
    AND s.ratelevel_code = 'L'
    AND s.seasonal = 'S'
    AND d.year >= 2024
  ORDER BY d.year DESC, d.period DESC;

  Compare hires vs quits (total nonfarm, seasonally adjusted):
  SELECT de.dataelement_text,
         d.year, d.period, d.value
  FROM bls_jt_series s
  JOIN bls_jt_dataelements de ON s.dataelement_code = de.dataelement_code
  JOIN bls_jt_data d ON s.series_id = d.series_id
  WHERE s.industry_code = '000000'
    AND s.dataelement_code IN ('HI', 'QU')
    AND s.ratelevel_code = 'L'
    AND s.seasonal = 'S'
    AND d.year = 2024
  ORDER BY de.dataelement_text, d.year DESC, d.period DESC;

  Job openings by establishment size (latest month):
  SELECT sc.sizeclass_text, d.year, d.period, d.value
  FROM bls_jt_series s
  JOIN bls_jt_sizeclasses sc ON s.sizeclass_code = sc.sizeclass_code
  JOIN bls_jt_data d ON s.series_id = d.series_id
  WHERE s.industry_code = '000000'
    AND s.dataelement_code = 'JO'
    AND s.ratelevel_code = 'L'
    AND s.seasonal = 'S'
    AND d.year = 2025
    AND d.period = 'M08'
  ORDER BY sc.sort_sequence;

  Layoffs vs quits ratio (total private, rates):
  SELECT de.dataelement_text,
         d.year, d.period, d.value
  FROM bls_jt_series s
  JOIN bls_jt_dataelements de ON s.dataelement_code = de.dataelement_code
  JOIN bls_jt_data d ON s.series_id = d.series_id
  WHERE s.industry_code = '100000'
    AND s.dataelement_code IN ('LD', 'QU')
    AND s.ratelevel_code = 'R'
    AND s.seasonal = 'S'
    AND d.year >= 2024
  ORDER BY de.dataelement_text, d.year DESC, d.period DESC;

  Important Notes

  - Level (L) values are in thousands (e.g., 8000.0 = 8,000,000 job openings)
  - Rate (R) values are per 100 employees (e.g., 5.5 = 5.5 job openings per 100 employees)
  - Most commonly used: Seasonally adjusted (S) level (L) series for total nonfarm (000000)
  - JOLTS data begins in December 2000 (2000-M12)
  - Series ID format: JTS[industry][state][area][size][element][rate/level]
  - Example: JTS000000000000000JOL = Total nonfarm, all states, all areas, all sizes, job openings, level,       
  seasonally adjusted

  ---

## EC Survey Employment Cost Index, Legacy Survey

---
  BLS EC Survey - Employment Cost Index

  Survey: Employment Cost Index (ECI)
  Dataset: 77,459 observations (1982-2025)
  Series: 851 time series
  Compensation Types: 3 (Total compensation, Wages/salaries, Benefits)
  Groups: 110 industry/occupation groups
  Ownership: 3 types (Civilian, Private industry, State/local government)

  Database Tables

  - bls_ec_compensations - Compensation types (3: Total, Wages, Benefits)
  - bls_ec_groups - Industry/occupation groups (110 groups)
  - bls_ec_ownerships - Ownership types (3: Civilian, Private, Government)
  - bls_ec_periodicities - Data types (3: Index, 3-month %, 12-month %)
  - bls_ec_series - Time series metadata (851 series)
  - bls_ec_data - Time series observations (77K+ data points)

  Scripts

  Initial Data Load (one-time, already completed):
  ### Load all historical data (77K observations, ~1 minute)
  python scripts/bls/load_ec_flat_files.py

  ### Load only reference tables
  python scripts/bls/load_ec_flat_files.py --skip-data

  Quarterly API Updates (recommended approach):
  ### Update all active series (~851 series = ~17 requests)
  python scripts/bls/update_ec_latest.py --start-year 2024

  ### Update total compensation only
  python scripts/bls/update_ec_latest.py --comp 1 --start-year 2024

  ### Update private industry only
  python scripts/bls/update_ec_latest.py --ownership 2 --start-year 2024

  ### Update index series only (not percent changes)
  python scripts/bls/update_ec_latest.py --periodicity I --start-year 2024

  ### Test with limited series
  python scripts/bls/update_ec_latest.py --limit 10

  Compensation Type Codes

  - 1 = Total compensation
  - 2 = Wages and salaries
  - 3 = Benefits

  Ownership Type Codes

  - 1 = Civilian (all workers)
  - 2 = Private industry
  - 3 = State and local government

  Periodicity Codes

  - I = Index number (base period = 100)
  - Q = 3 month percent change
  - A = 12 month percent change

  Key Group Codes (examples)

  - 000 = All workers
  - 101 = Production and non-supervisory occupations
  - 200 = Goods-producing industries
  - 300 = Service-providing industries
  - 400 = Manufacturing
  - 500 = Trade, transportation, and utilities
  - 600 = Professional and business services
  - 700 = Education and health services
  - 800 = Leisure and hospitality

  Example SQL Queries

  Total compensation index for all civilian workers:
  SELECT s.series_id, c.comp_text, g.group_name, o.ownership_name, p.periodicity_text,
         d.year, d.period, d.value
  FROM bls_ec_series s
  JOIN bls_ec_compensations c ON s.comp_code = c.comp_code
  JOIN bls_ec_groups g ON s.group_code = g.group_code
  JOIN bls_ec_ownerships o ON s.ownership_code = o.ownership_code
  JOIN bls_ec_periodicities p ON s.periodicity_code = p.periodicity_code
  JOIN bls_ec_data d ON s.series_id = d.series_id
  WHERE s.comp_code = '1'
    AND s.group_code = '000'
    AND s.ownership_code = '1'
    AND s.periodicity_code = 'I'
    AND d.year >= 2024
  ORDER BY d.year DESC, d.period DESC;

  12-month percent change in wages for private industry:
  SELECT g.group_name, d.year, d.period, d.value AS percent_change_12mo
  FROM bls_ec_series s
  JOIN bls_ec_groups g ON s.group_code = g.group_code
  JOIN bls_ec_data d ON s.series_id = d.series_id
  WHERE s.comp_code = '2'
    AND s.ownership_code = '2'
    AND s.periodicity_code = 'A'
    AND d.year >= 2023
  ORDER BY g.group_name, d.year DESC, d.period DESC;

  Compare compensation costs across ownership types:
  SELECT o.ownership_name, p.periodicity_text,
         d.year, d.period, d.value
  FROM bls_ec_series s
  JOIN bls_ec_ownerships o ON s.ownership_code = o.ownership_code
  JOIN bls_ec_periodicities p ON s.periodicity_code = p.periodicity_code
  JOIN bls_ec_data d ON s.series_id = d.series_id
  WHERE s.comp_code = '1'
    AND s.group_code = '000'
    AND s.periodicity_code = 'I'
    AND d.year = 2024
    AND d.period = 'Q02'
  ORDER BY o.ownership_name;

  Important Notes

  - Data is quarterly (Q01-Q04)
  - Index base period = 100 (typically December 2005 = 100)
  - Employment costs include wages, salaries, and employer costs for benefits
  - Private industry excludes farm and household workers
  - State/local government data available from 1982
  - Series typically begin in 1982 Q01
  - EC data released quarterly with a delay (typically ~1-2 months after quarter end)

  ---

## NC Survey National Compensation Survey, Lecacy Survey.

## OE Occupational Employment and Wage Statistics 

The oe.data.1.AllData file only contains 2024 data.

  This is different from other BLS surveys. For OE (OEWS):
  - oe.data.0.Current = Current year data (2024)
  - oe.data.1.AllData = All series for current year (also 2024)

  The "AllData" filename is misleading - it means "all series" (all 6M+ series), not "all years".

  Why OE is different:

  OE publishes annual snapshots, not cumulative historical files:
  - Each year, BLS publishes a new dataset with that year's May data
  - Historical data requires downloading multiple yearly files from the BLS website

  What this means for you:

  1. Your flat file data is already current (2024 = latest available)
  2. No API update needed - the flat files have the newest data
  3. For historical data, you would need to:
    - Download older yearly files from BLS (if available)
    - Or use the API to backfill specific series you care about

  Recommendation:

  Since you already have the latest 2024 data, you don't need to run the update script unless you want to:       
  - Backfill historical years for specific series (use filters!)
  - Update next year when 2025 data is published

  1. Query the data:
  SELECT * FROM bls_oe_series LIMIT 10;
  SELECT * FROM bls_oe_data WHERE year >= 2023 LIMIT 10;
  2. Update with latest data via API:
  python scripts/bls/update_oe_latest.py --start-year 2023




## PR Survey, Major Sector Productivity and Cost


  What was integrated:
  - Database models: 6 tables (classes, measures, durations, sectors, series, data)
  - Reference tables loaded:
    - 2 worker classes (Employees, All workers)
    - 22 measures (productivity, unit labor costs, output, hours, compensation, etc.)
    - 3 duration types (year-over-year %, quarter-over-quarter %, index)
    - 6 sectors (Business, Nonfarm Business, Manufacturing, etc.)
    - 282 series
  - Data loaded: 75,896 observations (1947-2025)
  - Files created:
    - src/bls/pr_flat_file_parser.py
    - scripts/bls/load_pr_flat_files.py
    - scripts/bls/update_pr_latest.py
    - Alembic migration (b91fe07325c9)

  Usage:
### Load data
  python scripts/bls/load_pr_flat_files.py

### Update via API
  python scripts/bls/update_pr_latest.py --sectors 8400,8500

### Queries

Useful PR Queries:

  1. Overview - What sectors and measures are available?

  -- See all sectors
  SELECT sector_code, sector_name
  FROM bls_pr_sectors
  ORDER BY sort_sequence;

  -- See all measures
  SELECT measure_code, measure_text
  FROM bls_pr_measures
  ORDER BY sort_sequence;

  2. Labor Productivity - Business Sector (Most important)

  -- Business sector labor productivity index (2017=100)
  SELECT s.series_id, d.year, d.period, d.value
  FROM bls_pr_data d
  JOIN bls_pr_series s ON d.series_id = s.series_id
  WHERE s.sector_code = '8400'  -- Business
    AND s.measure_code = '09'    -- Labor productivity
    AND s.duration_code = '3'     -- Index
    AND d.year >= 2020
  ORDER BY d.year DESC, d.period DESC
  LIMIT 20;

  3. Compare Productivity Across Sectors (Latest Quarter)

  SELECT
      sec.sector_name,
      d.year,
      d.period,
      d.value as productivity_index
  FROM bls_pr_data d
  JOIN bls_pr_series s ON d.series_id = s.series_id
  JOIN bls_pr_sectors sec ON s.sector_code = sec.sector_code
  WHERE s.measure_code = '09'    -- Labor productivity
    AND s.duration_code = '3'     -- Index
    AND s.class_code = '6'        -- All workers
    AND (d.year, d.period) = (
        SELECT year, period
        FROM bls_pr_data
        ORDER BY year DESC, period DESC
        LIMIT 1
    )
  ORDER BY sec.sort_sequence;

  4. Unit Labor Costs Trend (Year-over-year % change)

  -- Business sector unit labor costs, year-over-year change
  SELECT
      d.year,
      d.period,
      d.value as pct_change_yoy
  FROM bls_pr_data d
  JOIN bls_pr_series s ON d.series_id = s.series_id
  WHERE s.sector_code = '8400'   -- Business
    AND s.measure_code = '11'     -- Unit labor costs
    AND s.duration_code = '1'     -- % change year-over-year
    AND d.year >= 2020
  ORDER BY d.year DESC, d.period DESC;

  5. Productivity vs Compensation (Business Sector)

  -- Compare productivity and hourly compensation growth
  SELECT
      d.year,
      d.period,
      m.measure_text,
      d.value
  FROM bls_pr_data d
  JOIN bls_pr_series s ON d.series_id = s.series_id
  JOIN bls_pr_measures m ON s.measure_code = m.measure_code
  WHERE s.sector_code = '8400'   -- Business
    AND s.measure_code IN ('09', '10')  -- Productivity & Hourly comp
    AND s.duration_code = '1'     -- % change year-over-year
    AND d.year >= 2023
  ORDER BY d.year DESC, d.period DESC, m.measure_code;

  6. Manufacturing Productivity Detail

  -- Manufacturing productivity by durable/nondurable
  SELECT
      sec.sector_name,
      d.year,
      d.period,
      d.value as productivity_pct_change
  FROM bls_pr_data d
  JOIN bls_pr_series s ON d.series_id = s.series_id
  JOIN bls_pr_sectors sec ON s.sector_code = sec.sector_code
  WHERE s.sector_code LIKE '3%'  -- Manufacturing sectors
    AND s.measure_code = '09'     -- Labor productivity
    AND s.duration_code = '1'     -- % change year-over-year
    AND d.year = 2024
  ORDER BY d.year DESC, d.period DESC, sec.sector_code;

  7. All Key Metrics for Business Sector (Dashboard Query)

  -- Latest quarter - all key metrics for business sector
  SELECT
      m.measure_text,
      dur.duration_text,
      d.value
  FROM bls_pr_data d
  JOIN bls_pr_series s ON d.series_id = s.series_id
  JOIN bls_pr_measures m ON s.measure_code = m.measure_code
  JOIN bls_pr_durations dur ON s.duration_code = dur.duration_code
  WHERE s.sector_code = '8400'   -- Business
    AND s.class_code = '6'        -- All workers
    AND (d.year, d.period) = (
        SELECT year, period
        FROM bls_pr_data
        ORDER BY year DESC, period DESC
        LIMIT 1
    )
  ORDER BY m.sort_sequence;

  These queries will help you explore productivity trends, labor costs, and sector comparisons!

## IP Survey Industry Productivity

 Summary

  Database Schema:
  - 8 tables created with proper foreign keys and indexes
  - Migration applied successfully (dbae27c5b3be)

  Data Loaded:
  - 21 sectors (NAICS-based)
  - 806 industries (detailed NAICS codes)
  - 38 measures (productivity, costs, output, hours, compensation)
  - 2 duration types (index vs percent change)
  - 6 data types (Index, Percent, Hours, Currency, etc.)
  - 56 areas (U.S. total + states)
  - 21,186 series
  - 735,471 observations (1987-2024)

  Files Created:
  - src/bls/ip_flat_file_parser.py - Parser for IP flat files
  - scripts/bls/load_ip_flat_files.py - Loader script
  - scripts/bls/update_ip_latest.py - API update script

  Important Notes for API Updates:
  - 21K+ active series = ~424 API requests (within 500/day limit if no filters)
  - Recommended filters for efficiency:
    - --areas 00000 (U.S. total only, reduces by 50+ states)
    - --measures 18,19 (labor productivity + unit labor costs only)
    - --sectors 31-33 (manufacturing only)

### Check sector and measures 

#### To see all available measure codes:
  SELECT measure_code, measure_text
  FROM bls_ip_measures
  ORDER BY measure_code;

####  -- Check what sector codes exist
  SELECT DISTINCT sector_code, COUNT(*) as num_series
  FROM bls_ip_series
  WHERE is_active = true
  GROUP BY sector_code
  ORDER BY sector_code;

  The sector code might be stored as:
  - Individual digits: 31, 32, 33 (not the range 31-33)
  - Letters: A, B, C, etc.
  - Or another format

  Once you see the actual sector codes, try:

#### If stored individually:
  python scripts/bls/update_ip_latest.py --sectors 31,32,33 --start-year 2024

#### Or check a specific series to see its sector code:

  SELECT series_id, sector_code, industry_code, measure_code
  FROM bls_ip_series
  WHERE is_active = true
  LIMIT 10;

SELECT
      i.industry_text,
      i.naics_code,
      m.measure_text,
      d.value,
      d.year
  FROM bls_ip_data d
  INNER JOIN bls_ip_series s ON d.series_id = s.series_id
  INNER JOIN bls_ip_industries i ON s.industry_code = i.industry_code
  INNER JOIN bls_ip_measures m ON s.measure_code = m.measure_code
  WHERE d.year = 2024
      AND s.area_code = '000000'  -- Changed to 6 zeros
      AND d.value IS NOT NULL
  ORDER BY i.industry_text, m.measure_text
  LIMIT 20;

### IP Industry Productivity Queries (Corrected)

  -- 1. Labor Productivity by Industry (2024)
  SELECT
      i.industry_text,
      i.naics_code,
      d.value as productivity_index,
      d.year
  FROM bls_ip_data d
  INNER JOIN bls_ip_series s ON d.series_id = s.series_id
  INNER JOIN bls_ip_industries i ON s.industry_code = i.industry_code
  WHERE d.year = 2024
      AND s.measure_code = 'L00'  -- Labor productivity
      AND s.area_code = '000000'
      AND d.value IS NOT NULL
  ORDER BY d.value DESC
  LIMIT 20;

  -- 2. Unit Labor Costs by Industry (2024)
  SELECT
      i.industry_text,
      i.naics_code,
      d.value as unit_labor_cost_index,
      d.year
  FROM bls_ip_data d
  INNER JOIN bls_ip_series s ON d.series_id = s.series_id
  INNER JOIN bls_ip_industries i ON s.industry_code = i.industry_code
  WHERE d.year = 2024
      AND s.measure_code = 'U10'  -- Unit labor costs
      AND s.area_code = '000000'
      AND d.value IS NOT NULL
  ORDER BY d.value DESC
  LIMIT 20;

  -- 3. Productivity Growth (2023 vs 2024)
  SELECT
      i.industry_text,
      i.naics_code,
      d2023.value as productivity_2023,
      d2024.value as productivity_2024,
      ROUND(((d2024.value - d2023.value) / NULLIF(d2023.value, 0) * 100)::numeric, 2) as pct_change
  FROM bls_ip_series s
  INNER JOIN bls_ip_industries i ON s.industry_code = i.industry_code
  INNER JOIN bls_ip_data d2023 ON s.series_id = d2023.series_id AND d2023.year = 2023
  INNER JOIN bls_ip_data d2024 ON s.series_id = d2024.series_id AND d2024.year = 2024
  WHERE s.measure_code = 'L00'
      AND s.area_code = '000000'
      AND d2023.value IS NOT NULL
      AND d2024.value IS NOT NULL
  ORDER BY pct_change DESC
  LIMIT 20;

  -- 4. Productivity vs Compensation Trends (2020-2024)
  SELECT
      d.year,
      i.industry_text,
      MAX(CASE WHEN s.measure_code = 'L00' THEN d.value END) as productivity,
      MAX(CASE WHEN s.measure_code = 'U12' THEN d.value END) as hourly_compensation,
      MAX(CASE WHEN s.measure_code = 'U10' THEN d.value END) as unit_labor_costs
  FROM bls_ip_data d
  INNER JOIN bls_ip_series s ON d.series_id = s.series_id
  INNER JOIN bls_ip_industries i ON s.industry_code = i.industry_code
  WHERE s.industry_code IN ('N311___', 'N3254__', 'N336___')  -- Food, Pharma, Transportation
      AND s.measure_code IN ('L00', 'U12', 'U10')
      AND s.area_code = '000000'
      AND d.year >= 2020
  GROUP BY d.year, i.industry_text
  ORDER BY i.industry_text, d.year;

  -- 5. Total Factor Productivity vs Labor Productivity
  SELECT
      d.year,
      i.industry_text,
      MAX(CASE WHEN s.measure_code = 'M00' THEN d.value END) as total_factor_productivity,
      MAX(CASE WHEN s.measure_code = 'L00' THEN d.value END) as labor_productivity,
      MAX(CASE WHEN s.measure_code = 'C00' THEN d.value END) as capital_productivity
  FROM bls_ip_data d
  INNER JOIN bls_ip_series s ON d.series_id = s.series_id
  INNER JOIN bls_ip_industries i ON s.industry_code = i.industry_code
  WHERE s.measure_code IN ('M00', 'L00', 'C00')
      AND s.area_code = '000000'
      AND d.year >= 2019
      AND i.industry_code LIKE 'N31%'  -- Manufacturing industries
  GROUP BY d.year, i.industry_text
  ORDER BY i.industry_text, d.year
  LIMIT 50;

  -- 6. Output and Hours Decomposition
  SELECT
      d.year,
      i.industry_text,
      MAX(CASE WHEN s.measure_code = 'T01' THEN d.value END) as output_index,
      MAX(CASE WHEN s.measure_code = 'L01' THEN d.value END) as hours_index,
      MAX(CASE WHEN s.measure_code = 'L00' THEN d.value END) as productivity_index,
      MAX(CASE WHEN s.measure_code = 'W01' THEN d.value END) as employment_index
  FROM bls_ip_data d
  INNER JOIN bls_ip_series s ON d.series_id = s.series_id
  INNER JOIN bls_ip_industries i ON s.industry_code = i.industry_code
  WHERE s.measure_code IN ('T01', 'L01', 'L00', 'W01')
      AND s.area_code = '000000'
      AND d.year >= 2020
      AND i.industry_code IN ('N311___', 'N3364__')  -- Food, Aerospace
  GROUP BY d.year, i.industry_text
  ORDER BY i.industry_text, d.year;

  -- 7. Long-term Productivity Growth (1987-2024)
  SELECT
      i.industry_text,
      i.naics_code,
      MIN(d.value) FILTER (WHERE d.year = 1987) as productivity_1987,
      MAX(d.value) FILTER (WHERE d.year = 2024) as productivity_2024,
      ROUND(((MAX(d.value) FILTER (WHERE d.year = 2024) - MIN(d.value) FILTER (WHERE d.year = 1987))
             / NULLIF(MIN(d.value) FILTER (WHERE d.year = 1987), 0) * 100)::numeric, 1) as total_growth_pct      
  FROM bls_ip_data d
  INNER JOIN bls_ip_series s ON d.series_id = s.series_id
  INNER JOIN bls_ip_industries i ON s.industry_code = i.industry_code
  WHERE s.measure_code = 'L00'
      AND s.area_code = '000000'
      AND d.year IN (1987, 2024)
  GROUP BY i.industry_text, i.naics_code
  HAVING COUNT(DISTINCT d.year) = 2
  ORDER BY total_growth_pct DESC
  LIMIT 20;

  -- 8. Real Hourly Compensation Trends
  SELECT
      d.year,
      i.industry_text,
      MAX(CASE WHEN s.measure_code = 'U14' THEN d.value END) as real_hourly_comp_dollars,
      MAX(CASE WHEN s.measure_code = 'U15' THEN d.value END) as real_hourly_comp_index
  FROM bls_ip_data d
  INNER JOIN bls_ip_series s ON d.series_id = s.series_id
  INNER JOIN bls_ip_industries i ON s.industry_code = i.industry_code
  WHERE s.measure_code IN ('U14', 'U15')
      AND s.area_code = '000000'
      AND d.year >= 2019
  GROUP BY d.year, i.industry_text
  ORDER BY i.industry_text, d.year
  LIMIT 50;

  -- 9. Industries with Highest/Lowest Unit Labor Cost Changes
  SELECT
      i.industry_text,
      d2020.value as ulc_2020,
      d2024.value as ulc_2024,
      ROUND(((d2024.value - d2020.value) / NULLIF(d2020.value, 0) * 100)::numeric, 2) as pct_change
  FROM bls_ip_series s
  INNER JOIN bls_ip_industries i ON s.industry_code = i.industry_code
  INNER JOIN bls_ip_data d2020 ON s.series_id = d2020.series_id AND d2020.year = 2020
  INNER JOIN bls_ip_data d2024 ON s.series_id = d2024.series_id AND d2024.year = 2024
  WHERE s.measure_code = 'U10'
      AND s.area_code = '000000'
      AND d2020.value IS NOT NULL
      AND d2024.value IS NOT NULL
  ORDER BY pct_change DESC
  LIMIT 30;

  -- 10. Capital vs Labor Contribution to Productivity
  SELECT
      d.year,
      i.industry_text,
      MAX(CASE WHEN s.measure_code = 'L00' THEN d.value END) as labor_productivity,
      MAX(CASE WHEN s.measure_code = 'C06' THEN d.value END) as capital_intensity,
      MAX(CASE WHEN s.measure_code = 'C07' THEN d.value END) as capital_contribution
  FROM bls_ip_data d
  INNER JOIN bls_ip_series s ON d.series_id = s.series_id
  INNER JOIN bls_ip_industries i ON s.industry_code = i.industry_code
  WHERE s.measure_code IN ('L00', 'C06', 'C07')
      AND s.area_code = '000000'
      AND d.year >= 2019
  GROUP BY d.year, i.industry_text
  ORDER BY i.industry_text, d.year
  LIMIT 50;

  These queries should now work! The key corrections were:
  - area_code = '000000' (6 zeros)
  - Using actual measure codes like 'L00', 'U10', 'U12', etc.


## TU American Time Use Survey - for behavioral and productivity research, not updated for latest

Summary

  ✅ PR (Major Sector Productivity and Costs)

  - 6 tables (2 worker classes, 22 measures, 3 durations, 6 sectors, 282 series)
  - 75,896 observations (1947-2025, quarterly)

  ✅ IP (Industry Productivity)

  - 8 tables (21 sectors, 806 industries, 38 measures, 56 areas, 21,186 series)
  - 735,471 observations (1987-2024, annual)

  ✅ TU (American Time Use Survey)

  - 16 tables (13 dimension tables + series + data + aspect)
  - 87,387 series with 127 activity codes
  - 1,911,619 observations (2003-2024, annual)
  - 1,911,619 aspect observations (standard errors)

  ---
  Files Created

  TU Integration:
  - src/database/bls_models.py - Added 16 TU models
  - alembic/versions/44c313f1d260_add_bls_tu_american_time_use_survey_.py - Migration
  - src/bls/tu_flat_file_parser.py - Parser for TU flat files
  - scripts/bls/load_tu_flat_files.py - Loader script
  - scripts/bls/update_tu_latest.py - API update script

  ---
  Important Notes for API Updates

  TU has 87K+ active series = 1,748+ API requests, which exceeds the 500/day limit. Must use filters:
  - --activities (specific activities like sleeping, working)
  - --stattypes (10101 for avg hours/day)
  - --sex, --ages, --races, --education, --lfstat (demographic filters)

  Current Behavior

  When you hit the 500 request/day limit, the BLS API will return an error and the script will stop. The good    
   news is:

  1. All updates are UPSERTED - The scripts use PostgreSQL's ON CONFLICT DO UPDATE, so:
    - Already-updated data is safely committed
    - Re-running the script won't create duplicates
    - You can safely continue where you left off
  2. Safe to re-run - Just run the same command the next day and it will:
    - Update any series that were missed
    - Update existing data if values changed
    - Skip series that are already current

  Recommended Strategies

### Option 1: Use Filters (Best)

  Stay under the 500 request limit by filtering:

#### Day 1: Update sleeping & work activities
  python scripts/bls/update_tu_latest.py --activities 010100,050100 --start-year 2024

#### Day 2: Update eating & socializing
  python scripts/bls/update_tu_latest.py --activities 110100,120100 --start-year 2024

#### Day 3: Update by demographics
  python scripts/bls/update_tu_latest.py --sex 1 --start-year 2024  # Men only

#### Day 4:
  python scripts/bls/update_tu_latest.py --sex 2 --start-year 2024  # Women only

### Option 2: Batch by Series IDs

  Split series into chunks:

#### Get first 10,000 series IDs (200 requests)
  python scripts/bls/update_tu_latest.py --limit 10000

#### Next day, manually specify next batch
  (would require script modification to track progress)

###Option 3: Use --limit for Testing

#### Test with small batch first
  python scripts/bls/update_tu_latest.py --limit 500  # 10 requests only

  Check Your Request Count

  The scripts tell you how many requests are needed:
  API requests needed: ~1,748 (87,387 series ÷ 50 per request)
  ⚠️  WARNING: 1,748 requests exceeds daily limit of 500!

###Best Practice for Large Updates

  For surveys with many series (TU, OE), use a phased approach:

#### Week 1: Most important activities
  --activities 000000,010100,050100  # Total, Sleep, Work

#### Week 2: Household activities
  --activities 020100,030100,040100  # Housework, Childcare, Adult care

#### Week 3: Leisure
  --activities 120100,130100  # Socializing, Sports

   etc.

  The UPSERT design means you can safely stop/restart without losing data or creating duplicates!

### Queries

 1. Average Hours Per Day by Major Activity (Latest Year)

  -- How Americans spend their time (2024)
  SELECT
      a.actcode_text as activity,
      d.value as avg_hours_per_day,
      d.year
  FROM bls_tu_data d
  INNER JOIN bls_tu_series s ON d.series_id = s.series_id
  INNER JOIN bls_tu_actcodes a ON s.actcode_code = a.actcode_code
  INNER JOIN bls_tu_stattypes st ON s.stattype_code = st.stattype_code
  WHERE d.year = 2024
      AND st.stattype_code = '10101'  -- Average hours per day
      AND s.sex_code = '0'  -- Both sexes
      AND a.actcode_code IN ('010100', '050100', '110100', '120100', '130100', '020100')
      -- Sleep, Work, Eating, Socializing, Sports, Housework
  ORDER BY d.value DESC;

  2. Time Use by Sex (Gender Comparison)

  -- Compare how men and women spend time
  SELECT
      d.year,
      sex.sex_text,
      a.actcode_text as activity,
      d.value as avg_hours_per_day
  FROM bls_tu_data d
  INNER JOIN bls_tu_series s ON d.series_id = s.series_id
  INNER JOIN bls_tu_actcodes a ON s.actcode_code = a.actcode_code
  INNER JOIN bls_tu_sex sex ON s.sex_code = sex.sex_code
  INNER JOIN bls_tu_stattypes st ON s.stattype_code = st.stattype_code
  WHERE d.year >= 2020
      AND st.stattype_code = '10101'
      AND s.sex_code IN ('1', '2')  -- Men and Women
      AND a.actcode_code IN ('030100', '020100', '050100')  -- Childcare, Housework, Work
  ORDER BY a.actcode_text, d.year, sex.sex_text;

  3. Time Trends Over Years (Sleep and Work)

  -- How sleep and work hours have changed over time
  SELECT
      d.year,
      a.actcode_text,
      AVG(d.value) as avg_hours_per_day
  FROM bls_tu_data d
  INNER JOIN bls_tu_series s ON d.series_id = s.series_id
  INNER JOIN bls_tu_actcodes a ON s.actcode_code = a.actcode_code
  WHERE s.stattype_code = '10101'
      AND s.sex_code = '0'
      AND a.actcode_code IN ('010100', '050100')  -- Sleep, Work
  GROUP BY d.year, a.actcode_text
  ORDER BY a.actcode_text, d.year;

  4. Work-Life Balance by Age Group

  -- Time spent working vs leisure by age
  SELECT
      age.age_text,
      a.actcode_text as activity,
      d.value as avg_hours_per_day,
      d.year
  FROM bls_tu_data d
  INNER JOIN bls_tu_series s ON d.series_id = s.series_id
  INNER JOIN bls_tu_ages age ON s.age_code = age.age_code
  INNER JOIN bls_tu_actcodes a ON s.actcode_code = a.actcode_code
  WHERE d.year = 2024
      AND s.stattype_code = '10101'
      AND s.sex_code = '0'
      AND s.age_code != '000'  -- Exclude "All ages"
      AND a.actcode_code IN ('050100', '120100', '130100')  -- Work, Socializing, Sports
  ORDER BY age.age_text, a.actcode_text;

  5. Employed vs Unemployed Time Use

  -- How employed and unemployed people spend time differently
  SELECT
      lf.lfstat_text as labor_force_status,
      a.actcode_text as activity,
      d.value as avg_hours_per_day
  FROM bls_tu_data d
  INNER JOIN bls_tu_series s ON d.series_id = s.series_id
  INNER JOIN bls_tu_labor_force_status lf ON s.lfstat_code = lf.lfstat_code
  INNER JOIN bls_tu_actcodes a ON s.actcode_code = a.actcode_code
  WHERE d.year = 2024
      AND s.stattype_code = '10101'
      AND s.sex_code = '0'
      AND s.lfstat_code IN ('01', '02')  -- Employed, Unemployed
      AND a.actcode_code IN ('050100', '060100', '110100', '120100')
      -- Work, Education, Eating, Socializing
  ORDER BY lf.lfstat_text, d.value DESC;

  6. Childcare by Marital Status

  -- Time spent on childcare by marital status
  SELECT
      ms.maritlstat_text as marital_status,
      d.year,
      d.value as avg_hours_per_day_childcare
  FROM bls_tu_data d
  INNER JOIN bls_tu_series s ON d.series_id = s.series_id
  INNER JOIN bls_tu_marital_status ms ON s.maritlstat_code = ms.maritlstat_code
  INNER JOIN bls_tu_actcodes a ON s.actcode_code = a.actcode_code
  WHERE s.stattype_code = '10101'
      AND s.sex_code = '0'
      AND s.maritlstat_code != '00'  -- Exclude N/A
      AND a.actcode_code = '030100'  -- Caring for household children
      AND d.year >= 2020
  ORDER BY d.year, ms.maritlstat_text;

  7. Participation Rates (Who Does Activities)

  -- What percentage of people participate in each activity
  SELECT
      a.actcode_text as activity,
      MAX(CASE WHEN st.stattype_code = '10100' THEN d.value END) as num_persons_thousands,
      MAX(CASE WHEN st.stattype_code = '20100' THEN d.value END) as num_participants_thousands,
      ROUND((MAX(CASE WHEN st.stattype_code = '20100' THEN d.value END) /
             NULLIF(MAX(CASE WHEN st.stattype_code = '10100' THEN d.value END), 0) * 100)::numeric, 1)
             as participation_rate_pct
  FROM bls_tu_data d
  INNER JOIN bls_tu_series s ON d.series_id = s.series_id
  INNER JOIN bls_tu_actcodes a ON s.actcode_code = a.actcode_code
  INNER JOIN bls_tu_stattypes st ON s.stattype_code = st.stattype_code
  WHERE d.year = 2024
      AND s.sex_code = '0'
      AND s.stattype_code IN ('10100', '20100')
      AND a.actcode_code IN ('050100', '020100', '130100', '140100', '150100')
      -- Work, Housework, Sports, Religious, Volunteer
  GROUP BY a.actcode_text
  ORDER BY participation_rate_pct DESC;

  8. Average Hours Among Participants Only

  -- For people who DO the activity, how long do they spend?
  SELECT
      a.actcode_text as activity,
      MAX(CASE WHEN st.stattype_code = '10101' THEN d.value END) as avg_hours_all_persons,
      MAX(CASE WHEN st.stattype_code = '20101' THEN d.value END) as avg_hours_participants_only,
      d.year
  FROM bls_tu_data d
  INNER JOIN bls_tu_series s ON d.series_id = s.series_id
  INNER JOIN bls_tu_actcodes a ON s.actcode_code = a.actcode_code
  INNER JOIN bls_tu_stattypes st ON s.stattype_code = st.stattype_code
  WHERE d.year = 2024
      AND s.sex_code = '0'
      AND s.stattype_code IN ('10101', '20101')
      AND a.actcode_code IN ('130100', '140100', '150100', '060100')
      -- Sports, Religious, Volunteer, Education
  GROUP BY a.actcode_text, d.year
  ORDER BY avg_hours_participants_only DESC;

  9. Time Use by Education Level

  -- How education affects time allocation
  SELECT
      ed.educ_text as education_level,
      a.actcode_text as activity,
      d.value as avg_hours_per_day
  FROM bls_tu_data d
  INNER JOIN bls_tu_series s ON d.series_id = s.series_id
  INNER JOIN bls_tu_education ed ON s.educ_code = ed.educ_code
  INNER JOIN bls_tu_actcodes a ON s.actcode_code = a.actcode_code
  WHERE d.year = 2024
      AND s.stattype_code = '10101'
      AND s.sex_code = '0'
      AND s.educ_code != '00'  -- Exclude "All education levels"
      AND a.actcode_code IN ('050100', '060100', '120100')  -- Work, Education, Socializing
  ORDER BY ed.educ_text, a.actcode_text;

  10. Data Coverage Summary

  -- Overview of TU data by activity
  SELECT
      a.actcode_code,
      a.actcode_text,
      COUNT(DISTINCT s.series_id) as num_series,
      MIN(d.year) as earliest_year,
      MAX(d.year) as latest_year,
      COUNT(*) as total_observations
  FROM bls_tu_series s
  INNER JOIN bls_tu_actcodes a ON s.actcode_code = a.actcode_code
  LEFT JOIN bls_tu_data d ON s.series_id = d.series_id
  WHERE s.is_active = true
      AND a.display_level = 2  -- Major categories only
  GROUP BY a.actcode_code, a.actcode_text
  ORDER BY num_series DESC
  LIMIT 20;

  These queries help you analyze:
  - How Americans spend their time
  - Gender differences in time use
  - Work-life balance across demographics
  - Participation rates in different activities
  - Time trends over 20+ years
  - Impact of education, age, employment status on time allocation

  TU has 37+ dimensions making it very complex! Let me help you find the simplest summary         
  series.

  Better Approach: Use Series Title

  The series titles often indicate summary vs detailed breakdowns:

  -- Find summary series by looking for simple titles
  SELECT
      s.series_id,
      s.series_title,
      a.actcode_text,
      d.value,
      d.year
  FROM bls_tu_data d
  INNER JOIN bls_tu_series s ON d.series_id = s.series_id
  INNER JOIN bls_tu_actcodes a ON s.actcode_code = a.actcode_code
  WHERE d.year = 2024
      AND s.stattype_code = '10101'  -- Average hours per day
      AND a.actcode_code = '010100'  -- Sleeping
      AND s.series_title LIKE '%Average hours per day%'
      AND s.series_title NOT LIKE '%Men%'
      AND s.series_title NOT LIKE '%Women%'
      AND s.series_title NOT LIKE '%age%'
      AND s.series_title NOT LIKE '%employed%'
  ORDER BY LENGTH(s.series_title)  -- Shortest title = most general
  LIMIT 5;

  Or Find the "Total Population" Series

  -- The most general series (fewest dimensions specified)
  SELECT DISTINCT
      s.series_id,
      s.series_title,
      COUNT(*) OVER (PARTITION BY s.actcode_code) as series_per_activity
  FROM bls_tu_series s
  WHERE s.stattype_code = '10101'
      AND s.actcode_code IN ('010100', '050100', '110100')  -- Sleep, Work, Eating
      -- Try to find series with ALL dimensions set to "all/N/A"
      AND (s.series_title LIKE 'Average hours per day%' OR s.series_title LIKE 'Number of%')
  ORDER BY s.actcode_code, series_per_activity;

  Decode Series Structure

  Can you run this to see the unique dimension combinations?

  -- See what dimension codes actually exist for summary data
  SELECT DISTINCT
      s.where_code,
      s.who_code,
      s.timeday_code,
      s.work_code,
      s.lfstat_code,
      COUNT(*) as num_series
  FROM bls_tu_series s
  WHERE s.stattype_code = '10101'
      AND s.sex_code = '0'
      AND s.age_code = '000'
  GROUP BY s.where_code, s.who_code, s.timeday_code, s.work_code, s.lfstat_code
  ORDER BY num_series DESC
  LIMIT 10;

-- Show series_id to see if they're different series or duplicate data
  SELECT
      s.series_id,
      a.actcode_text as activity,
      d.value as avg_hours_per_day,
      s.series_title,
      d.year,
      d.period
  FROM bls_tu_data d
  INNER JOIN bls_tu_series s ON d.series_id = s.series_id
  INNER JOIN bls_tu_actcodes a ON s.actcode_code = a.actcode_code
  WHERE d.year = 2024
      AND s.stattype_code = '10101'
      AND s.series_title LIKE 'Avg hrs per day - %'
      AND s.series_title NOT LIKE '%,%'
      AND a.actcode_code IN ('010100', '050100')  -- Just Sleeping and Working
  ORDER BY a.actcode_text, d.value DESC;

  Are they different series_ids or the same series_id with different period codes (like Q01, Q02, Q03, Q04       
  for quarters)?

  If it's different periods, you might just need:

-- Average across all periods
  SELECT
      a.actcode_text as activity,
      AVG(d.value) as avg_hours_per_day,
      d.year
  FROM bls_tu_data d
  INNER JOIN bls_tu_series s ON d.series_id = s.series_id
  INNER JOIN bls_tu_actcodes a ON s.actcode_code = a.actcode_code
  WHERE d.year = 2024
      AND s.stattype_code = '10101'
      AND s.series_title NOT LIKE '%,%'
      AND a.actcode_code IN ('010100', '050100', '110100', '120100', '130100', '020100')
  GROUP BY a.actcode_text, d.year
  ORDER BY avg_hours_per_day DESC;



  Notice the series_id patterns:
  - TUU10101**QA**01000247 → Quarterly series
  - TUU10101**AA**01000247 → Annual series

  You want just the annual average, so filter for period = 'A01':

  -- Get annual averages only (no quarterly data)
  SELECT
      a.actcode_text as activity,
      d.value as avg_hours_per_day,
      s.series_title,
      d.year
  FROM bls_tu_data d
  INNER JOIN bls_tu_series s ON d.series_id = s.series_id
  INNER JOIN bls_tu_actcodes a ON s.actcode_code = a.actcode_code
  WHERE d.year = 2024
      AND d.period = 'A01'  -- Annual only!
      AND s.stattype_code = '10101'
      AND s.series_title NOT LIKE '%,%'
      AND a.actcode_code IN ('010100', '050100', '110100', '120100', '130100', '020100')
  ORDER BY d.value DESC;


  Survey Cost:
  - TU requires detailed 24-hour time diaries from ~10,000+ respondents annually
  - Extensive interviewing and activity coding
  - 37+ dimensions tracked per activity
  - One of BLS's most resource-intensive surveys

  Parsing Cost:
  - 87,387 series with complex multi-dimensional breakdowns
  - 3.8M+ rows of data (1.9M data + 1.9M standard errors)
  - Requires understanding quarterly vs annual periods
  - Need to filter through multiple demographic/contextual dimensions
  - Series titles are the most reliable way to identify summary vs detailed breakdowns

  Despite the complexity, we successfully integrated:
  - ✅ 16 tables (13 reference + series + data + aspect)
  - ✅ Full historical data (2003-2024)
  - ✅ Parser, loader, and API update scripts

  The key insight: For TU queries, always:
  1. Filter by period = 'A01' for annual data
  2. Use series_title NOT LIKE '%,%' to exclude detailed breakdowns
  3. Or use series_id LIKE 'TUU10101AA%' to get annual averages

  It's complex, but the data is incredibly valuable for understanding how Americans spend their time!

## LN Survey, Labor force statistics from the current population survey

LN (Labor Force Statistics from CPS) Integration - Complete!

  ✅ Models Created (35 total):
  - 33 reference tables covering all demographics and labor market dimensions
  - LNSeries model with 37 dimension codes
  - LNData model for time series observations

  ✅ Migration Applied:
  - Migration 7db74d2b38d3 created and applied successfully
  - All 35 tables created with proper foreign keys and indexes

  ✅ Parser Created:
  - src/bls/ln_flat_file_parser.py with 33 reference table parsers
  - Handles Windows line endings and header padding issues
  - Supports batch processing for large datasets

  ✅ Loader Script:
  - scripts/bls/load_ln_flat_files.py - CLI for loading flat files
  - Successfully loaded all 33 reference tables
  - Successfully loaded 67,244 series
  - Data load in progress (358 MB file)

  ✅ API Update Script:
  - scripts/bls/update_ln_latest.py - Ready for API updates
  - Supports filtering by demographics (sex, age, race, education, etc.)
  - Handles 67K series (requires heavy filtering to stay under 500/day API limit)

  The LN survey is one of the most comprehensive labor market datasets, tracking:
  - Employment status (employed, unemployed, not in labor force)
  - Demographics (age, sex, race, education, citizenship)
  - Industry and occupation
  - Hours worked, multiple job holding
  - Veteran status, disability status
  - Telework status (new!)

  The Current Population Survey (CPS) is the official source for:
  - 📊 U.S. Unemployment Rate (the headline number in monthly jobs reports)
  - 👥 Labor Force Participation Rate
  - 💼 Employment/Population Ratio
  - 🎓 Employment by education level
  - 👨‍💼 Occupation and industry employment
  - 🏠 Telework statistics (new!)
  - 🎖️ Veteran employment status
  - ♿ Disability employment data
### 1. Loading Data (Already Done!)

  You've already loaded the data, but for future reference:

  #### Load all LN data (358 MB)
  python scripts/bls/load_ln_flat_files.py

  #### Load only reference tables (skip data)
  python scripts/bls/load_ln_flat_files.py --skip-data

  #### Load only data (skip reference tables)
  python scripts/bls/load_ln_flat_files.py --skip-reference

### 2. Updating with Latest Data via API

  #### Update latest year (WARNING: 67K series = 1,345 requests - exceeds daily limit!)
  python scripts/bls/update_ln_latest.py --start-year 2024

  ##### Update with FILTERS (recommended to stay under 500/day API limit):

  #### Update key labor force metrics (employed, unemployed, unemployment rate)
  python scripts/bls/update_ln_latest.py --lfst 20,30,40 --start-year 2024

  #### Update by demographics (men vs women unemployment)
  python scripts/bls/update_ln_latest.py --lfst 40 --sexs 1,2 --start-year 2024

  #### Update seasonally adjusted series only
  python scripts/bls/update_ln_latest.py --seasonal S --lfst 20,30,40 --start-year 2024

  #### Test with limited series
  python scripts/bls/update_ln_latest.py --limit 50 --start-year 2024

### 3. Query examples, please check doc/ln_query_examples.sql

### 4. Comments

Key Series IDs to Remember:

  | Series ID   | Description                     |
  |-------------|---------------------------------|
  | LNS14000000 | U.S. Unemployment Rate (SA)     |
  | LNS11000000 | Civilian Labor Force Level (SA) |
  | LNS12000000 | Employment Level (SA)           |
  | LNS13000000 | Unemployment Level (SA)         |
  | LNU01300000 | Labor Force Participation Rate  |

  Common Query Patterns:

  1. Latest unemployment rate:
  SELECT year, period, value
  FROM bls_ln_data
  WHERE series_id = 'LNS14000000'
  ORDER BY year DESC, period DESC
  LIMIT 12;

  2. Employment by demographics:
  SELECT race.race_text, d.value
  FROM bls_ln_data d
  JOIN bls_ln_series s ON d.series_id = s.series_id
  JOIN bls_ln_race race ON s.race_code = race.race_code
  WHERE s.lfst_code = '20' -- Employed
  AND s.seasonal = 'S'
  AND d.year = 2024 AND d.period = 'M10';

  3. Historical trends:
  SELECT year, value as unemp_rate
  FROM bls_ln_data
  WHERE series_id = 'LNS14000000'
  AND period = 'M13' -- Annual average
  ORDER BY year DESC;

  Reference Tables:

  - bls_ln_lfst - Labor force status codes (employed, unemployed, etc.)
  - bls_ln_sexs - Sex codes (0=Total, 1=Men, 2=Women)
  - bls_ln_ages - Age groups (16+, 16-19, 20-24, etc.)
  - bls_ln_race - Race categories
  - bls_ln_education - Education levels
  - bls_ln_occupation - SOC occupation codes
  - bls_ln_indy - NAICS industry codes
  - bls_ln_vets - Veteran status
  - bls_ln_disa - Disability status
  - bls_ln_tlwk - Telework status

  Data Coverage:

  - 67,244 series (65,566 active)
  - 8.9 million observations
  - 1940-2025 (85 years!)
  - Monthly, quarterly, and annual data


## CW Consumer Price Index - Urban Wage Earners and Clerical Workers

CW Survey Scripts Documentation

  Scripts Overview

###  1. Parser - src/bls/cw_flat_file_parser.py

  Parses CW flat files and loads data into database.

  Loads:
  - Metadata: areas (bls_cw_areas), periods, periodicity, items, series
  - Data: time series observations, aspects

  Usage: Called by loader/update scripts (not run directly)

  ---
###  2. Initial Loader - scripts/bls/load_cw_flat_files.py

  Bulk loads all CW data from flat files (first-time setup).

  Usage:
  #### Load default (Current file only)
  python scripts/bls/load_cw_flat_files.py

  #### Load specific data files
  python scripts/bls/load_cw_flat_files.py --data-files cw.data.0.Current,cw.data.1.AllItems

  #### Load with aspects
  python scripts/bls/load_cw_flat_files.py --load-aspects

  #### Skip reference tables (if already loaded)
  python scripts/bls/load_cw_flat_files.py --skip-reference

  ---
###  3. Update Script - scripts/bls/update_cw_latest.py

  Updates CW data with latest observations from BLS API (monthly updates).

  Usage:
  #### Update current year (default)
  python scripts/bls/update_cw_latest.py

  #### Update specific year range
  python scripts/bls/update_cw_latest.py --start-year 2024 --end-year 2025

  #### Update specific series
  python scripts/bls/update_cw_latest.py --series-ids CWSR0000SA0,CWSR0000SA0E

  #### Test with limited series
  python scripts/bls/update_cw_latest.py --limit 100

  ---
  Typical Workflow

  1. First time: python scripts/bls/load_cw_flat_files.py (loads all historical data)
  2. Monthly updates: python scripts/bls/update_cw_latest.py (fetches latest from API)


## BLS SU (Chained Consumer Price Index - All Urban Consumers) Data Collection

  ### Overview

  The SU survey provides the **Chained Consumer Price Index for All Urban Consumers**, which uses    
   a superlative chaining methodology to account for consumer substitution behavior. Unlike the      
  traditional CPI-U, the Chained CPI-U updates the market basket weights more frequently,
  providing a more accurate measure of inflation.

  **Key characteristics:**
  - **Base period:** December 1999 = 100
  - **Coverage:** U.S. city average only (single geographic area)
  - **Items:** 29 commodity and service categories
  - **Frequency:** Monthly
  - **Historical range:** December 1999 - Present
  - **Database tables:** `bls_su_areas`, `bls_su_items`, `bls_su_series`, `bls_su_data`

  ### Initial Data Load

  Load all SU flat files (downloaded from https://download.bls.gov/pub/time.series/su/) into the     
  database:

  ```bash
  # Load all reference tables and time series data
  python scripts/bls/load_su_flat_files.py

  # Load with custom options
  python scripts/bls/load_su_flat_files.py --data-dir data/bls/su --batch-size 10000

  # Skip reference tables (if already loaded)
  python scripts/bls/load_su_flat_files.py --skip-reference

  # Skip time series data (load only reference tables)
  python scripts/bls/load_su_flat_files.py --skip-data

  Expected results:
  - 1 area loaded
  - 29 items loaded
  - 29 series loaded
  - ~9,300+ observations loaded

  Note: The files su.data.0.Current and su.data.1.AllItems are identical. The loader
  automatically deduplicates.

  Regular Updates

  Update SU data with latest monthly releases from the BLS API:

  # Update all active series with current year data
  python scripts/bls/update_su_latest.py

  # Update specific year range
  python scripts/bls/update_su_latest.py --start-year 2024 --end-year 2025

  # Update specific series
  python scripts/bls/update_su_latest.py --series-ids SUUR0000SA0,SUUR0000SA0E

  # Test with limited series
  python scripts/bls/update_su_latest.py --limit 10
  ```
  API considerations:
  - BLS API limit: 50 series per request
  - Daily limit: 500 requests (registered) or 25 requests (unregistered)
  - For 29 active SU series: ~1 request needed

  Useful Series IDs

  Common Chained CPI-U series:

  | Series ID      | Description                           |
  |----------------|---------------------------------------|
  | SUUR0000SA0    | All items (headline Chained CPI-U)    |
  | SUUR0000SA0E   | Energy                                |
  | SUUR0000SA0L1E | All items less food and energy (core) |
  | SUUR0000SAF    | Food and beverages                    |
  | SUUR0000SAH    | Housing                               |
  | SUUR0000SAM    | Medical care                          |
  | SUUR0000SAT    | Transportation                        |
  | SUUR0000SAE    | Education and communication           |

  Query Examples

  See docs/bls_su_queries.sql for comprehensive query documentation including:

  - Metadata queries (areas, items, series)
  - Time series data retrieval
  - Year-over-year inflation calculations
  - Moving averages and trend analysis
  - Chained CPI-U vs Traditional CPI-U comparisons
  - Data quality monitoring
  - Historical analysis

  Quick query example:
  -- Get latest Chained CPI-U value
  SELECT year, period, value
  FROM bls_su_data
  WHERE series_id = 'SUUR0000SA0'
  ORDER BY year DESC, period DESC
  LIMIT 1;

  -- Compare Chained vs Traditional CPI-U (requires CU data)
  SELECT
      su.year,
      su.period,
      su.value as chained_cpi_u,
      cu.value as traditional_cpi_u,
      ROUND((su.value - cu.value), 2) as difference
  FROM bls_su_data su
  JOIN bls_cu_data cu ON su.year = cu.year AND su.period = cu.period
  WHERE su.series_id = 'SUUR0000SA0'
    AND cu.series_id = 'CUSR0000SA0'
    AND su.year >= 2020
  ORDER BY su.year, su.period;

  Why Chained CPI-U Matters

  The Chained CPI-U typically shows lower inflation than traditional CPI-U because:
  1. Substitution effect: Captures consumer behavior shifts when relative prices change
  2. Updated weights: More frequently updates the market basket composition
  3. Policy use: Used for adjusting tax brackets and some federal benefits
  4. Economic accuracy: Considered more accurate measure of cost-of-living changes

  Typical difference: Chained CPI-U runs 0.2-0.3 percentage points lower than traditional CPI-U      
  annually.

  Maintenance

  Monthly routine:
  1. BLS releases new SU data around the middle of each month
  2. Run python scripts/bls/update_su_latest.py to fetch latest data
  3. Monitor data quality with queries from docs/bls_su_queries.sql

  Data refresh (if needed):
  1. Download latest flat files from https://download.bls.gov/pub/time.series/su/
  2. Run python scripts/bls/load_su_flat_files.py to reload

  Summary

  Database Schema:
  - Created 4 models: SUArea, SUItem, SUSeries, SUData
  - Generated and applied migration 85443bfe6cec_add_bls_su_chained_cpi_u_tables.py
  - Successfully migrated all tables to database

  Data Loading:
  - Loaded 1 area (U.S. city average only)
  - Loaded 29 items
  - Loaded 29 series
  - Loaded 9,354 observations
  - Fixed deduplication issue (su.data.0.Current and su.data.1.AllItems are identical)

  Scripts Created:
  - src/bls/su_flat_file_parser.py - Parser for SU flat files
  - scripts/bls/load_su_flat_files.py - CLI loader script
  - scripts/bls/update_su_latest.py - API updater for regular updates
  - docs/bls_su_queries.sql - Comprehensive SQL query documentation (450+ lines)

  Key Features of SU Survey:
  - Chained CPI-U: Uses superlative chaining methodology to account for consumer substitution        
  - Base period: December 1999 = 100
  - Coverage: U.S. city average only (single area)
  - Items: 29 commodity/service categories (same as CU/CW)
  - Data range: 1999-12 to 2025-09 (current)

  Query Documentation Includes:
  - Metadata queries (areas, items, series)
  - Time series data queries with YoY calculations
  - Analytical queries (moving averages, category breakdowns)
  - Unique: Comparison queries between Chained CPI-U (SU) and Traditional CPI-U (CU)
  - Data quality monitoring
  - Historical analysis (cumulative inflation, deflation periods)

  Current BLS Integration Status:
  - 16 major surveys integrated: AP, CU, LA, CE, PC, WP, SM, JT, EC, OE, PR, IP, TU, LN, CW, SU ✓    


## EI Survey, Import/Export Price Indexes
Database Models:
  - EIIndex - Index type definitions (13 types)
  - EISeries - Series metadata (1,625 series)
  - EIData - Time series data (203,256 observations)

  Scripts:
  - ei_flat_file_parser.py - Parser for EI flat files
  - load_ei_flat_files.py - Initial data loader
  - update_ei_latest.py - API updater for regular updates
  - bls_ei_queries.sql - Comprehensive query documentation

  Data Loaded:
  - 13 index types (Destination, Origin, Terms of Trade, Services, etc.)
  - 1,625 active series
  - 203,256 observations

  The EI survey tracks import and export price indexes across different countries, industries,       
  and classifications. The query documentation includes examples for tracking terms of trade,        
  comparing prices across trading partners, and analyzing manufacturing vs non-manufacturing
  trends.

   Perfect! I can see that update_ei_latest.py already exists and is complete. This means all         
  tasks for both EI and BD integrations are now finished!

  Let me provide you with a comprehensive completion summary:

  EI (Import/Export Price Indexes) Integration - COMPLETE ✓

  Database Tables:
  - bls_ei_indexes - 13 index types (destination, origin, commodity, etc.)
  - bls_ei_series - 1,625 time series
  - bls_ei_data - 203,256 observations

  Scripts Created:
  - ✅ src/bls/ei_flat_file_parser.py - Parser for flat files
  - ✅ scripts/bls/load_ei_flat_files.py - Initial bulk loader
  - ✅ scripts/bls/update_ei_latest.py - API updater for monthly updates
  - ✅ docs/bls_ei_queries.sql - Query documentation (580+ lines)

  Key Features:
  - Import/export price indexes across major countries
  - Terms of trade analysis (export/import price ratios)
  - Country-specific price comparisons
  - Monthly data (M01-M12)

  ---

  
## BD Business Employment Dynamics Surey

BD (Business Employment Dynamics) Integration - COMPLETE ✓

  Database Tables:
  - 8 reference tables: states, industries, dataclasses, dataelements, sizeclasses, ratelevels,
  unitanalysis, ownership
  - bls_bd_series - 34,464 time series
  - bls_bd_data - 3,446,400 observations (3.4+ million!)

  Scripts Created:
  - ✅ src/bls/bd_flat_file_parser.py - Parser for flat files
  - ✅ scripts/bls/load_bd_flat_files.py - Initial bulk loader
  - ✅ scripts/bls/update_bd_latest.py - API updater for quarterly updates
  - ✅ docs/bls_bd_queries.sql - Query documentation (700+ lines)

  Key Features:
  - Job gains, losses, expansions, contractions
  - Establishment births and deaths
  - Firm size class analysis (1-4 employees up to 1,000+)
  - State-level job dynamics
  - Quarterly data (Q01-Q04)

  ---

## Summary, 18 Major BLS Surveys

We now have 18 major BLS surveys fully integrated:

  | Survey | Name                                 | Series        | Status |
  |--------|--------------------------------------|----------|-------------|
  | AP     | Average Price Data                   | ~2,700            | ✅ |
  | CU     | Consumer Price Index                 | ~16,000           | ✅ |
  | LA     | Local Area Unemployment              | ~45,000 675 rqs   | ✅ |
  | CE     | Employment/Hours/Earnings (National) | ~24,000           | ✅ |
  | PC     | Producer Price Index - Industry      | ~115,000          | ✅ |
  | WP     | Producer Price Index - Commodities   | ~110,000          | ✅ |
  | SM     | State/Metro Employment               | ~3.1M             | ✅ |
  | JT     | Job Openings/Labor Turnover (JOLTS)  | ~46,000           | ✅ |
  | EC     | Employment Cost Index                | ~11,000           | ✅ |
  | OE     | Occupational Employment/Wages        | ~2.5M             | ✅ |
  | PR     | Major Sector Productivity            | ~10,000           | ✅ |
  | IP     | Industry Productivity                | ~21,000           | ✅ |
  | TU     | American Time Use Survey             | ~87,000           | ✅ |
  | LN     | Labor Force Stats (CPS)              | ~67,000           | ✅ |
  | CW     | Consumer Price Index Urban           | ~67,000           | ✅ |
  | SU     | Chained CPI-U                        | ~29               | ✅ |
  | EI     | Export-import price index            | ~33  rqs          | ✅ |
  | BD     | Business Employment Dynamics         | ~690 rqs          | ✅ |
  This represents comprehensive coverage of:
  - ✅ Inflation (CPI, PPI, Average Prices, EI)
  - ✅ Employment/Unemployment (CE, LA, SM, LN, JT)
  - ✅ Wages (OE, EC)
  - ✅ Productivity (PR, IP, BD)
  - ✅ Time Use (TU)

  The LN (Current Population Survey) integration is particularly significant as it's the official source for     
  U.S. unemployment rate and labor force participation metrics that appear in monthly jobs reports!

## How the Surveys share tables

Tables That Could Be Shared:

### 1. Area Codes (FIPS)

  Used by: LA, SM, OE, CE
  - Geographic areas (states, metros, counties)
  - Standard FIPS codes
  - Currently duplicated as: bls_la_areas, bls_sm_areas, bls_oe_areas, etc.

###  2. State Codes

  Used by: LA, SM, JT, OE, WP, and others
  - 50 states + DC + territories
  - Standard FIPS state codes
  - Currently duplicated as: bls_la_states, bls_jt_states, etc.

###  3. Industry Codes (NAICS)

  Used by: CE, OE, JT, SM, WP
  - North American Industry Classification System
  - Currently duplicated as: bls_ce_industries, bls_oe_industries, bls_jt_industries, etc.

###  4. Occupation Codes (SOC)

  Used by: OE, and potentially others
  - Standard Occupational Classification
  - Currently duplicated as: bls_oe_occupations, etc.

###  5. Seasonal Adjustment Codes

  Used by: Almost all surveys
  - S (Seasonally adjusted), U (Not seasonally adjusted)
  - Currently duplicated in each survey

###  Challenges with Consolidation:

  1. Different Detail Levels

  OE industries: 444 codes (very detailed)
  JT industries: 28 codes (aggregated)
  CE industries: Different granularity

  2. Survey-Specific Attributes

  -- OE has extra fields
  bls_oe_industries: industry_code, industry_name

  -- CE might have different fields
  bls_ce_industries: industry_code, industry_name, display_level, sort_sequence

  3. NAICS Version Changes

  - NAICS 2017 vs NAICS 2022
  - Different surveys update at different times

  4. Historical Compatibility

  - Older data uses older classification systems
  - Need to maintain backward compatibility

  Potential Consolidation Strategy:

  Option 1: Shared Reference Tables (More complex but cleaner)
  bls_shared_areas (FIPS codes)
  bls_shared_states
  bls_shared_industries_naics
  bls_shared_occupations_soc

  Option 2: Keep Survey-Specific (Current approach - simpler)
  - Easier to manage survey-specific quirks
  - No cross-survey dependencies
  - Clearer data lineage

  My recommendation: Keep the current approach (survey-specific tables) because:
  1. Each survey's flat files define their own codes
  2. Codes might differ slightly between surveys
  3. Easier to maintain and debug
  4. Disk space is cheap (the duplication is minimal)

  However, if you want to query across surveys (e.g., "compare employment data from CE and OE for the same       
  industry"), you could create mapping views that join on common codes:

  CREATE VIEW bls_common_industries AS
  SELECT DISTINCT industry_code, industry_name
  FROM bls_ce_industries
  UNION
  SELECT DISTINCT industry_code, industry_name
  FROM bls_oe_industries;

## Update strategies

 Summary

  You're absolutely correct! For massive surveys like OE, API updates are completely
  impractical. Here's what I've done:

  1. Created Survey Analysis Tool

  File: scripts/bls/check_series_counts.py

  Shows series counts and API request estimates for all surveys.

  2. Created Update Strategy Guide

  File: docs/bls_update_strategy.md

  Comprehensive guide categorizing all 18 surveys:

  ✅ Safe for API Updates (8 surveys):
  - AP, PC, WP, JT, PR, SU, EI - Monthly/weekly updates OK

  ⚠️ Feasible but Monitor (5 surveys):
  - CU, CE, SM, IP, CW - Monthly updates possible, ~1 day each

  🔶 Challenging (4 surveys):
  - LA, TU, LN, BD - Quarterly/annual only, or use flat files

  ❌ Use Flat Files Only (1 survey):
  - OE - 6M+ series, 120,740 requests, 241 days!

  3. Added Safety Guard to OE Script

  The OE update script now:
  - Shows prominent warning about 6M+ series
  - Blocks execution unless --limit or --series-ids is used
  - Recommends flat file downloads
  - Provides clear instructions

  Recommended Approach

  For OE (and other large surveys):
  # Download flat files
  wget -r -np -nH --cut-dirs=3 https://download.bls.gov/pub/time.series/oe/

  # Load using flat file loader
  python scripts/bls/load_oe_flat_files.py --data-files oe.data.0.Current

  For small/medium surveys:
  # Monthly API updates are fine
  python scripts/bls/update_ap_latest.py
  python scripts/bls/update_cu_latest.py
  # etc.

  API Quota Management:
  - With 500 requests/day and 25,000/year, you can comfortably update the 13
  safe+feasible surveys monthly (~300 requests/month)
  - Use flat files for OE, LA, TU, LN major updates

  This approach gives you the best of both worlds - quick API updates for manageable
  surveys, and efficient flat file loads for massive ones!