# BEA (Bureau of Economic Analysis) Integration

This document describes the BEA data integration in FinExus Data Collector.

## Overview

The BEA integration collects economic data from the Bureau of Economic Analysis public API, focusing on five key datasets:

- **NIPA** (National Income and Product Accounts) - GDP, income, consumption, investment data
- **Regional** - State/county level GDP, personal income, employment data
- **GDPbyIndustry** - GDP by industry sector breakdown, value added, contributions to growth
- **ITA** (International Transactions Accounts) - Trade balance, exports, imports by country/area
- **FixedAssets** - Current-cost and chain-type quantity indexes for stocks, depreciation, and investment

## Architecture

```
src/
├── bea/
│   ├── __init__.py
│   ├── bea_client.py      # API client with rate limiting
│   └── bea_collector.py   # Data collectors for NIPA & Regional
├── database/
│   ├── bea_models.py          # SQLAlchemy data models
│   └── bea_tracking_models.py # Freshness & tracking models
├── admin/
│   ├── api/v1/
│   │   ├── bea_dashboard.py   # Dashboard API endpoints
│   │   └── bea_explorer.py    # Data explorer API endpoints
│   └── schemas/
│       └── bea.py             # Pydantic schemas

scripts/
├── test_bea_api.py                # Test API connection
├── backfill_bea_nipa.py           # Backfill NIPA data
├── backfill_bea_regional.py       # Backfill Regional data
├── backfill_bea_gdpbyindustry.py  # Backfill GDP by Industry data
└── update_bea_data.py             # Incremental updates
```

## Setup

### 1. Get a BEA API Key

1. Go to https://apps.bea.gov/api/signup/
2. Register for a free API key (36-character UserID)
3. Add to your `.env` file:
   ```
   BEA_API_KEY=your-36-character-api-key-here
   ```

### 2. Run Database Migrations

The BEA tables were added in migration `946a41c247c0_add_bea_tables.py`. If you haven't run it yet:
```bash
alembic upgrade head
```

This creates tables including:
- `bea_datasets` - Dataset catalog
- `bea_nipa_tables`, `bea_nipa_series`, `bea_nipa_data` - NIPA data
- `bea_regional_tables`, `bea_regional_line_codes`, `bea_regional_geo_fips`, `bea_regional_data` - Regional data
- `bea_gdpbyindustry_tables`, `bea_gdpbyindustry_industries`, `bea_gdpbyindustry_data` - GDP by Industry data
- `bea_ita_indicators`, `bea_ita_areas`, `bea_ita_data` - ITA (International Transactions) data
- `bea_fixedassets_tables`, `bea_fixedassets_series`, `bea_fixedassets_data` - FixedAssets data
- `bea_gdp_summary`, `bea_personal_income_summary` - Summary tables
- `bea_api_usage_log`, `bea_dataset_freshness`, `bea_table_update_status`, `bea_sentinel_series`, `bea_collection_runs`, `bea_release_schedule` - Tracking tables

### 3. Test API Connection

```bash
python scripts/test_bea_api.py
```

This verifies:
- API key is valid
- Can fetch dataset list
- Can fetch NIPA tables and data
- Can fetch Regional tables and data

## CLI Scripts

### Test API Connection

```bash
python scripts/test_bea_api.py
```
Quick test to verify your API key works.

### Backfill NIPA Data

```bash
# Backfill ALL NIPA tables (annual data, all years)
python scripts/backfill_bea_nipa.py

# Backfill specific tables
python scripts/backfill_bea_nipa.py --tables T10101,T10105,T20100

# Backfill quarterly data
python scripts/backfill_bea_nipa.py --frequency Q

# Backfill last 10 years only
python scripts/backfill_bea_nipa.py --year LAST10

# Preview what would be collected (dry run)
python scripts/backfill_bea_nipa.py --dry-run
```

**Options:**
| Option | Values | Default | Description |
|--------|--------|---------|-------------|
| `--tables` | TABLE1,TABLE2,... | all | Specific tables to backfill |
| `--frequency` | A, Q, M | A | Annual, Quarterly, or Monthly |
| `--year` | ALL, LAST5, LAST10, 2020,2021,... | ALL | Year specification |
| `--dry-run` | flag | - | Preview without collecting |

**Important NIPA Tables:**
| Table | Description |
|-------|-------------|
| T10101 | GDP and Major Components |
| T10105 | GDP by Major Type of Product |
| T10506 | Real GDP by Major Type of Product |
| T20100 | Personal Income and Outlays |
| T20200 | Personal Income |
| T30100 | Government Receipts and Expenditures |

### Backfill Regional Data

```bash
# Backfill ALL Regional tables (state-level, all years)
python scripts/backfill_bea_regional.py

# Backfill specific tables
python scripts/backfill_bea_regional.py --tables SAGDP1,CAINC1

# Backfill county-level data (much larger!)
python scripts/backfill_bea_regional.py --geo COUNTY

# Backfill MSA (metro area) data
python scripts/backfill_bea_regional.py --geo MSA

# Preview what would be collected
python scripts/backfill_bea_regional.py --dry-run
```

**Options:**
| Option | Values | Default | Description |
|--------|--------|---------|-------------|
| `--tables` | TABLE1,TABLE2,... | all | Specific tables to backfill |
| `--geo` | STATE, COUNTY, MSA | STATE | Geographic scope |
| `--year` | ALL, LAST5, LAST10, ... | ALL | Year specification |
| `--dry-run` | flag | - | Preview without collecting |

**Important Regional Tables:**
| Table | Description |
|-------|-------------|
| SAGDP1 | State Annual GDP Summary |
| SAGDP2N | GDP by Major Component (State) |
| SQGDP1 | State Quarterly GDP Summary |
| CAINC1 | Personal Income Summary (County/State) |
| CAINC4 | Personal Income and Employment |
| CAINC5N | Personal Income by Major Component |
| SAINC1 | State Personal Income Summary |

### Backfill GDP by Industry Data

```bash
# Backfill ALL GDP by Industry tables (annual data, all years)
python scripts/backfill_bea_gdpbyindustry.py

# Backfill specific tables
python scripts/backfill_bea_gdpbyindustry.py --tables 1,5,6

# Backfill quarterly data (available from 2005)
python scripts/backfill_bea_gdpbyindustry.py --frequency Q

# Backfill last 10 years only
python scripts/backfill_bea_gdpbyindustry.py --year LAST10

# Preview what would be collected (dry run)
python scripts/backfill_bea_gdpbyindustry.py --dry-run
```

**Options:**
| Option | Values | Default | Description |
|--------|--------|---------|-------------|
| `--tables` | ID1,ID2,... | all | Specific table IDs to backfill |
| `--frequency` | A, Q | A | Annual or Quarterly |
| `--year` | ALL, or comma-separated years | ALL | Year specification |
| `--dry-run` | flag | - | Preview without collecting |

**Important GDP by Industry Tables:**
| Table ID | Description |
|----------|-------------|
| 1 | Value Added by Industry |
| 5 | Contributions to Percent Change in Real GDP by Industry |
| 6 | Real Value Added by Industry |
| 7 | Chain-Type Quantity Indexes for Value Added by Industry |
| 8 | Gross Output by Industry |
| 10 | Real Gross Output by Industry |
| 15 | Employment by Industry (Thousands) |

**Data Availability:**
- Annual data: 1997 to present
- Quarterly data: 2005 to present
- Note: Not all tables are available for quarterly frequency

### Backfill ITA (International Transactions) Data

```bash
# Backfill ALL ITA indicators (annual data, all years)
python scripts/backfill_bea_ita.py

# Backfill specific indicators
python scripts/backfill_bea_ita.py --indicators BalGds,BalServ,BalCAcc

# Backfill quarterly seasonally adjusted data
python scripts/backfill_bea_ita.py --frequency QSA

# Backfill quarterly non-seasonally adjusted data
python scripts/backfill_bea_ita.py --frequency QNSA

# Backfill last 10 years only
python scripts/backfill_bea_ita.py --year LAST10

# Preview what would be collected (dry run)
python scripts/backfill_bea_ita.py --dry-run
```

**Options:**
| Option | Values | Default | Description |
|--------|--------|---------|-------------|
| `--indicators` | IND1,IND2,... | all | Specific indicators to backfill |
| `--frequency` | A, QSA, QNSA | A | Annual, Quarterly SA, or Quarterly NSA |
| `--year` | ALL, LAST5, LAST10, ... | ALL | Year specification |
| `--dry-run` | flag | - | Preview without collecting |

**Important ITA Indicators:**
| Indicator | Description |
|-----------|-------------|
| BalGds | Balance on Goods |
| BalServ | Balance on Services |
| BalCAcc | Balance on Current Account |
| ExpGds | Exports of Goods |
| ImpGds | Imports of Goods |
| ExpServ | Exports of Services |
| ImpServ | Imports of Services |
| PrimInc | Primary Income |
| SecInc | Secondary Income |

**API Constraints:**
- Either one indicator OR one area/country must be specified (not multiple of both)
- The collector iterates through all indicators to collect complete data

### Backfill FixedAssets Data

```bash
# Backfill ALL Fixed Assets tables (annual data, all years)
python scripts/backfill_bea_fixedassets.py

# Backfill specific tables
python scripts/backfill_bea_fixedassets.py --tables FAAt101,FAAt102,FAAt103

# Backfill last 10 years only
python scripts/backfill_bea_fixedassets.py --year LAST10

# Preview what would be collected (dry run)
python scripts/backfill_bea_fixedassets.py --dry-run
```

**Options:**
| Option | Values | Default | Description |
|--------|--------|---------|-------------|
| `--tables` | TABLE1,TABLE2,... | all | Specific tables to backfill |
| `--year` | ALL, LAST5, LAST10, ... | ALL | Year specification |
| `--dry-run` | flag | - | Preview without collecting |

**Important FixedAssets Tables:**
| Table | Description |
|-------|-------------|
| FAAt101 | Current-Cost Net Stock of Private Fixed Assets |
| FAAt102 | Chain-Type Quantity Indexes for Net Stock of Private Fixed Assets |
| FAAt103 | Current-Cost Depreciation of Private Fixed Assets |
| FAAt104 | Chain-Type Quantity Indexes for Depreciation of Private Fixed Assets |
| FAAt105 | Investment in Private Fixed Assets |
| FAAt106 | Chain-Type Quantity Indexes for Investment in Private Fixed Assets |
| FAAt201 | Current-Cost Net Stock of Private Fixed Assets by Industry |
| FAAt301 | Current-Cost Net Stock of Government Fixed Assets |
| FAAt401 | Current-Cost Net Stock of Consumer Durable Goods |

**Data Availability:**
- Annual data only: 1901 to present (varying by table)
- Data covers private fixed assets, government fixed assets, and consumer durables
- Includes current-cost values and chain-type quantity indexes

### Incremental Updates

```bash
# Update all datasets (checks if update needed)
python scripts/update_bea_data.py

# Update specific dataset
python scripts/update_bea_data.py --dataset NIPA
python scripts/update_bea_data.py --dataset Regional
python scripts/update_bea_data.py --dataset GDPbyIndustry
python scripts/update_bea_data.py --dataset ITA
python scripts/update_bea_data.py --dataset FixedAssets

# Force update (ignore freshness check)
python scripts/update_bea_data.py --force

# Update last 5 years only (faster)
python scripts/update_bea_data.py --year LAST5
```

The update script:
- Checks `bea_dataset_freshness` table to see if update is needed
- Skips datasets updated within last 24 hours (unless `--force`)
- For Regional, only updates priority tables: SAGDP1, CAINC1, SAINC1
- For GDPbyIndustry, only updates priority tables: 1, 5, 6 (Value Added, Contributions, Real Value Added)
- For ITA, only updates priority indicators: BalGds, BalServ, BalCAcc (Trade Balances)
- For FixedAssets, updates all tables (annual data only)

**Options:**
| Option | Values | Default | Description |
|--------|--------|---------|-------------|
| `--dataset` | NIPA, Regional, GDPbyIndustry, ITA, FixedAssets, all | all | Dataset to update |
| `--force` | flag | - | Force update even if recent |
| `--year` | ALL, LAST5, LAST10, ... | LAST5 | Year specification |

## BEA Client (`bea_client.py`)

The `BEAClient` class handles all API communication with built-in rate limiting.

### Rate Limits (per BEA documentation)
- 100 requests per minute
- 100 MB data per minute
- 30 errors per minute
- 60-minute lockout after violation

The client automatically:
- Tracks request counts and sleeps when approaching limits
- Retries failed requests with exponential backoff
- Handles 429 (rate limit) responses gracefully

### Basic Usage

```python
from src.bea.bea_client import BEAClient

client = BEAClient(api_key="your-36-char-key")

# Get list of available datasets
datasets = client.get_dataset_list()

# Get NIPA data
data = client.get_nipa_data(
    table_name="T10101",  # GDP table
    frequency="A",        # Annual
    year="2020,2021,2022,2023"
)

# Get Regional data
data = client.get_regional_data(
    table_name="SAGDP1",  # State GDP
    line_code=1,          # Total GDP
    geo_fips="STATE",     # All states
    year="2023"
)

# Get GDP by Industry data
data = client.get_gdpbyindustry_data(
    table_id=1,           # Value Added by Industry
    frequency="A",        # Annual
    year="2020,2021,2022,2023",
    industry="ALL"        # All industries
)

# Get ITA (International Transactions) data
data = client.get_ita_data_by_indicator(
    indicator="BalGds",   # Balance on Goods
    frequency="A",        # Annual
    year="2020,2021,2022,2023"
)

# Get FixedAssets data
data = client.get_fixedassets_table_data(
    table_name="FAAt101", # Current-Cost Net Stock
    year="2020,2021,2022,2023"
)

# Check rate limit status
stats = client.get_request_stats()
print(f"Requests remaining: {stats['requests_remaining']}")
```

## Collectors (`bea_collector.py`)

### NIPACollector

Handles NIPA data collection with automatic:
- Table catalog sync
- Series metadata extraction
- Data point upserts (insert or update)
- Progress tracking
- Freshness updates

```python
from src.bea.bea_collector import NIPACollector

collector = NIPACollector(client, session)

# Sync table catalog from BEA
collector.sync_tables_catalog()

# Collect single table
stats = collector.collect_table_data(
    table_name="T10101",
    frequency="A",
    year="ALL"
)

# Backfill all tables
progress = collector.backfill_all_tables(
    frequency="A",
    year="ALL",
    tables=["T10101", "T20100"],  # optional filter
    progress_callback=lambda p: print(p.to_dict())
)
```

### RegionalCollector

Similar to NIPACollector but handles Regional data structure:
- Tables have multiple line codes (statistics)
- Geographic FIPS codes (nation, state, county, MSA)

```python
from src.bea.bea_collector import RegionalCollector

collector = RegionalCollector(client, session)

# Sync catalogs
collector.sync_tables_catalog()
collector.sync_line_codes("SAGDP1")
collector.sync_geo_fips("SAGDP1")

# Collect single table/line combination
stats = collector.collect_table_data(
    table_name="SAGDP1",
    line_code=1,
    geo_fips="STATE",
    year="ALL"
)

# Backfill all tables
progress = collector.backfill_all_tables(
    geo_fips="STATE",
    year="ALL"
)
```

### GDPByIndustryCollector

Handles GDP by Industry data collection:
- Tables catalog sync (table IDs and descriptions)
- Industries catalog sync (industry codes)
- Data point upserts with composite primary key

```python
from src.bea.bea_collector import GDPByIndustryCollector

collector = GDPByIndustryCollector(client, session)

# Sync catalogs
collector.sync_tables_catalog()
collector.sync_industries_catalog()

# Collect single table
stats = collector.collect_table_data(
    table_id=1,
    frequency="A",
    year="ALL"
)

# Backfill all tables
progress = collector.backfill_all_tables(
    frequency="A",
    year="ALL",
    tables=[1, 5, 6],  # optional filter
    progress_callback=lambda p: print(p.to_dict())
)
```

### ITACollector

Handles ITA (International Transactions) data collection:
- Indicators catalog sync (transaction types)
- Areas/countries catalog sync
- Data point upserts with composite primary key (indicator, area, frequency, time_period)

```python
from src.bea.bea_collector import ITACollector

collector = ITACollector(client, session)

# Sync catalogs
collector.sync_indicators_catalog()
collector.sync_areas_catalog()

# Collect data for a single indicator
stats = collector.collect_indicator_data(
    indicator="BalGds",
    frequency="A",
    year="ALL"
)

# Backfill all indicators
progress = collector.backfill_all_indicators(
    frequency="A",
    year="ALL",
    indicators=["BalGds", "BalServ", "BalCAcc"],  # optional filter
    progress_callback=lambda p: print(p.to_dict())
)
```

### FixedAssetsCollector

Handles FixedAssets data collection (annual data only):
- Table catalog sync
- Series metadata extraction
- Data point upserts with composite primary key (series_code, time_period)

```python
from src.bea.bea_collector import FixedAssetsCollector

collector = FixedAssetsCollector(client, session)

# Sync table catalog
collector.sync_tables_catalog()

# Collect data for a single table
stats = collector.collect_table_data(
    table_name="FAAt101",
    year="ALL"
)

# Backfill all tables
progress = collector.backfill_all_tables(
    year="ALL",
    tables=["FAAt101", "FAAt102", "FAAt103"],  # optional filter
    progress_callback=lambda p: print(p.to_dict())
)
```

### BEACollector

Unified collector that combines NIPA, Regional, GDPbyIndustry, ITA, and FixedAssets:

```python
from src.bea.bea_collector import BEACollector

collector = BEACollector(client, session)

# Sync BEA dataset catalog
collector.sync_dataset_catalog()

# Access sub-collectors
collector.nipa.backfill_all_tables(...)
collector.regional.backfill_all_tables(...)
collector.gdpbyindustry.backfill_all_tables(...)
collector.ita.backfill_all_indicators(...)
collector.fixedassets.backfill_all_tables(...)
```

## Database Models

### NIPA Data Models

```
bea_nipa_tables
├── table_name (PK)
├── table_description
├── has_annual, has_quarterly, has_monthly
└── is_active

bea_nipa_series
├── series_code (PK)
├── table_name (FK)
├── line_number
├── line_description
├── metric_name, cl_unit, unit_mult
└── is_active

bea_nipa_data
├── series_code (PK, FK)
├── time_period (PK) - e.g., "2023", "2023Q4", "2023M12"
├── value
└── note_ref
```

### Regional Data Models

```
bea_regional_tables
├── table_name (PK)
├── table_description
└── is_active

bea_regional_line_codes
├── table_name (PK, FK)
├── line_code (PK)
├── line_description
└── cl_unit, unit_mult

bea_regional_geo_fips
├── geo_fips (PK) - FIPS code
├── geo_name - "California", "Los Angeles County"
└── geo_type - "Nation", "State", "County", "MSA"

bea_regional_data
├── table_name (PK, FK)
├── line_code (PK, FK)
├── geo_fips (PK, FK)
├── time_period (PK) - year
├── value
└── cl_unit, unit_mult, note_ref
```

### GDP by Industry Data Models

```
bea_gdpbyindustry_tables
├── table_id (PK) - integer ID (1, 2, 3, ...)
├── table_description
├── has_annual, has_quarterly
├── first_annual_year, last_annual_year
├── first_quarterly_year, last_quarterly_year
└── is_active

bea_gdpbyindustry_industries
├── industry_code (PK) - e.g., "11", "21", "FIRE", "ALL"
├── industry_description
├── parent_code - for hierarchy
├── industry_level - 1=sector, 2=subsector, 3=industry group
└── is_active

bea_gdpbyindustry_data
├── table_id (PK, FK)
├── industry_code (PK, FK)
├── frequency (PK) - "A" or "Q"
├── time_period (PK) - e.g., "2023", "2023Q4"
├── value
├── table_description, industry_description (cached)
└── cl_unit, unit_mult, note_ref
```

### ITA (International Transactions) Data Models

```
bea_ita_indicators
├── indicator_code (PK) - e.g., "BalGds", "ExpServ"
├── indicator_description
├── is_active
└── created_at, updated_at

bea_ita_areas
├── area_code (PK) - e.g., "China", "Canada", "AllCountries"
├── area_name
├── area_type - "Country", "Region", "Aggregate"
├── is_active
└── created_at, updated_at

bea_ita_data
├── indicator_code (PK, FK)
├── area_code (PK, FK)
├── frequency (PK) - "A", "QSA", "QNSA"
├── time_period (PK) - e.g., "2023", "2023Q4"
├── value
├── time_series_id, time_series_description
└── cl_unit, unit_mult, note_ref
```

### FixedAssets Data Models

```
bea_fixedassets_tables
├── table_name (PK) - e.g., "FAAt101", "FAAt201"
├── table_description
├── first_year, last_year
├── is_active
└── created_at, updated_at

bea_fixedassets_series
├── series_code (PK) - e.g., "FAAt101-d001-a"
├── table_name (FK)
├── line_number
├── line_description
├── metric_name, cl_unit, unit_mult
└── is_active

bea_fixedassets_data
├── series_code (PK, FK)
├── time_period (PK) - e.g., "2023" (annual only)
├── value
└── note_ref
```

### Tracking Models

```
bea_dataset_freshness
├── dataset_name (PK) - "NIPA", "Regional", "GDPbyIndustry", "ITA", "FixedAssets"
├── latest_data_year, latest_data_period
├── last_checked_at, last_bea_update_detected
├── needs_update, update_in_progress
├── last_update_completed
└── tables_count, series_count, data_points_count

bea_collection_runs
├── run_id (PK)
├── dataset_name, run_type
├── started_at, completed_at
├── status - "running", "completed", "failed", "partial"
├── tables_processed, series_processed
├── data_points_inserted, data_points_updated
└── api_requests_made, error_message
```

## Frontend Dashboard

Access at: `http://localhost:3001/bea`

The dashboard displays:
- Summary cards (total data points, datasets current/need update, API requests left)
- Dataset cards for NIPA, Regional, GDPbyIndustry, ITA, and FixedAssets (tables/indicators, series, data points counts)
- Action buttons for backfill and update operations (by dataset, category, frequency)
- Sentinel monitoring panel for detecting BEA data updates
- API usage chart (last 7 days)
- Recent collection runs table

## Logs

All scripts log to `logs/` directory:
- `logs/bea_nipa_backfill.log`
- `logs/bea_regional_backfill.log`
- `logs/bea_gdpbyindustry_backfill.log`
- `logs/bea_updates.log`

## Recommended Workflow

### Initial Setup

1. Test API connection:
   ```bash
   python scripts/test_bea_api.py
   ```

2. Start with NIPA (smaller dataset):
   ```bash
   # Preview first
   python scripts/backfill_bea_nipa.py --dry-run

   # Backfill annual data
   python scripts/backfill_bea_nipa.py --frequency A
   ```

3. Backfill Regional (state-level first):
   ```bash
   # Preview
   python scripts/backfill_bea_regional.py --dry-run

   # Backfill state-level
   python scripts/backfill_bea_regional.py --geo STATE
   ```

4. Backfill GDP by Industry:
   ```bash
   # Preview
   python scripts/backfill_bea_gdpbyindustry.py --dry-run

   # Backfill annual data
   python scripts/backfill_bea_gdpbyindustry.py --frequency A
   ```

5. (Optional) Backfill county data (much larger):
   ```bash
   python scripts/backfill_bea_regional.py --geo COUNTY --tables CAINC1
   ```

### Daily Updates

Schedule `update_bea_data.py` to run daily:
```bash
# Cron example (6 AM daily)
0 6 * * * cd /path/to/finexus && python scripts/update_bea_data.py
```

Or Windows Task Scheduler equivalent.

## Troubleshooting

### "Invalid or missing BEA_API_KEY"
- Ensure `BEA_API_KEY` is in your `.env` file
- Key must be exactly 36 characters

### Rate Limit Errors
- Scripts automatically handle rate limiting
- If lockout occurs, wait 60 minutes
- Use `--year LAST5` for faster runs

### Database Errors
- Ensure migrations are up to date: `alembic upgrade head`
- Check database connection in `.env`

### No Data Returned
- Some tables may not have data for all years
- Check BEA's data availability at https://apps.bea.gov/

## API Reference

See the official BEA API documentation:
- User Guide: https://apps.bea.gov/api/_pdf/bea_web_service_api_user_guide.pdf
- Interactive API: https://apps.bea.gov/api/data/

## Data Notes

- NIPA data goes back to 1929 for some series
- Regional data typically starts from 1969 (state) or 2001 (county)
- Data is released with various lags (monthly indicators faster than annual)
- BEA revises historical data periodically
- Key PCE Tables (Section 2.3-2.8)

  | Table  | Description                                   | Frequency |
  |--------|-----------------------------------------------|-----------|
  | T20305 | PCE by Major Type of Product (nominal $)      | A, Q      |
  | T20306 | Real PCE by Major Type of Product (chained $) | A, Q      |
  | T20301 | % Change in Real PCE by Major Type            | A, Q      |
  | T20304 | Price Indexes for PCE by Major Type           | A, Q      |
  | T20405 | PCE by Type of Product (more detailed)        | A, Q      |
  | T20406 | Real PCE by Type of Product                   | A, Q      |
  | T20505 | PCE by Function                               | A only    |
  | T20805 | PCE by Major Type (Monthly)                   | M         |
  | T20806 | Real PCE by Major Type (Monthly)              | M         |

  Most commonly used:
  - T20305 - Nominal PCE values
  - T20306 - Real (inflation-adjusted) PCE values
  - T20805/T20806 - Monthly PCE data

  The T3xxxx tables with "consumption" are for Government consumption, not personal.
- Key GDP Tables (Section 1.1-1.17)

  | Table  | Description                               | Frequency |
  |--------|-------------------------------------------|-----------|
  | T10105 | GDP (nominal $) - The main GDP table      | A, Q      |
  | T10106 | Real GDP (chained $) - Inflation-adjusted | A, Q      |
  | T10101 | % Change in Real GDP                      | A, Q      |
  | T10102 | Contributions to % Change in Real GDP     | A, Q      |
  | T10104 | GDP Price Indexes (deflator)              | A, Q      |
  | T10109 | Implicit Price Deflators for GDP          | A, Q      |
  | T10110 | Percentage Shares of GDP                  | A, Q      |

  More Detailed GDP Breakdowns

  | Table  | Description                                | Frequency |
  |--------|--------------------------------------------|-----------|
  | T10505 | GDP, Expanded Detail (more line items)     | A, Q      |
  | T10506 | Real GDP, Expanded Detail                  | A, Q      |
  | T10205 | GDP by Major Type of Product               | A, Q      |
  | T10705 | GDP vs GNP vs National Income relationship | A, Q      |
  | T11705 | GDP vs Gross Domestic Income               | A, Q      |

  Not Seasonally Adjusted (Section 8)

  | Table  | Description                           |
  |--------|---------------------------------------|
  | T80105 | GDP, Not Seasonally Adjusted (Q)      |
  | T80106 | Real GDP, Not Seasonally Adjusted (Q) |

  Most commonly used:
  - T10105 - Nominal GDP
  - T10106 - Real GDP (chained dollars)
  - T10101 - GDP growth rate 
- Important Regional Tables

  State GDP (Annual & Quarterly)

  | Table  | Description             | Frequency |
  |--------|-------------------------|-----------|
  | SAGDP1 | State GDP summary       | Annual    |
  | SAGDP2 | GDP by state (detailed) | Annual    |
  | SAGDP9 | Real GDP by state       | Annual    |
  | SQGDP1 | State GDP summary       | Quarterly |
  | SQGDP2 | GDP by state            | Quarterly |
  | SQGDP9 | Real GDP by state       | Quarterly |

  State Personal Income

  | Table   | Description                                              | Frequency |
  |---------|----------------------------------------------------------|-----------|
  | SAINC1  | Personal income summary (income, population, per capita) | Annual    |
  | SAINC4  | Personal income & employment by component                | Annual    |
  | SAINC5N | Personal income & earnings by NAICS industry             | Annual    |
  | SAINC51 | Disposable personal income                               | Annual    |
  | SQINC1  | Personal income summary                                  | Quarterly |

  County Level

  | Table  | Description                         |
  |--------|-------------------------------------|
  | CAINC1 | County personal income summary      |
  | CAINC4 | County personal income by component |
  | CAGDP1 | County GDP summary                  |
  | CAGDP2 | County GDP detailed                 |
  | CAGDP9 | County real GDP                     |

  Other Useful

  | Table     | Description                                    |
  |-----------|------------------------------------------------|
  | SARPP     | Real personal income & regional price parities |
  | SAPCE1    | Personal consumption expenditures by state     |
  | SASUMMARY | State summary (income, GDP, PCE, employment)   |

  ---
  Important GDP by Industry Tables

  Value Added (Core)

  | Table | Description                                              | Frequency |
  |-------|----------------------------------------------------------|-----------|
  | 1     | Value Added by Industry (nominal $)                      | A, Q      |
  | 10    | Real Value Added by Industry                             | A, Q      |
  | 5     | Value Added as % of GDP                                  | A, Q      |
  | 6     | Components of Value Added (compensation, taxes, surplus) | A         |
  | 8     | Quantity Indexes for Value Added                         | A, Q      |
  | 13    | Contributions to % Change in Real GDP by Industry        | A, Q      |

  Gross Output

  | Table | Description                   | Frequency |
  |-------|-------------------------------|-----------|
  | 15    | Gross Output by Industry      | A, Q      |
  | 208   | Real Gross Output by Industry | A, Q      |

  Intermediate Inputs

  | Table | Description                          | Frequency |
  |-------|--------------------------------------|-----------|
  | 20    | Intermediate Inputs by Industry      | A, Q      |
  | 209   | Real Intermediate Inputs by Industry | A, Q      |

  ---
  Important ITA (International Transactions) Indicators

  Trade Balance

  | Indicator | Description                                |
  |-----------|-------------------------------------------|
  | BalGds    | Balance on Goods                          |
  | BalServ   | Balance on Services                       |
  | BalCAcc   | Balance on Current Account                |

  Exports & Imports

  | Indicator   | Description                              |
  |-------------|------------------------------------------|
  | ExpGds      | Exports of Goods                         |
  | ImpGds      | Imports of Goods                         |
  | ExpServ     | Exports of Services                      |
  | ImpServ     | Imports of Services                      |

  Income & Transfers

  | Indicator      | Description                           |
  |----------------|---------------------------------------|
  | PrimInc        | Primary Income (investment income)    |
  | SecInc         | Secondary Income (transfers)          |
  | IncReceipts    | Income Receipts                       |
  | IncPayments    | Income Payments                       |
  | PfInvAssets    | Portfolio Investment Assets           |

  **Frequencies:**
  - A: Annual
  - QSA: Quarterly Seasonally Adjusted
  - QNSA: Quarterly Not Seasonally Adjusted

  **Areas/Countries:** Data available by individual country (China, Canada, Mexico, etc.), regions (Europe, Asia), and aggregates (AllCountries).

  ---
  Most commonly used:
  - Regional: SAGDP1, SAINC1, CAINC1 (the "priority" tables)
  - Industry: Tables 1, 10, 13 (value added and contributions to GDP growth)
  - ITA: BalGds, BalServ, BalCAcc (trade balances for equity/macro analysis)

## Suggested Schedule

Suggested schedule:

  | Frequency | Datasets                                                       | Schedule                                 |
  |-----------|----------------------------------------------------------------|------------------------------------------|
  | Monthly   | NIPA (M)                                                       | 1st week of month                        |
  | Quarterly | NIPA (Q), GDPbyIndustry (Q), ITA (QSA/QNSA)                    | After quarter end (late Jan/Apr/Jul/Oct) |
  | Annually  | NIPA (A), Regional, GDPbyIndustry (A), ITA (A), FixedAssets    | Once a year or after major BEA revisions |