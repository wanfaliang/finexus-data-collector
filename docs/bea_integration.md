# BEA (Bureau of Economic Analysis) Integration

This document describes the BEA data integration in FinExus Data Collector.

## Overview

The BEA integration collects economic data from the Bureau of Economic Analysis public API, focusing on three key datasets:

- **NIPA** (National Income and Product Accounts) - GDP, income, consumption, investment data
- **Regional** - State/county level GDP, personal income, employment data
- **GDPbyIndustry** - GDP by industry sector breakdown, value added, contributions to growth

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

This creates 19 tables:
- `bea_datasets` - Dataset catalog
- `bea_nipa_tables`, `bea_nipa_series`, `bea_nipa_data` - NIPA data
- `bea_regional_tables`, `bea_regional_line_codes`, `bea_regional_geo_fips`, `bea_regional_data` - Regional data
- `bea_gdpbyindustry_tables`, `bea_gdpbyindustry_industries`, `bea_gdpbyindustry_data` - GDP by Industry data
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

### Incremental Updates

```bash
# Update all datasets (checks if update needed)
python scripts/update_bea_data.py

# Update specific dataset
python scripts/update_bea_data.py --dataset NIPA
python scripts/update_bea_data.py --dataset Regional
python scripts/update_bea_data.py --dataset GDPbyIndustry

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

**Options:**
| Option | Values | Default | Description |
|--------|--------|---------|-------------|
| `--dataset` | NIPA, Regional, GDPbyIndustry, all | all | Dataset to update |
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

### BEACollector

Unified collector that combines NIPA, Regional, and GDPbyIndustry:

```python
from src.bea.bea_collector import BEACollector

collector = BEACollector(client, session)

# Sync BEA dataset catalog
collector.sync_dataset_catalog()

# Access sub-collectors
collector.nipa.backfill_all_tables(...)
collector.regional.backfill_all_tables(...)
collector.gdpbyindustry.backfill_all_tables(...)
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

### Tracking Models

```
bea_dataset_freshness
├── dataset_name (PK) - "NIPA", "Regional", "GDPbyIndustry"
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
- Dataset cards for NIPA and Regional (tables, series, data points counts)
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
