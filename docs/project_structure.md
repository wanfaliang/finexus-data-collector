# FinExus Data Collector - Project Structure

## Overview

**FinExus Data Collector** is a PostgreSQL-based financial data aggregation system designed to collect, process, and maintain financial and economic data from multiple sources. It replaces Excel exports with centralized database storage and supports incremental updates for efficient API usage.

### Tech Stack

- **Backend:** Python 3.12, FastAPI, SQLAlchemy 2.0
- **Frontend:** React 19, TypeScript, Vite, Material-UI
- **Database:** PostgreSQL with connection pooling
- **Job Scheduling:** APScheduler (cron-based)
- **Development Tools:** Alembic (migrations), pytest, mypy, black, flake8

### Data Sources

| Source | Description |
|--------|-------------|
| FMP (Financial Modeling Prep) | Stock prices, financial statements, analyst data |
| FRED (Federal Reserve) | Economic indicators |
| BEA (Bureau of Economic Analysis) | National and regional economic accounts |
| BLS (Bureau of Labor Statistics) | Labor market and price data (18 surveys) |
| Treasury Fiscal Data API | U.S. Treasury auction data |
| Nasdaq | Stock/ETF screener data |

---

## Directory Structure

```
finexus-data-collector/
├── .env                          # Environment configuration (API keys, DB URL)
├── .env.example                  # Configuration template
├── alembic/                      # Database migrations
├── alembic.ini                   # Alembic configuration
├── data/                         # Local data storage
│   ├── bls/                      # BLS survey raw data
│   ├── bulk_csv/                 # Bulk financial data CSVs
│   ├── nasdaq_screener/          # Nasdaq stock screener data
│   ├── nasdaq_etf_screener/      # Nasdaq ETF screener data
│   ├── priority_lists/           # Priority company lists
│   └── exports/                  # Data exports
├── docs/                         # Documentation
├── frontend/                     # React admin UI
├── logs/                         # Application logs
├── scripts/                      # Utility scripts (90+)
├── src/                          # Main source code
│   ├── admin/                    # FastAPI admin backend
│   ├── bea/                      # Bureau of Economic Analysis module
│   ├── bls/                      # Bureau of Labor Statistics module
│   ├── collectors/               # Data collectors (23+)
│   ├── database/                 # Models & connection
│   ├── jobs/                     # Job orchestration
│   ├── treasury/                 # Treasury auction module
│   ├── utils/                    # Utility functions
│   └── config.py                 # Configuration management
├── requirements.txt              # Python dependencies
├── README.md                     # Main documentation
└── ROUTINE.md                    # Operational procedures
```

---

## Source Code (`src/`)

### `config.py`

Pydantic-based configuration management with settings classes:

- **DatabaseSettings:** Connection pooling, timeouts, recycling
- **APISettings:** API keys, rate limits, retry logic (3 retries, 0.7s backoff)
- **DataCollectionSettings:** Batch size, worker threads, feature flags
- **ScheduleSettings:** Cron expressions for automated jobs
- **ValidationSettings:** Data validation flags
- **MonitoringSettings:** Metrics collection

Also defines API endpoints for FMP and BEA datasets with rate limiting configurations.

### `database/`

#### `connection.py`
- `DatabaseConnection` singleton class with connection pooling
- Helper functions: `get_session()`, `get_scoped_session()`
- Database utilities: `execute_raw_sql()`, `get_table_row_count()`, `table_exists()`, `vacuum_analyze_table()`

#### `models.py` (Core Financial Models)
| Model | Description |
|-------|-------------|
| `Company` | Master company table (symbols, profiles, metadata) |
| `IncomeStatement` | Revenue, expenses, net income, EPS (40+ fields) |
| `BalanceSheet` | Assets, liabilities, equity |
| `CashFlow` | Operating, investing, financing activities |
| `KeyMetrics` | Financial ratios and metrics |
| `PriceHistory` | OHLCV data with dividend adjustments |
| `EnterpriseValue` | Enterprise values and metrics |
| `Ratios` | Financial ratios (profitability, liquidity, leverage) |
| `Analyst` | Analyst estimates and price targets |
| `InsiderTrading` | Insider transaction data |
| `InstitutionalOwnership` | Fund ownership data |
| `Employees` | Historical employee counts |
| `EarningsCalendar` | Earnings announcement dates |
| `TableUpdateTracking` | Tracks last update per symbol per table |
| `DataCollectionLog` | Audit trail of operations |

#### `bea_models.py` (BEA Models - 20+ tables)
- Reference tables: `BEADataset`, `NIPATable`, `NIPASeries`, `RegionalSeries`
- Data tables: NIPA, Regional, GDP by Industry, ITA, FixedAssets
- Tracking: `BEAFreshness`, `BEAAPICallLog`

#### `bls_models.py` (BLS Models)
- Reference: `BLSSurvey`, `BLSArea`, `BLSPeriod`
- 18 survey-specific tables: AP, BD, CE, CU, CW, EC, EI, IP, JT, LA, LN, OE, PC, PR, SM, SU, TU, WP
- Tracking: `BLSFreshness`, `BLSSentinels`

#### `treasury_models.py`
- `TreasurySecurityType` - Reference table (Note, Bond, Bill, TIPS, FRN)
- `TreasuryAuction` - Auction results (CUSIP, dates, bid-to-cover, yield, tails)

### `collectors/` (23 Collector Classes)

All collectors inherit from `BaseCollector` providing:
- API request handling with retry logic & exponential backoff
- Incremental update tracking
- Force refill mode
- Error handling and database logging
- Data sanitization

#### Financial Collectors
| Collector | Purpose |
|-----------|---------|
| `company_collector.py` | Company profiles |
| `financial_collector.py` | Income statements, balance sheets, cash flows |
| `price_collector.py` | Daily OHLCV prices |
| `key_metrics_ttm_bulk_collector.py` | TTM metrics |
| `ratios_ttm_bulk_collector.py` | TTM financial ratios |
| `enterprise_collector.py` | Enterprise values |
| `employee_collector.py` | Historical employee counts |
| `analyst_collector.py` | Analyst estimates and price targets |
| `insider_collector.py` | Insider trading and institutional ownership |
| `earnings_calendar_collector.py` | Earnings announcement dates |
| `economic_calendar_collector.py` | Economic calendar events |

#### Bulk Data Collectors
| Collector | Purpose |
|-----------|---------|
| `bulk_price_collector.py` | 100K+ symbols EOD prices |
| `bulk_financial_collector.py` | Bulk financial statement data |
| `bulk_peers_collector.py` | Peer relationships (75K+ symbols) |
| `bulk_profile_collector.py` | Bulk company profiles |
| `price_target_summary_bulk_collector.py` | Price target consensus |

#### External Data Collectors
| Collector | Purpose |
|-----------|---------|
| `fred_collector.py` | Federal Reserve economic data |
| `nasdaq_screener_collector.py` | Nasdaq stock screener data |
| `nasdaq_etf_screener_collector.py` | Nasdaq ETF screener data |

### `admin/` (FastAPI Admin Backend)

#### `main.py`
- FastAPI application with CORS middleware
- API versioning at `/api/v1`
- Docs at `/api/docs`, ReDoc at `/api/redoc`

#### `api/v1/` (API Endpoints)
| Route File | Purpose |
|------------|---------|
| `actions.py` | Core data collection actions |
| `bea_actions.py` | BEA-specific actions |
| `bea_dashboard.py` | BEA dashboard metrics |
| `bea_explorer.py` | BEA data exploration |
| `bea_sentinel.py` | BEA sentinel monitoring |
| `treasury_actions.py` | Treasury data actions |
| `treasury_dashboard.py` | Treasury dashboard |
| `treasury_explorer.py` | Treasury data exploration |
| `ce_explorer.py` | Census Employment explorer |
| `cu_explorer.py` | Consumer Price Index explorer |
| `la_explorer.py` | Local Area employment explorer |
| `ln_explorer.py` | Labor Force explorer |
| `freshness.py` | Data freshness monitoring |
| `quota.py` | API quota tracking |

#### `schemas/`
Pydantic response schemas for all API endpoints.

### `bea/` (Bureau of Economic Analysis)

| File | Purpose |
|------|---------|
| `bea_client.py` | API client with rate limiting (100 req/min) |
| `bea_collector.py` | Data collection for NIPA, Regional, GDP by Industry |
| `task_runner.py` | Task management for BEA collections |

### `bls/` (Bureau of Labor Statistics)

26 files covering:

- **Client & Catalog:** `bls_client.py`, `series_catalog.py`, `surveys_catalog.py`
- **18 Survey Parsers:** AP, BD, CE, CU, CW, EC, EI, IP, JT, LA, LN, OE, PC, PR, SM, SU, TU, WP
- **Monitoring:** `freshness_checker.py`, `update_manager.py`

### `treasury/` (U.S. Treasury Data)

| File | Purpose |
|------|---------|
| `treasury_client.py` | Fiscal Data API client |
| `treasury_collector.py` | Treasury data collection |
| `treasury_auction_calendar.py` | Auction calendar utilities |

### `jobs/` (Job Orchestration)

#### `update_all_data.py`
- Main job orchestrator
- APScheduler integration with CronTrigger
- Command-line: `--run-once` (test) or `--schedule` (production)

### `utils/` (Utilities)

| File | Purpose |
|------|---------|
| `bulk_utils.py` | Bulk data processing |
| `csv_reader.py` | CSV file reading |
| `data_transform.py` | Data transformation |
| `nasdaq_screener_downloader.py` | HTTP screener download |
| `nasdaq_screener_selenium.py` | Web scraping with Selenium |
| `nasdaq_etf_screener_selenium.py` | ETF screener scraping |
| `peers_helpers.py` | Peer relationship helpers |
| `price_helpers.py` | Price data utilities |

---

## Scripts (`scripts/`)

90+ utility scripts organized by function:

### Database & Setup
- `init_database.py` - Create all tables
- `add_companies.py` - Add companies to tracking
- `check_active_companies.py` - Verify active companies
- `prioritize_companies.py` - Set company priorities

### Data Backfill
- `backfill_data.py` - General historical data backfill
- `backfill_priority_data.py` - Backfill prioritized companies
- `backfill_prices_from_bulk.py` - Fill price gaps from bulk data

### BEA Backfill
- `backfill_bea_nipa.py` - NIPA data
- `backfill_bea_regional.py` - Regional data
- `backfill_bea_gdpbyindustry.py` - GDP by Industry
- `backfill_bea_ita.py` - International Trade Accounts
- `backfill_bea_fixedassets.py` - Fixed Assets

### BLS Scripts (40+)
- `bls/download_bls_survey.py` - Download survey files
- `bls/load_*.py` - Load 18 different BLS surveys
- `bls/update_*_latest.py` - Update latest data
- `bls/universal_update.py` - Universal update system
- `bls/check_*.py` - Monitoring and validation

### Bulk Data Collection
- `collect_bulk_eod.py` - 100K+ symbol prices
- `collect_bulk_peers.py` - Peer data
- `collect_company_profile_bulk.py` - Bulk profiles
- `collect_key_metrics_ttm_bulk.py` - TTM metrics
- `collect_ratios_ttm_bulk.py` - TTM ratios

### Treasury & Economic
- `backfill_treasury.py` - Treasury auction data
- `update_treasury_data.py` - Incremental treasury updates
- `update_economic_data.py` - Economic data updates
- `update_bea_data.py` - BEA data updates

---

## Frontend (`frontend/`)

React 19 admin dashboard with:

### Pages
| Page | Purpose |
|------|---------|
| `Dashboard.tsx` | Main dashboard |
| `BEADashboard.tsx` | BEA data dashboard |
| `TreasuryDashboard.tsx` | Treasury dashboard |
| `QuotaPage.tsx` | API quota tracking |

### Data Explorers
| Explorer | Data Type |
|----------|-----------|
| `NIPAExplorer.tsx` | NIPA time series |
| `RegionalExplorer.tsx` | Regional economic data |
| `GDPbyIndustryExplorer.tsx` | Industry breakdown |
| `FixedAssetsExplorer.tsx` | Fixed assets |
| `ITAExplorer.tsx` | International trade |
| `TreasuryExplorer.tsx` | Auction data |
| `CEExplorer.tsx` | Census Employment |
| `CUExplorer.tsx` | Consumer Price Index |
| `LAExplorer.tsx` | Local Area Employment |
| `LNExplorer.tsx` | Labor Force |

### Dependencies
- UI: Material-UI, Emotion
- Data Fetching: Axios, React Query
- Charting: ECharts, Recharts
- Mapping: Leaflet, TopoJSON
- Routing: React Router

---

## Configuration

### Environment Variables (`.env.example`)

```bash
# Database
DATABASE_URL=postgresql://user:pass@localhost:5432/finexus
DB_POOL_SIZE=20
DB_MAX_OVERFLOW=10

# API Keys
FMP_API_KEY=your_key
FRED_API_KEY=your_key
BLS_API_KEY=your_key
BEA_API_KEY=your_key
CENSUS_API_KEY=your_key

# Rate Limiting
API_SLEEP_SEC=0.2
API_TIMEOUT=30
API_RETRIES=3
API_BACKOFF=0.7

# Job Scheduling (Cron)
SCHEDULE_DAILY_PRICES=0 18 * * 1-5    # Weekdays 6 PM
SCHEDULE_FINANCIALS=0 19 * * *         # Daily 7 PM
SCHEDULE_ECONOMIC=0 8 * * *            # Daily 8 AM
SCHEDULE_ANALYST=0 10 * * *            # Daily 10 AM

# Feature Flags
INCLUDE_ANALYST_DATA=True
INCLUDE_INSTITUTIONAL_DATA=True
INCLUDE_INSIDER_DATA=True
INCLUDE_ECONOMIC_DATA=True

# Data Validation
ENABLE_DATA_VALIDATION=True
ALERT_ON_STALE_DATA_DAYS=7
```

---

## Key Architectural Features

### Incremental Update System
- Tracks last update timestamp per symbol per table
- `should_update_symbol()` determines if data needs refresh
- Force refill mode for manual data refreshes
- Reduces API calls and database strain

### Bulk Data Fallback
- Collects 100K+ symbol EOD prices in bulk
- Falls back to bulk prices when individual API limits reached
- 75K+ company peer relationships from bulk data

### Job Scheduling
- APScheduler with cron expressions
- Separate jobs for different data types
- Automated daily/weekly/monthly updates

### Data Validation
- Sanitization to prevent BigInteger overflow
- Index symbol detection
- Data constraint validation before insertion

### Monitoring & Tracking
- Data collection logging to database
- Freshness monitoring (alert if data >7 days old)
- API quota tracking
- Sentinel system for continuous monitoring

---

## Database Schema Summary

| Category | Table Count | Description |
|----------|-------------|-------------|
| Core Financial | 18+ | Companies, statements, prices, metrics |
| BEA | 20+ | NIPA, Regional, GDP by Industry, ITA, FixedAssets |
| BLS | 18 surveys | Labor market and price data |
| Treasury | 3+ | Auction data, investor categories |
| Tracking | 5+ | Update tracking, logs, freshness |

### Design Patterns
- Composite primary keys (symbol + date + period)
- Foreign key relationships
- Unique constraints
- Indexes on frequently queried columns
- Automatic timestamp tracking (created_at, updated_at)

---

## Documentation Files (`docs/`)

| File | Purpose |
|------|---------|
| `bea_integration.md` | BEA API setup and usage |
| `treasury_integration.md` | Treasury data integration |
| `bls_update_system_guide.md` | BLS update procedures |
| `sentinel_system.md` | Monitoring system documentation |
| `admin_ui.md` | Admin UI documentation |
| `bea_web_service_api_user_guide.pdf` | BEA API reference (PDF) |

---

## Getting Started

1. **Clone and Setup**
   ```bash
   git clone <repo>
   cd finexus-data-collector
   pip install -r requirements.txt
   cp .env.example .env
   # Edit .env with your API keys and database URL
   ```

2. **Initialize Database**
   ```bash
   python scripts/init_database.py
   alembic upgrade head
   ```

3. **Add Companies**
   ```bash
   python scripts/add_companies.py
   ```

4. **Run Data Collection**
   ```bash
   # Single run (testing)
   python src/jobs/update_all_data.py --run-once

   # Scheduled mode (production)
   python src/jobs/update_all_data.py --schedule
   ```

5. **Start Admin UI**
   ```bash
   # Backend
   cd src/admin && uvicorn main:app --reload

   # Frontend
   cd frontend && npm install && npm run dev
   ```

---

## Code Conventions

- **Classes:** PascalCase (`FinancialCollector`, `DatabaseConnection`)
- **Functions:** snake_case (`get_table_row_count`, `sanitize_record`)
- **Database tables:** snake_case pluralized (`income_statements`, `bls_areas`)
- **Columns:** snake_case (`created_at`, `fiscal_year`)

---

*Last Updated: December 2024*
