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

## Database Schema

### Core Tables (6)
- companies, income_statements, balance_sheets, cash_flows, financial_ratios, key_metrics

### Market Data (4)
- prices_daily, prices_monthly, enterprise_values, employee_history

### Analyst & Ownership (5)
- analyst_estimates, price_targets, insider_trading, institutional_ownership, insider_statistics

### Economic (1)
- economic_indicators (vertical format!)

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

## Documentation

See `/mnt/user-data/outputs/` for:
- COMPLETE_IMPLEMENTATION_GUIDE.md - Full code examples
- EXECUTIVE_SUMMARY.md - Overview
- ARCHITECTURE_DIAGRAM.txt - Visual reference

## Support

Phase 1 Status: ✅ Ready for Implementation
