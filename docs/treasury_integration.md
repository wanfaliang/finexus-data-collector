# Treasury Integration Guide

This document describes the Treasury auction data integration in Finexus Data Collector.

## Overview

The Treasury module collects U.S. Treasury Notes & Bonds auction data from the **Treasury Fiscal Data API**. It tracks auction results for 2-Year, 5-Year, 7-Year, 10-Year, 20-Year, and 30-Year securities.

## Data Source

**API**: U.S. Treasury Fiscal Data API
**Base URL**: `https://api.fiscaldata.treasury.gov/services/api/fiscal_service/`
**Documentation**: https://fiscaldata.treasury.gov/api-documentation/

No API key is required.

## What We Collect

### Auction Results
- **Security terms**: 2Y, 5Y, 7Y, 10Y, 20Y, 30Y Notes & Bonds
- **Core fields**: auction_date, issue_date, maturity_date, CUSIP, security_type
- **Size & demand**: offering_amount, total_tendered, total_accepted, bid_to_cover_ratio
- **Yield results**: high_yield, low_yield, median_yield, coupon_rate, price_per_100
- **Bidder breakdown**: primary_dealer, direct_bidder, indirect_bidder amounts
- **Computed fields**: tail_bps (high_yield - WI_yield), auction_result classification

### Upcoming Auctions
- Scheduled auction calendar
- Security type, term, expected auction date, issue date

## Database Tables

### `treasury_auctions`
Main table storing historical auction results with all metrics.

### `treasury_upcoming_auctions`
Upcoming auction calendar, updated regularly.

### `treasury_daily_rates`
Daily yield curve data (optional, for yield moves analysis).

### `treasury_auction_reaction`
Links auction results to market reactions (yields, equities, VIX).

### Tracking Tables
- `treasury_data_freshness` - Tracks when data was last updated
- `treasury_collection_runs` - Logs collection job history

## Scripts

### Backfill Historical Data
```bash
# Backfill 5 years of auction history
python scripts/backfill_treasury.py --years 5

# Backfill 20 years with upcoming calendar
python scripts/backfill_treasury.py --years 20 --include-upcoming

# Dry run to preview
python scripts/backfill_treasury.py --years 10 --dry-run
```

### Daily Updates
```bash
# Standard daily update (last 30 days)
python scripts/update_treasury_data.py

# Update with upcoming calendar
python scripts/update_treasury_data.py --include-upcoming

# Force update last 60 days
python scripts/update_treasury_data.py --days 60 --force
```

## Admin API Endpoints

### Dashboard (`/api/v1/treasury/`)
- `GET /stats` - Overall statistics
- `GET /freshness/overview` - Data freshness status
- `GET /runs/recent` - Recent collection runs
- `GET /auctions/recent` - Recent auction results
- `GET /auctions/summary` - Summary by term
- `GET /upcoming` - Upcoming auctions
- `GET /rates/latest` - Latest yield curve
- `GET /rates/history` - Yield curve history

### Actions (`/api/v1/treasury/`)
- `POST /backfill/auctions` - Start backfill task
- `POST /update/auctions` - Start update task
- `POST /refresh/upcoming` - Refresh upcoming calendar
- `POST /test-api` - Test API connectivity
- `GET /task-status` - Check running tasks

### Explorer (`/api/v1/treasury/explorer/`)
- `GET /terms` - Summary stats by term
- `GET /auctions` - Query auctions with filters
- `GET /auctions/{id}` - Detailed auction info
- `GET /history/{term}` - Yield history for charting
- `GET /upcoming` - Upcoming auctions
- `GET /compare` - Compare multiple terms
- `GET /snapshot` - Latest auction per term

## Frontend Pages

### Treasury Dashboard (`/treasury`)
- Statistics cards (total auctions, upcoming, date range)
- Action buttons (Backfill All Data, Refresh Upcoming)
- Upcoming auctions display
- Recent collection runs table with auto-polling

### Treasury Explorer (`/treasury/explorer`)
- Term summary cards with latest yield and change
- Interactive yield history chart
- Upcoming auctions display
- Recent auctions table with detail modal

## Architecture

```
src/treasury/
├── __init__.py           # Exports TreasuryClient, TreasuryCollector
├── treasury_client.py    # API client for Fiscal Data API
├── treasury_collector.py # Data collection and storage logic
└── treasury_auction_calendar.py  # Auction calendar utilities

src/database/
├── treasury_models.py          # SQLAlchemy models for auction data
└── treasury_tracking_models.py # Collection tracking models

src/admin/api/v1/
├── treasury_dashboard.py  # Dashboard endpoints
├── treasury_actions.py    # Background task endpoints
└── treasury_explorer.py   # Explorer endpoints

src/admin/schemas/
└── treasury.py            # Pydantic response models

frontend/src/pages/
├── TreasuryDashboard.tsx  # Dashboard UI
└── TreasuryExplorer.tsx   # Explorer UI
```

## Term Normalization

The API returns reopened securities with adjusted terms (e.g., "9-Year 11-Month" for a 10-Year reopening). The collector normalizes these to standard terms:

- 9-Year X-Month, 10-Year X-Month → **10-Year**
- 19-Year X-Month, 20-Year X-Month → **20-Year**
- 29-Year X-Month, 30-Year X-Month → **30-Year**

## Auction Classification

Auctions are classified based on tail (high_yield - WI_yield) and bid-to-cover:

- **Strong**: tail_bps ≤ -2 and bid_to_cover > 2.5
- **Neutral**: -2 < tail_bps < +2
- **Weak/Tailed**: tail_bps ≥ +2 or bid_to_cover < 2.2

## Usage Example

```python
from src.treasury import TreasuryClient, TreasuryCollector
from src.database.connection import get_session

# Create client
client = TreasuryClient()

# Test connectivity
auctions = client.get_upcoming_auctions()
print(f"Found {len(auctions)} upcoming auctions")

# Collect data
with get_session() as session:
    collector = TreasuryCollector(db_session=session, client=client)

    # Backfill 5 years
    inserted, updated = collector.backfill_auctions(years=5)
    print(f"Inserted: {inserted}, Updated: {updated}")

    # Get stats
    stats = collector.get_auction_stats()
    print(f"Total auctions: {stats['total_auctions']}")
```

## Future Enhancements

1. **Daily Yield Curve Collection** - Pull daily yields from FRED for yield move analysis
2. **Market Reaction Tracking** - Link auctions to SPX, VIX, and sector movements
3. **Auction Alerts** - Notify on upcoming auctions and unusual results
4. **Historical Analysis** - Trend analysis of bid-to-cover ratios and tails over time
