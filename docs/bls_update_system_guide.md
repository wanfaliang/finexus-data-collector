# BLS Update System Guide

## Overview

The BLS Update System provides intelligent, quota-aware updating of all BLS surveys through:
- **Update Cycles** - Track multi-day update operations
- **Smart series tracking** - Only update series not yet in the current cycle
- **Daily quota management** - Stay within API limits (500 requests/day)
- **Resume capability** - Interrupted updates continue where they left off
- **Dashboard integration** - Monitor and trigger updates from Admin UI

## Architecture

### Key Concepts

**Update Cycle** - A single update operation for a survey. May span multiple days due to API quota limits. Only one cycle per survey can be "current" at a time.

**Soft Update** - Resume existing cycle. Skips series already updated in this cycle.

**Force Update** - Create new cycle. Old cycle marked not current. All series start fresh.

**Freshness Check** - Compare BLS API data with our database to detect if new data is available.

### Database Tables

1. **`bls_update_cycles`**
   - Tracks each update cycle
   - `id`, `survey_code`, `is_current`, `started_at`, `completed_at`
   - `total_series`, `series_updated`, `requests_used`

2. **`bls_update_cycle_series`**
   - Tracks which series have been updated in a cycle
   - `cycle_id`, `series_id`, `updated_at`

3. **`bls_api_usage_log`**
   - Tracks daily API quota usage
   - `usage_date`, `requests_used`, `series_count`, `survey_code`

### Components

1. **Update Manager** (`src/bls/update_manager.py`)
   - Core update logic used by both CLI and API
   - Functions: `update_survey()`, `get_current_cycle()`, `create_new_cycle()`

2. **Freshness Checker** (`src/bls/freshness_checker.py`)
   - Stateless freshness checking
   - Compares 50 series per survey with BLS API
   - No persistent tracking needed

3. **CLI Scripts** (`scripts/bls/`)
   - `universal_update.py` - Main update script
   - `check_quota.py` - View API usage

4. **Admin API** (`src/admin/api/v1/actions.py`)
   - REST endpoints for Dashboard
   - Update triggers, status queries, freshness checks

## Update Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                     SOFT UPDATE                                  │
│  1. Check for current cycle                                      │
│  2. If exists and incomplete → resume                            │
│  3. If none or complete → create new cycle                       │
│  4. Query series NOT IN cycle_series table                       │
│  5. Update in batches of 50                                      │
│  6. Insert to cycle_series after each batch                      │
│  7. Stop when quota reached or all done                          │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                     FORCE UPDATE                                 │
│  1. Mark existing current cycle as not current                   │
│  2. Create new cycle                                             │
│  3. All series start fresh (no cycle_series records)             │
│  4. Update in batches of 50                                      │
│  5. Insert to cycle_series after each batch                      │
│  6. Stop when quota reached or all done                          │
└─────────────────────────────────────────────────────────────────┘
```

## Resume Example

**Day 1:**
- Force Update LA survey (33,725 series)
- Create cycle #1, update 25,050 series (501 requests)
- Hit quota limit, stop
- cycle #1: `series_updated = 25,050`, `is_current = True`, `completed_at = NULL`
- 25,050 records in `bls_update_cycle_series` for cycle #1

**Day 2:**
- Soft Update LA survey
- Find current cycle #1 (incomplete)
- Query: `SELECT series_id FROM la_series WHERE is_active = TRUE AND series_id NOT IN (SELECT series_id FROM bls_update_cycle_series WHERE cycle_id = 1)`
- Returns 8,675 remaining series
- Update 8,675 series (174 requests)
- Mark cycle #1 complete: `completed_at = now()`

## Quick Start

### CLI Usage

```bash
# Check freshness (compares API with database, uses ~17 requests for all surveys)
python scripts/bls/universal_update.py --check-freshness

# Check cycle status (no API calls)
python scripts/bls/universal_update.py --check-only

# Soft update (resume existing cycle or create new)
python scripts/bls/universal_update.py --surveys LA

# Force update (create new cycle, start fresh)
python scripts/bls/universal_update.py --surveys LA --force

# Update multiple surveys
python scripts/bls/universal_update.py --surveys CU,CE,AP

# Check quota usage
python scripts/bls/check_quota.py
```

### Dashboard Usage

1. Navigate to Admin Dashboard
2. Each survey card shows:
   - Current status (Current, Needs Update, Updating)
   - Series updated / total
   - Progress percentage
3. Click **Update** to resume/start cycle (skips already-updated series)
4. Click **Force** to create new cycle and start fresh
5. Click **Check All** to check freshness for all surveys

## API Endpoints

### Status Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/actions/status` | GET | Get status for all surveys |
| `/actions/status/{survey_code}` | GET | Get status for one survey |
| `/actions/surveys` | GET | List supported survey codes |

### Update Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/actions/update/{survey_code}` | POST | Trigger update. Body: `{force: bool}` |
| `/actions/freshness/check` | POST | Check freshness. Body: `{survey_codes?: string[]}` |

### Legacy Endpoints (Dashboard compatibility)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/actions/freshness/overview` | GET | Legacy format for Dashboard |

## Freshness Checking

The freshness check compares BLS API data with our database to detect if new data is available:

1. Select 50 series per survey
2. Fetch current data from BLS API
3. Compare latest (year, period) with our database
4. Report which surveys have new data

This is a **lightweight, stateless** check - no persistent sentinel tracking.

```python
# Example response
{
  "checked_at": "2025-11-30T14:30:00",
  "surveys_checked": 17,
  "surveys_with_new_data": 3,
  "results": [
    {
      "survey_code": "CU",
      "survey_name": "Consumer Price Index",
      "has_new_data": true,
      "series_checked": 50,
      "series_with_new_data": 48,
      "our_latest": "2024 M10",
      "bls_latest": "2024 M11"
    },
    ...
  ]
}
```

## Quota Management

Daily quota tracked in `bls_api_usage_log`:
- Default limit: 500 requests/day
- Each request fetches up to 50 series
- Maximum series per day: ~25,000

The system automatically:
- Checks remaining quota before starting
- Stops when quota reached
- Resumes next day where it left off

## Best Practices

1. **Use Soft Update normally**
   - Resumes existing cycle efficiently
   - Only use Force when you need a complete refresh

2. **Check freshness before updating**
   - `--check-freshness` to see if BLS has new data
   - Avoid unnecessary updates

3. **Monitor quota**
   - Keep buffer for ad-hoc queries
   - Use `--daily-limit 400` for safety margin

4. **Use flat files for large surveys**
   - OE (6M+ series) - always use flat files
   - LA, TU, LN - use flat files for initial load

## Troubleshooting

### "All cycles complete" but data is stale

Use freshness check to see if BLS has new data:
```bash
python scripts/bls/universal_update.py --check-freshness
```

If BLS has new data, use Force Update:
```bash
python scripts/bls/universal_update.py --surveys CU --force
```

### Quota exceeded mid-update

Don't worry! The system:
1. Stopped at quota limit
2. Recorded progress in cycle
3. Will resume tomorrow with Soft Update

### Want to check cycle progress

```bash
python scripts/bls/universal_update.py --check-only
```

### Update not resuming

If cycle exists but not resuming:
1. Check if cycle is marked complete
2. Use Force Update to start fresh
3. Check for errors in previous run

## Migration from Sentinel System

The old sentinel system has been replaced:

**Removed:**
- `bls_survey_sentinels` table
- `bls_series_update_status` table
- `bls_survey_freshness` table
- Sentinel-based freshness detection

**Added:**
- `bls_update_cycles` table
- `bls_update_cycle_series` table
- On-the-fly freshness checking

Run the migration:
```bash
alembic upgrade head
```

## Summary

The BLS Update System provides:
- **Cycle-based tracking** for multi-day updates
- **Smart resume** capability
- **Simple freshness checks** without persistent tracking
- **Dashboard integration** for easy monitoring
- **Force update** option when complete refresh needed
