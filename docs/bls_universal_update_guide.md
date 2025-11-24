# BLS Universal Update System Guide

## Overview

The Universal Update System provides intelligent, quota-aware updating of all BLS surveys through:
- **Smart series tracking** - Only update series that need it
- **Daily quota management** - Stay within API limits (500 requests/day)
- **Status tracking** - Know what's current and what needs updates
- **Resume capability** - Interrupted updates continue where they left off

## Quick Start

```bash
# Check what needs updating (no API calls)
python scripts/bls/universal_update.py --check-only

# Update specific surveys
python scripts/bls/universal_update.py --surveys CU,AP,EI

# Update all safe surveys (respects daily limit)
python scripts/bls/universal_update.py --surveys AP,PC,WP,JT,EI,SU,PR

# Check quota usage
python scripts/bls/check_quota.py

# View update status
python scripts/bls/show_status.py
```

## Core Scripts

### 1. `universal_update.py` - Main Update Script

Updates any BLS survey with intelligent series selection and quota management.

**Options:**
```bash
--surveys CU,CE,AP     # Specific surveys to update
--daily-limit 400      # Set daily quota limit (default: 500)
--start-year 2024      # Data year range
--end-year 2025
--check-only           # Preview without updating
--force                # Update even if marked current
```

**Examples:**
```bash
# Daily routine: Update high-priority surveys
python scripts/bls/universal_update.py --surveys CU,CE,AP,PC

# Monthly comprehensive: Update all feasible surveys
python scripts/bls/universal_update.py --surveys CU,CE,SM,IP,AP,PC,WP,JT,EI,SU,PR,CW

# Force re-check of CU series
python scripts/bls/universal_update.py --surveys CU --force

# Safe mode: Low daily limit
python scripts/bls/universal_update.py --daily-limit 100
```

### 2. `check_quota.py` - View API Usage

Shows today's API usage and remaining quota.

**Output:**
```
================================================================================
BLS API QUOTA STATUS - 2025-01-15
================================================================================

Requests:
  Used today: 156 / 500 (31.2%)
  Remaining:  344 requests (~17,200 series)

Series updated today: 7,800

Breakdown by survey:
  CU : 137 requests,   6,840 series
  AP :  11 requests,     544 series
  PC :   8 requests,     416 series
```

### 3. `show_status.py` - View Series Status

Shows which surveys are current and which need updates.

**Options:**
```bash
--surveys CU,CE        # Show specific surveys
--detailed             # Include last update times
```

**Output:**
```
================================================================================
BLS SERIES UPDATE STATUS
================================================================================

CU - Consumer Price Index
  Active series: 6,840
  Current: 6,840 (100.0%)
  Need update: 0

CE - Current Employment Statistics
  Active series: 22,049
  Current: 0 (0.0%)
  Need update: 22,049
```

### 4. `reset_status.py` - Force Re-check

Marks series as needing update (clears current status).

**Usage:**
```bash
# Reset specific surveys
python scripts/bls/reset_status.py --surveys CU,CE

# With confirmation
python scripts/bls/reset_status.py --surveys CU --confirm
```

**When to use:**
- New month's data is released
- Want to force full re-check
- After flat file import

## How It Works

### Series Status Tracking

The system tracks each series' status in `bls_series_update_status`:
- **series_id** - The BLS series identifier
- **survey_code** - Which survey (CU, CE, etc.)
- **last_data_year/period** - Latest data point
- **is_current** - Whether series has recent data
- **last_checked_at** - When last checked
- **last_updated_at** - When last updated

### Smart Update Logic

1. **Check status table** - Which series are marked current?
2. **Query active series** - Get all active series from survey
3. **Identify stale series** - Series not current or never checked
4. **Check quota** - How many requests available today?
5. **Update in batches** - Fetch 50 series per API request
6. **Record status** - Mark updated series as current
7. **Log usage** - Track API requests used

### Quota Management

Daily quota tracked in `bls_api_usage_log`:
- **usage_date** - Date of usage
- **requests_used** - Number of API requests
- **series_count** - Number of series updated
- **survey_code** - Which survey
- **script_name** - Which script ran

The system automatically:
- Checks remaining quota before starting
- Stops when quota reached
- Resumes next day where it left off

## Recommended Update Schedules

### Daily Updates (< 200 requests/day)

Safe, high-value surveys:
```bash
# Run once per day
python scripts/bls/universal_update.py --surveys CU,AP,PC,WP,JT,EI,SU,PR,CW
```

**Quota usage:** ~210 requests
**Coverage:** Consumer prices, producer prices, JOLTS

### Weekly Updates (< 500 requests/week)

Add medium-sized surveys:
```bash
# Monday
python scripts/bls/universal_update.py --surveys CU,CE

# Wednesday
python scripts/bls/universal_update.py --surveys SM,IP,AP,PC

# Friday
python scripts/bls/universal_update.py --surveys WP,JT,EI,SU,PR,CW
```

### Monthly Updates (Full)

Update all feasible surveys:
```bash
# Week 1: Consumer/Producer prices
python scripts/bls/universal_update.py --surveys CU,CW,PC,WP,AP

# Week 2: Employment
python scripts/bls/universal_update.py --surveys CE,SM,IP

# Week 3: Other monthly
python scripts/bls/universal_update.py --surveys JT,EI,SU,PR

# Week 4: Quarterly/Large (use flat files for LA, TU, LN, OE)
python scripts/bls/universal_update.py --surveys BD
```

## Integration with Individual Update Scripts

The 18 individual update scripts (`update_cu_latest.py`, etc.) still exist and work independently:

**Use individual scripts when:**
- Testing specific survey
- Need custom parameters
- Debugging issues

**Use universal script when:**
- Daily routine updates
- Managing multiple surveys
- Quota tracking needed

Both approaches:
- ✅ Have --dry-run mode
- ✅ Ask for confirmation
- ✅ Handle empty data
- ✅ Support --limit for testing

## Handling Large Surveys

### OE (Occupational Employment) - 6M+ series

**NEVER use API updates.** Always use flat files:
```bash
# Download
wget -r -np -nH --cut-dirs=3 https://download.bls.gov/pub/time.series/oe/

# Load
python scripts/bls/load_oe_flat_files.py --data-files oe.data.0.Current
```

### LA, TU, LN - 30K-90K series

**Recommended:** Use flat files for major updates, API for incremental:
```bash
# Annual: Flat file
python scripts/bls/load_la_flat_files.py --data-files la.data.0.Current

# Monthly: API for specific series
python scripts/bls/universal_update.py --surveys LA --limit 1000
```

## Troubleshooting

### "All surveys up-to-date" but data is stale

Series might be incorrectly marked current. Reset:
```bash
python scripts/bls/reset_status.py --surveys CU,CE
```

### Quota exceeded mid-update

Don't worry! The system:
1. Stops at quota limit
2. Records what was updated
3. Resumes tomorrow automatically

Check status:
```bash
python scripts/bls/check_quota.py
python scripts/bls/show_status.py --surveys CU
```

### Want to force full refresh

Use --force flag:
```bash
python scripts/bls/universal_update.py --surveys CU --force
```

### Update interrupted (error, crash)

Just run again - it will:
1. Skip already-updated series
2. Continue from where it stopped
3. No duplicate updates

## Best Practices

1. **Run check-only first**
   ```bash
   python scripts/bls/universal_update.py --check-only
   ```

2. **Monitor quota daily**
   ```bash
   python scripts/bls/check_quota.py
   ```

3. **Start small when testing**
   ```bash
   python scripts/bls/universal_update.py --surveys AP,SU --daily-limit 20
   ```

4. **Use flat files for initial loads**
   - Faster
   - No quota impact
   - More reliable for large datasets

5. **Schedule regular updates**
   - Daily: High-priority surveys
   - Weekly: Medium surveys
   - Monthly: Large surveys
   - Annual: OE, LA, TU via flat files

6. **Keep quota buffer**
   - Set daily-limit to 400 instead of 500
   - Leaves room for ad-hoc queries

## Database Tables

### `bls_series_update_status`
Tracks each series' update status

### `bls_api_usage_log`
Records all API usage for quota tracking

Both tables are:
- Automatically maintained
- Queried by all scripts
- Safe to inspect/query directly

## Example Daily Workflow

```bash
# Morning: Check status
python scripts/bls/check_quota.py
python scripts/bls/show_status.py

# Update high-priority surveys
python scripts/bls/universal_update.py --surveys CU,AP,PC
# Confirms: Continue? Y

# Check results
python scripts/bls/check_quota.py
python scripts/bls/show_status.py --surveys CU,AP,PC
```

## Summary

The Universal Update System transforms BLS data updates from:
- ❌ Manual, error-prone, quota-unaware
- ❌ Duplicated updates, wasted API calls
- ❌ No tracking of what's current

To:
- ✅ Automated, intelligent, quota-managed
- ✅ Skip already-current series
- ✅ Full visibility into status

Run with confidence knowing:
- Won't exceed quota
- Won't duplicate work
- Can resume anytime
- All activity logged
