# BLS Update System Guide

## Overview

The BLS Update System provides intelligent, quota-aware updating of all BLS surveys through:
- **Sentinel-based freshness detection** - Automatically detect when BLS publishes new data
- **Smart series tracking** - Only update series that need it
- **Daily quota management** - Stay within API limits (500 requests/day)
- **Resume capability** - Interrupted updates continue where they left off
- **Dashboard integration** - Monitor and trigger updates from Admin UI

## Architecture

### Components

1. **Sentinel System** - Lightweight freshness detection
   - Monitors representative series for each survey
   - Detects when BLS publishes new data
   - Automatically resets series status when changes detected

2. **Update Manager** (`src/bls/update_manager.py`)
   - Core update logic used by both CLI and API
   - Tracks series status (`is_current` flag)
   - Handles force update with status reset

3. **CLI Scripts** (`scripts/bls/`)
   - `universal_update.py` - Main update script
   - `check_quota.py` - View API usage
   - `show_status.py` - View series status
   - `reset_status.py` - Manual status reset

4. **Admin Dashboard** (`frontend/src/pages/Dashboard.tsx`)
   - Visual survey status
   - Update and Force Update buttons
   - Real-time progress tracking

## How It Works

### Series Status Tracking

Each series has an `is_current` flag in `bls_series_update_status`:
- `is_current = True` → Series has been updated, will be SKIPPED
- `is_current = False` → Series needs update, will be INCLUDED

### Update Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                     SENTINEL SYSTEM                              │
│  Periodically checks representative series for each survey       │
│  If values changed → BLS published new data                      │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼ (changes detected)
┌─────────────────────────────────────────────────────────────────┐
│                     RESET SERIES STATUS                          │
│  All series in survey → is_current = False                       │
│  Survey marked as needs_full_update = True                       │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     UPDATE EXECUTION                             │
│  Query series where is_current = False                           │
│  Update in batches of 50 series                                  │
│  Mark each updated series → is_current = True                    │
│  Stop when quota reached                                         │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼ (next day, if quota was reached)
┌─────────────────────────────────────────────────────────────────┐
│                     RESUME UPDATE                                │
│  Query series where is_current = False (remaining series)        │
│  Continue updating from where we left off                        │
│  Previously updated series are SKIPPED                           │
└─────────────────────────────────────────────────────────────────┘
```

### Normal Update vs Force Update

**Normal Update** (Dashboard "Update" button or CLI without `--force`):
- Skips series with `is_current = True`
- Only updates series with `is_current = False` or no status record
- Ideal for resuming interrupted updates

**Force Update** (Dashboard "Force" button or CLI with `--force`):
1. First resets ALL series to `is_current = False`
2. Then updates all active series
3. Use when you need a complete refresh

## Quick Start

### CLI Usage

```bash
# Check what needs updating (no API calls)
python scripts/bls/universal_update.py --check-only

# Update specific surveys (resumes if previously interrupted)
python scripts/bls/universal_update.py --surveys CU,AP,EI

# Force update - reset and update all series
python scripts/bls/universal_update.py --surveys CU --force

# Check quota usage
python scripts/bls/check_quota.py

# View update status
python scripts/bls/show_status.py
```

### Dashboard Usage

1. Navigate to Admin Dashboard
2. Each survey card shows:
   - Current status (Current, Needs Update, Updating)
   - Last sentinel check time
   - Last full update time
3. Click **Update** to resume/start update (skips current series)
4. Click **Force** to reset and update all series

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
--force                # Reset status and update all series
--fresh-only           # Only update surveys with sentinel-detected changes
```

**Examples:**
```bash
# Daily routine: Update high-priority surveys
python scripts/bls/universal_update.py --surveys CU,CE,AP,PC

# Only update surveys where sentinel detected new data
python scripts/bls/universal_update.py --fresh-only

# Force complete refresh of CU
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

### 4. `reset_status.py` - Manual Status Reset

Marks series as needing update (sets `is_current = False`).

**Usage:**
```bash
# Reset specific surveys
python scripts/bls/reset_status.py --surveys CU,CE

# With confirmation
python scripts/bls/reset_status.py --surveys CU --confirm
```

**When to use:**
- Sentinel system not detecting changes correctly
- Want to force full re-check manually
- After flat file import

## Sentinel System Details

### How Sentinels Work

Each survey has 50 representative "sentinel" series stored in `bls_survey_sentinels`. These are checked periodically to detect if BLS has published new data.

**Sentinel check process:**
1. Fetch current values for sentinel series from BLS API
2. Compare with stored values (year, period, value)
3. If ANY sentinel has changed → new data published
4. Reset all series in survey to `is_current = False`
5. Mark survey as `needs_full_update = True`

### Sentinel Tables

**`bls_survey_sentinels`**
- `survey_code` - Which survey
- `series_id` - The sentinel series
- `last_value`, `last_year`, `last_period` - Stored values for comparison
- `last_changed_at` - When sentinel last detected change

**`bls_survey_freshness`**
- `survey_code` - Which survey
- `needs_full_update` - Flag set when sentinel detects changes
- `last_bls_update_detected` - When changes were detected
- `last_sentinel_check` - When sentinels were last checked

## Resume Capability

The system correctly resumes interrupted updates across multiple days:

**Day 1:**
- Start updating CE survey (22,049 series)
- Update 12,500 series (250 requests)
- Hit daily quota limit, stop
- Series 1-12,500 have `is_current = True`
- Series 12,501-22,049 have `is_current = False`

**Day 2:**
- Resume update for CE
- Query for `is_current = False` → gets series 12,501-22,049
- Series 1-12,500 are SKIPPED (already current)
- Continue updating remaining 9,549 series

**Key:** The `is_current` flag persists until sentinel detects new data, enabling multi-day updates.

## Quota Management

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

Sentinel may not have detected changes. Options:
1. Check sentinel configuration
2. Manual reset: `python scripts/bls/reset_status.py --surveys CU`
3. Use Force Update from Dashboard

### Update not resuming correctly

Verify series status:
```bash
python scripts/bls/show_status.py --surveys CU --detailed
```

### Quota exceeded mid-update

Don't worry! The system:
1. Stops at quota limit
2. Records what was updated (`is_current = True`)
3. Resumes next day (skips already-current series)

Check status:
```bash
python scripts/bls/check_quota.py
python scripts/bls/show_status.py --surveys CU
```

### Want to force complete refresh

**From CLI:**
```bash
python scripts/bls/universal_update.py --surveys CU --force
```

**From Dashboard:**
Click the "Force" button on the survey card.

Both methods:
1. Reset all series to `is_current = False`
2. Then update all series

## Database Tables

### `bls_series_update_status`
Tracks each series' update status:
- `series_id` - Primary key
- `survey_code` - Which survey
- `is_current` - Whether series is up-to-date
- `last_updated_at` - When last updated

### `bls_survey_sentinels`
Sentinel series for freshness detection

### `bls_survey_freshness`
Survey-level freshness tracking

### `bls_api_usage_log`
Records all API usage for quota tracking

## Best Practices

1. **Let sentinel system work**
   - Avoid manual resets unless necessary
   - Sentinel automatically detects new data

2. **Use normal Update, not Force**
   - Force resets ALL progress
   - Normal update resumes efficiently

3. **Monitor quota daily**
   ```bash
   python scripts/bls/check_quota.py
   ```

4. **Use flat files for large surveys**
   - Faster, no quota impact
   - OE, LA, TU, LN

5. **Keep quota buffer**
   - Set daily-limit to 400 instead of 500
   - Leaves room for ad-hoc queries

## Summary

The BLS Update System provides:
- **Automatic detection** of new BLS data via sentinels
- **Smart updates** that skip already-current series
- **Resume capability** for multi-day large survey updates
- **Dashboard integration** for easy monitoring and control
- **Force update** option when complete refresh needed

Run with confidence knowing:
- Won't exceed quota
- Won't duplicate work
- Can resume anytime
- All activity logged
