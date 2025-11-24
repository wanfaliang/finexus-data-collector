# BLS Sentinel-Based Freshness Detection System

## Overview

The Sentinel System is an intelligent freshness detection mechanism that efficiently monitors when the Bureau of Labor Statistics (BLS) publishes new data for each survey, without wasting API quota on unnecessary checks.

### The Problem

Previously, the system used time-based windows (24 hours) to determine which series needed updates. This approach had critical flaws:

1. **Infinite Loop for Large Surveys**: Surveys requiring multiple days to update (e.g., LA survey with 90,000 series needs 4 days) would never complete because Day 2 would re-check Day 1's series after the 24-hour window expired
2. **API Quota Waste**: Checking all series even when BLS hasn't published new data
3. **No Visibility**: Users couldn't see when BLS actually updated data
4. **No Control**: Time windows provided poor transparency for large-scale operations

### The Solution

The Sentinel System uses a small, representative sample of 50 series per survey to detect when BLS publishes new data:

1. **Select 50 Sentinel Series**: Choose representative series once per survey
2. **Check Sentinels** (1 API request): Fetch current values for 50 series
3. **Compare Values**: Detect if any sentinels changed
4. **Trigger Full Update**: Only update entire survey when changes detected
5. **Efficiency**: 1 request to check vs 100s-1000s to blindly update

## Architecture

### Database Tables

#### `bls_survey_sentinels`
Stores the 50 sentinel series for each survey with their baseline values.

```sql
CREATE TABLE bls_survey_sentinels (
    survey_code VARCHAR(5),
    series_id VARCHAR(30),
    sentinel_order INTEGER,
    selection_reason VARCHAR(50),

    -- Stored values for comparison
    last_value NUMERIC(20, 6),
    last_year SMALLINT,
    last_period VARCHAR(5),
    last_footnotes VARCHAR(500),

    -- Tracking
    last_checked_at TIMESTAMP,
    last_changed_at TIMESTAMP,
    check_count INTEGER DEFAULT 0,
    change_count INTEGER DEFAULT 0,

    -- Metadata
    created_at TIMESTAMP,
    updated_at TIMESTAMP,

    PRIMARY KEY (survey_code, series_id)
);
```

#### `bls_survey_freshness`
Tracks freshness status for each survey.

```sql
CREATE TABLE bls_survey_freshness (
    survey_code VARCHAR(5) PRIMARY KEY,

    -- Freshness detection
    last_bls_update_detected TIMESTAMP,
    last_sentinel_check TIMESTAMP,
    sentinels_changed INTEGER DEFAULT 0,
    sentinels_total INTEGER DEFAULT 50,

    -- Update status
    needs_full_update BOOLEAN DEFAULT false,
    last_full_update_started TIMESTAMP,
    last_full_update_completed TIMESTAMP,
    full_update_in_progress BOOLEAN DEFAULT false,
    series_updated_count INTEGER DEFAULT 0,
    series_total_count INTEGER DEFAULT 0,

    -- Statistics
    bls_update_frequency_days NUMERIC(5, 2),
    total_checks INTEGER DEFAULT 0,
    total_updates_detected INTEGER DEFAULT 0,

    -- Metadata
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);
```

## Workflow

### 1. Initial Setup

Select 50 sentinel series for each survey:

```bash
# Select sentinels for all surveys
python scripts/bls/select_sentinels.py

# Select for specific surveys
python scripts/bls/select_sentinels.py --surveys CU,CE,AP

# Re-select (force override existing)
python scripts/bls/select_sentinels.py --surveys CU --force
```

**Selection Strategy:**
- 20 national/aggregate series (area_code='0000', etc.)
- 20 geographically diverse series
- 10 random series for edge case coverage

### 2. Regular Freshness Checks

Check if BLS has published new data:

```bash
# Check all surveys
python scripts/bls/check_freshness.py

# Check specific surveys
python scripts/bls/check_freshness.py --surveys CU,CE

# Show detailed changes
python scripts/bls/check_freshness.py --verbose

# Force recheck (ignore recent check window)
python scripts/bls/check_freshness.py --skip-recent 0
```

**What Happens:**
1. Fetches current values for 50 sentinels (1 API request)
2. Compares with stored baseline values
3. Detects changes in:
   - Year
   - Period
   - Value
   - Footnotes
4. Updates `needs_full_update` flag if changes detected

### 3. View Freshness Status

See which surveys need updates:

```bash
# View summary of all surveys
python scripts/bls/show_freshness.py

# View only surveys needing updates
python scripts/bls/show_freshness.py --needs-update

# View detailed information
python scripts/bls/show_freshness.py --detail

# View specific surveys
python scripts/bls/show_freshness.py --surveys CU,CE --detail
```

**Output Example:**
```
================================================================================
BLS DATA FRESHNESS STATUS
================================================================================

Survey Status          Last BLS Update      Last Check           Sentinels
--------------------------------------------------------------------------------
CU     🔔 Needs update 2 days ago           1 hour ago           50/50
CE     ✓ Current       5 days ago           2 hours ago          50/50
AP     ✓ Current       1 week ago           3 hours ago          50/50
LA     🔔 Needs update 1 day ago            30 minutes ago       50/50
```

### 4. Update Fresh Surveys

Update only surveys with detected changes:

```bash
# Update only surveys flagged by sentinel system
python scripts/bls/universal_update.py --fresh-only

# Or manually specify surveys
python scripts/bls/universal_update.py --surveys CU,LA
```

**What Happens:**
1. Filters to surveys where `needs_full_update = true`
2. Updates all series in those surveys
3. Marks `needs_full_update = false` when complete
4. Tracks progress in `series_updated_count`

## Benefits

### ✅ Massive Efficiency Gains

**Before (Time Windows):**
- Check all 6,840 CU series every 24 hours = 137 API requests
- May update even when no new BLS data published
- Wastes quota on surveys already current

**After (Sentinel System):**
- Check 50 sentinels = 1 API request
- Only update when BLS actually publishes new data
- **137x more efficient** for checking!

### ✅ Solves Multi-Day Survey Problem

**LA Survey Example (90,000 series, needs 4 days):**

**Before:**
```
Day 1: Update series 1-22,500     (450 requests)
Day 2: Re-check series 1-22,500 (24h passed) → INFINITE LOOP
```

**After:**
```
Day 1: Check 50 sentinels (1 request) → Changes detected
Day 1-4: Update all 90,000 series
After: needs_full_update = false
Future: Only update when sentinels detect new BLS data
```

### ✅ Transparency & Control

**Visibility:**
- "Last BLS update: Jan 15, 2025" (actual BLS status)
- vs "Last checked: 2 days ago" (your check time)

**Control:**
- See exactly which surveys need updates
- Choose when to update based on BLS freshness
- Track update progress for large surveys

### ✅ Resume Capability

Updates can be interrupted and resumed:

```bash
# Start update
python scripts/bls/universal_update.py --fresh-only

# [Interrupted at 50% progress]

# Resume (automatically continues where left off)
python scripts/bls/universal_update.py --fresh-only
```

Progress is tracked in `series_updated_count` field.

## Best Practices

### Regular Monitoring Schedule

**Recommended Schedule:**
```bash
# Daily: Check for freshness (1 request per survey = ~18 requests)
0 8 * * * python scripts/bls/check_freshness.py

# Daily: View status
0 9 * * * python scripts/bls/show_freshness.py --needs-update

# As needed: Update fresh surveys
# (Run manually after reviewing status, or automate if desired)
```

### Sentinel Selection Guidelines

**When to Re-select Sentinels:**
- Sentinel series become inactive
- BLS changes survey structure
- Want to improve detection accuracy

**How Often:**
- Typically once at setup
- Re-select if >10% of sentinels inactive
- Can run `--force` to override existing

### API Quota Management

**Daily Budget Allocation:**
```
500 requests/day total:
  - 18 requests: Daily freshness checks (all surveys)
  - 482 requests: Available for updates (~24,100 series)
```

**Efficiency:**
- Sentinels use <4% of daily quota for monitoring
- 96% available for actual data updates
- Updates only triggered when BLS publishes new data

## Comparison: Before vs After

### Scenario 1: Normal Daily Check

**Before (24-Hour Windows):**
```
Goal: Check if CU survey needs update
- Check database for series last updated >24h ago
- May find 6,840 series "need" update
- Update all 6,840 series (137 requests)
- But BLS may not have published new data!
- Waste: 137 requests
```

**After (Sentinel System):**
```
Goal: Check if CU survey needs update
- Fetch 50 sentinels from BLS (1 request)
- Compare with stored values
- No changes detected → Survey is current
- Waste: 0 requests (only 1 to check)
```

**Savings: 136 requests (99.3% reduction)**

### Scenario 2: Large Survey Update (LA - 90,000 series)

**Before (24-Hour Windows):**
```
Day 1: Update 22,500 series (450 requests)
       Quota exhausted
Day 2: Series from Day 1 now "stale" (>24h)
       Update 22,500 series again (450 requests)
       INFINITE LOOP - never completes all 90,000
```

**After (Sentinel System):**
```
Check: 50 sentinels show changes (1 request)
Day 1-4: Update all 90,000 series (1,800 requests)
Completion: Mark needs_full_update = false
Future: Only update when sentinels detect changes
```

**Result: Actually completes! No infinite loop.**

## Troubleshooting

### Problem: No Sentinels Configured

**Symptom:**
```
Status: Not configured
Run 'python scripts/bls/select_sentinels.py' to set up monitoring
```

**Solution:**
```bash
python scripts/bls/select_sentinels.py --surveys CU
```

### Problem: Sentinels Not Detecting Changes

**Symptom:** BLS published new data but sentinels show no changes

**Possible Causes:**
1. Sentinels not representative enough
2. BLS updated different series first
3. Timing: BLS update not yet propagated

**Solution:**
```bash
# Re-select sentinels with force
python scripts/bls/select_sentinels.py --surveys CU --force

# Or manually check a few known-active series
python scripts/bls/universal_update.py --surveys CU --limit 100
```

### Problem: Update Stuck "In Progress"

**Symptom:** `full_update_in_progress = true` but update not running

**Cause:** Update was interrupted/crashed without cleanup

**Solution:**
```sql
-- Reset the flag manually
UPDATE bls_survey_freshness
SET full_update_in_progress = false
WHERE survey_code = 'CU';
```

Or via Python:
```python
from sqlalchemy import create_engine, update
from database.bls_tracking_models import BLSSurveyFreshness
from config import settings

engine = create_engine(settings.database.url)
with engine.begin() as conn:
    stmt = update(BLSSurveyFreshness).where(
        BLSSurveyFreshness.survey_code == 'CU'
    ).values(full_update_in_progress=False)
    conn.execute(stmt)
```

### Problem: False Positives (Too Many Change Detections)

**Symptom:** Sentinels frequently show changes when data hasn't actually updated

**Possible Causes:**
1. Sentinels include volatile series
2. BLS revises historical data frequently

**Solution:**
```bash
# Re-select with emphasis on stable national series
python scripts/bls/select_sentinels.py --surveys CU --force
```

Consider manually selecting stable flagship series as sentinels.

## Advanced Usage

### Custom Sentinel Selection

For manual control, populate `bls_survey_sentinels` directly:

```python
from database.bls_tracking_models import BLSSurveySentinel

# Define custom sentinels
custom_sentinels = [
    'CUSR0000SA0',  # All items CPI
    'CUSR0000SAF',  # Food CPI
    'CUSR0000SAH',  # Housing CPI
    # ... (define 50 total)
]

# Insert manually
for order, series_id in enumerate(custom_sentinels, 1):
    sentinel = BLSSurveySentinel(
        survey_code='CU',
        series_id=series_id,
        sentinel_order=order,
        selection_reason='manual_selection'
    )
    session.add(sentinel)
session.commit()

# Then fetch initial values
python scripts/bls/select_sentinels.py --surveys CU --force
```

### Integration with Scheduler

Example cron setup:

```bash
# /etc/cron.d/bls-updates

# Check freshness every 6 hours
0 */6 * * * user cd /path && python scripts/bls/check_freshness.py

# Send daily status report
0 9 * * * user cd /path && python scripts/bls/show_freshness.py --needs-update | mail -s "BLS Status" admin@example.com

# Auto-update on weekends (when quota is fresh)
0 2 * * 0 user cd /path && python scripts/bls/universal_update.py --fresh-only
```

### Monitoring Metrics

Track sentinel system health:

```sql
-- Surveys without sentinels
SELECT survey_code
FROM bls_survey_freshness
WHERE sentinels_total < 50;

-- Sentinels that never change (possibly inactive)
SELECT survey_code, series_id, check_count, change_count
FROM bls_survey_sentinels
WHERE check_count > 20 AND change_count = 0;

-- Average update frequency by survey
SELECT
    survey_code,
    bls_update_frequency_days,
    total_updates_detected,
    total_checks,
    ROUND(100.0 * total_updates_detected / NULLIF(total_checks, 0), 2) as change_rate_pct
FROM bls_survey_freshness
ORDER BY change_rate_pct DESC;
```

## Summary

The Sentinel System transforms BLS data updates from:

❌ **Time-Based (Broken)**
- Infinite loops for large surveys
- Wastes API quota
- No visibility into BLS status
- Poor control

✅ **Intelligence-Based (Working)**
- Completes all surveys reliably
- 99% reduction in check overhead
- Clear BLS freshness visibility
- Full transparency and control

**Run with confidence** - the system knows when BLS actually has new data! 🎯
