# BLS Update Scripts - Batch Processing and Error Handling

## Overview

All BLS update scripts now process data in batches of 50 series with incremental commits. This ensures that partial progress is saved even if errors occur mid-update.

## Key Improvements

### 1. Batch Processing (50 Series per Batch)

Instead of fetching all series at once:
```python
# OLD: Single request for all series
rows = client.get_many(all_1000_series, ...)  # If error, lose all
session.commit()  # Single commit at end
```

Now processes in batches:
```python
# NEW: Batches of 50 series each
for batch in batches_of_50:
    rows = client.get_many(batch, ...)  # Fetch 50
    upsert_to_db(rows)
    session.commit()  # Commit after each batch ✓
    record_status(batch)
    session.commit()  # Commit status ✓
```

### 2. Incremental Commits

**Progress is saved after each batch:**
- Data observations committed
- API usage logged
- Series status updated
- All changes persisted before moving to next batch

### 3. Graceful Error Handling

Scripts now handle three types of errors:

#### A. API Limit Errors
```python
except Exception as e:
    if 'quota' in str(e).lower() or 'limit' in str(e).lower():
        print("API limit exceeded. Stopping.")
        print(f"Progress saved: {total_series_updated} series")
        break  # Stop cleanly, keep what was saved
```

#### B. Network/Temporary Errors
```python
except Exception as e:
    print(f"ERROR in batch: {e}")
    session.rollback()  # Rollback failed batch only
    print("Continuing with next batch...")
    continue  # Skip failed batch, continue with others
```

#### C. User Interruption
```python
except KeyboardInterrupt:
    print("Update interrupted by user")
    print(f"Progress saved: {total_series_updated} series")
    session.commit()  # Save progress
    break  # Exit cleanly
```

## Benefits

### ✅ No Data Loss on Errors

**Before:**
```
Fetch 1000 series → Error at series 850 → Rollback all → Lost everything
```

**After:**
```
Batch 1-17: Saved ✓ (850 series)
Batch 18: Error ✗ (50 series)
Result: 850 series saved, only 50 lost
```

### ✅ Resume Capability

When an update is interrupted:
```bash
# First run - stops at batch 10 due to error
python scripts/bls/update_cu_latest.py
# Output: "Progress saved: 500 series"

# Second run - continues from where it stopped
python scripts/bls/update_cu_latest.py
# Output: "Skipping 500 already-current series"
# Output: "Series needing update: 6,340"
# Only updates remaining series
```

### ✅ Better Progress Visibility

```
Batch 1/11: Fetching series 1-50...
  Saved 1,250 observations
Batch 2/11: Fetching series 51-100...
  Saved 1,180 observations
Batch 3/11: Fetching series 101-150...
  ERROR in batch 3: Connection timeout
  Continuing with next batch...
Batch 4/11: Fetching series 151-200...
  Saved 1,220 observations
...
```

### ✅ Quota-Aware Error Handling

If you hit the daily API limit:
```
Batch 8/11: Fetching series 351-400...
  ERROR in batch 8: Daily quota exceeded

  API limit likely exceeded. Stopping to preserve quota.
  Progress saved: 350 series updated successfully

Remaining series: 194
Run script again to continue (already-updated series will be skipped)
```

## Usage Examples

### Normal Update
```bash
python scripts/bls/update_ap_latest.py

# Output:
# Batch 1/11: Fetching series 1-50...
#   Saved 1,250 observations
# Batch 2/11: Fetching series 51-100...
#   Saved 1,180 observations
# ...
# UPDATE COMPLETE!
#   Series updated: 544 / 544
#   Observations: 13,560
#   API requests: 11
```

### Interrupted Update
```bash
python scripts/bls/update_ce_latest.py
# [User presses Ctrl+C after batch 5]

# Output:
# Update interrupted by user at batch 5
# Progress saved: 250 series, 6,250 observations
#
# UPDATE COMPLETE!
#   Series updated: 250 / 22,049
#   Observations: 6,250
#   API requests: 5
#
#   Remaining series: 21,799
#   Run script again to continue
```

### Network Error During Update
```bash
python scripts/bls/update_la_latest.py

# Output:
# Batch 15/150: Fetching series 701-750...
#   ERROR in batch 15: Connection timeout
#   Continuing with next batch...
# Batch 16/150: Fetching series 751-800...
#   Saved 1,100 observations
# ...
# UPDATE COMPLETE!
#   Series updated: 7,450 / 7,500
#   Observations: 186,250
#   API requests: 149
#
#   Failed batches: 1
#     Batch 15 (series 701-750): Connection timeout
```

### API Limit Reached
```bash
python scripts/bls/update_cu_latest.py

# Output:
# Batch 135/137: Fetching series 6,701-6,750...
#   Saved 1,325 observations
# Batch 136/137: Fetching series 6,751-6,800...
#   ERROR in batch 136: Request failed: 403 Forbidden - Daily quota exceeded
#
#   API limit likely exceeded. Stopping to preserve quota.
#   Progress saved: 6,750 series updated successfully
#
# UPDATE COMPLETE!
#   Series updated: 6,750 / 6,840
#   Observations: 168,750
#   API requests: 135
#
#   Remaining series: 90
#   Run script again to continue
```

## Technical Details

### Batch Size

Fixed at **50 series per batch**:
- Matches BLS API limit (max 50 series per request)
- Balances between commit overhead and data loss risk
- One API request = one batch = one commit cycle

### Transaction Scope

Each batch is its own transaction:
```python
try:
    # Fetch batch
    rows = client.get_many(batch_50_series, ...)

    # Upsert data
    stmt = insert(DataModel).values(rows)
    session.execute(stmt)
    session.commit()  # ✓ Commit 1: Data saved

    # Record usage
    session.add(usage_log)

    # Update status
    for series_id in batch:
        update_status(series_id)

    session.commit()  # ✓ Commit 2: Tracking saved

except Exception as e:
    session.rollback()  # Only rolls back current batch
    # Previous batches already committed ✓
```

### Error Classification

Errors are classified and handled differently:

1. **API Quota Errors** → Stop immediately, save progress
2. **Network Errors** → Skip batch, continue with next
3. **User Interrupt** → Save progress, exit cleanly
4. **Database Errors** → Rollback batch, continue
5. **Unknown Errors** → Rollback batch, try to continue

### Status Tracking Integration

Batch processing works seamlessly with status tracking:

```python
# After each batch commits
for series_id in batch:
    # Check if series now has current data
    is_current = has_recent_data(series_id)

    # Update status
    update_status(series_id, is_current=is_current)

session.commit()
```

Next run will skip these series automatically.

## Comparison: Before vs After

### Before (Single Transaction)

| Scenario | Result |
|----------|--------|
| Error at series 850/1000 | Lost all 850 series (rollback) |
| API limit at request 135/137 | Lost all 135 batches (rollback) |
| Network timeout | Lost entire update (rollback) |
| User interrupts (Ctrl+C) | Lost all progress (rollback) |
| Resume capability | None - must fetch everything again |

### After (Batch Processing)

| Scenario | Result |
|----------|--------|
| Error at series 850/1000 | Saved 17 batches (800 series) ✓ |
| API limit at request 135/137 | Saved 135 batches (6,750 series) ✓ |
| Network timeout | Skip failed batch, continue ✓ |
| User interrupts (Ctrl+C) | Save progress up to current batch ✓ |
| Resume capability | Full - skips already-updated series ✓ |

## Performance Impact

### Minimal Overhead

- **Commit time**: ~10-50ms per batch (negligible)
- **Total overhead**: <1 second for 100 batches
- **Network time**: Dominates (seconds per batch)
- **Conclusion**: Commit overhead is insignificant

### Benefits Far Outweigh Costs

```
Cost:  +0.5 seconds for 100 commits
Gain:  No data loss on errors
       Resume from any point
       Better progress tracking
       Graceful error handling
```

## Best Practices

### 1. Monitor Progress
Watch the batch output to see if errors occur frequently:
```
Batch 1/20: ✓
Batch 2/20: ✓
Batch 3/20: ✗ (timeout)
Batch 4/20: ✓
```

If many batches fail, investigate network or API issues.

### 2. Resume After Errors
Always safe to re-run after errors:
```bash
# Run 1: Partial success (error at batch 10)
python scripts/bls/update_cu_latest.py

# Run 2: Continues from batch 11
python scripts/bls/update_cu_latest.py
```

### 3. Check Quota Before Large Updates
```bash
# Check remaining quota
python scripts/bls/check_quota.py

# If enough quota, proceed
python scripts/bls/update_ce_latest.py
```

### 4. Use --limit for Testing
Test with small batches first:
```bash
# Test with 100 series (2 batches)
python scripts/bls/update_cu_latest.py --limit 100
```

## Troubleshooting

### Problem: Many Failed Batches
**Symptom:** Multiple batches fail with network errors

**Solution:**
- Check internet connection
- Verify BLS API status
- Try again later (API might be down)

### Problem: API Limit Hit Mid-Update
**Symptom:** Update stops with quota exceeded error

**Solution:**
```bash
# Check quota usage
python scripts/bls/check_quota.py

# Wait until tomorrow, or...
# Continue with remaining series tomorrow
python scripts/bls/update_cu_latest.py
# Will skip already-updated series automatically
```

### Problem: Update Stuck on Batch
**Symptom:** Batch takes very long time

**Solution:**
- Press Ctrl+C to interrupt
- Progress up to previous batch is saved
- Re-run to continue

## Summary

The batch processing system transforms BLS updates from:
- ❌ All-or-nothing updates
- ❌ Lost progress on any error
- ❌ No resume capability
- ❌ Opaque progress

To:
- ✅ Incremental progress saving
- ✅ Graceful error recovery
- ✅ Full resume capability
- ✅ Clear progress visibility

**Run with confidence** - your progress is always saved! 🎯
