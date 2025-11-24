# BLS Update Script Filter Reference

This document lists the available filter parameters for each BLS survey update script.

## How to Use Filters

Filters allow you to update only specific subsets of series within a survey. For example:

```bash
# Update only AP series for area 0000 (U.S. city average)
python scripts/bls/update_ap_latest.py --areas 0000

# Update CE series for specific industries and seasonal adjustment
python scripts/bls/update_ce_latest.py --industries 00000000,10000000 --seasonal S

# Combine filters with other options
python scripts/bls/update_la_latest.py --areas ST0600000000000 --measures 03 --dry-run --limit 100
```

## Filter Parameters by Survey

### AP (Average Price Data)
- `--areas` - Area codes (comma-separated)
- `--items` - Item codes (comma-separated)

**Example:**
```bash
python scripts/bls/update_ap_latest.py --areas 0000 --items 701111,703111
```

### CU (Consumer Price Index - All Urban Consumers)
- `--areas` - Area codes (comma-separated)
- `--items` - Item codes (comma-separated)
- `--seasonal` - Seasonal adjustment: S or U

**Example:**
```bash
python scripts/bls/update_cu_latest.py --areas 0000 --items SA0,SAH --seasonal S
```

### CW (Consumer Price Index - Urban Wage Earners)
- `--areas` - Area codes (comma-separated)
- `--items` - Item codes (comma-separated)
- `--seasonal` - Seasonal adjustment: S or U

### SU (Chained Consumer Price Index)
- `--areas` - Area codes (comma-separated)
- `--items` - Item codes (comma-separated)
- `--seasonal` - Seasonal adjustment: S or U

### LA (Local Area Unemployment Statistics)
- `--areas` - Area codes (comma-separated)
- `--measures` - Measure codes (comma-separated)
- `--seasonal` - Seasonal adjustment: S or U

**Example:**
```bash
python scripts/bls/update_la_latest.py --areas ST0600000000000 --measures 03,04,05
```

### CE (Current Employment Statistics)
- `--supersectors` - Supersector codes (comma-separated)
- `--industries` - Industry codes (comma-separated)
- `--data-types` - Data type codes (comma-separated)
- `--seasonal` - Seasonal adjustment: S or U

**Example:**
```bash
python scripts/bls/update_ce_latest.py --supersectors 00,10 --seasonal S
```

### PC (Producer Price Index - Commodity)
- `--industries` - Industry codes (comma-separated)
- `--products` - Product codes (comma-separated)

### WP (Producer Price Index)
- `--groups` - Group codes (comma-separated)
- `--items` - Item codes (comma-separated)

### SM (State and Metro Area Employment)
- `--states` - State codes (comma-separated)
- `--areas` - Area codes (comma-separated)
- `--supersectors` - Supersector codes (comma-separated)
- `--industries` - Industry codes (comma-separated)
- `--data-types` - Data type codes (comma-separated)
- `--seasonal` - Seasonal adjustment: S or U

**Example:**
```bash
python scripts/bls/update_sm_latest.py --states 06 --seasonal S
```

### JT (JOLTS - Job Openings and Labor Turnover Survey)
- `--industries` - Industry codes (comma-separated)
- `--states` - State codes (comma-separated)
- `--areas` - Area codes (comma-separated)
- `--size-classes` - Size class codes (comma-separated)
- `--data-elements` - Data element codes (comma-separated)
- `--rate-levels` - Rate/level codes (comma-separated)
- `--seasonal` - Seasonal adjustment: S or U

### EC (Employment Cost Index)
- `--compensations` - Compensation codes (comma-separated)
- `--groups` - Group codes (comma-separated)
- `--ownerships` - Ownership codes (comma-separated)
- `--seasonal` - Seasonal adjustment: S or U

### OE (Occupational Employment and Wage Statistics)
- `--area-types` - Area type codes (comma-separated)
- `--industries` - Industry codes (comma-separated)
- `--occupations` - Occupation codes (comma-separated)
- `--data-types` - Data type codes (comma-separated)
- `--states` - State codes (comma-separated)
- `--areas` - Area codes (comma-separated)
- `--sectors` - Sector codes (comma-separated)

**Example:**
```bash
# IMPORTANT: Always use --limit with OE to avoid massive API usage
python scripts/bls/update_oe_latest.py --states 06 --limit 100
```

### PR (Major Sector Productivity and Costs)
- `--sectors` - Sector codes (comma-separated)
- `--classes` - Class codes (comma-separated)
- `--measures` - Measure codes (comma-separated)
- `--durations` - Duration codes (comma-separated)
- `--seasonal` - Seasonal adjustment: S

### TU (American Time Use Survey)
- `--stat-types` - Statistic type codes (comma-separated)
- `--sex` - Sex codes (comma-separated)
- `--regions` - Region codes (comma-separated)
- `--labor-force-status` - Labor force status codes (comma-separated)
- `--activities` - Activity codes (comma-separated)

### IP (Industry Productivity)
- `--sectors` - Sector codes (comma-separated)
- `--industries` - Industry codes (comma-separated)
- `--measures` - Measure codes (comma-separated)
- `--durations` - Duration codes (comma-separated)
- `--types` - Type codes (comma-separated)
- `--areas` - Area codes (comma-separated)

### LN (Labor Force Statistics from Current Population Survey)
- `--labor-force-status` - Labor force status codes (comma-separated)
- `--ages` - Age group codes (comma-separated)
- `--sex` - Sex codes (comma-separated)
- `--race` - Race codes (comma-separated)
- `--education` - Education codes (comma-separated)
- `--occupations` - Occupation codes (comma-separated)
- `--industries` - Industry codes (comma-separated)
- `--seasonal` - Seasonal adjustment: S or U

### EI (Import/Export Price Indexes)
- `--indexes` - Index codes (comma-separated)
- `--seasonal` - Seasonal adjustment: S or U

### BD (Business Employment Dynamics)
- `--states` - State codes (comma-separated)
- `--industries` - Industry codes (comma-separated)
- `--unit-analysis` - Unit of analysis codes (comma-separated)
- `--data-elements` - Data element codes (comma-separated)
- `--size-classes` - Size class codes (comma-separated)
- `--data-classes` - Data class codes (comma-separated)
- `--rate-levels` - Rate/level codes (comma-separated)
- `--seasonal` - Seasonal adjustment: S or U

## Common Filter Patterns

### Seasonal vs Non-Seasonal
Many surveys offer both seasonally adjusted (S) and non-seasonally adjusted (U) data:
```bash
# Get only seasonally adjusted series
python scripts/bls/update_cu_latest.py --seasonal S

# Get only non-adjusted series
python scripts/bls/update_cu_latest.py --seasonal U
```

### Geographic Filters
Surveys with geographic dimensions (areas, states):
```bash
# Update only national data
python scripts/bls/update_ap_latest.py --areas 0000

# Update specific state
python scripts/bls/update_sm_latest.py --states 06  # California
```

### Multiple Filter Values
All filters accept comma-separated lists:
```bash
python scripts/bls/update_ce_latest.py --industries 00000000,10000000,20000000
```

## Combining Filters with Other Options

Filters work seamlessly with other script options:

```bash
# Preview with filters
python scripts/bls/update_ap_latest.py --areas 0000 --items 701111 --dry-run

# Limit number of series (useful for testing)
python scripts/bls/update_la_latest.py --measures 03 --limit 50

# Force update even if series are marked current
python scripts/bls/update_cu_latest.py --areas 0000 --force

# Update specific year range
python scripts/bls/update_ce_latest.py --seasonal S --start-year 2023 --end-year 2025
```

## Integration with Tracking System

Filters work with the series status tracking system:

1. **Filters are applied BEFORE status checking** - The script first filters series by your criteria, then checks which of those filtered series need updates
2. **Status tracking works per series** - If you filter to 100 series and 50 are already current, only 50 will be updated
3. **Use `--force` to override** - If you want to update filtered series regardless of status:
   ```bash
   python scripts/bls/update_cu_latest.py --areas 0000 --force
   ```

## Finding Valid Filter Codes

To find valid codes for filters, query the corresponding reference tables:

```python
from sqlalchemy import create_engine
from database.bls_models import CUArea, CUItem, LAMeasure
from config import settings

engine = create_engine(settings.database.url)

# Find CU area codes
with engine.connect() as conn:
    result = conn.execute("SELECT area_code, area_name FROM bls_cu_areas LIMIT 10")
    for row in result:
        print(f"{row.area_code}: {row.area_name}")

# Find LA measure codes
with engine.connect() as conn:
    result = conn.execute("SELECT measure_code, measure_name FROM bls_la_measures")
    for row in result:
        print(f"{row.measure_code}: {row.measure_name}")
```

Or use the BLS website documentation for each survey.

## Best Practices

1. **Start with `--dry-run`** - Always preview what will be updated before running actual updates
2. **Use `--limit` for testing** - Test filters with a small number of series first
3. **Combine geographic and data type filters** - For example, filter by both state and seasonal adjustment
4. **Be careful with OE** - Always use `--limit` with the OE survey due to its massive size
5. **Check filter results** - The script will print how many series matched your filters

## Examples by Use Case

### Update only national-level data
```bash
python scripts/bls/update_cu_latest.py --areas 0000
python scripts/bls/update_ap_latest.py --areas 0000
```

### Update employment data for California
```bash
python scripts/bls/update_sm_latest.py --states 06
python scripts/bls/update_la_latest.py --areas ST0600000000000
```

### Update only unemployment rates
```bash
python scripts/bls/update_la_latest.py --measures 03
```

### Update seasonally adjusted employment
```bash
python scripts/bls/update_ce_latest.py --seasonal S
```

### Test with small subset
```bash
python scripts/bls/update_cu_latest.py --areas 0000 --items SA0 --limit 10 --dry-run
```
