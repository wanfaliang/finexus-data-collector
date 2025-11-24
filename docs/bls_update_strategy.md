# BLS Data Update Strategy

This document provides recommendations for updating BLS survey data based on series counts and API constraints.

## API Constraints

**BLS API Limits (with API key):**
- Daily limit: 500 requests/day
- Annual limit: 25,000 requests/year
- Each request can fetch up to 50 series

## Survey Update Recommendations

### ✅ Safe for Regular API Updates (< 100 requests)

| Survey | Series Count | API Requests | Update Frequency |
|--------|-------------|--------------|------------------|
| AP     | 544         | 11           | Weekly/Monthly   |
| PC     | 3,839       | 77           | Monthly          |
| WP     | 4,504       | 91           | Monthly          |
| JT     | 1,984       | 40           | Monthly          |
| PR     | 237         | 5            | Quarterly        |
| SU     | 29          | 1            | Monthly          |
| EI     | 1,625       | 33           | Monthly          |

**Recommendation:** These surveys can be updated regularly via API with minimal impact on quota.

### ⚠️ Feasible but Monitor Quota (100-500 requests)

| Survey | Series Count | API Requests | Update Frequency |
|--------|-------------|--------------|------------------|
| CU     | 6,840       | 137          | Monthly          |
| CE     | 22,049      | 441          | Monthly          |
| SM     | 23,876      | 478          | Monthly          |
| IP     | 15,112      | 303          | Quarterly        |
| CW     | 6,640       | 133          | Monthly          |

**Recommendation:** Can be updated via API but monitor your quota usage. Takes ~1 day per update.

### 🔶 Challenging - Use Sparingly (500-2,500 requests)

| Survey | Series Count | API Requests | Update Frequency |
|--------|-------------|--------------|------------------|
| LA     | 33,725      | 675          | Quarterly/Annual |
| TU     | 87,361      | 1,748        | Annual           |
| LN     | 65,566      | 1,312        | Quarterly/Annual |
| BD     | 34,464      | 690          | Quarterly        |

**Recommendation:** API updates possible but expensive. Takes multiple days. Consider flat file downloads for major updates.

### ❌ NOT Feasible - Use Flat Files Only (> 2,500 requests)

| Survey | Series Count | API Requests | Days Needed | % of Annual Quota |
|--------|-------------|--------------|-------------|-------------------|
| OE     | 6,036,958   | 120,740      | 241 days    | 507%              |

**Recommendation:** **NEVER use API updates for OE.** Always use flat file downloads from:
- https://download.bls.gov/pub/time.series/oe/

## Update Strategies by Use Case

### Strategy 1: Regular Monitoring (Monthly)
**Target:** Keep small/medium surveys up-to-date
**Quota usage:** ~300 requests/month (~10/day)

```bash
# Safe surveys (monthly updates)
python scripts/bls/update_ap_latest.py
python scripts/bls/update_pc_latest.py
python scripts/bls/update_wp_latest.py
python scripts/bls/update_jt_latest.py
python scripts/bls/update_ei_latest.py
python scripts/bls/update_su_latest.py
python scripts/bls/update_pr_latest.py

# Feasible surveys (monthly)
python scripts/bls/update_cu_latest.py
python scripts/bls/update_cw_latest.py
```

### Strategy 2: Quarterly Updates
**Target:** Medium surveys
**Quota usage:** ~1,000 requests/quarter (~11/day)

```bash
# Add to monthly updates:
python scripts/bls/update_ce_latest.py
python scripts/bls/update_sm_latest.py
python scripts/bls/update_ip_latest.py
python scripts/bls/update_bd_latest.py
```

### Strategy 3: Annual Major Updates
**Target:** Large surveys via flat files
**Method:** Download and load flat files

```bash
# Download latest flat files from BLS
# OE - REQUIRED (too large for API)
wget -r -np -nH --cut-dirs=3 https://download.bls.gov/pub/time.series/oe/

# LA, TU, LN - RECOMMENDED (more efficient than API)
wget -r -np -nH --cut-dirs=3 https://download.bls.gov/pub/time.series/la/
wget -r -np -nH --cut-dirs=3 https://download.bls.gov/pub/time.series/tu/
wget -r -np -nH --cut-dirs=3 https://download.bls.gov/pub/time.series/ln/

# Load using flat file loaders
python scripts/bls/load_oe_flat_files.py --data-files oe.data.0.Current
python scripts/bls/load_la_flat_files.py --data-files la.data.0.Current
python scripts/bls/load_tu_flat_files.py --data-files tu.data.0.Current
python scripts/bls/load_ln_flat_files.py --data-files ln.data.0.Current
```

## Quota Management

### Daily Quota Allocation
With 500 requests/day, you can:
- Update all "Safe" surveys daily (< 200 requests)
- Update 1-2 "Feasible" surveys daily
- Or save quota for occasional large updates

### Annual Quota Allocation
With 25,000 requests/year, you can:
- Monthly updates of Safe + Feasible surveys: ~300 × 12 = 3,600 requests
- Quarterly updates of Challenging surveys: ~4,400 × 4 = 17,600 requests
- **Total:** ~21,200 requests/year (85% of quota)
- Leaves buffer for ad-hoc queries

## Best Practices

1. **Use --dry-run first** to preview request counts:
   ```bash
   python scripts/bls/update_cu_latest.py --dry-run
   ```

2. **Use --limit for testing:**
   ```bash
   python scripts/bls/update_ce_latest.py --limit 100 --dry-run
   ```

3. **Monitor your quota usage** - Keep track of daily/annual requests

4. **Prefer flat files for:**
   - Initial data loads
   - Major backfills
   - OE survey (always)
   - Large surveys (LA, TU, LN) for annual updates

5. **Use API updates for:**
   - Regular incremental updates (new months/quarters)
   - Small/medium surveys
   - Specific series monitoring

## Flat File Download Locations

All BLS flat files available at:
```
https://download.bls.gov/pub/time.series/{SURVEY_CODE}/
```

Examples:
- OE: https://download.bls.gov/pub/time.series/oe/
- LA: https://download.bls.gov/pub/time.series/la/
- BD: https://download.bls.gov/pub/time.series/bd/
- CU: https://download.bls.gov/pub/time.series/cu/

## Recommended Update Schedule

**Weekly:**
- None (too expensive on quota)

**Monthly:**
- AP, PC, WP, JT, EI, SU, PR (safe surveys)
- CU, CW (consumer price indexes - high value)

**Quarterly:**
- CE, SM, IP (employment surveys)
- BD (business dynamics)
- LN (labor force - or use flat files)

**Annual:**
- OE (flat files only - 6M+ series)
- LA (flat files recommended - 34K series)
- TU (flat files recommended - 87K series)

**As Needed:**
- EC (Employment Cost Index - low volume)
