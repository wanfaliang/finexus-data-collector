-- ============================================================================
-- BLS CU (Consumer Price Index - All Urban Consumers) SQL Query Documentation
-- ============================================================================
-- Survey: Consumer Price Index for All Urban Consumers
-- Tables: bls_cu_areas, bls_cu_items, bls_cu_series, bls_cu_data, bls_cu_aspects
-- Shared Tables: bls_periods, bls_periodicity
-- ============================================================================

-- ============================================================================
-- METADATA QUERIES
-- ============================================================================

-- List all CU areas with hierarchy
SELECT
    area_code,
    area_name,
    display_level,
    selectable,
    sort_sequence
FROM bls_cu_areas
ORDER BY sort_sequence;

-- List all CU items (commodities/services)
SELECT
    item_code,
    item_name,
    display_level,
    selectable,
    sort_sequence
FROM bls_cu_items
ORDER BY sort_sequence;

-- Count series by status
SELECT
    is_active,
    COUNT(*) as series_count
FROM bls_cu_series
GROUP BY is_active;

-- List top-level areas (major regions)
SELECT
    area_code,
    area_name,
    sort_sequence
FROM bls_cu_areas
WHERE display_level = 0
ORDER BY sort_sequence;

-- List selectable items only
SELECT
    item_code,
    item_name,
    display_level
FROM bls_cu_items
WHERE selectable = 'T'
ORDER BY sort_sequence;


-- ============================================================================
-- SERIES QUERIES
-- ============================================================================

-- Find series for U.S. All Items (headline CPI-U)
SELECT
    s.series_id,
    s.series_title,
    a.area_name,
    i.item_name,
    s.seasonal_code,
    s.begin_year,
    s.end_year,
    s.is_active
FROM bls_cu_series s
JOIN bls_cu_areas a ON s.area_code = a.area_code
JOIN bls_cu_items i ON s.item_code = i.item_code
WHERE a.area_code = '0000'
  AND i.item_code = 'SA0'
  AND s.is_active = true;

-- Find all seasonally adjusted series for a specific area
SELECT
    s.series_id,
    s.series_title,
    i.item_name,
    s.begin_year,
    s.end_year
FROM bls_cu_series s
JOIN bls_cu_items i ON s.item_code = i.item_code
WHERE s.area_code = '0000'
  AND s.seasonal_code = 'S'
  AND s.is_active = true
ORDER BY i.sort_sequence;

-- Search series by title keyword
SELECT
    series_id,
    series_title,
    area_code,
    item_code,
    is_active
FROM bls_cu_series
WHERE series_title ILIKE '%energy%'
  AND is_active = true
ORDER BY series_title;

-- Get series metadata with full descriptions
SELECT
    s.series_id,
    s.series_title,
    a.area_name,
    i.item_name,
    CASE s.seasonal_code
        WHEN 'S' THEN 'Seasonally Adjusted'
        WHEN 'U' THEN 'Not Seasonally Adjusted'
        ELSE 'Unknown'
    END as seasonal_adjustment,
    p.periodicity_name,
    s.base_period,
    s.begin_year || '-' || s.begin_period as start_date,
    s.end_year || '-' || s.end_period as end_date,
    s.is_active
FROM bls_cu_series s
JOIN bls_cu_areas a ON s.area_code = a.area_code
JOIN bls_cu_items i ON s.item_code = i.item_code
LEFT JOIN bls_periodicity p ON s.periodicity_code = p.periodicity_code
WHERE s.series_id = 'CUSR0000SA0';


-- ============================================================================
-- TIME SERIES DATA QUERIES
-- ============================================================================

-- Get latest CPI-U value (U.S. All Items, Seasonally Adjusted)
SELECT
    d.year,
    d.period,
    d.value,
    d.footnote_codes,
    d.updated_at
FROM bls_cu_data d
WHERE d.series_id = 'CUSR0000SA0'
ORDER BY d.year DESC, d.period DESC
LIMIT 1;

-- Get time series data for specific series and date range
SELECT
    year,
    period,
    value,
    footnote_codes
FROM bls_cu_data
WHERE series_id = 'CUSR0000SA0'
  AND year >= 2020
ORDER BY year, period;

-- Get monthly data with period names
SELECT
    d.year,
    p.period_name,
    d.value,
    d.footnote_codes
FROM bls_cu_data d
JOIN bls_periods p ON d.period = p.period_code
WHERE d.series_id = 'CUSR0000SA0'
  AND d.year = 2024
  AND p.period_type = 'MONTHLY'
ORDER BY d.year, d.period;

-- Calculate year-over-year percent change
WITH current_year AS (
    SELECT year, period, value
    FROM bls_cu_data
    WHERE series_id = 'CUSR0000SA0'
      AND year = 2024
),
prior_year AS (
    SELECT year, period, value
    FROM bls_cu_data
    WHERE series_id = 'CUSR0000SA0'
      AND year = 2023
)
SELECT
    c.year,
    c.period,
    c.value as current_value,
    p.value as prior_value,
    ROUND(((c.value - p.value) / p.value * 100), 2) as pct_change
FROM current_year c
JOIN prior_year p ON c.period = p.period
ORDER BY c.period;

-- Get all series data for a specific month
SELECT
    s.series_title,
    d.value,
    a.area_name,
    i.item_name
FROM bls_cu_data d
JOIN bls_cu_series s ON d.series_id = s.series_id
JOIN bls_cu_areas a ON s.area_code = a.area_code
JOIN bls_cu_items i ON s.item_code = i.item_code
WHERE d.year = 2024
  AND d.period = 'M09'
  AND s.seasonal_code = 'S'
  AND s.area_code = '0000'
ORDER BY i.sort_sequence;


-- ============================================================================
-- ANALYTICAL QUERIES
-- ============================================================================

-- Compare CPI-U across major regions (latest month)
WITH latest_data AS (
    SELECT
        series_id,
        year,
        period,
        value,
        ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY year DESC, period DESC) as rn
    FROM bls_cu_data
)
SELECT
    a.area_name,
    s.series_title,
    d.year,
    d.period,
    d.value
FROM latest_data d
JOIN bls_cu_series s ON d.series_id = s.series_id
JOIN bls_cu_areas a ON s.area_code = a.area_code
WHERE d.rn = 1
  AND s.item_code = 'SA0'
  AND s.seasonal_code = 'S'
  AND a.display_level = 0
ORDER BY a.sort_sequence;

-- Track inflation trends (12-month moving average)
SELECT
    year,
    period,
    value,
    AVG(value) OVER (
        ORDER BY year, period
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) as moving_avg_12mo
FROM bls_cu_data
WHERE series_id = 'CUSR0000SA0'
  AND year >= 2020
ORDER BY year, period;

-- Find series with most recent updates
SELECT
    s.series_id,
    s.series_title,
    MAX(d.year) as latest_year,
    MAX(d.period) as latest_period,
    MAX(d.updated_at) as last_updated
FROM bls_cu_series s
JOIN bls_cu_data d ON s.series_id = d.series_id
WHERE s.is_active = true
GROUP BY s.series_id, s.series_title
ORDER BY last_updated DESC
LIMIT 20;

-- Compare energy vs. all items inflation
SELECT
    d.year,
    d.period,
    MAX(CASE WHEN s.item_code = 'SA0' THEN d.value END) as all_items,
    MAX(CASE WHEN s.item_code = 'SA0E' THEN d.value END) as energy
FROM bls_cu_data d
JOIN bls_cu_series s ON d.series_id = s.series_id
WHERE s.area_code = '0000'
  AND s.seasonal_code = 'S'
  AND s.item_code IN ('SA0', 'SA0E')
  AND d.year >= 2020
GROUP BY d.year, d.period
ORDER BY d.year, d.period;

-- Compare Core CPI (ex food and energy) vs. Headline CPI
SELECT
    d.year,
    d.period,
    MAX(CASE WHEN s.item_code = 'SA0' THEN d.value END) as headline_cpi,
    MAX(CASE WHEN s.item_code = 'SA0L1E' THEN d.value END) as core_cpi
FROM bls_cu_data d
JOIN bls_cu_series s ON d.series_id = s.series_id
WHERE s.area_code = '0000'
  AND s.seasonal_code = 'S'
  AND s.item_code IN ('SA0', 'SA0L1E')
  AND d.year >= 2020
GROUP BY d.year, d.period
ORDER BY d.year, d.period;

-- Major expenditure categories breakdown (latest month)
WITH latest_month AS (
    SELECT MAX(year) as year, MAX(period) as period
    FROM bls_cu_data
    WHERE series_id = 'CUSR0000SA0'
)
SELECT
    i.item_name,
    d.value as index_value,
    LAG(d.value) OVER (PARTITION BY d.series_id ORDER BY d.year, d.period) as prev_month_value,
    ROUND(
        ((d.value - LAG(d.value) OVER (PARTITION BY d.series_id ORDER BY d.year, d.period)) /
        LAG(d.value) OVER (PARTITION BY d.series_id ORDER BY d.year, d.period) * 100),
        2
    ) as mom_pct_change
FROM bls_cu_data d
JOIN bls_cu_series s ON d.series_id = s.series_id
JOIN bls_cu_items i ON s.item_code = i.item_code
CROSS JOIN latest_month lm
WHERE d.year = lm.year
  AND d.period = lm.period
  AND s.area_code = '0000'
  AND s.seasonal_code = 'S'
  AND i.item_code IN ('SAF', 'SAH', 'SAA', 'SAT', 'SAM', 'SAR', 'SAE', 'SAG')
  AND i.display_level = 1
ORDER BY i.sort_sequence;


-- ============================================================================
-- DATA QUALITY & MONITORING QUERIES
-- ============================================================================

-- Count observations by year
SELECT
    year,
    COUNT(*) as observation_count,
    COUNT(DISTINCT series_id) as series_count
FROM bls_cu_data
GROUP BY year
ORDER BY year DESC;

-- Find missing data (series with gaps)
SELECT
    s.series_id,
    s.series_title,
    s.begin_year,
    s.end_year,
    COUNT(d.series_id) as data_points,
    (s.end_year - s.begin_year + 1) * 12 as expected_months
FROM bls_cu_series s
LEFT JOIN bls_cu_data d ON s.series_id = d.series_id
WHERE s.is_active = true
  AND s.periodicity_code = 'R' -- Monthly
GROUP BY s.series_id, s.series_title, s.begin_year, s.end_year
HAVING COUNT(d.series_id) < (s.end_year - s.begin_year + 1) * 12
ORDER BY s.series_id;

-- Check for NULL values in time series
SELECT
    series_id,
    COUNT(*) as null_count
FROM bls_cu_data
WHERE value IS NULL
GROUP BY series_id
ORDER BY null_count DESC;

-- Recent data updates (last 7 days)
SELECT
    s.series_id,
    s.series_title,
    COUNT(*) as updated_records,
    MAX(d.updated_at) as last_update
FROM bls_cu_data d
JOIN bls_cu_series s ON d.series_id = s.series_id
WHERE d.updated_at >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY s.series_id, s.series_title
ORDER BY last_update DESC;


-- ============================================================================
-- ASPECT DATA QUERIES
-- ============================================================================

-- View aspect types available for CPI-U series
SELECT DISTINCT
    aspect_type,
    COUNT(*) as occurrence_count
FROM bls_cu_aspects
GROUP BY aspect_type
ORDER BY aspect_type;

-- Get specific aspect data for a series
SELECT
    year,
    period,
    aspect_type,
    value
FROM bls_cu_aspects
WHERE series_id = 'CUSR0000SA0'
  AND year = 2024
ORDER BY year, period, aspect_type;


-- ============================================================================
-- EXPORT / REPORTING QUERIES
-- ============================================================================

-- Export complete time series with metadata
SELECT
    s.series_id,
    s.series_title,
    a.area_name,
    i.item_name,
    CASE s.seasonal_code
        WHEN 'S' THEN 'Seasonally Adjusted'
        WHEN 'U' THEN 'Not Seasonally Adjusted'
    END as seasonal_adjustment,
    s.base_period,
    d.year,
    p.period_name,
    d.value,
    d.footnote_codes
FROM bls_cu_data d
JOIN bls_cu_series s ON d.series_id = s.series_id
JOIN bls_cu_areas a ON s.area_code = a.area_code
JOIN bls_cu_items i ON s.item_code = i.item_code
JOIN bls_periods p ON d.period = p.period_code
WHERE s.series_id = 'CUSR0000SA0'
  AND d.year >= 2020
ORDER BY d.year, d.period;

-- Summary statistics by item category
SELECT
    i.item_name,
    COUNT(DISTINCT s.series_id) as series_count,
    COUNT(d.value) as data_points,
    MIN(d.year) as earliest_year,
    MAX(d.year) as latest_year,
    AVG(d.value) as avg_index,
    MIN(d.value) as min_index,
    MAX(d.value) as max_index
FROM bls_cu_items i
JOIN bls_cu_series s ON i.item_code = s.item_code
LEFT JOIN bls_cu_data d ON s.series_id = d.series_id
WHERE s.is_active = true
  AND i.display_level = 1
GROUP BY i.item_code, i.item_name
ORDER BY i.sort_sequence;


-- ============================================================================
-- USEFUL REFERENCE QUERIES
-- ============================================================================

-- List all available periods
SELECT
    period_code,
    period_abbr,
    period_name,
    period_type
FROM bls_periods
ORDER BY sort_order;

-- Check database freshness
SELECT
    'bls_cu_areas' as table_name,
    COUNT(*) as record_count,
    MAX(updated_at) as last_updated
FROM bls_cu_areas
UNION ALL
SELECT
    'bls_cu_items',
    COUNT(*),
    MAX(updated_at)
FROM bls_cu_items
UNION ALL
SELECT
    'bls_cu_series',
    COUNT(*),
    MAX(updated_at)
FROM bls_cu_series
UNION ALL
SELECT
    'bls_cu_data',
    COUNT(*),
    MAX(updated_at)
FROM bls_cu_data;


-- ============================================================================
-- HISTORICAL ANALYSIS QUERIES
-- ============================================================================

-- Calculate cumulative inflation since base year
WITH base_value AS (
    SELECT value as base_index
    FROM bls_cu_data
    WHERE series_id = 'CUSR0000SA0'
      AND year = 1982
      AND period = 'M12'
)
SELECT
    d.year,
    d.period,
    d.value,
    b.base_index,
    ROUND(((d.value - b.base_index) / b.base_index * 100), 2) as cumulative_inflation_pct
FROM bls_cu_data d
CROSS JOIN base_value b
WHERE d.series_id = 'CUSR0000SA0'
  AND d.year >= 2000
ORDER BY d.year, d.period;

-- Find periods of deflation (negative monthly change)
WITH monthly_changes AS (
    SELECT
        year,
        period,
        value,
        LAG(value) OVER (ORDER BY year, period) as prev_value
    FROM bls_cu_data
    WHERE series_id = 'CUSR0000SA0'
)
SELECT
    year,
    period,
    value as current_index,
    prev_value as previous_index,
    ROUND(((value - prev_value) / prev_value * 100), 3) as mom_pct_change
FROM monthly_changes
WHERE prev_value IS NOT NULL
  AND value < prev_value
ORDER BY year DESC, period DESC;

-- Maximum inflation rate by decade
SELECT
    FLOOR(year / 10) * 10 as decade,
    MAX(ROUND(((value - LAG(value, 12) OVER (ORDER BY year, period)) / LAG(value, 12) OVER (ORDER BY year, period) * 100), 2)) as max_yoy_inflation
FROM bls_cu_data
WHERE series_id = 'CUSR0000SA0'
GROUP BY FLOOR(year / 10) * 10
ORDER BY decade DESC;
