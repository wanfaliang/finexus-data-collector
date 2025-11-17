-- ============================================================================
-- BLS WP (Producer Price Index) SQL Query Documentation
-- ============================================================================
-- Survey: Producer Price Index
-- Tables: bls_wp_groups, bls_wp_items, bls_wp_series, bls_wp_data
-- Shared Tables: bls_periods, bls_periodicity
-- ============================================================================

-- ============================================================================
-- METADATA QUERIES
-- ============================================================================

-- List all WP groups
SELECT
    group_code,
    group_name,
    display_level,
    selectable,
    sort_sequence
FROM bls_wp_groups
ORDER BY sort_sequence;

-- List all WP items
SELECT
    item_code,
    item_name,
    display_level,
    selectable,
    sort_sequence
FROM bls_wp_items
ORDER BY sort_sequence
LIMIT 50;

-- Count series by status
SELECT
    is_active,
    COUNT(*) as series_count
FROM bls_wp_series
GROUP BY is_active;

-- List top-level groups
SELECT
    group_code,
    group_name
FROM bls_wp_groups
WHERE display_level = 0
ORDER BY sort_sequence;


-- ============================================================================
-- SERIES QUERIES
-- ============================================================================

-- Find PPI series for specific item
SELECT
    s.series_id,
    s.series_title,
    g.group_name,
    i.item_name,
    s.seasonal_code,
    s.begin_year,
    s.end_year,
    s.is_active
FROM bls_wp_series s
LEFT JOIN bls_wp_groups g ON s.group_code = g.group_code
LEFT JOIN bls_wp_items i ON s.item_code = i.item_code
WHERE i.item_name ILIKE '%steel%'
  AND s.is_active = true;

-- Get series metadata with full descriptions
SELECT
    s.series_id,
    s.series_title,
    g.group_name,
    i.item_name,
    CASE s.seasonal_code
        WHEN 'S' THEN 'Seasonally Adjusted'
        WHEN 'U' THEN 'Not Seasonally Adjusted'
        ELSE 'Unknown'
    END as seasonal_adjustment,
    per.periodicity_name,
    s.base_period,
    s.begin_year || '-' || s.begin_period as start_date,
    s.end_year || '-' || s.end_period as end_date,
    s.is_active
FROM bls_wp_series s
LEFT JOIN bls_wp_groups g ON s.group_code = g.group_code
LEFT JOIN bls_wp_items i ON s.item_code = i.item_code
LEFT JOIN bls_periodicity per ON s.periodicity_code = per.periodicity_code
WHERE s.is_active = true
LIMIT 1;

-- Search series by title keyword
SELECT
    series_id,
    series_title,
    group_code,
    item_code,
    is_active
FROM bls_wp_series
WHERE series_title ILIKE '%energy%'
  AND is_active = true
ORDER BY series_title
LIMIT 20;


-- ============================================================================
-- TIME SERIES DATA QUERIES
-- ============================================================================

-- Get latest PPI value
SELECT
    s.series_title,
    d.year,
    d.period,
    d.value,
    d.footnote_codes,
    d.updated_at
FROM bls_wp_data d
JOIN bls_wp_series s ON d.series_id = s.series_id
ORDER BY d.year DESC, d.period DESC
LIMIT 10;

-- Get time series data for specific series and date range
SELECT
    year,
    period,
    value,
    footnote_codes
FROM bls_wp_data
WHERE series_id LIKE 'WPU%'
  AND year >= 2020
ORDER BY year, period
LIMIT 100;

-- Get monthly data with period names
SELECT
    d.year,
    p.period_name,
    d.value,
    d.footnote_codes
FROM bls_wp_data d
JOIN bls_periods p ON d.period = p.period_code
WHERE d.year = 2024
  AND p.period_type = 'MONTHLY'
ORDER BY d.year, d.period
LIMIT 100;

-- Calculate year-over-year percent change
WITH current_year AS (
    SELECT series_id, year, period, value
    FROM bls_wp_data
    WHERE year = 2024
),
prior_year AS (
    SELECT series_id, year, period, value
    FROM bls_wp_data
    WHERE year = 2023
)
SELECT
    s.series_title,
    c.period,
    c.value as current_value,
    p.value as prior_value,
    ROUND(((c.value - p.value) / p.value * 100), 2) as pct_change
FROM current_year c
JOIN prior_year p ON c.series_id = p.series_id AND c.period = p.period
JOIN bls_wp_series s ON c.series_id = s.series_id
WHERE s.is_active = true
ORDER BY pct_change DESC
LIMIT 20;


-- ============================================================================
-- ANALYTICAL QUERIES
-- ============================================================================

-- Compare PPI across groups (latest month)
WITH latest_data AS (
    SELECT
        series_id,
        year,
        period,
        value,
        ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY year DESC, period DESC) as rn
    FROM bls_wp_data
)
SELECT
    g.group_name,
    s.series_title,
    d.year,
    d.period,
    d.value
FROM latest_data d
JOIN bls_wp_series s ON d.series_id = s.series_id
LEFT JOIN bls_wp_groups g ON s.group_code = g.group_code
WHERE d.rn = 1
  AND s.is_active = true
  AND g.display_level = 0
ORDER BY g.sort_sequence
LIMIT 20;

-- Track PPI trends (12-month moving average)
SELECT
    s.series_title,
    d.year,
    d.period,
    d.value,
    AVG(d.value) OVER (
        PARTITION BY d.series_id
        ORDER BY d.year, d.period
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) as moving_avg_12mo
FROM bls_wp_data d
JOIN bls_wp_series s ON d.series_id = s.series_id
WHERE d.year >= 2020
  AND s.is_active = true
ORDER BY s.series_id, d.year, d.period
LIMIT 100;

-- Find series with most recent updates
SELECT
    s.series_id,
    s.series_title,
    MAX(d.year) as latest_year,
    MAX(d.period) as latest_period,
    MAX(d.updated_at) as last_updated
FROM bls_wp_series s
JOIN bls_wp_data d ON s.series_id = d.series_id
WHERE s.is_active = true
GROUP BY s.series_id, s.series_title
ORDER BY last_updated DESC
LIMIT 20;


-- ============================================================================
-- DATA QUALITY & MONITORING QUERIES
-- ============================================================================

-- Count observations by year
SELECT
    year,
    COUNT(*) as observation_count,
    COUNT(DISTINCT series_id) as series_count
FROM bls_wp_data
GROUP BY year
ORDER BY year DESC;

-- Check for NULL values
SELECT
    series_id,
    COUNT(*) as null_count
FROM bls_wp_data
WHERE value IS NULL
GROUP BY series_id
ORDER BY null_count DESC
LIMIT 20;

-- Recent data updates (last 7 days)
SELECT
    s.series_id,
    s.series_title,
    COUNT(*) as updated_records,
    MAX(d.updated_at) as last_update
FROM bls_wp_data d
JOIN bls_wp_series s ON d.series_id = s.series_id
WHERE d.updated_at >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY s.series_id, s.series_title
ORDER BY last_update DESC;


-- ============================================================================
-- EXPORT / REPORTING QUERIES
-- ============================================================================

-- Export complete time series with metadata
SELECT
    s.series_id,
    s.series_title,
    g.group_name,
    i.item_name,
    CASE s.seasonal_code
        WHEN 'S' THEN 'Seasonally Adjusted'
        WHEN 'U' THEN 'Not Seasonally Adjusted'
    END as seasonal_adjustment,
    s.base_period,
    d.year,
    per.period_name,
    d.value,
    d.footnote_codes
FROM bls_wp_data d
JOIN bls_wp_series s ON d.series_id = s.series_id
LEFT JOIN bls_wp_groups g ON s.group_code = g.group_code
LEFT JOIN bls_wp_items i ON s.item_code = i.item_code
JOIN bls_periods per ON d.period = per.period_code
WHERE d.year >= 2020
  AND s.is_active = true
ORDER BY s.series_id, d.year, d.period
LIMIT 1000;

-- Summary statistics by group
SELECT
    g.group_name,
    COUNT(DISTINCT s.series_id) as series_count,
    COUNT(d.value) as data_points,
    MIN(d.year) as earliest_year,
    MAX(d.year) as latest_year
FROM bls_wp_groups g
JOIN bls_wp_series s ON g.group_code = s.group_code
LEFT JOIN bls_wp_data d ON s.series_id = d.series_id
WHERE s.is_active = true
  AND g.display_level <= 1
GROUP BY g.group_code, g.group_name
ORDER BY g.sort_sequence
LIMIT 20;
