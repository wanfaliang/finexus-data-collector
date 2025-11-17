-- ============================================================================
-- BLS PC (Producer Price Index - Commodities) SQL Query Documentation
-- ============================================================================
-- Survey: Producer Price Index for Commodities
-- Tables: bls_pc_industries, bls_pc_products, bls_pc_series, bls_pc_data
-- Shared Tables: bls_periods, bls_periodicity
-- ============================================================================

-- ============================================================================
-- METADATA QUERIES
-- ============================================================================

-- List all PC industries
SELECT
    industry_code,
    industry_name,
    display_level,
    selectable,
    sort_sequence
FROM bls_pc_industries
ORDER BY sort_sequence;

-- List all PC products
SELECT
    product_code,
    product_name,
    display_level,
    selectable,
    sort_sequence
FROM bls_pc_products
ORDER BY sort_sequence;

-- Count series by status
SELECT
    is_active,
    COUNT(*) as series_count
FROM bls_pc_series
GROUP BY is_active;

-- List top-level industries
SELECT
    industry_code,
    industry_name,
    sort_sequence
FROM bls_pc_industries
WHERE display_level = 0
ORDER BY sort_sequence;

-- List selectable products only
SELECT
    product_code,
    product_name,
    display_level
FROM bls_pc_products
WHERE selectable = 'T'
ORDER BY sort_sequence
LIMIT 50;


-- ============================================================================
-- SERIES QUERIES
-- ============================================================================

-- Find series for specific product
SELECT
    s.series_id,
    s.series_title,
    i.industry_name,
    p.product_name,
    s.seasonal_code,
    s.begin_year,
    s.end_year,
    s.is_active
FROM bls_pc_series s
LEFT JOIN bls_pc_industries i ON s.industry_code = i.industry_code
LEFT JOIN bls_pc_products p ON s.product_code = p.product_code
WHERE p.product_name ILIKE '%crude oil%'
  AND s.is_active = true;

-- Get series metadata with full descriptions
SELECT
    s.series_id,
    s.series_title,
    i.industry_name,
    p.product_name,
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
FROM bls_pc_series s
LEFT JOIN bls_pc_industries i ON s.industry_code = i.industry_code
LEFT JOIN bls_pc_products p ON s.product_code = p.product_code
LEFT JOIN bls_periodicity per ON s.periodicity_code = per.periodicity_code
WHERE s.series_id = 'WPU0000'
LIMIT 1;

-- Search series by title keyword
SELECT
    series_id,
    series_title,
    industry_code,
    product_code,
    is_active
FROM bls_pc_series
WHERE series_title ILIKE '%petroleum%'
  AND is_active = true
ORDER BY series_title
LIMIT 20;


-- ============================================================================
-- TIME SERIES DATA QUERIES
-- ============================================================================

-- Get latest PPI value for a commodity
SELECT
    d.year,
    d.period,
    d.value,
    d.footnote_codes,
    d.updated_at
FROM bls_pc_data d
WHERE d.series_id = 'WPU0000'
ORDER BY d.year DESC, d.period DESC
LIMIT 1;

-- Get time series data for specific series and date range
SELECT
    year,
    period,
    value,
    footnote_codes
FROM bls_pc_data
WHERE series_id = 'WPU0000'
  AND year >= 2020
ORDER BY year, period;

-- Get monthly data with period names
SELECT
    d.year,
    p.period_name,
    d.value,
    d.footnote_codes
FROM bls_pc_data d
JOIN bls_periods p ON d.period = p.period_code
WHERE d.series_id = 'WPU0000'
  AND d.year = 2024
  AND p.period_type = 'MONTHLY'
ORDER BY d.year, d.period;

-- Calculate year-over-year percent change
WITH current_year AS (
    SELECT year, period, value
    FROM bls_pc_data
    WHERE series_id = 'WPU0000'
      AND year = 2024
),
prior_year AS (
    SELECT year, period, value
    FROM bls_pc_data
    WHERE series_id = 'WPU0000'
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


-- ============================================================================
-- ANALYTICAL QUERIES
-- ============================================================================

-- Compare PPI across industries (latest month)
WITH latest_data AS (
    SELECT
        series_id,
        year,
        period,
        value,
        ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY year DESC, period DESC) as rn
    FROM bls_pc_data
)
SELECT
    i.industry_name,
    s.series_title,
    d.year,
    d.period,
    d.value
FROM latest_data d
JOIN bls_pc_series s ON d.series_id = s.series_id
LEFT JOIN bls_pc_industries i ON s.industry_code = i.industry_code
WHERE d.rn = 1
  AND s.is_active = true
  AND i.display_level = 0
ORDER BY i.sort_sequence
LIMIT 20;

-- Track PPI trends (12-month moving average)
SELECT
    year,
    period,
    value,
    AVG(value) OVER (
        ORDER BY year, period
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) as moving_avg_12mo
FROM bls_pc_data
WHERE series_id = 'WPU0000'
  AND year >= 2020
ORDER BY year, period;

-- Find series with most recent updates
SELECT
    s.series_id,
    s.series_title,
    MAX(d.year) as latest_year,
    MAX(d.period) as latest_period,
    MAX(d.updated_at) as last_updated
FROM bls_pc_series s
JOIN bls_pc_data d ON s.series_id = d.series_id
WHERE s.is_active = true
GROUP BY s.series_id, s.series_title
ORDER BY last_updated DESC
LIMIT 20;

-- Compare product price changes
SELECT
    d.year,
    d.period,
    MAX(CASE WHEN p.product_name ILIKE '%crude petroleum%' THEN d.value END) as crude_oil,
    MAX(CASE WHEN p.product_name ILIKE '%gasoline%' THEN d.value END) as gasoline,
    MAX(CASE WHEN p.product_name ILIKE '%natural gas%' THEN d.value END) as natural_gas
FROM bls_pc_data d
JOIN bls_pc_series s ON d.series_id = s.series_id
LEFT JOIN bls_pc_products p ON s.product_code = p.product_code
WHERE d.year >= 2020
  AND s.seasonal_code = 'U'
GROUP BY d.year, d.period
ORDER BY d.year, d.period;


-- ============================================================================
-- DATA QUALITY & MONITORING QUERIES
-- ============================================================================

-- Count observations by year
SELECT
    year,
    COUNT(*) as observation_count,
    COUNT(DISTINCT series_id) as series_count
FROM bls_pc_data
GROUP BY year
ORDER BY year DESC;

-- Check for NULL values
SELECT
    series_id,
    COUNT(*) as null_count
FROM bls_pc_data
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
FROM bls_pc_data d
JOIN bls_pc_series s ON d.series_id = s.series_id
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
    i.industry_name,
    p.product_name,
    CASE s.seasonal_code
        WHEN 'S' THEN 'Seasonally Adjusted'
        WHEN 'U' THEN 'Not Seasonally Adjusted'
    END as seasonal_adjustment,
    s.base_period,
    d.year,
    per.period_name,
    d.value,
    d.footnote_codes
FROM bls_pc_data d
JOIN bls_pc_series s ON d.series_id = s.series_id
LEFT JOIN bls_pc_industries i ON s.industry_code = i.industry_code
LEFT JOIN bls_pc_products p ON s.product_code = p.product_code
JOIN bls_periods per ON d.period = per.period_code
WHERE s.series_id = 'WPU0000'
  AND d.year >= 2020
ORDER BY d.year, d.period;

-- Summary statistics by industry
SELECT
    i.industry_name,
    COUNT(DISTINCT s.series_id) as series_count,
    COUNT(d.value) as data_points,
    MIN(d.year) as earliest_year,
    MAX(d.year) as latest_year
FROM bls_pc_industries i
JOIN bls_pc_series s ON i.industry_code = s.industry_code
LEFT JOIN bls_pc_data d ON s.series_id = d.series_id
WHERE s.is_active = true
GROUP BY i.industry_code, i.industry_name
ORDER BY i.sort_sequence
LIMIT 20;
