-- ============================================================================
-- BLS PR (Major Sector Productivity and Costs) SQL Query Documentation
-- ============================================================================
-- Survey: Major Sector Productivity and Costs
-- Tables: bls_pr_sectors, bls_pr_measures, bls_pr_series, bls_pr_data
-- Shared Tables: bls_periods, bls_periodicity
-- ============================================================================

-- ============================================================================
-- METADATA QUERIES
-- ============================================================================

-- List all sectors
SELECT
    sector_code,
    sector_name
FROM bls_pr_sectors
ORDER BY sector_code;

-- List all measures
SELECT
    measure_code,
    measure_text
FROM bls_pr_measures
ORDER BY measure_code;
-- Output, Hours, Output per hour (productivity), Unit labor costs, Compensation, etc.

-- Count series by sector
SELECT
    s.sector_name,
    COUNT(*) as series_count
FROM bls_pr_series ser
JOIN bls_pr_sectors s ON ser.sector_code = s.sector_code
WHERE ser.is_active = true
GROUP BY s.sector_code, s.sector_name
ORDER BY s.sector_code;

-- Count series by measure
SELECT
    m.measure_text,
    COUNT(*) as series_count
FROM bls_pr_series s
JOIN bls_pr_measures m ON s.measure_code = m.measure_code
WHERE s.is_active = true
GROUP BY m.measure_code, m.measure_text
ORDER BY series_count DESC;


-- ============================================================================
-- SERIES QUERIES
-- ============================================================================

-- Find productivity series for business sector
SELECT
    s.series_id,
    s.series_title,
    sec.sector_name,
    m.measure_text,
    s.seasonal_code,
    s.is_active
FROM bls_pr_series s
JOIN bls_pr_sectors sec ON s.sector_code = sec.sector_code
JOIN bls_pr_measures m ON s.measure_code = m.measure_code
WHERE sec.sector_name ILIKE '%business%'
  AND m.measure_text ILIKE '%output per hour%'
  AND s.is_active = true;

-- Find all measures for a specific sector
SELECT
    s.series_id,
    s.series_title,
    m.measure_text,
    s.seasonal_code,
    s.is_active
FROM bls_pr_series s
JOIN bls_pr_sectors sec ON s.sector_code = sec.sector_code
JOIN bls_pr_measures m ON s.measure_code = m.measure_code
WHERE sec.sector_name ILIKE '%manufacturing%'
  AND s.is_active = true
ORDER BY m.measure_code;

-- Search series by title keyword
SELECT
    series_id,
    series_title,
    sector_code,
    measure_code,
    is_active
FROM bls_pr_series
WHERE series_title ILIKE '%nonfarm%'
  AND is_active = true
ORDER BY series_title;


-- ============================================================================
-- TIME SERIES DATA QUERIES
-- ============================================================================

-- Get latest productivity data
SELECT
    sec.sector_name,
    m.measure_text,
    d.year,
    d.period,
    d.value,
    d.footnote_codes,
    d.updated_at
FROM bls_pr_data d
JOIN bls_pr_series s ON d.series_id = s.series_id
JOIN bls_pr_sectors sec ON s.sector_code = sec.sector_code
JOIN bls_pr_measures m ON s.measure_code = m.measure_code
ORDER BY d.year DESC, d.period DESC
LIMIT 10;

-- Get quarterly productivity data for specific sector
SELECT
    d.year,
    p.period_name,
    d.value as productivity_index,
    d.footnote_codes
FROM bls_pr_data d
JOIN bls_pr_series s ON d.series_id = s.series_id
JOIN bls_pr_sectors sec ON s.sector_code = sec.sector_code
JOIN bls_pr_measures m ON s.measure_code = m.measure_code
JOIN bls_periods p ON d.period = p.period_code
WHERE sec.sector_name ILIKE '%business%'
  AND m.measure_text ILIKE '%output per hour%'
  AND d.year >= 2020
  AND p.period_type = 'QUARTERLY'
ORDER BY d.year, d.period;

-- Compare seasonally adjusted vs not adjusted
SELECT
    d.year,
    d.period,
    MAX(CASE WHEN s.seasonal_code = 'S' THEN d.value END) as seasonally_adjusted,
    MAX(CASE WHEN s.seasonal_code = 'U' THEN d.value END) as not_adjusted
FROM bls_pr_data d
JOIN bls_pr_series s ON d.series_id = s.series_id
JOIN bls_pr_measures m ON s.measure_code = m.measure_code
WHERE m.measure_text ILIKE '%output per hour%'
  AND d.year >= 2023
GROUP BY d.year, d.period
ORDER BY d.year, d.period;


-- ============================================================================
-- ANALYTICAL QUERIES
-- ============================================================================

-- Calculate year-over-year productivity change
WITH current_year AS (
    SELECT sector_code, year, period, value
    FROM bls_pr_data d
    JOIN bls_pr_series s ON d.series_id = s.series_id
    JOIN bls_pr_measures m ON s.measure_code = m.measure_code
    WHERE m.measure_text ILIKE '%output per hour%'
      AND s.seasonal_code = 'S'
      AND year = 2024
),
prior_year AS (
    SELECT sector_code, year, period, value
    FROM bls_pr_data d
    JOIN bls_pr_series s ON d.series_id = s.series_id
    JOIN bls_pr_measures m ON s.measure_code = m.measure_code
    WHERE m.measure_text ILIKE '%output per hour%'
      AND s.seasonal_code = 'S'
      AND year = 2023
)
SELECT
    sec.sector_name,
    c.period,
    c.value as current_productivity,
    p.value as prior_productivity,
    ROUND(((c.value - p.value) / p.value * 100), 2) as pct_change
FROM current_year c
JOIN prior_year p ON c.sector_code = p.sector_code AND c.period = p.period
JOIN bls_pr_sectors sec ON c.sector_code = sec.sector_code
ORDER BY pct_change DESC;

-- Track productivity trend (4-quarter moving average)
SELECT
    d.year,
    d.period,
    d.value as productivity,
    AVG(d.value) OVER (
        ORDER BY d.year, d.period
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ) as moving_avg_4q
FROM bls_pr_data d
JOIN bls_pr_series s ON d.series_id = s.series_id
JOIN bls_pr_sectors sec ON s.sector_code = sec.sector_code
JOIN bls_pr_measures m ON s.measure_code = m.measure_code
WHERE sec.sector_name ILIKE '%business%'
  AND m.measure_text ILIKE '%output per hour%'
  AND s.seasonal_code = 'S'
  AND d.year >= 2020
ORDER BY d.year, d.period;

-- Multi-measure analysis (productivity, costs, compensation)
SELECT
    d.year,
    d.period,
    MAX(CASE WHEN m.measure_text ILIKE '%output per hour%' THEN d.value END) as productivity,
    MAX(CASE WHEN m.measure_text ILIKE '%unit labor costs%' THEN d.value END) as unit_labor_costs,
    MAX(CASE WHEN m.measure_text ILIKE '%compensation%' THEN d.value END) as compensation
FROM bls_pr_data d
JOIN bls_pr_series s ON d.series_id = s.series_id
JOIN bls_pr_sectors sec ON s.sector_code = sec.sector_code
JOIN bls_pr_measures m ON s.measure_code = m.measure_code
WHERE sec.sector_name ILIKE '%business%'
  AND s.seasonal_code = 'S'
  AND d.year >= 2020
GROUP BY d.year, d.period
ORDER BY d.year, d.period;

-- Compare productivity across sectors (latest quarter)
WITH latest_quarter AS (
    SELECT MAX(year) as year, MAX(period) as period
    FROM bls_pr_data d
    JOIN bls_pr_series s ON d.series_id = s.series_id
    WHERE s.seasonal_code = 'S'
)
SELECT
    sec.sector_name,
    d.value as productivity,
    d.year,
    d.period
FROM bls_pr_data d
JOIN bls_pr_series s ON d.series_id = s.series_id
JOIN bls_pr_sectors sec ON s.sector_code = sec.sector_code
JOIN bls_pr_measures m ON s.measure_code = m.measure_code
CROSS JOIN latest_quarter lq
WHERE m.measure_text ILIKE '%output per hour%'
  AND s.seasonal_code = 'S'
  AND d.year = lq.year
  AND d.period = lq.period
ORDER BY sec.sector_code;


-- ============================================================================
-- DATA QUALITY & MONITORING QUERIES
-- ============================================================================

-- Count observations by year and measure
SELECT
    year,
    m.measure_text,
    COUNT(*) as observation_count
FROM bls_pr_data d
JOIN bls_pr_series s ON d.series_id = s.series_id
JOIN bls_pr_measures m ON s.measure_code = m.measure_code
WHERE year >= 2020
GROUP BY year, m.measure_code, m.measure_text
ORDER BY year DESC, m.measure_code;

-- Find series with most recent updates
SELECT
    s.series_id,
    s.series_title,
    MAX(d.year) as latest_year,
    MAX(d.period) as latest_period,
    MAX(d.updated_at) as last_updated
FROM bls_pr_series s
JOIN bls_pr_data d ON s.series_id = d.series_id
WHERE s.is_active = true
GROUP BY s.series_id, s.series_title
ORDER BY last_updated DESC
LIMIT 20;

-- Check for NULL values
SELECT
    series_id,
    COUNT(*) as null_count
FROM bls_pr_data
WHERE value IS NULL
GROUP BY series_id
ORDER BY null_count DESC
LIMIT 20;


-- ============================================================================
-- EXPORT / REPORTING QUERIES
-- ============================================================================

-- Export complete time series with metadata
SELECT
    s.series_id,
    s.series_title,
    sec.sector_name,
    m.measure_text,
    CASE s.seasonal_code
        WHEN 'S' THEN 'Seasonally Adjusted'
        WHEN 'U' THEN 'Not Seasonally Adjusted'
    END as seasonal_adjustment,
    d.year,
    p.period_name,
    d.value,
    d.footnote_codes
FROM bls_pr_data d
JOIN bls_pr_series s ON d.series_id = s.series_id
JOIN bls_pr_sectors sec ON s.sector_code = sec.sector_code
JOIN bls_pr_measures m ON s.measure_code = m.measure_code
JOIN bls_periods p ON d.period = p.period_code
WHERE d.year >= 2020
ORDER BY s.series_id, d.year, d.period
LIMIT 1000;

-- Summary statistics by sector
SELECT
    sec.sector_name,
    COUNT(DISTINCT s.series_id) as series_count,
    COUNT(d.value) as data_points,
    MIN(d.year) as earliest_year,
    MAX(d.year) as latest_year
FROM bls_pr_sectors sec
JOIN bls_pr_series s ON sec.sector_code = s.sector_code
LEFT JOIN bls_pr_data d ON s.series_id = d.series_id
WHERE s.is_active = true
GROUP BY sec.sector_code, sec.sector_name
ORDER BY sec.sector_code;

-- Productivity dashboard (latest quarter all sectors)
WITH latest_quarter AS (
    SELECT MAX(year) as year, MAX(period) as period
    FROM bls_pr_data d
    JOIN bls_pr_series s ON d.series_id = s.series_id
    WHERE s.seasonal_code = 'S'
)
SELECT
    sec.sector_name,
    MAX(CASE WHEN m.measure_text ILIKE '%output%' THEN d.value END) as output,
    MAX(CASE WHEN m.measure_text ILIKE '%hours%' THEN d.value END) as hours,
    MAX(CASE WHEN m.measure_text ILIKE '%output per hour%' THEN d.value END) as productivity,
    MAX(CASE WHEN m.measure_text ILIKE '%unit labor costs%' THEN d.value END) as unit_labor_costs,
    MAX(d.year) as year,
    MAX(d.period) as period
FROM bls_pr_data d
JOIN bls_pr_series s ON d.series_id = s.series_id
JOIN bls_pr_sectors sec ON s.sector_code = sec.sector_code
JOIN bls_pr_measures m ON s.measure_code = m.measure_code
CROSS JOIN latest_quarter lq
WHERE s.seasonal_code = 'S'
  AND d.year = lq.year
  AND d.period = lq.period
GROUP BY sec.sector_code, sec.sector_name
ORDER BY sec.sector_code;
