-- ============================================================================
-- BLS IP (Industry Productivity and Costs) SQL Query Documentation
-- ============================================================================
-- Survey: Industry Productivity and Costs
-- Tables: bls_ip_industries, bls_ip_measures, bls_ip_series, bls_ip_data
-- Shared Tables: bls_periods, bls_periodicity
-- ============================================================================

-- ============================================================================
-- METADATA QUERIES
-- ============================================================================

-- List all industries
SELECT
    industry_code,
    industry_name,
    display_level,
    selectable,
    sort_sequence
FROM bls_ip_industries
ORDER BY sort_sequence
LIMIT 50;

-- List all measures
SELECT
    measure_code,
    measure_text
FROM bls_ip_measures
ORDER BY measure_code;
-- Output, Hours, Output per hour (productivity), Unit labor costs, etc.

-- Count series by industry
SELECT
    i.industry_name,
    COUNT(*) as series_count
FROM bls_ip_series s
JOIN bls_ip_industries i ON s.industry_code = i.industry_code
WHERE s.is_active = true
GROUP BY i.industry_code, i.industry_name
ORDER BY series_count DESC
LIMIT 20;

-- Count series by measure
SELECT
    m.measure_text,
    COUNT(*) as series_count
FROM bls_ip_series s
JOIN bls_ip_measures m ON s.measure_code = m.measure_code
WHERE s.is_active = true
GROUP BY m.measure_code, m.measure_text
ORDER BY series_count DESC;


-- ============================================================================
-- SERIES QUERIES
-- ============================================================================

-- Find productivity series for specific industry
SELECT
    s.series_id,
    s.series_title,
    i.industry_name,
    m.measure_text,
    s.seasonal_code,
    s.is_active
FROM bls_ip_series s
JOIN bls_ip_industries i ON s.industry_code = i.industry_code
JOIN bls_ip_measures m ON s.measure_code = m.measure_code
WHERE i.industry_name ILIKE '%manufacturing%'
  AND m.measure_text ILIKE '%output per hour%'
  AND s.is_active = true;

-- Find all measures for a specific industry
SELECT
    s.series_id,
    s.series_title,
    m.measure_text,
    s.seasonal_code,
    s.is_active
FROM bls_ip_series s
JOIN bls_ip_industries i ON s.industry_code = i.industry_code
JOIN bls_ip_measures m ON s.measure_code = m.measure_code
WHERE i.industry_name ILIKE '%retail%'
  AND s.is_active = true
ORDER BY m.measure_code;

-- Search series by title keyword
SELECT
    series_id,
    series_title,
    industry_code,
    measure_code,
    is_active
FROM bls_ip_series
WHERE series_title ILIKE '%computer%'
  AND is_active = true
ORDER BY series_title
LIMIT 20;


-- ============================================================================
-- TIME SERIES DATA QUERIES
-- ============================================================================

-- Get latest productivity data by industry
SELECT
    i.industry_name,
    m.measure_text,
    d.year,
    d.period,
    d.value,
    d.footnote_codes,
    d.updated_at
FROM bls_ip_data d
JOIN bls_ip_series s ON d.series_id = s.series_id
JOIN bls_ip_industries i ON s.industry_code = i.industry_code
JOIN bls_ip_measures m ON s.measure_code = m.measure_code
WHERE m.measure_text ILIKE '%output per hour%'
ORDER BY d.year DESC, d.period DESC
LIMIT 10;

-- Get annual productivity data for specific industry
SELECT
    d.year,
    d.value as productivity_index,
    d.footnote_codes
FROM bls_ip_data d
JOIN bls_ip_series s ON d.series_id = s.series_id
JOIN bls_ip_industries i ON s.industry_code = i.industry_code
JOIN bls_ip_measures m ON s.measure_code = m.measure_code
WHERE i.industry_name ILIKE '%automobile%'
  AND m.measure_text ILIKE '%output per hour%'
  AND d.year >= 2010
ORDER BY d.year;

-- Compare seasonally adjusted vs not adjusted
SELECT
    d.year,
    d.period,
    MAX(CASE WHEN s.seasonal_code = 'S' THEN d.value END) as seasonally_adjusted,
    MAX(CASE WHEN s.seasonal_code = 'U' THEN d.value END) as not_adjusted
FROM bls_ip_data d
JOIN bls_ip_series s ON d.series_id = s.series_id
JOIN bls_ip_measures m ON s.measure_code = m.measure_code
WHERE m.measure_text ILIKE '%output per hour%'
  AND d.year >= 2023
GROUP BY d.year, d.period
ORDER BY d.year, d.period;


-- ============================================================================
-- ANALYTICAL QUERIES
-- ============================================================================

-- Calculate year-over-year productivity change by industry
WITH current_year AS (
    SELECT industry_code, year, value
    FROM bls_ip_data d
    JOIN bls_ip_series s ON d.series_id = s.series_id
    JOIN bls_ip_measures m ON s.measure_code = m.measure_code
    WHERE m.measure_text ILIKE '%output per hour%'
      AND s.seasonal_code = 'S'
      AND year = 2023
),
prior_year AS (
    SELECT industry_code, year, value
    FROM bls_ip_data d
    JOIN bls_ip_series s ON d.series_id = s.series_id
    JOIN bls_ip_measures m ON s.measure_code = m.measure_code
    WHERE m.measure_text ILIKE '%output per hour%'
      AND s.seasonal_code = 'S'
      AND year = 2022
)
SELECT
    i.industry_name,
    c.value as current_productivity,
    p.value as prior_productivity,
    ROUND(((c.value - p.value) / p.value * 100), 2) as pct_change
FROM current_year c
JOIN prior_year p ON c.industry_code = p.industry_code
JOIN bls_ip_industries i ON c.industry_code = i.industry_code
WHERE i.selectable = 'T'
ORDER BY pct_change DESC
LIMIT 20;

-- Track productivity trend for an industry
SELECT
    d.year,
    d.value as productivity,
    AVG(d.value) OVER (
        ORDER BY d.year
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as moving_avg_3yr
FROM bls_ip_data d
JOIN bls_ip_series s ON d.series_id = s.series_id
JOIN bls_ip_industries i ON s.industry_code = i.industry_code
JOIN bls_ip_measures m ON s.measure_code = m.measure_code
WHERE i.industry_name ILIKE '%computer%'
  AND m.measure_text ILIKE '%output per hour%'
  AND s.seasonal_code = 'S'
  AND d.year >= 2010
ORDER BY d.year;

-- Multi-measure analysis for an industry
SELECT
    d.year,
    MAX(CASE WHEN m.measure_text ILIKE '%output%' THEN d.value END) as output,
    MAX(CASE WHEN m.measure_text ILIKE '%hours%' THEN d.value END) as hours,
    MAX(CASE WHEN m.measure_text ILIKE '%output per hour%' THEN d.value END) as productivity,
    MAX(CASE WHEN m.measure_text ILIKE '%unit labor costs%' THEN d.value END) as unit_labor_costs
FROM bls_ip_data d
JOIN bls_ip_series s ON d.series_id = s.series_id
JOIN bls_ip_industries i ON s.industry_code = i.industry_code
JOIN bls_ip_measures m ON s.measure_code = m.measure_code
WHERE i.industry_name ILIKE '%manufacturing%'
  AND s.seasonal_code = 'S'
  AND d.year >= 2015
GROUP BY d.year
ORDER BY d.year;

-- Compare productivity across industries (latest year)
WITH latest_year AS (
    SELECT MAX(year) as year
    FROM bls_ip_data
)
SELECT
    i.industry_name,
    d.value as productivity,
    d.year
FROM bls_ip_data d
JOIN bls_ip_series s ON d.series_id = s.series_id
JOIN bls_ip_industries i ON s.industry_code = i.industry_code
JOIN bls_ip_measures m ON s.measure_code = m.measure_code
CROSS JOIN latest_year ly
WHERE m.measure_text ILIKE '%output per hour%'
  AND s.seasonal_code = 'S'
  AND d.year = ly.year
  AND i.display_level = 1
ORDER BY d.value DESC
LIMIT 20;


-- ============================================================================
-- DATA QUALITY & MONITORING QUERIES
-- ============================================================================

-- Count observations by year and measure
SELECT
    year,
    m.measure_text,
    COUNT(*) as observation_count
FROM bls_ip_data d
JOIN bls_ip_series s ON d.series_id = s.series_id
JOIN bls_ip_measures m ON s.measure_code = m.measure_code
WHERE year >= 2015
GROUP BY year, m.measure_code, m.measure_text
ORDER BY year DESC, m.measure_code;

-- Find series with most recent updates
SELECT
    s.series_id,
    s.series_title,
    MAX(d.year) as latest_year,
    MAX(d.updated_at) as last_updated
FROM bls_ip_series s
JOIN bls_ip_data d ON s.series_id = d.series_id
WHERE s.is_active = true
GROUP BY s.series_id, s.series_title
ORDER BY last_updated DESC
LIMIT 20;

-- Check for NULL values
SELECT
    series_id,
    COUNT(*) as null_count
FROM bls_ip_data
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
    i.industry_name,
    m.measure_text,
    CASE s.seasonal_code
        WHEN 'S' THEN 'Seasonally Adjusted'
        WHEN 'U' THEN 'Not Seasonally Adjusted'
    END as seasonal_adjustment,
    d.year,
    d.value,
    d.footnote_codes
FROM bls_ip_data d
JOIN bls_ip_series s ON d.series_id = s.series_id
JOIN bls_ip_industries i ON s.industry_code = i.industry_code
JOIN bls_ip_measures m ON s.measure_code = m.measure_code
WHERE d.year >= 2015
ORDER BY s.series_id, d.year
LIMIT 1000;

-- Summary statistics by industry
SELECT
    i.industry_name,
    COUNT(DISTINCT s.series_id) as series_count,
    COUNT(d.value) as data_points,
    MIN(d.year) as earliest_year,
    MAX(d.year) as latest_year
FROM bls_ip_industries i
JOIN bls_ip_series s ON i.industry_code = s.industry_code
LEFT JOIN bls_ip_data d ON s.series_id = d.series_id
WHERE s.is_active = true
  AND i.display_level <= 2
GROUP BY i.industry_code, i.industry_name
ORDER BY i.sort_sequence
LIMIT 50;

-- Industry productivity dashboard (latest year)
WITH latest_year AS (
    SELECT MAX(year) as year FROM bls_ip_data
)
SELECT
    i.industry_name,
    MAX(CASE WHEN m.measure_text ILIKE '%output%' THEN d.value END) as output,
    MAX(CASE WHEN m.measure_text ILIKE '%hours%' THEN d.value END) as hours,
    MAX(CASE WHEN m.measure_text ILIKE '%output per hour%' THEN d.value END) as productivity,
    MAX(CASE WHEN m.measure_text ILIKE '%unit labor costs%' THEN d.value END) as unit_labor_costs,
    MAX(d.year) as year
FROM bls_ip_data d
JOIN bls_ip_series s ON d.series_id = s.series_id
JOIN bls_ip_industries i ON s.industry_code = i.industry_code
JOIN bls_ip_measures m ON s.measure_code = m.measure_code
CROSS JOIN latest_year ly
WHERE s.seasonal_code = 'S'
  AND d.year = ly.year
  AND i.display_level = 1
GROUP BY i.industry_code, i.industry_name
ORDER BY productivity DESC
LIMIT 20;
