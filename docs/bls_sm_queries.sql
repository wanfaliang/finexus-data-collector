-- ============================================================================
-- BLS SM (State and Metro Area Employment) SQL Query Documentation
-- ============================================================================
-- Survey: State and Metro Area Employment, Hours, and Earnings
-- Tables: bls_sm_states, bls_sm_areas, bls_sm_supersectors, bls_sm_industries,
--         bls_sm_series, bls_sm_data
-- Shared Tables: bls_periods, bls_periodicity
-- ============================================================================

-- ============================================================================
-- METADATA QUERIES
-- ============================================================================

-- List all states
SELECT
    state_code,
    state_name
FROM bls_sm_states
ORDER BY state_name;

-- List all metro areas by state
SELECT
    s.state_name,
    a.area_code,
    a.area_name
FROM bls_sm_areas a
JOIN bls_sm_states s ON a.state_code = s.state_code
ORDER BY s.state_name, a.area_name;

-- List all supersectors
SELECT
    supersector_code,
    supersector_name
FROM bls_sm_supersectors
ORDER BY supersector_code;

-- List all industries
SELECT
    industry_code,
    industry_name,
    display_level,
    selectable,
    sort_sequence
FROM bls_sm_industries
ORDER BY sort_sequence
LIMIT 50;

-- Count series by data type
SELECT
    data_type_code,
    COUNT(*) as series_count
FROM bls_sm_series
WHERE is_active = true
GROUP BY data_type_code
ORDER BY series_count DESC;
-- 01=Employment, 02=Avg Weekly Hours, 03=Avg Hourly Earnings, 11=Avg Weekly Earnings


-- ============================================================================
-- SERIES QUERIES
-- ============================================================================

-- Find employment series for specific state and industry
SELECT
    s.series_id,
    s.series_title,
    st.state_name,
    i.industry_name,
    s.data_type_code,
    s.seasonal_code,
    s.is_active
FROM bls_sm_series s
LEFT JOIN bls_sm_states st ON s.state_code = st.state_code
LEFT JOIN bls_sm_industries i ON s.industry_code = i.industry_code
WHERE st.state_name = 'California'
  AND i.industry_name ILIKE '%manufacturing%'
  AND s.data_type_code = '01'  -- Employment
  AND s.is_active = true;

-- Find series for specific metro area
SELECT
    s.series_id,
    s.series_title,
    a.area_name,
    i.industry_name,
    CASE s.data_type_code
        WHEN '01' THEN 'Employment'
        WHEN '02' THEN 'Avg Weekly Hours'
        WHEN '03' THEN 'Avg Hourly Earnings'
        WHEN '11' THEN 'Avg Weekly Earnings'
    END as data_type,
    s.seasonal_code
FROM bls_sm_series s
LEFT JOIN bls_sm_areas a ON s.area_code = a.area_code
LEFT JOIN bls_sm_industries i ON s.industry_code = i.industry_code
WHERE a.area_name ILIKE '%Los Angeles%'
  AND s.is_active = true
ORDER BY s.data_type_code, i.sort_sequence
LIMIT 20;

-- Search series by title keyword
SELECT
    series_id,
    series_title,
    state_code,
    area_code,
    data_type_code,
    is_active
FROM bls_sm_series
WHERE series_title ILIKE '%tech%'
  AND is_active = true
ORDER BY series_title
LIMIT 20;


-- ============================================================================
-- TIME SERIES DATA QUERIES
-- ============================================================================

-- Get latest employment data for a state
SELECT
    d.year,
    d.period,
    d.value as employment_thousands,
    d.footnote_codes,
    d.updated_at
FROM bls_sm_data d
JOIN bls_sm_series s ON d.series_id = s.series_id
JOIN bls_sm_states st ON s.state_code = st.state_code
WHERE st.state_name = 'Texas'
  AND s.data_type_code = '01'
  AND s.seasonal_code = 'U'
ORDER BY d.year DESC, d.period DESC
LIMIT 1;

-- Get monthly employment data for specific state and year
SELECT
    d.year,
    p.period_name,
    d.value as employment
FROM bls_sm_data d
JOIN bls_sm_series s ON d.series_id = s.series_id
JOIN bls_sm_states st ON s.state_code = st.state_code
JOIN bls_periods p ON d.period = p.period_code
WHERE st.state_name = 'Florida'
  AND s.data_type_code = '01'
  AND s.seasonal_code = 'U'
  AND d.year = 2024
  AND p.period_type = 'MONTHLY'
ORDER BY d.period;

-- Compare seasonally adjusted vs not adjusted
SELECT
    d.year,
    d.period,
    MAX(CASE WHEN s.seasonal_code = 'S' THEN d.value END) as seasonally_adjusted,
    MAX(CASE WHEN s.seasonal_code = 'U' THEN d.value END) as not_adjusted
FROM bls_sm_data d
JOIN bls_sm_series s ON d.series_id = s.series_id
JOIN bls_sm_states st ON s.state_code = st.state_code
WHERE st.state_name = 'New York'
  AND s.data_type_code = '01'
  AND d.year >= 2023
GROUP BY d.year, d.period
ORDER BY d.year, d.period;


-- ============================================================================
-- GEOGRAPHIC COMPARISON QUERIES
-- ============================================================================

-- Compare latest employment across all states
WITH latest_month AS (
    SELECT MAX(year) as year, MAX(period) as period
    FROM bls_sm_data d
    JOIN bls_sm_series s ON d.series_id = s.series_id
    WHERE s.data_type_code = '01' AND s.seasonal_code = 'U'
)
SELECT
    st.state_name,
    d.value as employment_thousands,
    d.year,
    d.period
FROM bls_sm_data d
JOIN bls_sm_series s ON d.series_id = s.series_id
JOIN bls_sm_states st ON s.state_code = st.state_code
CROSS JOIN latest_month lm
WHERE s.data_type_code = '01'
  AND s.seasonal_code = 'U'
  AND d.year = lm.year
  AND d.period = lm.period
ORDER BY d.value DESC;

-- Compare employment across metro areas
WITH latest_month AS (
    SELECT MAX(year) as year, MAX(period) as period
    FROM bls_sm_data d
    JOIN bls_sm_series s ON d.series_id = s.series_id
    WHERE s.data_type_code = '01' AND s.seasonal_code = 'U'
)
SELECT
    a.area_name,
    st.state_name,
    d.value as employment_thousands,
    d.year,
    d.period
FROM bls_sm_data d
JOIN bls_sm_series s ON d.series_id = s.series_id
LEFT JOIN bls_sm_areas a ON s.area_code = a.area_code
LEFT JOIN bls_sm_states st ON a.state_code = st.state_code
CROSS JOIN latest_month lm
WHERE s.data_type_code = '01'
  AND s.seasonal_code = 'U'
  AND a.area_code IS NOT NULL
  AND d.year = lm.year
  AND d.period = lm.period
ORDER BY d.value DESC
LIMIT 20;


-- ============================================================================
-- ANALYTICAL QUERIES
-- ============================================================================

-- Calculate year-over-year employment change
WITH current_year AS (
    SELECT state_code, year, period, value
    FROM bls_sm_data d
    JOIN bls_sm_series s ON d.series_id = s.series_id
    WHERE s.data_type_code = '01'
      AND s.seasonal_code = 'U'
      AND year = 2024
),
prior_year AS (
    SELECT state_code, year, period, value
    FROM bls_sm_data d
    JOIN bls_sm_series s ON d.series_id = s.series_id
    WHERE s.data_type_code = '01'
      AND s.seasonal_code = 'U'
      AND year = 2023
)
SELECT
    st.state_name,
    c.period,
    c.value as current_employment,
    p.value as prior_employment,
    ROUND((c.value - p.value), 1) as change_thousands,
    ROUND(((c.value - p.value) / p.value * 100), 2) as pct_change
FROM current_year c
JOIN prior_year p ON c.state_code = p.state_code AND c.period = p.period
JOIN bls_sm_states st ON c.state_code = st.state_code
ORDER BY pct_change DESC;

-- Track employment trend (12-month moving average)
SELECT
    d.year,
    d.period,
    d.value as employment,
    AVG(d.value) OVER (
        ORDER BY d.year, d.period
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) as moving_avg_12mo
FROM bls_sm_data d
JOIN bls_sm_series s ON d.series_id = s.series_id
JOIN bls_sm_states st ON s.state_code = st.state_code
WHERE st.state_name = 'California'
  AND s.data_type_code = '01'
  AND s.seasonal_code = 'U'
  AND d.year >= 2020
ORDER BY d.year, d.period;

-- Multi-metric analysis for a state (employment, hours, earnings)
SELECT
    d.year,
    d.period,
    MAX(CASE WHEN s.data_type_code = '01' THEN d.value END) as employment_thousands,
    MAX(CASE WHEN s.data_type_code = '02' THEN d.value END) as avg_weekly_hours,
    MAX(CASE WHEN s.data_type_code = '03' THEN d.value END) as avg_hourly_earnings,
    MAX(CASE WHEN s.data_type_code = '11' THEN d.value END) as avg_weekly_earnings
FROM bls_sm_data d
JOIN bls_sm_series s ON d.series_id = s.series_id
JOIN bls_sm_states st ON s.state_code = st.state_code
WHERE st.state_name = 'New York'
  AND s.seasonal_code = 'U'
  AND d.year >= 2023
GROUP BY d.year, d.period
ORDER BY d.year, d.period;


-- ============================================================================
-- DATA QUALITY & MONITORING QUERIES
-- ============================================================================

-- Count observations by year and data type
SELECT
    year,
    s.data_type_code,
    COUNT(*) as observation_count
FROM bls_sm_data d
JOIN bls_sm_series s ON d.series_id = s.series_id
WHERE year >= 2020
GROUP BY year, s.data_type_code
ORDER BY year DESC, s.data_type_code;

-- Find series with most recent updates
SELECT
    s.series_id,
    st.state_name,
    a.area_name,
    i.industry_name,
    MAX(d.year) as latest_year,
    MAX(d.period) as latest_period,
    MAX(d.updated_at) as last_updated
FROM bls_sm_series s
JOIN bls_sm_data d ON s.series_id = d.series_id
LEFT JOIN bls_sm_states st ON s.state_code = st.state_code
LEFT JOIN bls_sm_areas a ON s.area_code = a.area_code
LEFT JOIN bls_sm_industries i ON s.industry_code = i.industry_code
WHERE s.is_active = true
GROUP BY s.series_id, st.state_name, a.area_name, i.industry_name
ORDER BY last_updated DESC
LIMIT 20;

-- Check for NULL values
SELECT
    series_id,
    COUNT(*) as null_count
FROM bls_sm_data
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
    st.state_name,
    a.area_name,
    i.industry_name,
    CASE s.data_type_code
        WHEN '01' THEN 'Employment'
        WHEN '02' THEN 'Avg Weekly Hours'
        WHEN '03' THEN 'Avg Hourly Earnings'
        WHEN '11' THEN 'Avg Weekly Earnings'
    END as data_type,
    CASE s.seasonal_code
        WHEN 'S' THEN 'Seasonally Adjusted'
        WHEN 'U' THEN 'Not Seasonally Adjusted'
    END as seasonal_adjustment,
    d.year,
    p.period_name,
    d.value,
    d.footnote_codes
FROM bls_sm_data d
JOIN bls_sm_series s ON d.series_id = s.series_id
LEFT JOIN bls_sm_states st ON s.state_code = st.state_code
LEFT JOIN bls_sm_areas a ON s.area_code = a.area_code
LEFT JOIN bls_sm_industries i ON s.industry_code = i.industry_code
JOIN bls_periods p ON d.period = p.period_code
WHERE d.year >= 2020
ORDER BY s.series_id, d.year, d.period
LIMIT 1000;

-- Summary statistics by state
SELECT
    st.state_name,
    COUNT(DISTINCT s.series_id) as series_count,
    COUNT(d.value) as data_points,
    MIN(d.year) as earliest_year,
    MAX(d.year) as latest_year
FROM bls_sm_states st
JOIN bls_sm_series s ON st.state_code = s.state_code
LEFT JOIN bls_sm_data d ON s.series_id = d.series_id
WHERE s.is_active = true
GROUP BY st.state_code, st.state_name
ORDER BY st.state_name;
