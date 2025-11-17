-- ============================================================================
-- BLS LA (Local Area Unemployment Statistics) SQL Query Documentation
-- ============================================================================
-- Survey: Local Area Unemployment Statistics
-- Tables: bls_la_areas, bls_la_measures, bls_la_series, bls_la_data
-- Shared Tables: bls_periods
-- ============================================================================

-- ============================================================================
-- METADATA QUERIES
-- ============================================================================

-- List all area types
SELECT
    area_type_code,
    COUNT(*) as area_count,
    MIN(area_text) as example_area
FROM bls_la_areas
GROUP BY area_type_code
ORDER BY area_type_code;
-- Common types: A=State, B=Metro, C=County/City, F=Multi-state regions

-- List all states
SELECT
    area_code,
    area_text,
    display_level,
    selectable,
    sort_sequence
FROM bls_la_areas
WHERE area_type_code = 'A'
ORDER BY area_text;

-- List all metropolitan statistical areas
SELECT
    area_code,
    area_text,
    display_level,
    selectable
FROM bls_la_areas
WHERE area_type_code = 'B'
  AND selectable = 'T'
ORDER BY area_text
LIMIT 50;

-- List all measures (unemployment rate, unemployment level, etc.)
SELECT
    measure_code,
    measure_text
FROM bls_la_measures
ORDER BY measure_code;

-- Count series by measure type
SELECT
    m.measure_text,
    COUNT(*) as series_count
FROM bls_la_series s
JOIN bls_la_measures m ON s.measure_code = m.measure_code
WHERE s.is_active = true
GROUP BY m.measure_text
ORDER BY series_count DESC;

-- Count series by area type
SELECT
    a.area_type_code,
    COUNT(*) as series_count,
    COUNT(CASE WHEN s.seasonal_code = 'S' THEN 1 END) as seasonally_adjusted,
    COUNT(CASE WHEN s.seasonal_code = 'U' THEN 1 END) as not_adjusted
FROM bls_la_series s
JOIN bls_la_areas a ON s.area_code = a.area_code
WHERE s.is_active = true
GROUP BY a.area_type_code
ORDER BY a.area_type_code;


-- ============================================================================
-- SERIES QUERIES
-- ============================================================================

-- Find unemployment rate series for a specific state (e.g., California)
SELECT
    s.series_id,
    s.series_title,
    a.area_text,
    m.measure_text,
    s.seasonal_code,
    s.begin_year,
    s.end_year
FROM bls_la_series s
JOIN bls_la_areas a ON s.area_code = a.area_code
JOIN bls_la_measures m ON s.measure_code = m.measure_code
WHERE a.area_text ILIKE '%California%'
  AND a.area_type_code = 'A'
  AND m.measure_text ILIKE '%unemployment rate%'
  AND s.is_active = true;

-- Find all series for a metropolitan area (e.g., New York)
SELECT
    s.series_id,
    s.series_title,
    m.measure_text,
    CASE s.seasonal_code
        WHEN 'S' THEN 'Seasonally Adjusted'
        WHEN 'U' THEN 'Not Seasonally Adjusted'
    END as seasonal_adjustment
FROM bls_la_series s
JOIN bls_la_areas a ON s.area_code = a.area_code
JOIN bls_la_measures m ON s.measure_code = m.measure_code
WHERE a.area_text ILIKE '%New York%'
  AND a.area_type_code = 'B'
  AND s.is_active = true
ORDER BY m.measure_code, s.seasonal_code;

-- Search series by area name keyword
SELECT
    s.series_id,
    s.series_title,
    a.area_text,
    a.area_type_code,
    m.measure_text
FROM bls_la_series s
JOIN bls_la_areas a ON s.area_code = a.area_code
JOIN bls_la_measures m ON s.measure_code = m.measure_code
WHERE a.area_text ILIKE '%Los Angeles%'
  AND s.is_active = true
ORDER BY a.area_text, m.measure_code;


-- ============================================================================
-- TIME SERIES DATA QUERIES
-- ============================================================================

-- Get latest unemployment rate for a specific area
SELECT
    d.year,
    d.period,
    d.value as unemployment_rate,
    d.footnote_codes,
    d.updated_at
FROM bls_la_data d
JOIN bls_la_series s ON d.series_id = s.series_id
JOIN bls_la_areas a ON s.area_code = a.area_code
WHERE a.area_text = 'California'
  AND a.area_type_code = 'A'
  AND s.measure_code = '03'  -- Unemployment rate
  AND s.seasonal_code = 'U'
ORDER BY d.year DESC, d.period DESC
LIMIT 1;

-- Get monthly unemployment data for specific area and year
SELECT
    d.year,
    p.period_name,
    d.value as unemployment_rate
FROM bls_la_data d
JOIN bls_la_series s ON d.series_id = s.series_id
JOIN bls_la_areas a ON s.area_code = a.area_code
JOIN bls_periods p ON d.period = p.period_code
WHERE a.area_text = 'Texas'
  AND a.area_type_code = 'A'
  AND s.measure_code = '03'
  AND s.seasonal_code = 'U'
  AND d.year = 2024
  AND p.period_type = 'MONTHLY'
ORDER BY d.period;

-- Compare seasonally adjusted vs not adjusted unemployment rate
SELECT
    d.year,
    d.period,
    MAX(CASE WHEN s.seasonal_code = 'S' THEN d.value END) as seasonally_adjusted,
    MAX(CASE WHEN s.seasonal_code = 'U' THEN d.value END) as not_adjusted
FROM bls_la_data d
JOIN bls_la_series s ON d.series_id = s.series_id
JOIN bls_la_areas a ON s.area_code = a.area_code
WHERE a.area_text = 'Florida'
  AND a.area_type_code = 'A'
  AND s.measure_code = '03'
  AND d.year >= 2023
GROUP BY d.year, d.period
ORDER BY d.year, d.period;


-- ============================================================================
-- GEOGRAPHIC COMPARISON QUERIES
-- ============================================================================

-- Compare latest unemployment rates across all states
WITH latest_month AS (
    SELECT MAX(year) as year, MAX(period) as period
    FROM bls_la_data d
    JOIN bls_la_series s ON d.series_id = s.series_id
    WHERE s.measure_code = '03' AND s.seasonal_code = 'U'
)
SELECT
    a.area_text as state,
    d.value as unemployment_rate,
    d.year,
    d.period
FROM bls_la_data d
JOIN bls_la_series s ON d.series_id = s.series_id
JOIN bls_la_areas a ON s.area_code = a.area_code
CROSS JOIN latest_month lm
WHERE a.area_type_code = 'A'
  AND s.measure_code = '03'
  AND s.seasonal_code = 'U'
  AND d.year = lm.year
  AND d.period = lm.period
ORDER BY d.value DESC;

-- Compare unemployment rates across major metropolitan areas
WITH latest_month AS (
    SELECT MAX(year) as year, MAX(period) as period
    FROM bls_la_data d
    JOIN bls_la_series s ON d.series_id = s.series_id
    WHERE s.measure_code = '03' AND s.seasonal_code = 'U'
)
SELECT
    a.area_text as metro_area,
    d.value as unemployment_rate,
    d.year,
    d.period
FROM bls_la_data d
JOIN bls_la_series s ON d.series_id = s.series_id
JOIN bls_la_areas a ON s.area_code = a.area_code
CROSS JOIN latest_month lm
WHERE a.area_type_code = 'B'
  AND a.selectable = 'T'
  AND s.measure_code = '03'
  AND s.seasonal_code = 'U'
  AND d.year = lm.year
  AND d.period = lm.period
ORDER BY d.value DESC
LIMIT 20;

-- States with highest and lowest unemployment rates (latest)
WITH latest_rates AS (
    SELECT
        a.area_text as state,
        d.value as rate,
        d.year,
        d.period,
        ROW_NUMBER() OVER (PARTITION BY a.area_code ORDER BY d.year DESC, d.period DESC) as rn
    FROM bls_la_data d
    JOIN bls_la_series s ON d.series_id = s.series_id
    JOIN bls_la_areas a ON s.area_code = a.area_code
    WHERE a.area_type_code = 'A'
      AND s.measure_code = '03'
      AND s.seasonal_code = 'U'
)
SELECT * FROM (
    SELECT 'Highest' as category, state, rate, year, period
    FROM latest_rates
    WHERE rn = 1
    ORDER BY rate DESC
    LIMIT 5
) highest
UNION ALL
SELECT * FROM (
    SELECT 'Lowest' as category, state, rate, year, period
    FROM latest_rates
    WHERE rn = 1
    ORDER BY rate ASC
    LIMIT 5
) lowest;


-- ============================================================================
-- ANALYTICAL QUERIES
-- ============================================================================

-- Calculate year-over-year change in unemployment rate
WITH current_year AS (
    SELECT area_code, year, period, value
    FROM bls_la_data d
    JOIN bls_la_series s ON d.series_id = s.series_id
    WHERE s.measure_code = '03'
      AND s.seasonal_code = 'U'
      AND year = 2024
),
prior_year AS (
    SELECT area_code, year, period, value
    FROM bls_la_data d
    JOIN bls_la_series s ON d.series_id = s.series_id
    WHERE s.measure_code = '03'
      AND s.seasonal_code = 'U'
      AND year = 2023
)
SELECT
    a.area_text as state,
    c.period,
    c.value as current_rate,
    p.value as prior_rate,
    ROUND((c.value - p.value), 2) as change_pct_points
FROM current_year c
JOIN prior_year p ON c.area_code = p.area_code AND c.period = p.period
JOIN bls_la_areas a ON c.area_code = a.area_code
WHERE a.area_type_code = 'A'
ORDER BY change_pct_points DESC;

-- Track unemployment rate trend for specific state (12-month moving average)
SELECT
    d.year,
    d.period,
    d.value as unemployment_rate,
    ROUND(AVG(d.value) OVER (
        ORDER BY d.year, d.period
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ), 2) as moving_avg_12mo
FROM bls_la_data d
JOIN bls_la_series s ON d.series_id = s.series_id
JOIN bls_la_areas a ON s.area_code = a.area_code
WHERE a.area_text = 'California'
  AND a.area_type_code = 'A'
  AND s.measure_code = '03'
  AND s.seasonal_code = 'U'
  AND d.year >= 2020
ORDER BY d.year, d.period;

-- Find areas with largest unemployment rate increases (past 12 months)
WITH latest_data AS (
    SELECT
        s.area_code,
        d.year,
        d.period,
        d.value,
        ROW_NUMBER() OVER (PARTITION BY s.area_code ORDER BY d.year DESC, d.period DESC) as rn
    FROM bls_la_data d
    JOIN bls_la_series s ON d.series_id = s.series_id
    WHERE s.measure_code = '03'
      AND s.seasonal_code = 'U'
),
year_ago_data AS (
    SELECT
        s.area_code,
        d.value,
        ROW_NUMBER() OVER (PARTITION BY s.area_code ORDER BY d.year DESC, d.period DESC) as rn
    FROM bls_la_data d
    JOIN bls_la_series s ON d.series_id = s.series_id
    WHERE s.measure_code = '03'
      AND s.seasonal_code = 'U'
      AND (d.year = EXTRACT(YEAR FROM CURRENT_DATE) - 1
           OR (d.year = EXTRACT(YEAR FROM CURRENT_DATE) AND d.period < TO_CHAR(CURRENT_DATE, 'FMM')))
)
SELECT
    a.area_text,
    a.area_type_code,
    l.value as current_rate,
    y.value as year_ago_rate,
    ROUND((l.value - y.value), 2) as change
FROM latest_data l
JOIN year_ago_data y ON l.area_code = y.area_code AND y.rn = 1
JOIN bls_la_areas a ON l.area_code = a.area_code
WHERE l.rn = 1
  AND a.area_type_code = 'A'
ORDER BY (l.value - y.value) DESC
LIMIT 10;

-- Multi-measure analysis for a specific area (unemployment rate, level, labor force)
SELECT
    d.year,
    d.period,
    MAX(CASE WHEN m.measure_code = '03' THEN d.value END) as unemployment_rate,
    MAX(CASE WHEN m.measure_code = '04' THEN d.value END) as unemployment_level,
    MAX(CASE WHEN m.measure_code = '05' THEN d.value END) as employment_level,
    MAX(CASE WHEN m.measure_code = '06' THEN d.value END) as labor_force
FROM bls_la_data d
JOIN bls_la_series s ON d.series_id = s.series_id
JOIN bls_la_measures m ON s.measure_code = m.measure_code
JOIN bls_la_areas a ON s.area_code = a.area_code
WHERE a.area_text = 'New York'
  AND a.area_type_code = 'A'
  AND s.seasonal_code = 'U'
  AND d.year >= 2023
GROUP BY d.year, d.period
ORDER BY d.year, d.period;


-- ============================================================================
-- DATA QUALITY & MONITORING QUERIES
-- ============================================================================

-- Count observations by year and measure
SELECT
    year,
    m.measure_text,
    COUNT(*) as observation_count
FROM bls_la_data d
JOIN bls_la_series s ON d.series_id = s.series_id
JOIN bls_la_measures m ON s.measure_code = m.measure_code
WHERE year >= 2020
GROUP BY year, m.measure_text
ORDER BY year DESC, m.measure_text;

-- Find series with most recent updates
SELECT
    s.series_id,
    a.area_text,
    m.measure_text,
    MAX(d.year) as latest_year,
    MAX(d.period) as latest_period,
    MAX(d.updated_at) as last_updated
FROM bls_la_series s
JOIN bls_la_data d ON s.series_id = d.series_id
JOIN bls_la_areas a ON s.area_code = a.area_code
JOIN bls_la_measures m ON s.measure_code = m.measure_code
WHERE s.is_active = true
GROUP BY s.series_id, a.area_text, m.measure_text
ORDER BY last_updated DESC
LIMIT 20;

-- Check for NULL values in time series
SELECT
    s.series_id,
    a.area_text,
    m.measure_text,
    COUNT(*) as null_count
FROM bls_la_data d
JOIN bls_la_series s ON d.series_id = s.series_id
JOIN bls_la_areas a ON s.area_code = a.area_code
JOIN bls_la_measures m ON s.measure_code = m.measure_code
WHERE d.value IS NULL
GROUP BY s.series_id, a.area_text, m.measure_text
ORDER BY null_count DESC
LIMIT 20;

-- Recent data updates (last 7 days)
SELECT
    a.area_text,
    m.measure_text,
    COUNT(*) as updated_records,
    MAX(d.updated_at) as last_update
FROM bls_la_data d
JOIN bls_la_series s ON d.series_id = s.series_id
JOIN bls_la_areas a ON s.area_code = a.area_code
JOIN bls_la_measures m ON s.measure_code = m.measure_code
WHERE d.updated_at >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY a.area_text, m.measure_text
ORDER BY last_update DESC;


-- ============================================================================
-- EXPORT / REPORTING QUERIES
-- ============================================================================

-- Export complete time series with metadata for a state
SELECT
    s.series_id,
    s.series_title,
    a.area_text,
    a.area_type_code,
    m.measure_text,
    CASE s.seasonal_code
        WHEN 'S' THEN 'Seasonally Adjusted'
        WHEN 'U' THEN 'Not Seasonally Adjusted'
    END as seasonal_adjustment,
    d.year,
    p.period_name,
    d.value,
    d.footnote_codes
FROM bls_la_data d
JOIN bls_la_series s ON d.series_id = s.series_id
JOIN bls_la_areas a ON s.area_code = a.area_code
JOIN bls_la_measures m ON s.measure_code = m.measure_code
JOIN bls_periods p ON d.period = p.period_code
WHERE a.area_text = 'Texas'
  AND a.area_type_code = 'A'
  AND d.year >= 2020
ORDER BY m.measure_code, s.seasonal_code, d.year, d.period;

-- Summary statistics by area type
SELECT
    a.area_type_code,
    COUNT(DISTINCT a.area_code) as area_count,
    COUNT(DISTINCT s.series_id) as series_count,
    COUNT(d.value) as data_points,
    MIN(d.year) as earliest_year,
    MAX(d.year) as latest_year
FROM bls_la_areas a
LEFT JOIN bls_la_series s ON a.area_code = s.area_code
LEFT JOIN bls_la_data d ON s.series_id = d.series_id
GROUP BY a.area_type_code
ORDER BY a.area_type_code;

-- State unemployment dashboard (latest month)
WITH latest_month AS (
    SELECT MAX(year) as year, MAX(period) as period
    FROM bls_la_data d
    JOIN bls_la_series s ON d.series_id = s.series_id
    WHERE s.measure_code = '03' AND s.seasonal_code = 'U'
)
SELECT
    a.area_text as state,
    d.value as unemployment_rate,
    LAG(d.value, 1) OVER (PARTITION BY a.area_code ORDER BY d.year, d.period) as prev_month_rate,
    LAG(d.value, 12) OVER (PARTITION BY a.area_code ORDER BY d.year, d.period) as year_ago_rate,
    ROUND((d.value - LAG(d.value, 1) OVER (PARTITION BY a.area_code ORDER BY d.year, d.period)), 2) as mom_change,
    ROUND((d.value - LAG(d.value, 12) OVER (PARTITION BY a.area_code ORDER BY d.year, d.period)), 2) as yoy_change
FROM bls_la_data d
JOIN bls_la_series s ON d.series_id = s.series_id
JOIN bls_la_areas a ON s.area_code = a.area_code
CROSS JOIN latest_month lm
WHERE a.area_type_code = 'A'
  AND s.measure_code = '03'
  AND s.seasonal_code = 'U'
  AND d.year = lm.year
  AND d.period = lm.period
ORDER BY a.area_text;


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
    'bls_la_areas' as table_name,
    COUNT(*) as record_count,
    MAX(updated_at) as last_updated
FROM bls_la_areas
UNION ALL
SELECT
    'bls_la_measures',
    COUNT(*),
    MAX(created_at)
FROM bls_la_measures
UNION ALL
SELECT
    'bls_la_series',
    COUNT(*),
    MAX(updated_at)
FROM bls_la_series
UNION ALL
SELECT
    'bls_la_data',
    COUNT(*),
    MAX(updated_at)
FROM bls_la_data;

-- Area hierarchy example (find all areas within a state)
SELECT
    area_code,
    area_type_code,
    area_text,
    display_level
FROM bls_la_areas
WHERE area_text ILIKE '%California%'
ORDER BY area_type_code, display_level, area_text;


-- ============================================================================
-- HISTORICAL ANALYSIS QUERIES
-- ============================================================================

-- Find historical peaks and troughs in unemployment rate for a state
WITH state_data AS (
    SELECT
        d.year,
        d.period,
        d.value as rate,
        LAG(d.value) OVER (ORDER BY d.year, d.period) as prev_rate,
        LEAD(d.value) OVER (ORDER BY d.year, d.period) as next_rate
    FROM bls_la_data d
    JOIN bls_la_series s ON d.series_id = s.series_id
    JOIN bls_la_areas a ON s.area_code = a.area_code
    WHERE a.area_text = 'California'
      AND a.area_type_code = 'A'
      AND s.measure_code = '03'
      AND s.seasonal_code = 'U'
)
SELECT
    year,
    period,
    rate,
    CASE
        WHEN rate > prev_rate AND rate > next_rate THEN 'Local Peak'
        WHEN rate < prev_rate AND rate < next_rate THEN 'Local Trough'
    END as turning_point
FROM state_data
WHERE (rate > prev_rate AND rate > next_rate)
   OR (rate < prev_rate AND rate < next_rate)
ORDER BY year DESC, period DESC
LIMIT 20;

-- Recession periods analysis (unemployment rate increases > 0.5 percentage points)
WITH monthly_changes AS (
    SELECT
        a.area_text,
        d.year,
        d.period,
        d.value as current_rate,
        LAG(d.value) OVER (PARTITION BY a.area_code ORDER BY d.year, d.period) as prev_rate
    FROM bls_la_data d
    JOIN bls_la_series s ON d.series_id = s.series_id
    JOIN bls_la_areas a ON s.area_code = a.area_code
    WHERE a.area_type_code = 'A'
      AND s.measure_code = '03'
      AND s.seasonal_code = 'U'
)
SELECT
    area_text as state,
    year,
    period,
    current_rate,
    prev_rate,
    ROUND((current_rate - prev_rate), 2) as change
FROM monthly_changes
WHERE (current_rate - prev_rate) >= 0.5
ORDER BY change DESC, year DESC
LIMIT 50;

-- Long-term trend: Decade averages by state
SELECT
    a.area_text as state,
    FLOOR(d.year / 10) * 10 as decade,
    ROUND(AVG(d.value), 2) as avg_unemployment_rate,
    ROUND(MIN(d.value), 2) as min_rate,
    ROUND(MAX(d.value), 2) as max_rate
FROM bls_la_data d
JOIN bls_la_series s ON d.series_id = s.series_id
JOIN bls_la_areas a ON s.area_code = a.area_code
WHERE a.area_type_code = 'A'
  AND s.measure_code = '03'
  AND s.seasonal_code = 'U'
GROUP BY a.area_text, FLOOR(d.year / 10) * 10
ORDER BY a.area_text, decade DESC;
