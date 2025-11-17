-- ============================================================================
-- BLS JT (Job Openings and Labor Turnover Survey - JOLTS) SQL Query Documentation
-- ============================================================================
-- Survey: Job Openings and Labor Turnover Survey (JOLTS)
-- Tables: bls_jt_data_elements, bls_jt_industries, bls_jt_states, bls_jt_areas,
--         bls_jt_size_classes, bls_jt_rate_levels, bls_jt_series, bls_jt_data
-- Shared Tables: bls_periods, bls_periodicity
-- ============================================================================

-- ============================================================================
-- METADATA QUERIES
-- ============================================================================

-- List all data elements
SELECT
    dataelement_code,
    dataelement_text
FROM bls_jt_data_elements
ORDER BY dataelement_code;
-- JO=Job Openings, HI=Hires, QU=Quits, LD=Layoffs/Discharges, OS=Other Separations, TS=Total Separations

-- List all industries
SELECT
    industry_code,
    industry_name,
    display_level,
    selectable,
    sort_sequence
FROM bls_jt_industries
ORDER BY sort_sequence
LIMIT 50;

-- List all states
SELECT
    state_code,
    state_name
FROM bls_jt_states
ORDER BY state_name;

-- List all areas
SELECT
    area_code,
    area_name
FROM bls_jt_areas
ORDER BY area_name;

-- List all size classes
SELECT
    sizeclass_code,
    sizeclass_text
FROM bls_jt_size_classes
ORDER BY sizeclass_code;

-- List all rate/level indicators
SELECT
    ratelevel_code,
    ratelevel_text
FROM bls_jt_rate_levels
ORDER BY ratelevel_code;
-- L=Level (thousands), R=Rate (percent)


-- ============================================================================
-- SERIES QUERIES
-- ============================================================================

-- Find job openings series for specific industry
SELECT
    s.series_id,
    s.series_title,
    de.dataelement_text,
    i.industry_name,
    st.state_name,
    rl.ratelevel_text,
    s.is_active
FROM bls_jt_series s
LEFT JOIN bls_jt_data_elements de ON s.dataelement_code = de.dataelement_code
LEFT JOIN bls_jt_industries i ON s.industry_code = i.industry_code
LEFT JOIN bls_jt_states st ON s.state_code = st.state_code
LEFT JOIN bls_jt_rate_levels rl ON s.ratelevel_code = rl.ratelevel_code
WHERE de.dataelement_text ILIKE '%job openings%'
  AND i.industry_name ILIKE '%tech%'
  AND s.is_active = true;

-- Find hires and separations series for total nonfarm
SELECT
    s.series_id,
    s.series_title,
    de.dataelement_text,
    rl.ratelevel_text,
    s.seasonal_code,
    s.is_active
FROM bls_jt_series s
JOIN bls_jt_data_elements de ON s.dataelement_code = de.dataelement_code
LEFT JOIN bls_jt_industries i ON s.industry_code = i.industry_code
LEFT JOIN bls_jt_rate_levels rl ON s.ratelevel_code = rl.ratelevel_code
WHERE i.industry_name ILIKE '%total nonfarm%'
  AND de.dataelement_code IN ('HI', 'TS')
  AND s.is_active = true;

-- Search series by title keyword
SELECT
    series_id,
    series_title,
    dataelement_code,
    industry_code,
    state_code,
    is_active
FROM bls_jt_series
WHERE series_title ILIKE '%manufacturing%'
  AND is_active = true
ORDER BY series_title
LIMIT 20;


-- ============================================================================
-- TIME SERIES DATA QUERIES
-- ============================================================================

-- Get latest job openings data
SELECT
    s.series_title,
    d.year,
    d.period,
    d.value,
    rl.ratelevel_text,
    d.footnote_codes,
    d.updated_at
FROM bls_jt_data d
JOIN bls_jt_series s ON d.series_id = s.series_id
JOIN bls_jt_data_elements de ON s.dataelement_code = de.dataelement_code
LEFT JOIN bls_jt_rate_levels rl ON s.ratelevel_code = rl.ratelevel_code
WHERE de.dataelement_code = 'JO'
  AND s.seasonal_code = 'U'
ORDER BY d.year DESC, d.period DESC
LIMIT 10;

-- Get monthly JOLTS data for specific series and year
SELECT
    d.year,
    p.period_name,
    d.value,
    d.footnote_codes
FROM bls_jt_data d
JOIN bls_jt_series s ON d.series_id = s.series_id
JOIN bls_periods p ON d.period = p.period_code
WHERE d.year = 2024
  AND p.period_type = 'MONTHLY'
  AND s.seasonal_code = 'U'
ORDER BY d.period
LIMIT 50;

-- Compare seasonally adjusted vs not adjusted
SELECT
    d.year,
    d.period,
    MAX(CASE WHEN s.seasonal_code = 'S' THEN d.value END) as seasonally_adjusted,
    MAX(CASE WHEN s.seasonal_code = 'U' THEN d.value END) as not_adjusted
FROM bls_jt_data d
JOIN bls_jt_series s ON d.series_id = s.series_id
JOIN bls_jt_data_elements de ON s.dataelement_code = de.dataelement_code
WHERE de.dataelement_code = 'JO'
  AND d.year >= 2023
GROUP BY d.year, d.period
ORDER BY d.year, d.period;


-- ============================================================================
-- ANALYTICAL QUERIES
-- ============================================================================

-- Compare job market indicators (latest month)
WITH latest_month AS (
    SELECT MAX(year) as year, MAX(period) as period
    FROM bls_jt_data d
    JOIN bls_jt_series s ON d.series_id = s.series_id
    WHERE s.seasonal_code = 'U'
)
SELECT
    de.dataelement_text as indicator,
    d.value,
    rl.ratelevel_text,
    d.year,
    d.period
FROM bls_jt_data d
JOIN bls_jt_series s ON d.series_id = s.series_id
JOIN bls_jt_data_elements de ON s.dataelement_code = de.dataelement_code
LEFT JOIN bls_jt_rate_levels rl ON s.ratelevel_code = rl.ratelevel_code
CROSS JOIN latest_month lm
WHERE s.seasonal_code = 'U'
  AND rl.ratelevel_code = 'L'  -- Level
  AND d.year = lm.year
  AND d.period = lm.period
ORDER BY de.dataelement_code;

-- Calculate year-over-year change in job openings
WITH current_year AS (
    SELECT year, period, value
    FROM bls_jt_data d
    JOIN bls_jt_series s ON d.series_id = s.series_id
    JOIN bls_jt_data_elements de ON s.dataelement_code = de.dataelement_code
    WHERE de.dataelement_code = 'JO'
      AND s.ratelevel_code = 'L'
      AND s.seasonal_code = 'U'
      AND year = 2024
),
prior_year AS (
    SELECT year, period, value
    FROM bls_jt_data d
    JOIN bls_jt_series s ON d.series_id = s.series_id
    JOIN bls_jt_data_elements de ON s.dataelement_code = de.dataelement_code
    WHERE de.dataelement_code = 'JO'
      AND s.ratelevel_code = 'L'
      AND s.seasonal_code = 'U'
      AND year = 2023
)
SELECT
    c.period,
    c.value as current_openings,
    p.value as prior_openings,
    ROUND((c.value - p.value), 0) as change_thousands,
    ROUND(((c.value - p.value) / p.value * 100), 2) as pct_change
FROM current_year c
JOIN prior_year p ON c.period = p.period
ORDER BY c.period;

-- Track job openings trend (12-month moving average)
SELECT
    d.year,
    d.period,
    d.value as job_openings,
    AVG(d.value) OVER (
        ORDER BY d.year, d.period
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) as moving_avg_12mo
FROM bls_jt_data d
JOIN bls_jt_series s ON d.series_id = s.series_id
JOIN bls_jt_data_elements de ON s.dataelement_code = de.dataelement_code
WHERE de.dataelement_code = 'JO'
  AND s.ratelevel_code = 'L'
  AND s.seasonal_code = 'U'
  AND d.year >= 2020
ORDER BY d.year, d.period;

-- Multi-indicator analysis (openings, hires, quits, layoffs)
SELECT
    d.year,
    d.period,
    MAX(CASE WHEN de.dataelement_code = 'JO' THEN d.value END) as job_openings,
    MAX(CASE WHEN de.dataelement_code = 'HI' THEN d.value END) as hires,
    MAX(CASE WHEN de.dataelement_code = 'QU' THEN d.value END) as quits,
    MAX(CASE WHEN de.dataelement_code = 'LD' THEN d.value END) as layoffs
FROM bls_jt_data d
JOIN bls_jt_series s ON d.series_id = s.series_id
JOIN bls_jt_data_elements de ON s.dataelement_code = de.dataelement_code
WHERE s.seasonal_code = 'U'
  AND s.ratelevel_code = 'L'
  AND d.year >= 2023
GROUP BY d.year, d.period
ORDER BY d.year, d.period;

-- Quit rate as economic indicator (confidence measure)
SELECT
    d.year,
    d.period,
    d.value as quit_rate_pct
FROM bls_jt_data d
JOIN bls_jt_series s ON d.series_id = s.series_id
JOIN bls_jt_data_elements de ON s.dataelement_code = de.dataelement_code
WHERE de.dataelement_code = 'QU'
  AND s.ratelevel_code = 'R'  -- Rate
  AND s.seasonal_code = 'U'
  AND d.year >= 2020
ORDER BY d.year, d.period;


-- ============================================================================
-- DATA QUALITY & MONITORING QUERIES
-- ============================================================================

-- Count observations by year and data element
SELECT
    year,
    de.dataelement_text,
    COUNT(*) as observation_count
FROM bls_jt_data d
JOIN bls_jt_series s ON d.series_id = s.series_id
JOIN bls_jt_data_elements de ON s.dataelement_code = de.dataelement_code
WHERE year >= 2020
GROUP BY year, de.dataelement_text
ORDER BY year DESC, de.dataelement_code;

-- Find series with most recent updates
SELECT
    s.series_id,
    s.series_title,
    MAX(d.year) as latest_year,
    MAX(d.period) as latest_period,
    MAX(d.updated_at) as last_updated
FROM bls_jt_series s
JOIN bls_jt_data d ON s.series_id = d.series_id
WHERE s.is_active = true
GROUP BY s.series_id, s.series_title
ORDER BY last_updated DESC
LIMIT 20;

-- Check for NULL values
SELECT
    series_id,
    COUNT(*) as null_count
FROM bls_jt_data
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
    de.dataelement_text,
    i.industry_name,
    st.state_name,
    rl.ratelevel_text,
    CASE s.seasonal_code
        WHEN 'S' THEN 'Seasonally Adjusted'
        WHEN 'U' THEN 'Not Seasonally Adjusted'
    END as seasonal_adjustment,
    d.year,
    p.period_name,
    d.value,
    d.footnote_codes
FROM bls_jt_data d
JOIN bls_jt_series s ON d.series_id = s.series_id
JOIN bls_jt_data_elements de ON s.dataelement_code = de.dataelement_code
LEFT JOIN bls_jt_industries i ON s.industry_code = i.industry_code
LEFT JOIN bls_jt_states st ON s.state_code = st.state_code
LEFT JOIN bls_jt_rate_levels rl ON s.ratelevel_code = rl.ratelevel_code
JOIN bls_periods p ON d.period = p.period_code
WHERE d.year >= 2020
ORDER BY s.series_id, d.year, d.period
LIMIT 1000;

-- Summary statistics by data element
SELECT
    de.dataelement_text,
    COUNT(DISTINCT s.series_id) as series_count,
    COUNT(d.value) as data_points,
    MIN(d.year) as earliest_year,
    MAX(d.year) as latest_year
FROM bls_jt_data_elements de
JOIN bls_jt_series s ON de.dataelement_code = s.dataelement_code
LEFT JOIN bls_jt_data d ON s.series_id = d.series_id
WHERE s.is_active = true
GROUP BY de.dataelement_code, de.dataelement_text
ORDER BY de.dataelement_code;

-- JOLTS dashboard (latest month all indicators)
WITH latest_month AS (
    SELECT MAX(year) as year, MAX(period) as period
    FROM bls_jt_data d
    JOIN bls_jt_series s ON d.series_id = s.series_id
    WHERE s.seasonal_code = 'U'
)
SELECT
    de.dataelement_text as indicator,
    MAX(CASE WHEN rl.ratelevel_code = 'L' THEN d.value END) as level_thousands,
    MAX(CASE WHEN rl.ratelevel_code = 'R' THEN d.value END) as rate_percent,
    MAX(d.year) as year,
    MAX(d.period) as period
FROM bls_jt_data d
JOIN bls_jt_series s ON d.series_id = s.series_id
JOIN bls_jt_data_elements de ON s.dataelement_code = de.dataelement_code
LEFT JOIN bls_jt_rate_levels rl ON s.ratelevel_code = rl.ratelevel_code
CROSS JOIN latest_month lm
WHERE s.seasonal_code = 'U'
  AND d.year = lm.year
  AND d.period = lm.period
GROUP BY de.dataelement_code, de.dataelement_text
ORDER BY de.dataelement_code;
