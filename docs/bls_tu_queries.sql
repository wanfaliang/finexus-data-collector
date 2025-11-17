-- ============================================================================
-- BLS TU (American Time Use Survey) SQL Query Documentation
-- ============================================================================
-- Survey: American Time Use Survey (ATUS)
-- Tables: bls_tu_activities, bls_tu_demographics, bls_tu_series, bls_tu_data
-- Shared Tables: bls_periods, bls_periodicity
-- ============================================================================

-- ============================================================================
-- METADATA QUERIES
-- ============================================================================

-- List all activities
SELECT
    activity_code,
    activity_name,
    display_level,
    selectable,
    sort_sequence
FROM bls_tu_activities
ORDER BY sort_sequence
LIMIT 50;

-- List all demographic characteristics
SELECT
    demographic_code,
    demographic_name
FROM bls_tu_demographics
ORDER BY demographic_code;

-- Count series by activity
SELECT
    a.activity_name,
    COUNT(*) as series_count
FROM bls_tu_series s
LEFT JOIN bls_tu_activities a ON s.activity_code = a.activity_code
WHERE s.is_active = true
GROUP BY a.activity_code, a.activity_name
ORDER BY series_count DESC
LIMIT 20;

-- Count series by demographic
SELECT
    d.demographic_name,
    COUNT(*) as series_count
FROM bls_tu_series s
LEFT JOIN bls_tu_demographics d ON s.demographic_code = d.demographic_code
WHERE s.is_active = true
GROUP BY d.demographic_code, d.demographic_name
ORDER BY series_count DESC;

-- List top-level activities
SELECT
    activity_code,
    activity_name,
    display_level
FROM bls_tu_activities
WHERE display_level = 0
ORDER BY sort_sequence;


-- ============================================================================
-- SERIES QUERIES
-- ============================================================================

-- Find time use series for specific activity
SELECT
    s.series_id,
    s.series_title,
    a.activity_name,
    d.demographic_name,
    s.is_active
FROM bls_tu_series s
LEFT JOIN bls_tu_activities a ON s.activity_code = a.activity_code
LEFT JOIN bls_tu_demographics d ON s.demographic_code = d.demographic_code
WHERE a.activity_name ILIKE '%work%'
  AND s.is_active = true
LIMIT 20;

-- Find series for specific demographic
SELECT
    s.series_id,
    s.series_title,
    a.activity_name,
    d.demographic_name,
    s.is_active
FROM bls_tu_series s
LEFT JOIN bls_tu_activities a ON s.activity_code = a.activity_code
LEFT JOIN bls_tu_demographics d ON s.demographic_code = d.demographic_code
WHERE d.demographic_name ILIKE '%employed%'
  AND s.is_active = true
ORDER BY a.sort_sequence
LIMIT 20;

-- Search series by title keyword
SELECT
    series_id,
    series_title,
    activity_code,
    demographic_code,
    is_active
FROM bls_tu_series
WHERE series_title ILIKE '%sleep%'
  AND is_active = true
ORDER BY series_title
LIMIT 20;


-- ============================================================================
-- TIME SERIES DATA QUERIES
-- ============================================================================

-- Get latest time use data
SELECT
    a.activity_name,
    d.demographic_name,
    dt.year,
    dt.value as hours_per_day,
    dt.footnote_codes,
    dt.updated_at
FROM bls_tu_data dt
JOIN bls_tu_series s ON dt.series_id = s.series_id
LEFT JOIN bls_tu_activities a ON s.activity_code = a.activity_code
LEFT JOIN bls_tu_demographics d ON s.demographic_code = d.demographic_code
ORDER BY dt.year DESC
LIMIT 20;

-- Get annual time use data for specific activity
SELECT
    dt.year,
    dt.value as hours_per_day,
    dt.footnote_codes
FROM bls_tu_data dt
JOIN bls_tu_series s ON dt.series_id = s.series_id
JOIN bls_tu_activities a ON s.activity_code = a.activity_code
WHERE a.activity_name ILIKE '%household activities%'
  AND dt.year >= 2010
ORDER BY dt.year;

-- Compare time use across demographics for same activity
SELECT
    d.demographic_name,
    dt.year,
    dt.value as hours_per_day
FROM bls_tu_data dt
JOIN bls_tu_series s ON dt.series_id = s.series_id
LEFT JOIN bls_tu_activities a ON s.activity_code = a.activity_code
LEFT JOIN bls_tu_demographics d ON s.demographic_code = d.demographic_code
WHERE a.activity_name ILIKE '%leisure%'
  AND dt.year = 2023
ORDER BY dt.value DESC
LIMIT 20;


-- ============================================================================
-- ANALYTICAL QUERIES
-- ============================================================================

-- Calculate year-over-year change in time use
WITH current_year AS (
    SELECT activity_code, year, value
    FROM bls_tu_data dt
    JOIN bls_tu_series s ON dt.series_id = s.series_id
    WHERE year = 2023
),
prior_year AS (
    SELECT activity_code, year, value
    FROM bls_tu_data dt
    JOIN bls_tu_series s ON dt.series_id = s.series_id
    WHERE year = 2022
)
SELECT
    a.activity_name,
    c.value as current_hours,
    p.value as prior_hours,
    ROUND((c.value - p.value), 2) as change_hours,
    ROUND(((c.value - p.value) / p.value * 100), 2) as pct_change
FROM current_year c
JOIN prior_year p ON c.activity_code = p.activity_code
LEFT JOIN bls_tu_activities a ON c.activity_code = a.activity_code
WHERE a.selectable = 'T'
ORDER BY change_hours DESC
LIMIT 20;

-- Track time use trend for an activity
SELECT
    dt.year,
    dt.value as hours_per_day
FROM bls_tu_data dt
JOIN bls_tu_series s ON dt.series_id = s.series_id
JOIN bls_tu_activities a ON s.activity_code = a.activity_code
WHERE a.activity_name ILIKE '%work%'
  AND dt.year >= 2003
ORDER BY dt.year;

-- Compare time allocation across major activities (latest year)
WITH latest_year AS (
    SELECT MAX(year) as year
    FROM bls_tu_data
)
SELECT
    a.activity_name,
    dt.value as hours_per_day,
    ROUND((dt.value / 24.0 * 100), 1) as pct_of_day,
    dt.year
FROM bls_tu_data dt
JOIN bls_tu_series s ON dt.series_id = s.series_id
LEFT JOIN bls_tu_activities a ON s.activity_code = a.activity_code
CROSS JOIN latest_year ly
WHERE dt.year = ly.year
  AND a.display_level = 0
ORDER BY dt.value DESC;

-- Gender differences in time use (latest year)
WITH latest_year AS (
    SELECT MAX(year) as year FROM bls_tu_data
)
SELECT
    a.activity_name,
    MAX(CASE WHEN d.demographic_name ILIKE '%male%' THEN dt.value END) as male_hours,
    MAX(CASE WHEN d.demographic_name ILIKE '%female%' THEN dt.value END) as female_hours,
    ROUND(
        MAX(CASE WHEN d.demographic_name ILIKE '%male%' THEN dt.value END) -
        MAX(CASE WHEN d.demographic_name ILIKE '%female%' THEN dt.value END),
        2
    ) as difference
FROM bls_tu_data dt
JOIN bls_tu_series s ON dt.series_id = s.series_id
LEFT JOIN bls_tu_activities a ON s.activity_code = a.activity_code
LEFT JOIN bls_tu_demographics d ON s.demographic_code = d.demographic_code
CROSS JOIN latest_year ly
WHERE dt.year = ly.year
  AND a.display_level <= 1
GROUP BY a.activity_code, a.activity_name
HAVING MAX(CASE WHEN d.demographic_name ILIKE '%male%' THEN dt.value END) IS NOT NULL
ORDER BY ABS(difference) DESC
LIMIT 20;

-- Time use by employment status (latest year)
WITH latest_year AS (
    SELECT MAX(year) as year FROM bls_tu_data
)
SELECT
    a.activity_name,
    d.demographic_name as employment_status,
    dt.value as hours_per_day,
    dt.year
FROM bls_tu_data dt
JOIN bls_tu_series s ON dt.series_id = s.series_id
LEFT JOIN bls_tu_activities a ON s.activity_code = a.activity_code
LEFT JOIN bls_tu_demographics d ON s.demographic_code = d.demographic_code
CROSS JOIN latest_year ly
WHERE dt.year = ly.year
  AND (d.demographic_name ILIKE '%employed%' OR d.demographic_name ILIKE '%unemployed%')
  AND a.display_level = 0
ORDER BY a.sort_sequence, d.demographic_name;


-- ============================================================================
-- DATA QUALITY & MONITORING QUERIES
-- ============================================================================

-- Count observations by year
SELECT
    year,
    COUNT(*) as observation_count,
    COUNT(DISTINCT series_id) as series_count
FROM bls_tu_data
GROUP BY year
ORDER BY year DESC;

-- Find series with most recent updates
SELECT
    s.series_id,
    s.series_title,
    MAX(dt.year) as latest_year,
    MAX(dt.updated_at) as last_updated
FROM bls_tu_series s
JOIN bls_tu_data dt ON s.series_id = dt.series_id
WHERE s.is_active = true
GROUP BY s.series_id, s.series_title
ORDER BY last_updated DESC
LIMIT 20;

-- Check for NULL values
SELECT
    series_id,
    COUNT(*) as null_count
FROM bls_tu_data
WHERE value IS NULL
GROUP BY series_id
ORDER BY null_count DESC
LIMIT 20;

-- Coverage by activity
SELECT
    a.activity_name,
    COUNT(DISTINCT s.series_id) as series_count,
    COUNT(dt.value) as data_points,
    MIN(dt.year) as earliest_year,
    MAX(dt.year) as latest_year
FROM bls_tu_activities a
JOIN bls_tu_series s ON a.activity_code = s.activity_code
LEFT JOIN bls_tu_data dt ON s.series_id = dt.series_id
WHERE s.is_active = true
  AND a.display_level <= 1
GROUP BY a.activity_code, a.activity_name
ORDER BY a.sort_sequence
LIMIT 50;


-- ============================================================================
-- EXPORT / REPORTING QUERIES
-- ============================================================================

-- Export complete time series with metadata
SELECT
    s.series_id,
    s.series_title,
    a.activity_name,
    d.demographic_name,
    dt.year,
    dt.value as hours_per_day,
    dt.footnote_codes
FROM bls_tu_data dt
JOIN bls_tu_series s ON dt.series_id = s.series_id
LEFT JOIN bls_tu_activities a ON s.activity_code = a.activity_code
LEFT JOIN bls_tu_demographics d ON s.demographic_code = d.demographic_code
WHERE dt.year >= 2010
ORDER BY s.series_id, dt.year
LIMIT 1000;

-- Summary statistics by activity
SELECT
    a.activity_name,
    COUNT(DISTINCT s.series_id) as series_count,
    COUNT(dt.value) as data_points,
    ROUND(AVG(dt.value), 2) as avg_hours_per_day,
    ROUND(MIN(dt.value), 2) as min_hours,
    ROUND(MAX(dt.value), 2) as max_hours,
    MIN(dt.year) as earliest_year,
    MAX(dt.year) as latest_year
FROM bls_tu_activities a
JOIN bls_tu_series s ON a.activity_code = s.activity_code
LEFT JOIN bls_tu_data dt ON s.series_id = dt.series_id
WHERE s.is_active = true
  AND a.display_level = 0
GROUP BY a.activity_code, a.activity_name
ORDER BY a.sort_sequence;

-- Time use dashboard (latest year, all major activities)
WITH latest_year AS (
    SELECT MAX(year) as year FROM bls_tu_data
)
SELECT
    a.activity_name,
    dt.value as hours_per_day,
    ROUND((dt.value / 24.0 * 100), 1) as pct_of_day,
    ROUND((dt.value * 365), 0) as hours_per_year,
    dt.year
FROM bls_tu_data dt
JOIN bls_tu_series s ON dt.series_id = s.series_id
LEFT JOIN bls_tu_activities a ON s.activity_code = a.activity_code
CROSS JOIN latest_year ly
WHERE dt.year = ly.year
  AND a.display_level = 0
ORDER BY dt.value DESC;

-- Historical trends (major activities over time)
SELECT
    dt.year,
    MAX(CASE WHEN a.activity_name ILIKE '%work%' THEN dt.value END) as work,
    MAX(CASE WHEN a.activity_name ILIKE '%sleep%' THEN dt.value END) as sleep,
    MAX(CASE WHEN a.activity_name ILIKE '%leisure%' THEN dt.value END) as leisure,
    MAX(CASE WHEN a.activity_name ILIKE '%household%' THEN dt.value END) as household,
    MAX(CASE WHEN a.activity_name ILIKE '%eating%' THEN dt.value END) as eating
FROM bls_tu_data dt
JOIN bls_tu_series s ON dt.series_id = s.series_id
LEFT JOIN bls_tu_activities a ON s.activity_code = a.activity_code
WHERE dt.year >= 2003
  AND a.display_level = 0
GROUP BY dt.year
ORDER BY dt.year;
