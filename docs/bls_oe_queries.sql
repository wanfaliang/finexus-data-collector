-- ============================================================================
-- BLS OE (Occupational Employment and Wages) SQL Query Documentation
-- ============================================================================
-- Survey: Occupational Employment and Wage Statistics
-- Tables: bls_oe_areas, bls_oe_industries, bls_oe_occupations, bls_oe_data_types,
--         bls_oe_series, bls_oe_data
-- Shared Tables: bls_periods, bls_periodicity
-- ============================================================================

-- ============================================================================
-- METADATA QUERIES
-- ============================================================================

-- List all areas
SELECT
    area_code,
    area_name,
    display_level,
    selectable,
    sort_sequence
FROM bls_oe_areas
ORDER BY sort_sequence
LIMIT 50;

-- List all industries
SELECT
    industry_code,
    industry_name,
    display_level,
    selectable,
    sort_sequence
FROM bls_oe_industries
ORDER BY sort_sequence
LIMIT 50;

-- List all occupations
SELECT
    occupation_code,
    occupation_name,
    display_level,
    selectable,
    sort_sequence
FROM bls_oe_occupations
ORDER BY sort_sequence
LIMIT 50;

-- List all data types
SELECT
    datatype_code,
    datatype_name
FROM bls_oe_data_types
ORDER BY datatype_code;
-- 01=Employment, 02=Hourly mean wage, 03=Annual mean wage, 04=Wage percent relative standard error, etc.

-- Count series by data type
SELECT
    dt.datatype_name,
    COUNT(*) as series_count
FROM bls_oe_series s
JOIN bls_oe_data_types dt ON s.datatype_code = dt.datatype_code
WHERE s.is_active = true
GROUP BY dt.datatype_code, dt.datatype_name
ORDER BY series_count DESC;


-- ============================================================================
-- SERIES QUERIES
-- ============================================================================

-- Find employment series for specific occupation
SELECT
    s.series_id,
    s.series_title,
    a.area_name,
    i.industry_name,
    o.occupation_name,
    dt.datatype_name,
    s.is_active
FROM bls_oe_series s
LEFT JOIN bls_oe_areas a ON s.area_code = a.area_code
LEFT JOIN bls_oe_industries i ON s.industry_code = i.industry_code
LEFT JOIN bls_oe_occupations o ON s.occupation_code = o.occupation_code
JOIN bls_oe_data_types dt ON s.datatype_code = dt.datatype_code
WHERE o.occupation_name ILIKE '%software developer%'
  AND dt.datatype_code = '01'  -- Employment
  AND s.is_active = true;

-- Find wage series for specific area and occupation
SELECT
    s.series_id,
    s.series_title,
    a.area_name,
    o.occupation_name,
    dt.datatype_name,
    s.is_active
FROM bls_oe_series s
LEFT JOIN bls_oe_areas a ON s.area_code = a.area_code
LEFT JOIN bls_oe_occupations o ON s.occupation_code = o.occupation_code
JOIN bls_oe_data_types dt ON s.datatype_code = dt.datatype_code
WHERE a.area_name ILIKE '%San Francisco%'
  AND dt.datatype_code IN ('02', '03')  -- Hourly/Annual mean wage
  AND s.is_active = true
ORDER BY o.occupation_name
LIMIT 20;

-- Search series by title keyword
SELECT
    series_id,
    series_title,
    area_code,
    occupation_code,
    datatype_code,
    is_active
FROM bls_oe_series
WHERE series_title ILIKE '%nurse%'
  AND is_active = true
ORDER BY series_title
LIMIT 20;


-- ============================================================================
-- TIME SERIES DATA QUERIES
-- ============================================================================

-- Get latest employment data for an occupation
SELECT
    s.series_title,
    o.occupation_name,
    d.year,
    d.period,
    d.value as employment,
    d.footnote_codes,
    d.updated_at
FROM bls_oe_data d
JOIN bls_oe_series s ON d.series_id = s.series_id
LEFT JOIN bls_oe_occupations o ON s.occupation_code = o.occupation_code
JOIN bls_oe_data_types dt ON s.datatype_code = dt.datatype_code
WHERE dt.datatype_code = '01'
ORDER BY d.year DESC, d.period DESC
LIMIT 10;

-- Get annual wage data for specific occupation
SELECT
    d.year,
    d.value as annual_mean_wage,
    d.footnote_codes
FROM bls_oe_data d
JOIN bls_oe_series s ON d.series_id = s.series_id
LEFT JOIN bls_oe_occupations o ON s.occupation_code = o.occupation_code
WHERE o.occupation_name ILIKE '%registered nurse%'
  AND s.datatype_code = '03'  -- Annual mean wage
  AND d.year >= 2018
ORDER BY d.year;

-- Compare wages across areas for same occupation
SELECT
    a.area_name,
    d.year,
    d.value as annual_mean_wage
FROM bls_oe_data d
JOIN bls_oe_series s ON d.series_id = s.series_id
LEFT JOIN bls_oe_areas a ON s.area_code = a.area_code
LEFT JOIN bls_oe_occupations o ON s.occupation_code = o.occupation_code
WHERE o.occupation_name ILIKE '%software developer%'
  AND s.datatype_code = '03'
  AND d.year = 2023
  AND a.display_level = 0  -- Top-level areas
ORDER BY d.value DESC
LIMIT 20;


-- ============================================================================
-- ANALYTICAL QUERIES
-- ============================================================================

-- Top paying occupations (latest year)
WITH latest_year AS (
    SELECT MAX(year) as year
    FROM bls_oe_data
)
SELECT
    o.occupation_name,
    d.value as annual_mean_wage,
    d.year
FROM bls_oe_data d
JOIN bls_oe_series s ON d.series_id = s.series_id
LEFT JOIN bls_oe_occupations o ON s.occupation_code = o.occupation_code
CROSS JOIN latest_year ly
WHERE s.datatype_code = '03'  -- Annual mean wage
  AND d.year = ly.year
  AND o.selectable = 'T'
ORDER BY d.value DESC
LIMIT 20;

-- Calculate year-over-year wage change
WITH current_year AS (
    SELECT occupation_code, year, value
    FROM bls_oe_data d
    JOIN bls_oe_series s ON d.series_id = s.series_id
    WHERE s.datatype_code = '03'
      AND year = 2023
),
prior_year AS (
    SELECT occupation_code, year, value
    FROM bls_oe_data d
    JOIN bls_oe_series s ON d.series_id = s.series_id
    WHERE s.datatype_code = '03'
      AND year = 2022
)
SELECT
    o.occupation_name,
    c.value as current_wage,
    p.value as prior_wage,
    ROUND((c.value - p.value), 0) as change_dollars,
    ROUND(((c.value - p.value) / p.value * 100), 2) as pct_change
FROM current_year c
JOIN prior_year p ON c.occupation_code = p.occupation_code
LEFT JOIN bls_oe_occupations o ON c.occupation_code = o.occupation_code
WHERE o.selectable = 'T'
ORDER BY pct_change DESC
LIMIT 20;

-- Employment and wage analysis for an occupation
SELECT
    d.year,
    MAX(CASE WHEN dt.datatype_code = '01' THEN d.value END) as employment,
    MAX(CASE WHEN dt.datatype_code = '02' THEN d.value END) as hourly_mean_wage,
    MAX(CASE WHEN dt.datatype_code = '03' THEN d.value END) as annual_mean_wage
FROM bls_oe_data d
JOIN bls_oe_series s ON d.series_id = s.series_id
JOIN bls_oe_data_types dt ON s.datatype_code = dt.datatype_code
LEFT JOIN bls_oe_occupations o ON s.occupation_code = o.occupation_code
WHERE o.occupation_name ILIKE '%registered nurse%'
  AND d.year >= 2018
GROUP BY d.year, o.occupation_code
ORDER BY d.year;

-- Compare wages across industries for same occupation
SELECT
    i.industry_name,
    d.value as annual_mean_wage,
    d.year
FROM bls_oe_data d
JOIN bls_oe_series s ON d.series_id = s.series_id
LEFT JOIN bls_oe_industries i ON s.industry_code = i.industry_code
LEFT JOIN bls_oe_occupations o ON s.occupation_code = o.occupation_code
WHERE o.occupation_name ILIKE '%accountant%'
  AND s.datatype_code = '03'
  AND d.year = 2023
  AND i.display_level = 1
ORDER BY d.value DESC
LIMIT 20;


-- ============================================================================
-- DATA QUALITY & MONITORING QUERIES
-- ============================================================================

-- Count observations by year and data type
SELECT
    year,
    dt.datatype_name,
    COUNT(*) as observation_count
FROM bls_oe_data d
JOIN bls_oe_series s ON d.series_id = s.series_id
JOIN bls_oe_data_types dt ON s.datatype_code = dt.datatype_code
WHERE year >= 2018
GROUP BY year, dt.datatype_code, dt.datatype_name
ORDER BY year DESC, dt.datatype_code;

-- Find series with most recent updates
SELECT
    s.series_id,
    s.series_title,
    MAX(d.year) as latest_year,
    MAX(d.updated_at) as last_updated
FROM bls_oe_series s
JOIN bls_oe_data d ON s.series_id = d.series_id
WHERE s.is_active = true
GROUP BY s.series_id, s.series_title
ORDER BY last_updated DESC
LIMIT 20;

-- Check for NULL values
SELECT
    s.series_id,
    dt.datatype_name,
    COUNT(*) as null_count
FROM bls_oe_data d
JOIN bls_oe_series s ON d.series_id = s.series_id
JOIN bls_oe_data_types dt ON s.datatype_code = dt.datatype_code
WHERE d.value IS NULL
GROUP BY s.series_id, dt.datatype_name
ORDER BY null_count DESC
LIMIT 20;


-- ============================================================================
-- EXPORT / REPORTING QUERIES
-- ============================================================================

-- Export complete time series with metadata
SELECT
    s.series_id,
    s.series_title,
    a.area_name,
    i.industry_name,
    o.occupation_name,
    dt.datatype_name,
    d.year,
    d.value,
    d.footnote_codes
FROM bls_oe_data d
JOIN bls_oe_series s ON d.series_id = s.series_id
LEFT JOIN bls_oe_areas a ON s.area_code = a.area_code
LEFT JOIN bls_oe_industries i ON s.industry_code = i.industry_code
LEFT JOIN bls_oe_occupations o ON s.occupation_code = o.occupation_code
JOIN bls_oe_data_types dt ON s.datatype_code = dt.datatype_code
WHERE d.year >= 2018
ORDER BY s.series_id, d.year
LIMIT 1000;

-- Summary statistics by occupation
SELECT
    o.occupation_name,
    COUNT(DISTINCT s.series_id) as series_count,
    COUNT(d.value) as data_points,
    MIN(d.year) as earliest_year,
    MAX(d.year) as latest_year
FROM bls_oe_occupations o
JOIN bls_oe_series s ON o.occupation_code = s.occupation_code
LEFT JOIN bls_oe_data d ON s.series_id = d.series_id
WHERE s.is_active = true
  AND o.display_level <= 2
GROUP BY o.occupation_code, o.occupation_name
ORDER BY o.sort_sequence
LIMIT 50;

-- Occupation wage rankings dashboard (latest year)
WITH latest_year AS (
    SELECT MAX(year) as year FROM bls_oe_data
)
SELECT
    o.occupation_name,
    MAX(CASE WHEN dt.datatype_code = '01' THEN d.value END) as employment,
    MAX(CASE WHEN dt.datatype_code = '02' THEN d.value END) as hourly_mean,
    MAX(CASE WHEN dt.datatype_code = '03' THEN d.value END) as annual_mean,
    MAX(d.year) as year
FROM bls_oe_data d
JOIN bls_oe_series s ON d.series_id = s.series_id
LEFT JOIN bls_oe_occupations o ON s.occupation_code = o.occupation_code
JOIN bls_oe_data_types dt ON s.datatype_code = dt.datatype_code
CROSS JOIN latest_year ly
WHERE d.year = ly.year
  AND o.selectable = 'T'
  AND o.display_level = 1
GROUP BY o.occupation_code, o.occupation_name
ORDER BY annual_mean DESC
LIMIT 50;
