-- ============================================================================
-- BLS BD (Business Employment Dynamics) SQL Query Documentation
-- ============================================================================
-- Survey: Business Employment Dynamics
-- Tables: bls_bd_states, bls_bd_industries, bls_bd_dataclasses, bls_bd_dataelements,
--         bls_bd_sizeclasses, bls_bd_ratelevels, bls_bd_unitanalysis, bls_bd_ownership,
--         bls_bd_series, bls_bd_data
-- Shared Tables: bls_periods, bls_periodicity
-- ============================================================================

-- ============================================================================
-- METADATA QUERIES
-- ============================================================================

-- List all states
SELECT
    state_code,
    state_name
FROM bls_bd_states
ORDER BY state_name;

-- List all industries with hierarchy
SELECT
    industry_code,
    industry_name,
    display_level,
    selectable,
    sort_sequence
FROM bls_bd_industries
ORDER BY sort_sequence
LIMIT 50;

-- List all data classes (job gains, losses, etc.)
SELECT
    dataclass_code,
    dataclass_name,
    display_level,
    selectable,
    sort_sequence
FROM bls_bd_dataclasses
ORDER BY sort_sequence;
-- 01=Gross Job Gains, 02=Expansions, 03=Openings, 04=Gross Job Losses,
-- 05=Contractions, 06=Closings, 07=Establishment Births, 08=Establishment Deaths

-- List all size classes
SELECT
    sizeclass_code,
    sizeclass_name
FROM bls_bd_sizeclasses
ORDER BY sizeclass_code;

-- Count series by data class
SELECT
    dc.dataclass_name,
    COUNT(*) as series_count
FROM bls_bd_series s
JOIN bls_bd_dataclasses dc ON s.dataclass_code = dc.dataclass_code
WHERE s.is_active = true
GROUP BY dc.dataclass_code, dc.dataclass_name
ORDER BY series_count DESC;

-- Count series by state
SELECT
    st.state_name,
    COUNT(*) as series_count
FROM bls_bd_series s
JOIN bls_bd_states st ON s.state_code = st.state_code
WHERE s.is_active = true
GROUP BY st.state_code, st.state_name
ORDER BY series_count DESC
LIMIT 20;

-- Count series by industry
SELECT
    i.industry_name,
    COUNT(*) as series_count
FROM bls_bd_series s
JOIN bls_bd_industries i ON s.industry_code = i.industry_code
WHERE s.is_active = true
  AND i.display_level <= 1
GROUP BY i.industry_code, i.industry_name
ORDER BY series_count DESC;


-- ============================================================================
-- SERIES QUERIES
-- ============================================================================

-- Find job gains series for specific industry
SELECT
    s.series_id,
    s.series_title,
    i.industry_name,
    dc.dataclass_name,
    rl.ratelevel_name,
    s.is_active
FROM bls_bd_series s
JOIN bls_bd_industries i ON s.industry_code = i.industry_code
JOIN bls_bd_dataclasses dc ON s.dataclass_code = dc.dataclass_code
JOIN bls_bd_ratelevels rl ON s.ratelevel_code = rl.ratelevel_code
WHERE i.industry_name ILIKE '%manufacturing%'
  AND dc.dataclass_name ILIKE '%job gains%'
  AND s.is_active = true
LIMIT 20;

-- Find employment series by state
SELECT
    s.series_id,
    s.series_title,
    st.state_name,
    de.dataelement_name,
    s.is_active
FROM bls_bd_series s
JOIN bls_bd_states st ON s.state_code = st.state_code
JOIN bls_bd_dataelements de ON s.dataelement_code = de.dataelement_code
WHERE st.state_name = 'California'
  AND de.dataelement_name ILIKE '%employment%'
  AND s.is_active = true
ORDER BY s.series_title
LIMIT 20;

-- Find series by establishment size class
SELECT
    s.series_id,
    s.series_title,
    sc.sizeclass_name,
    dc.dataclass_name,
    s.is_active
FROM bls_bd_series s
JOIN bls_bd_sizeclasses sc ON s.sizeclass_code = sc.sizeclass_code
JOIN bls_bd_dataclasses dc ON s.dataclass_code = dc.dataclass_code
WHERE sc.sizeclass_name ILIKE '%1,000 or more%'
  AND s.is_active = true
ORDER BY s.series_title
LIMIT 20;

-- Search series by keyword
SELECT
    series_id,
    series_title,
    industry_code,
    dataclass_code,
    state_code,
    is_active
FROM bls_bd_series
WHERE series_title ILIKE '%retail%'
  AND is_active = true
ORDER BY series_title
LIMIT 20;


-- ============================================================================
-- TIME SERIES DATA QUERIES
-- ============================================================================

-- Get latest job dynamics data
SELECT
    dc.dataclass_name,
    i.industry_name,
    d.year,
    d.period,
    d.value,
    rl.ratelevel_name,
    d.footnote_codes,
    d.updated_at
FROM bls_bd_data d
JOIN bls_bd_series s ON d.series_id = s.series_id
JOIN bls_bd_dataclasses dc ON s.dataclass_code = dc.dataclass_code
JOIN bls_bd_industries i ON s.industry_code = i.industry_code
JOIN bls_bd_ratelevels rl ON s.ratelevel_code = rl.ratelevel_code
ORDER BY d.year DESC, d.period DESC
LIMIT 20;

-- Get quarterly job gains/losses for total private sector
SELECT
    d.year,
    p.period_name,
    dc.dataclass_name,
    d.value,
    rl.ratelevel_name
FROM bls_bd_data d
JOIN bls_bd_series s ON d.series_id = s.series_id
JOIN bls_bd_dataclasses dc ON s.dataclass_code = dc.dataclass_code
JOIN bls_bd_ratelevels rl ON s.ratelevel_code = rl.ratelevel_code
JOIN bls_periods p ON d.period = p.period_code
WHERE s.industry_code = '000000'  -- Total private
  AND s.state_code = '00'  -- U.S. totals
  AND s.sizeclass_code = '00'  -- All sizes
  AND d.year >= 2020
  AND p.period_type = 'QUARTERLY'
ORDER BY d.year, d.period, dc.sort_sequence;

-- Get job dynamics by establishment size
SELECT
    sc.sizeclass_name,
    d.year,
    d.period,
    dc.dataclass_name,
    d.value as jobs_thousands
FROM bls_bd_data d
JOIN bls_bd_series s ON d.series_id = s.series_id
JOIN bls_bd_sizeclasses sc ON s.sizeclass_code = sc.sizeclass_code
JOIN bls_bd_dataclasses dc ON s.dataclass_code = dc.dataclass_code
WHERE s.industry_code = '000000'  -- Total private
  AND s.ratelevel_code = 'L'  -- Level
  AND dc.dataclass_code = '01'  -- Gross Job Gains
  AND d.year = 2024
ORDER BY d.year, d.period, sc.sizeclass_code;


-- ============================================================================
-- ANALYTICAL QUERIES
-- ============================================================================

-- Calculate year-over-year change in job gains/losses
WITH current_year AS (
    SELECT s.dataclass_code, d.year, d.period, d.value
    FROM bls_bd_data d
    JOIN bls_bd_series s ON d.series_id = s.series_id
    WHERE s.industry_code = '000000'
      AND s.ratelevel_code = 'L'
      AND d.year = 2024
),
prior_year AS (
    SELECT s.dataclass_code, d.year, d.period, d.value
    FROM bls_bd_data d
    JOIN bls_bd_series s ON d.series_id = s.series_id
    WHERE s.industry_code = '000000'
      AND s.ratelevel_code = 'L'
      AND d.year = 2023
)
SELECT
    dc.dataclass_name,
    c.period,
    c.value as current_value,
    p.value as prior_value,
    ROUND((c.value - p.value), 0) as change_thousands,
    ROUND(((c.value - p.value) / p.value * 100), 2) as pct_change
FROM current_year c
JOIN prior_year p ON c.dataclass_code = p.dataclass_code AND c.period = p.period
JOIN bls_bd_dataclasses dc ON c.dataclass_code = dc.dataclass_code
ORDER BY dc.sort_sequence, c.period;

-- Track job creation/destruction trend (4-quarter moving average)
SELECT
    d.year,
    d.period,
    dc.dataclass_name,
    d.value as jobs_thousands,
    AVG(d.value) OVER (
        PARTITION BY s.dataclass_code
        ORDER BY d.year, d.period
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ) as moving_avg_4q
FROM bls_bd_data d
JOIN bls_bd_series s ON d.series_id = s.series_id
JOIN bls_bd_dataclasses dc ON s.dataclass_code = dc.dataclass_code
WHERE s.industry_code = '000000'  -- Total private
  AND s.ratelevel_code = 'L'  -- Level
  AND dc.dataclass_code IN ('01', '04')  -- Job Gains and Losses
  AND d.year >= 2020
ORDER BY dc.dataclass_code, d.year, d.period;

-- Compare job dynamics across industries (latest quarter)
WITH latest_quarter AS (
    SELECT MAX(year) as year, MAX(period) as period
    FROM bls_bd_data
)
SELECT
    i.industry_name,
    MAX(CASE WHEN dc.dataclass_code = '01' THEN d.value END) as job_gains,
    MAX(CASE WHEN dc.dataclass_code = '04' THEN d.value END) as job_losses,
    MAX(CASE WHEN dc.dataclass_code = '01' THEN d.value END) -
    MAX(CASE WHEN dc.dataclass_code = '04' THEN d.value END) as net_change,
    MAX(d.year) as year,
    MAX(d.period) as period
FROM bls_bd_data d
JOIN bls_bd_series s ON d.series_id = s.series_id
JOIN bls_bd_industries i ON s.industry_code = i.industry_code
JOIN bls_bd_dataclasses dc ON s.dataclass_code = dc.dataclass_code
CROSS JOIN latest_quarter lq
WHERE s.ratelevel_code = 'L'  -- Level
  AND s.state_code = '00'  -- U.S. totals
  AND i.display_level = 1  -- Top-level industries
  AND d.year = lq.year
  AND d.period = lq.period
GROUP BY i.industry_code, i.industry_name
ORDER BY net_change DESC;

-- Net job creation rate (gains minus losses as percentage)
SELECT
    d.year,
    d.period,
    MAX(CASE WHEN dc.dataclass_code = '01' THEN d.value END) as gross_gains,
    MAX(CASE WHEN dc.dataclass_code = '04' THEN d.value END) as gross_losses,
    MAX(CASE WHEN dc.dataclass_code = '01' THEN d.value END) -
    MAX(CASE WHEN dc.dataclass_code = '04' THEN d.value END) as net_job_creation,
    ROUND((
        (MAX(CASE WHEN dc.dataclass_code = '01' THEN d.value END) -
         MAX(CASE WHEN dc.dataclass_code = '04' THEN d.value END)) /
        MAX(CASE WHEN dc.dataclass_code = '01' THEN d.value END) * 100
    ), 2) as net_creation_rate_pct
FROM bls_bd_data d
JOIN bls_bd_series s ON d.series_id = s.series_id
JOIN bls_bd_dataclasses dc ON s.dataclass_code = dc.dataclass_code
WHERE s.industry_code = '000000'  -- Total private
  AND s.ratelevel_code = 'L'  -- Level
  AND d.year >= 2020
GROUP BY d.year, d.period
ORDER BY d.year, d.period;

-- Establishment births vs deaths (business formation trends)
SELECT
    d.year,
    d.period,
    MAX(CASE WHEN dc.dataclass_code = '07' THEN d.value END) as establishment_births,
    MAX(CASE WHEN dc.dataclass_code = '08' THEN d.value END) as establishment_deaths,
    MAX(CASE WHEN dc.dataclass_code = '07' THEN d.value END) -
    MAX(CASE WHEN dc.dataclass_code = '08' THEN d.value END) as net_formation
FROM bls_bd_data d
JOIN bls_bd_series s ON d.series_id = s.series_id
JOIN bls_bd_dataclasses dc ON s.dataclass_code = dc.dataclass_code
WHERE s.industry_code = '000000'  -- Total private
  AND s.ratelevel_code = 'L'  -- Level
  AND d.year >= 2020
GROUP BY d.year, d.period
ORDER BY d.year, d.period;


-- ============================================================================
-- SIZE CLASS ANALYSIS
-- ============================================================================

-- Job creation by firm size (latest quarter)
WITH latest_quarter AS (
    SELECT MAX(year) as year, MAX(period) as period
    FROM bls_bd_data
)
SELECT
    sc.sizeclass_name,
    MAX(CASE WHEN dc.dataclass_code = '01' THEN d.value END) as job_gains,
    MAX(CASE WHEN dc.dataclass_code = '04' THEN d.value END) as job_losses,
    MAX(CASE WHEN dc.dataclass_code = '01' THEN d.value END) -
    MAX(CASE WHEN dc.dataclass_code = '04' THEN d.value END) as net_job_creation,
    MAX(d.year) as year,
    MAX(d.period) as period
FROM bls_bd_data d
JOIN bls_bd_series s ON d.series_id = s.series_id
JOIN bls_bd_sizeclasses sc ON s.sizeclass_code = sc.sizeclass_code
JOIN bls_bd_dataclasses dc ON s.dataclass_code = dc.dataclass_code
CROSS JOIN latest_quarter lq
WHERE s.ratelevel_code = 'L'  -- Level
  AND s.industry_code = '000000'  -- Total private
  AND d.year = lq.year
  AND d.period = lq.period
  AND sc.sizeclass_code IN ('00', '01', '02', '03', '04', '05', '06', '07', '08', '09')
GROUP BY sc.sizeclass_code, sc.sizeclass_name
ORDER BY sc.sizeclass_code;

-- Small vs large firm job dynamics comparison
SELECT
    d.year,
    d.period,
    dc.dataclass_name,
    MAX(CASE WHEN sc.sizeclass_name ILIKE '%1 to 4%' THEN d.value END) as small_firms,
    MAX(CASE WHEN sc.sizeclass_name ILIKE '%1,000 or more%' THEN d.value END) as large_firms
FROM bls_bd_data d
JOIN bls_bd_series s ON d.series_id = s.series_id
JOIN bls_bd_sizeclasses sc ON s.sizeclass_code = sc.sizeclass_code
JOIN bls_bd_dataclasses dc ON s.dataclass_code = dc.dataclass_code
WHERE s.industry_code = '000000'  -- Total private
  AND s.ratelevel_code = 'L'  -- Level
  AND dc.dataclass_code IN ('01', '04')  -- Gains and Losses
  AND d.year >= 2020
GROUP BY d.year, d.period, dc.dataclass_code, dc.dataclass_name, dc.sort_sequence
ORDER BY d.year, d.period, dc.sort_sequence;


-- ============================================================================
-- STATE-LEVEL ANALYSIS
-- ============================================================================

-- Compare job creation across states (latest quarter)
WITH latest_quarter AS (
    SELECT MAX(year) as year, MAX(period) as period
    FROM bls_bd_data d
    JOIN bls_bd_series s ON d.series_id = s.series_id
    WHERE s.state_code != '00'
)
SELECT
    st.state_name,
    MAX(CASE WHEN dc.dataclass_code = '01' THEN d.value END) as job_gains,
    MAX(CASE WHEN dc.dataclass_code = '04' THEN d.value END) as job_losses,
    MAX(CASE WHEN dc.dataclass_code = '01' THEN d.value END) -
    MAX(CASE WHEN dc.dataclass_code = '04' THEN d.value END) as net_job_creation,
    MAX(d.year) as year,
    MAX(d.period) as period
FROM bls_bd_data d
JOIN bls_bd_series s ON d.series_id = s.series_id
JOIN bls_bd_states st ON s.state_code = st.state_code
JOIN bls_bd_dataclasses dc ON s.dataclass_code = dc.dataclass_code
CROSS JOIN latest_quarter lq
WHERE s.ratelevel_code = 'L'  -- Level
  AND s.industry_code = '000000'  -- Total private
  AND s.state_code != '00'  -- Exclude U.S. total
  AND d.year = lq.year
  AND d.period = lq.period
GROUP BY st.state_code, st.state_name
ORDER BY net_job_creation DESC
LIMIT 20;

-- State job creation rates
WITH latest_quarter AS (
    SELECT MAX(year) as year, MAX(period) as period
    FROM bls_bd_data d
    JOIN bls_bd_series s ON d.series_id = s.series_id
    WHERE s.state_code != '00'
)
SELECT
    st.state_name,
    MAX(CASE WHEN dc.dataclass_code = '01' AND rl.ratelevel_code = 'R' THEN d.value END) as job_gain_rate_pct,
    MAX(CASE WHEN dc.dataclass_code = '04' AND rl.ratelevel_code = 'R' THEN d.value END) as job_loss_rate_pct,
    MAX(d.year) as year,
    MAX(d.period) as period
FROM bls_bd_data d
JOIN bls_bd_series s ON d.series_id = s.series_id
JOIN bls_bd_states st ON s.state_code = st.state_code
JOIN bls_bd_dataclasses dc ON s.dataclass_code = dc.dataclass_code
JOIN bls_bd_ratelevels rl ON s.ratelevel_code = rl.ratelevel_code
CROSS JOIN latest_quarter lq
WHERE s.industry_code = '000000'  -- Total private
  AND s.state_code != '00'  -- Exclude U.S. total
  AND d.year = lq.year
  AND d.period = lq.period
GROUP BY st.state_code, st.state_name
ORDER BY job_gain_rate_pct DESC
LIMIT 20;


-- ============================================================================
-- DATA QUALITY & MONITORING QUERIES
-- ============================================================================

-- Count observations by year and data class
SELECT
    year,
    dc.dataclass_name,
    COUNT(*) as observation_count
FROM bls_bd_data d
JOIN bls_bd_series s ON d.series_id = s.series_id
JOIN bls_bd_dataclasses dc ON s.dataclass_code = dc.dataclass_code
WHERE year >= 2020
GROUP BY year, dc.dataclass_code, dc.dataclass_name
ORDER BY year DESC, dc.dataclass_code;

-- Find series with most recent updates
SELECT
    s.series_id,
    s.series_title,
    MAX(d.year) as latest_year,
    MAX(d.period) as latest_period,
    MAX(d.updated_at) as last_updated
FROM bls_bd_series s
JOIN bls_bd_data d ON s.series_id = d.series_id
WHERE s.is_active = true
GROUP BY s.series_id, s.series_title
ORDER BY last_updated DESC
LIMIT 20;

-- Check for NULL values
SELECT
    series_id,
    COUNT(*) as null_count
FROM bls_bd_data
WHERE value IS NULL
GROUP BY series_id
ORDER BY null_count DESC
LIMIT 20;

-- Check data coverage by industry
SELECT
    i.industry_name,
    COUNT(DISTINCT s.series_id) as series_count,
    COUNT(d.value) as data_points,
    MIN(d.year) as earliest_year,
    MAX(d.year) as latest_year
FROM bls_bd_industries i
JOIN bls_bd_series s ON i.industry_code = s.industry_code
LEFT JOIN bls_bd_data d ON s.series_id = d.series_id
WHERE s.is_active = true
  AND i.display_level <= 1
GROUP BY i.industry_code, i.industry_name
ORDER BY i.sort_sequence
LIMIT 50;


-- ============================================================================
-- EXPORT / REPORTING QUERIES
-- ============================================================================

-- Export complete time series with metadata
SELECT
    s.series_id,
    s.series_title,
    st.state_name,
    i.industry_name,
    dc.dataclass_name,
    sc.sizeclass_name,
    rl.ratelevel_name,
    CASE s.seasonal_code
        WHEN 'S' THEN 'Seasonally Adjusted'
        WHEN 'U' THEN 'Not Seasonally Adjusted'
    END as seasonal_adjustment,
    d.year,
    p.period_name,
    d.value,
    d.footnote_codes
FROM bls_bd_data d
JOIN bls_bd_series s ON d.series_id = s.series_id
LEFT JOIN bls_bd_states st ON s.state_code = st.state_code
LEFT JOIN bls_bd_industries i ON s.industry_code = i.industry_code
LEFT JOIN bls_bd_dataclasses dc ON s.dataclass_code = dc.dataclass_code
LEFT JOIN bls_bd_sizeclasses sc ON s.sizeclass_code = sc.sizeclass_code
LEFT JOIN bls_bd_ratelevels rl ON s.ratelevel_code = rl.ratelevel_code
JOIN bls_periods p ON d.period = p.period_code
WHERE d.year >= 2020
ORDER BY s.series_id, d.year, d.period
LIMIT 1000;

-- Summary statistics by data class
SELECT
    dc.dataclass_name,
    COUNT(DISTINCT s.series_id) as series_count,
    COUNT(d.value) as data_points,
    MIN(d.year) as earliest_year,
    MAX(d.year) as latest_year
FROM bls_bd_dataclasses dc
JOIN bls_bd_series s ON dc.dataclass_code = s.dataclass_code
LEFT JOIN bls_bd_data d ON s.series_id = d.series_id
WHERE s.is_active = true
GROUP BY dc.dataclass_code, dc.dataclass_name
ORDER BY dc.sort_sequence;

-- Job dynamics dashboard (latest quarter all metrics)
WITH latest_quarter AS (
    SELECT MAX(year) as year, MAX(period) as period
    FROM bls_bd_data
)
SELECT
    dc.dataclass_name,
    MAX(CASE WHEN rl.ratelevel_code = 'L' THEN d.value END) as level_thousands,
    MAX(CASE WHEN rl.ratelevel_code = 'R' THEN d.value END) as rate_percent,
    MAX(d.year) as year,
    MAX(d.period) as period
FROM bls_bd_data d
JOIN bls_bd_series s ON d.series_id = s.series_id
JOIN bls_bd_dataclasses dc ON s.dataclass_code = dc.dataclass_code
LEFT JOIN bls_bd_ratelevels rl ON s.ratelevel_code = rl.ratelevel_code
CROSS JOIN latest_quarter lq
WHERE s.industry_code = '000000'  -- Total private
  AND s.state_code = '00'  -- U.S. totals
  AND s.sizeclass_code = '00'  -- All sizes
  AND d.year = lq.year
  AND d.period = lq.period
GROUP BY dc.dataclass_code, dc.dataclass_name, dc.sort_sequence
ORDER BY dc.sort_sequence;

-- Historical trends (job gains/losses over time)
SELECT
    d.year,
    MAX(CASE WHEN dc.dataclass_code = '01' THEN d.value END) as gross_job_gains,
    MAX(CASE WHEN dc.dataclass_code = '02' THEN d.value END) as expansions,
    MAX(CASE WHEN dc.dataclass_code = '03' THEN d.value END) as openings,
    MAX(CASE WHEN dc.dataclass_code = '04' THEN d.value END) as gross_job_losses,
    MAX(CASE WHEN dc.dataclass_code = '05' THEN d.value END) as contractions,
    MAX(CASE WHEN dc.dataclass_code = '06' THEN d.value END) as closings,
    MAX(CASE WHEN dc.dataclass_code = '07' THEN d.value END) as establishment_births,
    MAX(CASE WHEN dc.dataclass_code = '08' THEN d.value END) as establishment_deaths
FROM bls_bd_data d
JOIN bls_bd_series s ON d.series_id = s.series_id
JOIN bls_bd_dataclasses dc ON s.dataclass_code = dc.dataclass_code
WHERE s.industry_code = '000000'  -- Total private
  AND s.ratelevel_code = 'L'  -- Level
  AND d.year >= 2015
  AND d.period = 'Q04'  -- Q4 only for annual comparison
GROUP BY d.year
ORDER BY d.year;
