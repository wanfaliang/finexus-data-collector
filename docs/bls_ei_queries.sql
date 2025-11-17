-- ============================================================================
-- BLS EI (Import/Export Price Indexes) SQL Query Documentation
-- ============================================================================
-- Survey: Import/Export Price Indexes
-- Tables: bls_ei_indexes, bls_ei_series, bls_ei_data
-- Shared Tables: bls_periods
-- ============================================================================

-- ============================================================================
-- METADATA QUERIES
-- ============================================================================

-- List all index types
SELECT
    index_code,
    index_name
FROM bls_ei_indexes
ORDER BY index_code;
-- CD=Destination, CO=Origin, CT=Terms of Trade, IC=Services Inbound, etc.

-- Count series by index type
SELECT
    idx.index_name,
    COUNT(*) as series_count
FROM bls_ei_series s
JOIN bls_ei_indexes idx ON s.index_code = idx.index_code
WHERE s.is_active = true
GROUP BY idx.index_code, idx.index_name
ORDER BY series_count DESC;

-- List all series for specific index type
SELECT
    series_id,
    series_name,
    base_period,
    seasonal_code,
    series_title
FROM bls_ei_series
WHERE index_code = 'CD'  -- Locality of Destination
  AND is_active = true
ORDER BY series_name
LIMIT 20;

-- Count series by base period
SELECT
    base_period,
    COUNT(*) as series_count
FROM bls_ei_series
WHERE is_active = true
GROUP BY base_period
ORDER BY base_period DESC;


-- ============================================================================
-- SERIES QUERIES
-- ============================================================================

-- Find export price series for specific country
SELECT
    s.series_id,
    s.series_name,
    idx.index_name,
    s.base_period,
    s.seasonal_code,
    s.is_active
FROM bls_ei_series s
JOIN bls_ei_indexes idx ON s.index_code = idx.index_code
WHERE s.series_name ILIKE '%Canada%'
  AND idx.index_name ILIKE '%Destination%'  -- Export
  AND s.is_active = true;

-- Find import price series for specific country
SELECT
    s.series_id,
    s.series_name,
    idx.index_name,
    s.base_period,
    s.seasonal_code,
    s.is_active
FROM bls_ei_series s
JOIN bls_ei_indexes idx ON s.index_code = idx.index_code
WHERE s.series_name ILIKE '%China%'
  AND idx.index_name ILIKE '%Origin%'  -- Import
  AND s.is_active = true;

-- Find terms of trade series
SELECT
    series_id,
    series_name,
    base_period,
    series_title,
    seasonal_code,
    is_active
FROM bls_ei_series
WHERE index_code = 'CT'  -- Terms of Trade
  AND is_active = true
ORDER BY series_name;

-- Find services price series
SELECT
    s.series_id,
    s.series_name,
    idx.index_name,
    s.base_period,
    s.seasonal_code,
    s.is_active
FROM bls_ei_series s
JOIN bls_ei_indexes idx ON s.index_code = idx.index_code
WHERE idx.index_name ILIKE '%Services%'
  AND s.is_active = true
ORDER BY s.series_name
LIMIT 20;

-- Search series by keyword
SELECT
    series_id,
    series_name,
    series_title,
    index_code,
    is_active
FROM bls_ei_series
WHERE series_title ILIKE '%manufacturing%'
  AND is_active = true
ORDER BY series_name
LIMIT 20;


-- ============================================================================
-- TIME SERIES DATA QUERIES
-- ============================================================================

-- Get latest import/export price data
SELECT
    s.series_name,
    idx.index_name,
    d.year,
    d.period,
    d.value as price_index,
    d.footnote_codes,
    d.updated_at
FROM bls_ei_data d
JOIN bls_ei_series s ON d.series_id = s.series_id
JOIN bls_ei_indexes idx ON s.index_code = idx.index_code
ORDER BY d.year DESC, d.period DESC
LIMIT 20;

-- Get monthly price data for specific series
SELECT
    d.year,
    p.period_name as month,
    d.value as price_index,
    d.footnote_codes
FROM bls_ei_data d
JOIN bls_ei_series s ON d.series_id = s.series_id
JOIN bls_periods p ON d.period = p.period_code
WHERE s.series_name ILIKE '%Canada-Manufacturing%'
  AND d.year >= 2020
  AND p.period_type = 'MONTHLY'
ORDER BY d.year, d.period;

-- Get annual average for specific series
SELECT
    d.year,
    AVG(d.value) as avg_price_index,
    MIN(d.value) as min_price_index,
    MAX(d.value) as max_price_index
FROM bls_ei_data d
JOIN bls_ei_series s ON d.series_id = s.series_id
WHERE s.series_name ILIKE '%China%'
  AND d.year >= 2015
GROUP BY d.year
ORDER BY d.year;

-- Compare seasonally adjusted vs not adjusted
SELECT
    d.year,
    d.period,
    MAX(CASE WHEN s.seasonal_code = 'S' THEN d.value END) as seasonally_adjusted,
    MAX(CASE WHEN s.seasonal_code = 'U' THEN d.value END) as not_adjusted
FROM bls_ei_data d
JOIN bls_ei_series s ON d.series_id = s.series_id
WHERE s.series_name ILIKE '%Canada%'
  AND d.year >= 2023
GROUP BY d.year, d.period
ORDER BY d.year, d.period;


-- ============================================================================
-- ANALYTICAL QUERIES
-- ============================================================================

-- Calculate year-over-year change in export prices
WITH current_year AS (
    SELECT series_id, year, period, value
    FROM bls_ei_data
    WHERE year = 2024
),
prior_year AS (
    SELECT series_id, year, period, value
    FROM bls_ei_data
    WHERE year = 2023
)
SELECT
    s.series_name,
    c.period,
    c.value as current_price,
    p.value as prior_price,
    ROUND((c.value - p.value), 2) as change,
    ROUND(((c.value - p.value) / p.value * 100), 2) as pct_change
FROM current_year c
JOIN prior_year p ON c.series_id = p.series_id AND c.period = p.period
JOIN bls_ei_series s ON c.series_id = s.series_id
JOIN bls_ei_indexes idx ON s.index_code = idx.index_code
WHERE idx.index_name ILIKE '%Destination%'  -- Export
  AND s.seasonal_code = 'U'
ORDER BY pct_change DESC
LIMIT 20;

-- Track import price trend (12-month moving average)
SELECT
    d.year,
    d.period,
    d.value as price_index,
    AVG(d.value) OVER (
        ORDER BY d.year, d.period
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) as moving_avg_12mo
FROM bls_ei_data d
JOIN bls_ei_series s ON d.series_id = s.series_id
WHERE s.series_name ILIKE '%China%'
  AND s.seasonal_code = 'U'
  AND d.year >= 2020
ORDER BY d.year, d.period;

-- Compare export prices across major countries (latest month)
WITH latest_month AS (
    SELECT MAX(year) as year, MAX(period) as period
    FROM bls_ei_data
)
SELECT
    s.series_name as country,
    d.value as export_price_index,
    d.year,
    d.period
FROM bls_ei_data d
JOIN bls_ei_series s ON d.series_id = s.series_id
JOIN bls_ei_indexes idx ON s.index_code = idx.index_code
CROSS JOIN latest_month lm
WHERE idx.index_name ILIKE '%Destination%'  -- Export
  AND s.seasonal_code = 'U'
  AND d.year = lm.year
  AND d.period = lm.period
  AND s.series_name NOT ILIKE '%All%'  -- Exclude aggregates
ORDER BY d.value DESC
LIMIT 20;

-- Calculate terms of trade (export prices / import prices)
-- Terms of trade improvement = ratio > 100 (exports becoming relatively more expensive)
WITH latest_month AS (
    SELECT MAX(year) as year, MAX(period) as period
    FROM bls_ei_data
)
SELECT
    d.year,
    d.period,
    d.value as terms_of_trade_index
FROM bls_ei_data d
JOIN bls_ei_series s ON d.series_id = s.series_id
CROSS JOIN latest_month lm
WHERE s.index_code = 'CT'  -- Terms of Trade
  AND s.seasonal_code = 'U'
  AND d.year = lm.year
  AND d.period = lm.period;

-- Track price changes for major trading partners
SELECT
    s.series_name as country,
    d.year,
    d.period,
    d.value as price_index,
    LAG(d.value, 12) OVER (PARTITION BY s.series_id ORDER BY d.year, d.period) as price_year_ago,
    ROUND(((d.value - LAG(d.value, 12) OVER (PARTITION BY s.series_id ORDER BY d.year, d.period)) /
           LAG(d.value, 12) OVER (PARTITION BY s.series_id ORDER BY d.year, d.period) * 100), 2) as yoy_pct_change
FROM bls_ei_data d
JOIN bls_ei_series s ON d.series_id = s.series_id
WHERE s.series_name IN ('Canada-All Industries', 'China-All Industries', 'Mexico-All Industries')
  AND s.index_code = 'CD'  -- Export
  AND s.seasonal_code = 'U'
  AND d.year >= 2022
ORDER BY s.series_name, d.year, d.period;


-- ============================================================================
-- INDUSTRY-SPECIFIC QUERIES
-- ============================================================================

-- Compare manufacturing vs non-manufacturing export prices
SELECT
    d.year,
    d.period,
    MAX(CASE WHEN s.series_name ILIKE '%Manufacturing%' THEN d.value END) as manufacturing,
    MAX(CASE WHEN s.series_name ILIKE '%Nonmanufacturing%' THEN d.value END) as nonmanufacturing
FROM bls_ei_data d
JOIN bls_ei_series s ON d.series_id = s.series_id
WHERE s.series_name ILIKE '%Canada%'
  AND s.index_code = 'CD'  -- Export
  AND s.seasonal_code = 'U'
  AND d.year >= 2020
GROUP BY d.year, d.period
ORDER BY d.year, d.period;

-- Find industries with highest import price increases
WITH latest_month AS (
    SELECT MAX(year) as year, MAX(period) as period
    FROM bls_ei_data
),
year_ago AS (
    SELECT MAX(year) as year, MAX(period) as period
    FROM bls_ei_data d
    CROSS JOIN latest_month lm
    WHERE d.year = lm.year - 1
)
SELECT
    s.series_name,
    curr.value as current_price,
    prev.value as year_ago_price,
    ROUND((curr.value - prev.value), 2) as change,
    ROUND(((curr.value - prev.value) / prev.value * 100), 2) as pct_change
FROM bls_ei_data curr
JOIN bls_ei_data prev ON curr.series_id = prev.series_id
JOIN bls_ei_series s ON curr.series_id = s.series_id
CROSS JOIN latest_month lm
CROSS JOIN year_ago ya
WHERE curr.year = lm.year
  AND curr.period = lm.period
  AND prev.year = ya.year
  AND prev.period = ya.period
  AND s.index_code = 'CO'  -- Import
  AND s.seasonal_code = 'U'
ORDER BY pct_change DESC
LIMIT 20;


-- ============================================================================
-- DATA QUALITY & MONITORING QUERIES
-- ============================================================================

-- Count observations by year and index type
SELECT
    year,
    idx.index_name,
    COUNT(*) as observation_count
FROM bls_ei_data d
JOIN bls_ei_series s ON d.series_id = s.series_id
JOIN bls_ei_indexes idx ON s.index_code = idx.index_code
WHERE year >= 2020
GROUP BY year, idx.index_code, idx.index_name
ORDER BY year DESC, idx.index_code;

-- Find series with most recent updates
SELECT
    s.series_id,
    s.series_name,
    idx.index_name,
    MAX(d.year) as latest_year,
    MAX(d.period) as latest_period,
    MAX(d.updated_at) as last_updated
FROM bls_ei_series s
JOIN bls_ei_indexes idx ON s.index_code = idx.index_code
JOIN bls_ei_data d ON s.series_id = d.series_id
WHERE s.is_active = true
GROUP BY s.series_id, s.series_name, idx.index_name
ORDER BY last_updated DESC
LIMIT 20;

-- Check for NULL values
SELECT
    series_id,
    COUNT(*) as null_count
FROM bls_ei_data
WHERE value IS NULL
GROUP BY series_id
ORDER BY null_count DESC
LIMIT 20;

-- Check data coverage by series
SELECT
    s.series_id,
    s.series_name,
    idx.index_name,
    COUNT(d.value) as data_points,
    MIN(d.year) as earliest_year,
    MAX(d.year) as latest_year,
    COUNT(DISTINCT d.year) as years_covered
FROM bls_ei_series s
JOIN bls_ei_indexes idx ON s.index_code = idx.index_code
LEFT JOIN bls_ei_data d ON s.series_id = d.series_id
WHERE s.is_active = true
GROUP BY s.series_id, s.series_name, idx.index_name
ORDER BY data_points DESC
LIMIT 50;


-- ============================================================================
-- EXPORT / REPORTING QUERIES
-- ============================================================================

-- Export complete time series with metadata
SELECT
    s.series_id,
    s.series_name,
    idx.index_name,
    s.base_period,
    CASE s.seasonal_code
        WHEN 'S' THEN 'Seasonally Adjusted'
        WHEN 'U' THEN 'Not Seasonally Adjusted'
    END as seasonal_adjustment,
    d.year,
    p.period_name,
    d.value as price_index,
    d.footnote_codes
FROM bls_ei_data d
JOIN bls_ei_series s ON d.series_id = s.series_id
JOIN bls_ei_indexes idx ON s.index_code = idx.index_code
JOIN bls_periods p ON d.period = p.period_code
WHERE d.year >= 2020
ORDER BY s.series_id, d.year, d.period
LIMIT 1000;

-- Summary statistics by index type
SELECT
    idx.index_name,
    COUNT(DISTINCT s.series_id) as series_count,
    COUNT(d.value) as data_points,
    MIN(d.year) as earliest_year,
    MAX(d.year) as latest_year
FROM bls_ei_indexes idx
JOIN bls_ei_series s ON idx.index_code = s.index_code
LEFT JOIN bls_ei_data d ON s.series_id = d.series_id
WHERE s.is_active = true
GROUP BY idx.index_code, idx.index_name
ORDER BY idx.index_code;

-- Import/Export price dashboard (latest month)
WITH latest_month AS (
    SELECT MAX(year) as year, MAX(period) as period
    FROM bls_ei_data
)
SELECT
    idx.index_name,
    AVG(d.value) as avg_price_index,
    MIN(d.value) as min_price_index,
    MAX(d.value) as max_price_index,
    COUNT(*) as series_count,
    MAX(d.year) as year,
    MAX(d.period) as period
FROM bls_ei_data d
JOIN bls_ei_series s ON d.series_id = s.series_id
JOIN bls_ei_indexes idx ON s.index_code = idx.index_code
CROSS JOIN latest_month lm
WHERE s.seasonal_code = 'U'
  AND d.year = lm.year
  AND d.period = lm.period
GROUP BY idx.index_code, idx.index_name
ORDER BY idx.index_code;

-- Historical trends (major indexes over time)
SELECT
    d.year,
    MAX(CASE WHEN idx.index_name ILIKE '%Destination%' THEN d.value END) as export_prices,
    MAX(CASE WHEN idx.index_name ILIKE '%Origin%' THEN d.value END) as import_prices,
    MAX(CASE WHEN idx.index_name ILIKE '%Terms of Trade%' THEN d.value END) as terms_of_trade
FROM bls_ei_data d
JOIN bls_ei_series s ON d.series_id = s.series_id
JOIN bls_ei_indexes idx ON s.index_code = idx.index_code
WHERE d.year >= 2015
  AND s.seasonal_code = 'U'
  AND s.series_name ILIKE '%All Industries%'
  AND d.period = 'M12'  -- December only for simplicity
GROUP BY d.year
ORDER BY d.year;

-- Trade balance indicator (export/import price ratio)
-- Higher ratio suggests exports becoming more expensive relative to imports
WITH latest_data AS (
    SELECT
        d.year,
        d.period,
        MAX(CASE WHEN idx.index_name ILIKE '%Destination%' THEN d.value END) as export_price,
        MAX(CASE WHEN idx.index_name ILIKE '%Origin%' THEN d.value END) as import_price
    FROM bls_ei_data d
    JOIN bls_ei_series s ON d.series_id = s.series_id
    JOIN bls_ei_indexes idx ON s.index_code = idx.index_code
    WHERE s.seasonal_code = 'U'
      AND s.series_name ILIKE '%All Industries%'
      AND d.year >= 2020
    GROUP BY d.year, d.period
)
SELECT
    year,
    period,
    export_price,
    import_price,
    ROUND((export_price / import_price * 100), 2) as trade_price_ratio
FROM latest_data
WHERE export_price IS NOT NULL
  AND import_price IS NOT NULL
ORDER BY year, period;
