-- ============================================================================
-- LN (Labor Force Statistics from CPS) - Practical Query Examples
-- ============================================================================

-- ============================================================================
-- 1. CURRENT LABOR MARKET OVERVIEW
-- ============================================================================

-- Get latest unemployment rate (U.S. total, seasonally adjusted)
SELECT
    d.year,
    d.period,
    d.value as unemployment_rate_pct,
    s.series_title
FROM bls_ln_data d
INNER JOIN bls_ln_series s ON d.series_id = s.series_id
WHERE s.lfst_code = '40'  -- Unemployment rate
    AND s.sexs_code = '0'  -- Total (both sexes)
    AND s.ages_code = '00' -- All ages 16+
    AND s.race_code = '00' -- All races
    AND s.seasonal = 'S'   -- Seasonally adjusted
    AND d.year >= 2023
ORDER BY d.year DESC, d.period DESC
LIMIT 12;

-- Get latest employment levels by sex
SELECT
    sex.sexs_text,
    d.year,
    d.period,
    d.value as employed_thousands,
    s.series_title
FROM bls_ln_data d
INNER JOIN bls_ln_series s ON d.series_id = s.series_id
INNER JOIN bls_ln_sexs sex ON s.sexs_code = sex.sexs_code
WHERE s.lfst_code = '20'  -- Employed
    AND s.ages_code = '00' -- All ages 16+
    AND s.race_code = '00' -- All races
    AND s.seasonal = 'S'   -- Seasonally adjusted
    AND d.year = 2024
    AND d.period LIKE 'M%' -- Monthly data
ORDER BY d.period DESC, sex.sexs_text
LIMIT 6;


-- ============================================================================
-- 2. DEMOGRAPHIC ANALYSIS
-- ============================================================================

-- Unemployment rate by race (latest month)
SELECT
    race.race_text,
    d.value as unemployment_rate_pct,
    d.year,
    d.period
FROM bls_ln_data d
INNER JOIN bls_ln_series s ON d.series_id = s.series_id
INNER JOIN bls_ln_race race ON s.race_code = race.race_code
WHERE s.lfst_code = '40'  -- Unemployment rate
    AND s.sexs_code = '0'  -- Total
    AND s.ages_code = '00' -- All ages
    AND s.seasonal = 'S'
    AND d.year = 2024
    AND d.period = (SELECT MAX(period) FROM bls_ln_data WHERE year = 2024)
    AND s.race_code != '00' -- Exclude total
ORDER BY d.value DESC;

-- Unemployment rate by age group (latest month)
SELECT
    age.ages_text,
    d.value as unemployment_rate_pct,
    d.year,
    d.period
FROM bls_ln_data d
INNER JOIN bls_ln_series s ON d.series_id = s.series_id
INNER JOIN bls_ln_ages age ON s.ages_code = age.ages_code
WHERE s.lfst_code = '40'  -- Unemployment rate
    AND s.sexs_code = '0'  -- Total
    AND s.race_code = '00' -- All races
    AND s.seasonal = 'S'
    AND d.year = 2024
    AND d.period = (SELECT MAX(period) FROM bls_ln_data WHERE year = 2024)
    AND s.ages_code IN ('16', '16-19', '20-24', '25-34', '35-44', '45-54', '55-64', '65')
ORDER BY d.value DESC;

-- Employment by education level (bachelor's degree and higher vs high school)
SELECT
    educ.education_text,
    d.value as employed_thousands,
    d.year,
    d.period
FROM bls_ln_data d
INNER JOIN bls_ln_series s ON d.series_id = s.series_id
INNER JOIN bls_ln_education educ ON s.education_code = educ.education_code
WHERE s.lfst_code = '20'  -- Employed
    AND s.sexs_code = '0'  -- Total
    AND s.seasonal = 'S'
    AND d.year = 2024
    AND s.education_code IN ('40', '43', '45') -- High school, some college, bachelor's+
ORDER BY d.period DESC, educ.education_text
LIMIT 12;


-- ============================================================================
-- 3. INDUSTRY & OCCUPATION ANALYSIS
-- ============================================================================

-- Employment by major industry (latest month)
SELECT
    ind.indy_text,
    d.value as employed_thousands,
    d.year,
    d.period
FROM bls_ln_data d
INNER JOIN bls_ln_series s ON d.series_id = s.series_id
INNER JOIN bls_ln_indy ind ON s.indy_code = ind.indy_code
WHERE s.lfst_code = '20'  -- Employed
    AND s.sexs_code = '0'  -- Total
    AND s.seasonal = 'S'
    AND d.year = 2024
    AND d.period = (SELECT MAX(period) FROM bls_ln_data WHERE year = 2024)
    AND s.indy_code LIKE '____0' -- Major industry groups
    AND s.indy_code != '0000'    -- Exclude total
ORDER BY d.value DESC
LIMIT 15;

-- Employment by major occupation (latest month)
SELECT
    occ.occupation_text,
    d.value as employed_thousands,
    d.year,
    d.period
FROM bls_ln_data d
INNER JOIN bls_ln_series s ON d.series_id = s.series_id
INNER JOIN bls_ln_occupation occ ON s.occupation_code = occ.occupation_code
WHERE s.lfst_code = '20'  -- Employed
    AND s.sexs_code = '0'  -- Total
    AND s.seasonal = 'S'
    AND d.year = 2024
    AND d.period = (SELECT MAX(period) FROM bls_ln_data WHERE year = 2024)
    AND LENGTH(s.occupation_code) = 4 -- Major occupation groups
    AND s.occupation_code != '0000'   -- Exclude total
ORDER BY d.value DESC
LIMIT 15;


-- ============================================================================
-- 4. LABOR FORCE PARTICIPATION
-- ============================================================================

-- Labor force participation rate by sex (trend over past 2 years)
SELECT
    sex.sexs_text,
    d.year,
    d.period,
    d.value as participation_rate_pct
FROM bls_ln_data d
INNER JOIN bls_ln_series s ON d.series_id = s.series_id
INNER JOIN bls_ln_sexs sex ON s.sexs_code = sex.sexs_code
WHERE s.lfst_code = '13'  -- Labor force participation rate
    AND s.ages_code = '00' -- All ages
    AND s.race_code = '00' -- All races
    AND s.seasonal = 'S'
    AND d.year >= 2023
    AND d.period LIKE 'M%'
ORDER BY sex.sexs_text, d.year, d.period;

-- Labor force participation rate by age group (latest year annual average)
SELECT
    age.ages_text,
    d.value as participation_rate_pct,
    d.year
FROM bls_ln_data d
INNER JOIN bls_ln_series s ON d.series_id = s.series_id
INNER JOIN bls_ln_ages age ON s.ages_code = age.ages_code
WHERE s.lfst_code = '13'  -- Labor force participation rate
    AND s.sexs_code = '0'  -- Total
    AND s.race_code = '00' -- All races
    AND s.seasonal = 'S'
    AND d.year = 2024
    AND d.period = 'M13' -- Annual average
    AND s.ages_code IN ('16', '16-19', '20-24', '25-54', '55-64', '65')
ORDER BY d.value DESC;


-- ============================================================================
-- 5. TELEWORK STATISTICS (NEW!)
-- ============================================================================

-- Employed persons who teleworked (latest month)
SELECT
    tlwk.tlwk_text,
    d.value as employed_thousands,
    d.year,
    d.period
FROM bls_ln_data d
INNER JOIN bls_ln_series s ON d.series_id = s.series_id
INNER JOIN bls_ln_tlwk tlwk ON s.tlwk_code = tlwk.tlwk_code
WHERE s.lfst_code = '20'  -- Employed
    AND s.sexs_code = '0'  -- Total
    AND s.seasonal = 'U'   -- Not seasonally adjusted
    AND d.year >= 2023
    AND s.tlwk_code != '00' -- Exclude "not available"
ORDER BY d.year DESC, d.period DESC, tlwk.tlwk_text
LIMIT 20;


-- ============================================================================
-- 6. VETERAN EMPLOYMENT STATUS
-- ============================================================================

-- Employment status of veterans vs non-veterans (latest month)
SELECT
    vets.vets_text,
    lfst.lfst_text,
    d.value as count_thousands,
    d.year,
    d.period
FROM bls_ln_data d
INNER JOIN bls_ln_series s ON d.series_id = s.series_id
INNER JOIN bls_ln_vets vets ON s.vets_code = vets.vets_code
INNER JOIN bls_ln_lfst lfst ON s.lfst_code = lfst.lfst_code
WHERE s.lfst_code IN ('20', '30', '40') -- Employed, Unemployed, Rate
    AND s.sexs_code = '0'  -- Total
    AND s.seasonal = 'S'
    AND d.year = 2024
    AND d.period = (SELECT MAX(period) FROM bls_ln_data WHERE year = 2024)
    AND s.vets_code IN ('02', '03') -- Veterans, Non-veterans
ORDER BY vets.vets_text, lfst.lfst_code;


-- ============================================================================
-- 7. DISABILITY EMPLOYMENT
-- ============================================================================

-- Employment status by disability status (latest month)
SELECT
    disa.disa_text,
    lfst.lfst_text,
    d.value,
    d.year,
    d.period
FROM bls_ln_data d
INNER JOIN bls_ln_series s ON d.series_id = s.series_id
INNER JOIN bls_ln_disa disa ON s.disa_code = disa.disa_code
INNER JOIN bls_ln_lfst lfst ON s.lfst_code = lfst.lfst_code
WHERE s.lfst_code IN ('20', '30', '40') -- Employed, Unemployed, Rate
    AND s.sexs_code = '0'  -- Total
    AND s.seasonal = 'U'   -- Not seasonally adjusted (disability data is NSA)
    AND d.year = 2024
    AND d.period = (SELECT MAX(period) FROM bls_ln_data WHERE year = 2024)
    AND s.disa_code IN ('01', '02') -- With disability, No disability
ORDER BY disa.disa_text, lfst.lfst_code;


-- ============================================================================
-- 8. HISTORICAL TRENDS (Long-term analysis)
-- ============================================================================

-- U.S. unemployment rate - Annual averages since 1950
SELECT
    d.year,
    d.value as unemployment_rate_pct
FROM bls_ln_data d
INNER JOIN bls_ln_series s ON d.series_id = s.series_id
WHERE s.lfst_code = '40'  -- Unemployment rate
    AND s.sexs_code = '0'  -- Total
    AND s.ages_code = '00' -- All ages
    AND s.race_code = '00' -- All races
    AND s.seasonal = 'S'
    AND d.period = 'M13'   -- Annual average
    AND d.year >= 1950
ORDER BY d.year DESC;

-- Labor force participation rate - Annual trend since 1950
SELECT
    d.year,
    d.value as participation_rate_pct
FROM bls_ln_data d
INNER JOIN bls_ln_series s ON d.series_id = s.series_id
WHERE s.lfst_code = '13'  -- Participation rate
    AND s.sexs_code = '0'  -- Total
    AND s.ages_code = '00' -- All ages
    AND s.seasonal = 'S'
    AND d.period = 'M13'   -- Annual average
    AND d.year >= 1950
ORDER BY d.year DESC;

-- Women's labor force participation - Historical trend
SELECT
    d.year,
    d.value as women_participation_rate_pct
FROM bls_ln_data d
INNER JOIN bls_ln_series s ON d.series_id = s.series_id
WHERE s.lfst_code = '13'  -- Participation rate
    AND s.sexs_code = '2'  -- Women
    AND s.ages_code = '00' -- All ages
    AND s.seasonal = 'S'
    AND d.period = 'M13'   -- Annual average
    AND d.year >= 1950
ORDER BY d.year DESC;


-- ============================================================================
-- 9. PART-TIME EMPLOYMENT
-- ============================================================================

-- Part-time employment by reason (latest month)
SELECT
    rwns.rwns_text as part_time_reason,
    d.value as employed_thousands,
    d.year,
    d.period
FROM bls_ln_data d
INNER JOIN bls_ln_series s ON d.series_id = s.series_id
INNER JOIN bls_ln_rwns rwns ON s.rwns_code = rwns.rwns_code
WHERE s.lfst_code = '22'  -- Employed part time
    AND s.sexs_code = '0'  -- Total
    AND s.seasonal = 'S'
    AND d.year = 2024
    AND d.period = (SELECT MAX(period) FROM bls_ln_data WHERE year = 2024)
    AND s.rwns_code != '00' -- Exclude total
ORDER BY d.value DESC;


-- ============================================================================
-- 10. MULTIPLE JOBHOLDERS
-- ============================================================================

-- Multiple jobholder statistics (latest month)
SELECT
    mjhs.mjhs_text,
    d.value as count_thousands,
    d.year,
    d.period
FROM bls_ln_data d
INNER JOIN bls_ln_series s ON d.series_id = s.series_id
INNER JOIN bls_ln_mjhs mjhs ON s.mjhs_code = mjhs.mjhs_code
WHERE s.lfst_code = '20'  -- Employed
    AND s.sexs_code = '0'  -- Total
    AND s.seasonal = 'S'
    AND d.year = 2024
    AND d.period = (SELECT MAX(period) FROM bls_ln_data WHERE year = 2024)
    AND s.mjhs_code != '00' -- Exclude total
ORDER BY mjhs.mjhs_code;


-- ============================================================================
-- 11. COMPARATIVE ANALYSIS
-- ============================================================================

-- Compare unemployment rates across demographics (latest month)
SELECT
    'By Sex' as category,
    sex.sexs_text as subcategory,
    d.value as unemployment_rate_pct
FROM bls_ln_data d
INNER JOIN bls_ln_series s ON d.series_id = s.series_id
INNER JOIN bls_ln_sexs sex ON s.sexs_code = sex.sexs_code
WHERE s.lfst_code = '40' AND s.ages_code = '00' AND s.race_code = '00'
    AND s.seasonal = 'S' AND d.year = 2024
    AND d.period = (SELECT MAX(period) FROM bls_ln_data WHERE year = 2024)

UNION ALL

SELECT
    'By Age' as category,
    age.ages_text as subcategory,
    d.value as unemployment_rate_pct
FROM bls_ln_data d
INNER JOIN bls_ln_series s ON d.series_id = s.series_id
INNER JOIN bls_ln_ages age ON s.ages_code = age.ages_code
WHERE s.lfst_code = '40' AND s.sexs_code = '0' AND s.race_code = '00'
    AND s.seasonal = 'S' AND d.year = 2024
    AND d.period = (SELECT MAX(period) FROM bls_ln_data WHERE year = 2024)
    AND s.ages_code IN ('16-19', '20-24', '25-54', '55')

UNION ALL

SELECT
    'By Race' as category,
    race.race_text as subcategory,
    d.value as unemployment_rate_pct
FROM bls_ln_data d
INNER JOIN bls_ln_series s ON d.series_id = s.series_id
INNER JOIN bls_ln_race race ON s.race_code = race.race_code
WHERE s.lfst_code = '40' AND s.sexs_code = '0' AND s.ages_code = '00'
    AND s.seasonal = 'S' AND d.year = 2024
    AND d.period = (SELECT MAX(period) FROM bls_ln_data WHERE year = 2024)
    AND s.race_code IN ('01', '02', '03', '04')

ORDER BY category, unemployment_rate_pct DESC;


-- ============================================================================
-- 12. FINDING SPECIFIC SERIES
-- ============================================================================

-- Search for series by title keyword
SELECT
    series_id,
    series_title,
    begin_year,
    end_year,
    is_active
FROM bls_ln_series
WHERE series_title ILIKE '%unemployment%women%'
    AND is_active = true
LIMIT 20;

-- List all available labor force status types
SELECT DISTINCT
    lfst.lfst_code,
    lfst.lfst_text,
    COUNT(*) as series_count
FROM bls_ln_series s
INNER JOIN bls_ln_lfst lfst ON s.lfst_code = lfst.lfst_code
WHERE s.is_active = true
GROUP BY lfst.lfst_code, lfst.lfst_text
ORDER BY lfst.lfst_code;

-- Count series by demographic dimension
SELECT
    'Sex' as dimension,
    COUNT(DISTINCT series_id) as series_count
FROM bls_ln_series WHERE sexs_code != '0'
UNION ALL
SELECT 'Age', COUNT(DISTINCT series_id) FROM bls_ln_series WHERE ages_code != '00'
UNION ALL
SELECT 'Race', COUNT(DISTINCT series_id) FROM bls_ln_series WHERE race_code != '00'
UNION ALL
SELECT 'Education', COUNT(DISTINCT series_id) FROM bls_ln_series WHERE education_code != '00'
UNION ALL
SELECT 'Occupation', COUNT(DISTINCT series_id) FROM bls_ln_series WHERE occupation_code != '0000'
UNION ALL
SELECT 'Industry', COUNT(DISTINCT series_id) FROM bls_ln_series WHERE indy_code != '0000'
UNION ALL
SELECT 'Veteran Status', COUNT(DISTINCT series_id) FROM bls_ln_series WHERE vets_code != '00'
UNION ALL
SELECT 'Disability', COUNT(DISTINCT series_id) FROM bls_ln_series WHERE disa_code != '00'
UNION ALL
SELECT 'Telework', COUNT(DISTINCT series_id) FROM bls_ln_series WHERE tlwk_code != '00'
ORDER BY series_count DESC;
