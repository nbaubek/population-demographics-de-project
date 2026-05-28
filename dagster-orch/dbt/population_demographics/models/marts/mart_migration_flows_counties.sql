{{ config(
    materialized='table',
    table_type='iceberg',
    partitioned_by=['survey_year'],
    s3_data_dir='s3://population-demographics-athena-results/dbt/'
) }}

SELECT
    f.origin_geography_id,
    f.dest_geography_id,
    f.survey_year,
    f.households,
    f.individuals,
    f.agi,
    f.is_non_migrant,

    -- Origin context
    origin.geography_name                           AS origin_county_name,
    origin.state_fips                               AS origin_state_fips,
    origin.total_population                         AS origin_total_population,
    origin.median_household_income                  AS origin_median_income,
    origin.poverty_rate                             AS origin_poverty_rate,

    -- Destination context
    dest.geography_name                             AS dest_county_name,
    dest.state_fips                                 AS dest_state_fips,
    dest.total_population                           AS dest_total_population,
    dest.median_household_income                    AS dest_median_income,
    dest.poverty_rate                               AS dest_poverty_rate,

    -- Income differential
    dest.median_household_income - origin.median_household_income
        AS income_differential,

    -- AGI per household
    ROUND(CAST(f.agi AS DOUBLE) / NULLIF(f.households, 0), 2)
        AS agi_per_household

FROM {{ ref('stg_irs_county_outflows') }} f
LEFT JOIN {{ ref('mart_socioeconomic_counties') }} origin
    ON f.origin_geography_id = origin.geography_id
    AND f.survey_year = origin.survey_year
LEFT JOIN {{ ref('mart_socioeconomic_counties') }} dest
    ON f.dest_geography_id = dest.geography_id
    AND f.survey_year = dest.survey_year
WHERE f.is_non_migrant = false