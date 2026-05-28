{{ config(
    materialized='table',
    table_type='iceberg',
    partitioned_by=['survey_year'],
    bucket_by=['state_fips'],
    bucket_count=32,
    s3_data_dir='s3://population-demographics-athena-results/dbt/'
) }}

SELECT
    geography_id,
    survey_year,
    geography_name,
    state_fips,
    county_fips,
    tract_fips,
    total_population,
    median_age,
    median_household_income,
    median_home_value,
    median_gross_rent,

    -- Race/ethnicity shares
    ROUND(CAST(white_alone_not_hispanic AS DOUBLE) / NULLIF(total_population, 0) * 100, 2)
        AS pct_white_alone,
    ROUND(CAST(black_alone_not_hispanic AS DOUBLE) / NULLIF(total_population, 0) * 100, 2)
        AS pct_black_alone,
    ROUND(CAST(asian_alone_not_hispanic AS DOUBLE) / NULLIF(total_population, 0) * 100, 2)
        AS pct_asian_alone,
    ROUND(CAST(hispanic_or_latino AS DOUBLE) / NULLIF(total_population, 0) * 100, 2)
        AS pct_hispanic_or_latino,

    -- Poverty
    ROUND(CAST(below_poverty_level AS DOUBLE) / NULLIF(poverty_total, 0) * 100, 2)
        AS poverty_rate,

    -- Education
    ROUND(
        CAST(bachelors_degree + masters_degree + professional_degree + doctorate_degree AS DOUBLE)
        / NULLIF(education_total_25y_plus, 0) * 100, 2
    ) AS bachelors_plus_rate,
    ROUND(CAST(high_school_diploma AS DOUBLE) / NULLIF(education_total_25y_plus, 0) * 100, 2)
        AS high_school_rate,

    -- Housing
    ROUND(CAST(owner_occupied AS DOUBLE) / NULLIF(total_occupied, 0) * 100, 2)
        AS owner_occupancy_rate,
    ROUND(CAST(renter_occupied AS DOUBLE) / NULLIF(total_occupied, 0) * 100, 2)
        AS renter_rate,

    -- Commute
    ROUND(CAST(drove_alone_to_work AS DOUBLE) / NULLIF(commute_total_workers_16_plus, 0) * 100, 2)
        AS drove_alone_rate,
    ROUND(CAST(walked_to_work AS DOUBLE) / NULLIF(commute_total_workers_16_plus, 0) * 100, 2)
        AS walked_to_work_rate,
    ROUND(CAST(worked_from_home AS DOUBLE) / NULLIF(commute_total_workers_16_plus, 0) * 100, 2)
        AS remote_work_rate,

    -- Geometry
    geometry_wkt,
    ALAND,
    AWATER

FROM {{ ref('stg_acs_tracts') }}