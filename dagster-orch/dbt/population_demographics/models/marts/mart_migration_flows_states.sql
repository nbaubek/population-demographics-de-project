{{ config(
    materialized='table',
    external_location='s3://population-demographics-athena-results/dbt/marts/mart_migration_flows_states',
    partitioned_by=['survey_year'],
    table_type='iceberg'
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
    origin.geography_name                           AS origin_state_name,
    origin.total_population                         AS origin_total_population,
    origin.median_household_income                  AS origin_median_income,
    origin.poverty_rate                             AS origin_poverty_rate,
    origin.remote_work_rate                         AS origin_remote_work_rate,

    -- Origin centroid
    origin.centroid_lat AS origin_lat,
    origin.centroid_lon AS origin_lon,

    -- Destination context
    dest.geography_name                             AS dest_state_name,
    dest.total_population                           AS dest_total_population,
    dest.median_household_income                    AS dest_median_income,
    dest.poverty_rate                               AS dest_poverty_rate,
    dest.remote_work_rate                           AS dest_remote_work_rate,

    -- Destination centroid  
    dest.centroid_lat AS dest_lat,
    dest.centroid_lon AS dest_lon,

    -- Income differential
    dest.median_household_income - origin.median_household_income
        AS income_differential,

    -- AGI per household (wealth of migrants)
    ROUND(CAST(f.agi AS DOUBLE) / NULLIF(f.households, 0), 2)
        AS agi_per_household

FROM {{ ref('stg_irs_state_outflows') }} f
LEFT JOIN {{ ref('mart_socioeconomic_states') }} origin
    ON f.origin_geography_id = origin.geography_id
    AND f.survey_year = origin.survey_year
LEFT JOIN {{ ref('mart_socioeconomic_states') }} dest
    ON f.dest_geography_id = dest.geography_id
    AND f.survey_year = dest.survey_year
WHERE f.is_non_migrant = false