{{
  config(
    materialized='view',
    schema='staging'
  )
}}

SELECT
    origin_geography_id,
    dest_geography_id,
    dest_state,
    dest_name,
    households,
    individuals,
    agi,
    survey_year,
    is_non_migrant
FROM {{ source('gold', 'gold_irs_state_outflows') }}