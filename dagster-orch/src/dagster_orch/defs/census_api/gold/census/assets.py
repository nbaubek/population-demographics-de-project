"""Gold census assets - joined ACS5 + TIGER tables.

Gold tables are NOT partitioned by year - they contain all years in a single Iceberg table,
partitioned internally by the 'survey_year' column. This allows querying across years efficiently.

Composite key: geography_id + survey_year = unique row (horizontal JOIN between ACS and TIGER).
"""

import time as time_module

import dagster as dg

SILVER_DB = "population_demographics_silver"
GOLD_DB = "population_demographics_gold"

# S3 locations for gold tables
GOLD_LOCATIONS = {
    "states": "s3://population-demographics-iceberg/gold/census_acs5/states",
    "counties": "s3://population-demographics-iceberg/gold/census_acs5/counties",
    "tracts": "s3://population-demographics-iceberg/gold/census_acs5/tracts",
}


def _build_gold_asset(geography: str):
    """Factory to create gold assets for each geography.

    Gold asset JOINs ACS and TIGER on composite key (geography_id + survey_year).
    Result: one row per geography_id + survey_year with combined ACS metrics and TIGER geometry.
    """

    gold_location = GOLD_LOCATIONS[geography]

    @dg.asset(
        name=f"gold_{geography}",
        group_name="gold",
        deps=[f"silver_acs5_{geography}", f"silver_tiger_{geography}"],
        required_resource_keys={"athena"},
    )
    def gold_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        athena = context.resources.athena

        context.log.info(f"Starting gold_{geography} — full refresh CTAS")
        start_time = time_module.time()

        # Drop existing table if exists (full refresh — not incremental)
        drop_sql = f"DROP TABLE IF EXISTS {GOLD_DB}.gold_{geography}"
        try:
            athena.execute_query(query=drop_sql)
            context.log.info(f"Dropped existing table {GOLD_DB}.gold_{geography}")
        except Exception as e:
            context.log.warning(f"Table drop failed (may not exist), continuing: {e}")

        # Create Iceberg table with explicit schema
        create_table_sql = f"""
        CREATE TABLE {GOLD_DB}.gold_{geography} (
            geography_id STRING,
            survey_year INT,
            geography_name STRING,
            state_fips STRING,
            county_fips STRING,
            tract_fips STRING,
            total_population BIGINT,
            median_age DOUBLE,
            white_alone_not_hispanic BIGINT,
            black_alone_not_hispanic BIGINT,
            asian_alone_not_hispanic BIGINT,
            hispanic_or_latino BIGINT,
            median_household_income BIGINT,
            poverty_total BIGINT,
            below_poverty_level BIGINT,
            education_total_25y_plus BIGINT,
            bachelors_degree BIGINT,
            masters_degree BIGINT,
            professional_degree BIGINT,
            doctorate_degree BIGINT,
            high_school_diploma BIGINT,
            no_high_school_diploma BIGINT,
            median_home_value BIGINT,
            total_occupied BIGINT,
            owner_occupied BIGINT,
            renter_occupied BIGINT,
            median_gross_rent BIGINT,
            migration_total BIGINT,
            commute_total_workers_16_plus BIGINT,
            drove_alone_to_work BIGINT,
            walked_to_work BIGINT,
            worked_from_home BIGINT,
            ALAND BIGINT,
            AWATER BIGINT,
            geometry_wkt STRING
        )
        PARTITIONED BY (survey_year)
        LOCATION '{gold_location}'
        TBLPROPERTIES (
            'table_type'='ICEBERG',
            'format'='parquet',
            'write_compression'='snappy'
        )
        """
        context.log.info(f"Creating Iceberg table {GOLD_DB}.gold_{geography}")
        athena.execute_query(query=create_table_sql)

        # Build geography_id expression per geography level
        if geography == "states":
            geo_id_expr = "LPAD(acs.state_fips, 2, '0')"
        elif geography == "counties":
            geo_id_expr = "CONCAT(LPAD(acs.state_fips, 2, '0'), LPAD(acs.county_fips, 3, '0'))"
        else:
            geo_id_expr = "CONCAT(LPAD(acs.state_fips, 2, '0'), LPAD(acs.county_fips, 3, '0'), acs.tract_fips)"

        insert_sql = f"""
        INSERT INTO {GOLD_DB}.gold_{geography}
        SELECT
            {geo_id_expr} AS geography_id,
            acs.survey_year,
            acs.geography_name,
            acs.state_fips,
            acs.county_fips,
            acs.tract_fips,
            CAST(acs.total_population AS BIGINT),
            CAST(acs.median_age AS DOUBLE),
            CAST(acs.white_alone_not_hispanic AS BIGINT),
            CAST(acs.black_alone_not_hispanic AS BIGINT),
            CAST(acs.asian_alone_not_hispanic AS BIGINT),
            CAST(acs.hispanic_or_latino AS BIGINT),
            CAST(acs.median_household_income AS BIGINT),
            CAST(acs.poverty_total AS BIGINT),
            CAST(acs.below_poverty_level AS BIGINT),
            CAST(acs.education_total_25y_plus AS BIGINT),
            CAST(acs.bachelors_degree AS BIGINT),
            CAST(acs.masters_degree AS BIGINT),
            CAST(acs.professional_degree AS BIGINT),
            CAST(acs.doctorate_degree AS BIGINT),
            CAST(acs.high_school_diploma AS BIGINT),
            CAST(acs.no_high_school_diploma AS BIGINT),
            CAST(acs.median_home_value AS BIGINT),
            CAST(acs.total_occupied AS BIGINT),
            CAST(acs.owner_occupied AS BIGINT),
            CAST(acs.renter_occupied AS BIGINT),
            CAST(acs.median_gross_rent AS BIGINT),
            CAST(acs.migration_total AS BIGINT),
            CAST(acs.commute_total_workers_16_plus AS BIGINT),
            CAST(acs.drove_alone_to_work AS BIGINT),
            CAST(acs.walked_to_work AS BIGINT),
            CAST(acs.worked_from_home AS BIGINT),
            CAST(tiger.ALAND AS BIGINT),
            CAST(tiger.AWATER AS BIGINT),
            tiger.geometry_wkt
        FROM {SILVER_DB}.silver_acs5_{geography} acs
        INNER JOIN {SILVER_DB}.silver_tiger_{geography} tiger
            ON {geo_id_expr} = tiger.geography_id
            AND acs.survey_year = tiger.survey_year
        """
        context.log.info(f"Using INNER JOIN — unmatched ACS/TIGER rows (e.g. territories) will be excluded")
        context.log.info(f"Inserting joined ACS+TIGER data for all years")
        athena.execute_query(query=insert_sql)

        # Get row count for metadata
        from dagster_orch.defs.census_api.shared.athena_query import athena_query
        count_result = athena_query(
            athena, f"SELECT COUNT(*) as cnt FROM {GOLD_DB}.gold_{geography}"
        )
        row_count = count_result[0]["cnt"] if count_result else 0
        context.log.info(f"Gold table {GOLD_DB}.gold_{geography} created with {row_count} rows")
        elapsed = time_module.time() - start_time
        context.log.info(f"Gold {geography} completed in {elapsed:.1f}s")

        return dg.MaterializeResult(
            metadata={
                "geography": geography,
                "gold_table": f"{GOLD_DB}.gold_{geography}",
                "join_type": "INNER JOIN on geography_id + survey_year",
                "row_count": row_count,
                "duration_seconds": round(elapsed, 2),
            }
        )

    return gold_asset


# Create the three gold assets
gold_states = _build_gold_asset("states")
gold_counties = _build_gold_asset("counties")
gold_tracts = _build_gold_asset("tracts")