"""Gold assets - joined ACS5 + TIGER tables.

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

        # Create Iceberg table with JOIN between ACS and TIGER
        # Silver ACS5 columns: geography_name, state_fips, county_fips, tract_fips,
        #   geography_id, survey_year, total_population, median_age, white_alone_not_hispanic,
        #   black_alone_not_hispanic, asian_alone_not_hispanic, hispanic_or_latino,
        #   median_household_income, poverty_total, below_poverty_level, education_total_25y_plus,
        #   bachelors_degree, masters_degree, professional_degree, doctorate_degree,
        #   high_school_diploma, no_high_school_diploma, median_home_value, total_occupied,
        #   owner_occupied, renter_occupied, median_gross_rent, migration_total,
        #   commute_total_workers_16_plus, drove_alone_to_work, walked_to_work, worked_from_home,
        #   state, county, tract
        # Silver TIGER columns: geography_id, survey_year, geography_name, state_fips, county_fips,
        #   tract_fips, namelsad, mtfcc, funcstat, ALAND, AWATER, intptlat, intptlon, geometry_wkt
        context.log.info(
            f"Using INNER JOIN — unmatched ACS/TIGER rows (e.g. territories) will be excluded"
        )

        # Step 1: Create empty Iceberg table with explicit schema
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

        # Step 2: Insert from silver JOIN
        context.log.info(f"Inserting joined ACS+TIGER data for all years")
        athena.execute_query(query=insert_sql)

        # Get row count for metadata
        count_result = athena.execute_query(
            query=f"SELECT COUNT(*) as cnt FROM {GOLD_DB}.gold_{geography}"
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