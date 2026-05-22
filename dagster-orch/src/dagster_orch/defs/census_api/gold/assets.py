"""Gold assets - joined ACS5 + TIGER tables.

Gold tables are NOT partitioned by year - they contain all years in a single Iceberg table,
partitioned internally by the 'survey_year' column. This allows querying across years efficiently.

Composite key: geography_id + survey_year = unique row (horizontal JOIN between ACS and TIGER).
"""

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

        # Drop existing table if exists (full refresh — not incremental)
        drop_sql = f"DROP TABLE IF EXISTS {GOLD_DB}.gold_{geography}"
        try:
            athena.execute_query(query=drop_sql)
        except Exception:
            context.log.warning("Table drop failed (may not exist), continuing")

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
        athena.execute_query(query=create_table_sql)

        # Step 2: Insert from silver JOIN
        insert_sql = f"""
        INSERT INTO {GOLD_DB}.gold_{geography}
        SELECT
            acs.geography_id,
            CAST(acs.survey_year AS INT) AS survey_year,
            acs.geography_name,
            acs.state_fips,
            acs.county_fips,
            acs.tract_fips,
            CAST(acs.total_population AS BIGINT) AS total_population,
            CAST(acs.median_age AS DOUBLE) AS median_age,
            CAST(acs.white_alone_not_hispanic AS BIGINT) AS white_alone_not_hispanic,
            CAST(acs.black_alone_not_hispanic AS BIGINT) AS black_alone_not_hispanic,
            CAST(acs.asian_alone_not_hispanic AS BIGINT) AS asian_alone_not_hispanic,
            CAST(acs.hispanic_or_latino AS BIGINT) AS hispanic_or_latino,
            CAST(acs.median_household_income AS BIGINT) AS median_household_income,
            CAST(acs.poverty_total AS BIGINT) AS poverty_total,
            CAST(acs.below_poverty_level AS BIGINT) AS below_poverty_level,
            CAST(acs.education_total_25y_plus AS BIGINT) AS education_total_25y_plus,
            CAST(acs.bachelors_degree AS BIGINT) AS bachelors_degree,
            CAST(acs.masters_degree AS BIGINT) AS masters_degree,
            CAST(acs.professional_degree AS BIGINT) AS professional_degree,
            CAST(acs.doctorate_degree AS BIGINT) AS doctorate_degree,
            CAST(acs.high_school_diploma AS BIGINT) AS high_school_diploma,
            CAST(acs.no_high_school_diploma AS BIGINT) AS no_high_school_diploma,
            CAST(acs.median_home_value AS BIGINT) AS median_home_value,
            CAST(acs.total_occupied AS BIGINT) AS total_occupied,
            CAST(acs.owner_occupied AS BIGINT) AS owner_occupied,
            CAST(acs.renter_occupied AS BIGINT) AS renter_occupied,
            CAST(acs.median_gross_rent AS BIGINT) AS median_gross_rent,
            CAST(acs.migration_total AS BIGINT) AS migration_total,
            CAST(acs.commute_total_workers_16_plus AS BIGINT) AS commute_total_workers_16_plus,
            CAST(acs.drove_alone_to_work AS BIGINT) AS drove_alone_to_work,
            CAST(acs.walked_to_work AS BIGINT) AS walked_to_work,
            CAST(acs.worked_from_home AS BIGINT) AS worked_from_home,
            tiger.ALAND,
            tiger.AWATER,
            tiger.geometry_wkt
        FROM {SILVER_DB}.silver_acs5_{geography} acs
        JOIN {SILVER_DB}.silver_tiger_{geography} tiger
            ON acs.geography_id = tiger.geography_id
            AND acs.survey_year = tiger.survey_year
        """

        athena.execute_query(query=insert_sql)

        # Get row count for metadata
        count_result = athena.execute_query(
            query=f"SELECT COUNT(*) as cnt FROM {GOLD_DB}.gold_{geography}"
        )
        row_count = count_result[0]["cnt"] if count_result else 0

        return dg.MaterializeResult(
            metadata={
                "geography": geography,
                "gold_table": f"{GOLD_DB}.gold_{geography}",
                "join_type": "INNER JOIN on geography_id + survey_year",
                "row_count": row_count,
            }
        )

    return gold_asset


# Create the three gold assets
gold_states = _build_gold_asset("states")
gold_counties = _build_gold_asset("counties")
gold_tracts = _build_gold_asset("tracts")