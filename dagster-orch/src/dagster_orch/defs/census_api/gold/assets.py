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

    silver_location = GOLD_LOCATIONS[geography]

    @dg.asset(
        name=f"gold_{geography}",
        group_name="gold",
        deps=[f"silver_acs5_{geography}", f"silver_tiger_{geography}"],
        required_resource_keys={"athena"},
    )
    def gold_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        athena = context.resources.athena

        # Drop existing table if exists
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
        ctas_sql = f"""
        CREATE TABLE {GOLD_DB}.gold_{geography}
        WITH (
            table_type = 'ICEBERG',
            format = 'PARQUET',
            write_compression = 'SNAPPY',
            is_external = false,
            partitioning = ARRAY['survey_year'],
            location = '{silver_location}'
        )
        AS
        SELECT
            acs.geography_id,
            acs.survey_year,
            acs.geography_name,
            acs.state_fips,
            acs.county_fips,
            acs.tract_fips,
            acs.total_population,
            acs.median_age,
            acs.white_alone_not_hispanic,
            acs.black_alone_not_hispanic,
            acs.asian_alone_not_hispanic,
            acs.hispanic_or_latino,
            acs.median_household_income,
            acs.poverty_total,
            acs.below_poverty_level,
            acs.education_total_25y_plus,
            acs.bachelors_degree,
            acs.masters_degree,
            acs.professional_degree,
            acs.doctorate_degree,
            acs.high_school_diploma,
            acs.no_high_school_diploma,
            acs.median_home_value,
            acs.total_occupied,
            acs.owner_occupied,
            acs.renter_occupied,
            acs.median_gross_rent,
            acs.migration_total,
            acs.commute_total_workers_16_plus,
            acs.drove_alone_to_work,
            acs.walked_to_work,
            acs.worked_from_home,
            tiger.ALAND,
            tiger.AWATER,
            tiger.geometry_wkt
        FROM {SILVER_DB}.silver_acs5_{geography} acs
        JOIN {SILVER_DB}.silver_tiger_{geography} tiger
            ON acs.geography_id = tiger.geography_id
            AND acs.survey_year = tiger.survey_year
        """

        athena.execute_query(query=ctas_sql)

        return dg.MaterializeResult(
            metadata={
                "geography": geography,
                "gold_table": f"{GOLD_DB}.gold_{geography}",
                "join_type": "INNER JOIN on geography_id + survey_year",
            }
        )

    return gold_asset


# Create the three gold assets
gold_states = _build_gold_asset("states")
gold_counties = _build_gold_asset("counties")
gold_tracts = _build_gold_asset("tracts")