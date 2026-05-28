"""Silver ACS5 assets - cleaned, typed, Iceberg-registered tables.

These assets read from bronze parquet and INSERT INTO a single Iceberg table per geography.
The Iceberg table is partitioned by 'survey_year' column and contains all years (2012-2024).
Each Dagster partition run appends one year's data to the Iceberg table.
"""

import time as time_module

import dagster as dg

from dagster_orch.defs.census_api.shared.constants import YEAR_PARTITIONS

SILVER_DB = "population_demographics_silver"

# S3 locations for silver tables (single Iceberg table per geography, all years)
SILVER_LOCATIONS = {
    "states": "s3://population-demographics-iceberg/silver/census_acs5/states",
    "counties": "s3://population-demographics-iceberg/silver/census_acs5/counties",
    "tracts": "s3://population-demographics-iceberg/silver/census_acs5/tracts",
}


def _build_silver_acs5_asset(geography: str):
    """Factory to create silver ACS5 assets for each geography.

    Silver Iceberg table is partitioned by 'survey_year' and contains all years.
    Each partition run INSERT INTO the existing table (append behavior).
    """

    silver_location = SILVER_LOCATIONS[geography]

    @dg.asset(
        name=f"silver_acs5_{geography}",
        partitions_def=YEAR_PARTITIONS,
        group_name="silver_acs5",
        deps=[f"bronze_acs5_{geography}"],
        required_resource_keys={"athena"},
        auto_materialize_policy=dg.AutoMaterializePolicy.eager(),
    )
    def silver_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        athena = context.resources.athena
        year = context.partition_key
        bronze_base = f"s3://population-demographics-iceberg/bronze/census_acs5/{geography}/year={year}"

        # Check if Iceberg table exists
        check_sql = f"""
        SELECT 1 FROM information_schema.tables
        WHERE table_name = 'silver_acs5_{geography}'
        AND table_schema = '{SILVER_DB}'
        """
        table_exists = False
        try:
            result = athena.execute_query(query=check_sql)
            table_exists = result is not None and len(result) > 0
        except Exception as e:
            context.log.warning(f"Table check failed: {e}")
            table_exists = False

        if not table_exists:
            # Create Iceberg table with partitioning on first run
            create_iceberg_sql = f"""
            CREATE TABLE {SILVER_DB}.silver_acs5_{geography} (
                geography_name STRING,
                state_fips STRING,
                county_fips STRING,
                tract_fips STRING,
                geography_id STRING,
                survey_year INT,
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
                worked_from_home BIGINT
            )
            PARTITIONED BY (survey_year)
            LOCATION '{silver_location}'
            TBLPROPERTIES (
                'table_type'='ICEBERG',
                'format'='parquet',
                'write_compression'='snappy'
            )
            """
            try:
                athena.execute_query(query=create_iceberg_sql)
                context.log.info(f"Created Iceberg table {SILVER_DB}.silver_acs5_{geography}")
            except Exception as e:
                if "already exists" in str(e).lower():
                    context.log.info(f"Iceberg table {SILVER_DB}.silver_acs5_{geography} already exists, skipping creation")
                else:
                    raise

        # Create external table over bronze parquet for this year
        # For tracts, bronze has year=YYYY/state=FF partitions - we query all states for the year
        ext_table_name = f"bronze_acs5_{geography}_ext_{year}"

        create_external_sql = f"""
        CREATE EXTERNAL TABLE {SILVER_DB}.{ext_table_name} (
            NAME STRING,
            total_population STRING,
            median_age STRING,
            white_alone_not_hispanic STRING,
            black_alone_not_hispanic STRING,
            asian_alone_not_hispanic STRING,
            hispanic_or_latino STRING,
            median_household_income STRING,
            poverty_total STRING,
            below_poverty_level STRING,
            education_total_25y_plus STRING,
            bachelors_degree STRING,
            masters_degree STRING,
            professional_degree STRING,
            doctorate_degree STRING,
            high_school_diploma STRING,
            no_high_school_diploma STRING,
            median_home_value STRING,
            total_occupied STRING,
            owner_occupied STRING,
            renter_occupied STRING,
            median_gross_rent STRING,
            migration_total STRING,
            commute_total_workers_16_plus STRING,
            drove_alone_to_work STRING,
            walked_to_work STRING,
            worked_from_home STRING,
            state STRING,
            county STRING,
            tract STRING,
            survey_year BIGINT,
            ingest_date STRING
        )
        STORED AS PARQUET
        LOCATION '{bronze_base}'
        TBLPROPERTIES ('parquet.compression' = 'SNAPPY')
        """

        # INSERT INTO Iceberg table from external table
        # Build geography_id expression per geography level
        if geography == "states":
            geo_id_expr = "LPAD(state, 2, '0')"
        elif geography == "counties":
            geo_id_expr = "CONCAT(LPAD(state, 2, '0'), LPAD(county, 3, '0'))"
        else:
            geo_id_expr = "CONCAT(LPAD(state, 2, '0'), LPAD(county, 3, '0'), tract)"

        insert_sql = f"""
        INSERT INTO {SILVER_DB}.silver_acs5_{geography}
        SELECT
            NAME AS geography_name,
            LPAD(state, 2, '0') AS state_fips,
            LPAD(county, 3, '0') AS county_fips,
            tract AS tract_fips,
            {geo_id_expr} AS geography_id,
            survey_year AS survey_year,
            CAST(total_population AS BIGINT) AS total_population,
            CAST(median_age AS DOUBLE) AS median_age,
            CAST(white_alone_not_hispanic AS BIGINT) AS white_alone_not_hispanic,
            CAST(black_alone_not_hispanic AS BIGINT) AS black_alone_not_hispanic,
            CAST(asian_alone_not_hispanic AS BIGINT) AS asian_alone_not_hispanic,
            CAST(hispanic_or_latino AS BIGINT) AS hispanic_or_latino,
            CAST(median_household_income AS BIGINT) AS median_household_income,
            CAST(poverty_total AS BIGINT) AS poverty_total,
            CAST(below_poverty_level AS BIGINT) AS below_poverty_level,
            CAST(education_total_25y_plus AS BIGINT) AS education_total_25y_plus,
            CAST(bachelors_degree AS BIGINT) AS bachelors_degree,
            CAST(masters_degree AS BIGINT) AS masters_degree,
            CAST(professional_degree AS BIGINT) AS professional_degree,
            CAST(doctorate_degree AS BIGINT) AS doctorate_degree,
            CAST(high_school_diploma AS BIGINT) AS high_school_diploma,
            CAST(no_high_school_diploma AS BIGINT) AS no_high_school_diploma,
            CAST(median_home_value AS BIGINT) AS median_home_value,
            CAST(total_occupied AS BIGINT) AS total_occupied,
            CAST(owner_occupied AS BIGINT) AS owner_occupied,
            CAST(renter_occupied AS BIGINT) AS renter_occupied,
            CAST(median_gross_rent AS BIGINT) AS median_gross_rent,
            CAST(migration_total AS BIGINT) AS migration_total,
            CAST(commute_total_workers_16_plus AS BIGINT) AS commute_total_workers_16_plus,
            CAST(drove_alone_to_work AS BIGINT) AS drove_alone_to_work,
            CAST(walked_to_work AS BIGINT) AS walked_to_work,
            CAST(worked_from_home AS BIGINT) AS worked_from_home
        FROM {SILVER_DB}.{ext_table_name}
        """

        # Execute - idempotent backfill pattern
        start_time = time_module.time()
        context.log.info(f"Starting silver_acs5_{geography} for year={year}")
        athena.execute_query(query=f"DROP TABLE IF EXISTS {SILVER_DB}.{ext_table_name}")
        context.log.info(f"Created external table over bronze: {bronze_base}")
        athena.execute_query(query=create_external_sql)
        context.log.info(f"Deleted existing data for survey_year={year}")
        athena.execute_query(query=f"""
            DELETE FROM {SILVER_DB}.silver_acs5_{geography}
            WHERE survey_year = {year}
        """)
        context.log.info(f"Inserting fresh data for survey_year={year}")
        athena.execute_query(query=insert_sql)
        context.log.info(f"Cleaning up external table")
        athena.execute_query(query=f"DROP TABLE {SILVER_DB}.{ext_table_name}")
        elapsed = time_module.time() - start_time

        return dg.MaterializeResult(
            metadata={
                "year": year,
                "geography": geography,
                "silver_table": f"{SILVER_DB}.silver_acs5_{geography}",
                "action": "INSERT",
                "duration_seconds": round(elapsed, 2),
            }
        )

    return silver_asset


# Create the three silver assets
silver_acs5_states = _build_silver_acs5_asset("states")
silver_acs5_counties = _build_silver_acs5_asset("counties")
silver_acs5_tracts = _build_silver_acs5_asset("tracts")