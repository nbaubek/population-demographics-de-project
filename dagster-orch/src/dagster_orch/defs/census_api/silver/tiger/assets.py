"""Silver TIGER assets - cleaned geography data, Iceberg-registered.

These assets read from bronze parquet and INSERT INTO a single Iceberg table per geography.
The Iceberg table is partitioned by 'survey_year' column and contains all years (2012-2024).
Each Dagster partition run appends one year's data to the Iceberg table.
"""

import time as time_module

import dagster as dg

from dagster_orch.defs.census_api.shared.constants import YEAR_PARTITIONS

SILVER_DB = "population_demographics_silver"

# S3 locations for silver TIGER tables (single Iceberg table per geography, all years)
SILVER_LOCATIONS = {
    "states": "s3://population-demographics-iceberg/silver/tiger/states",
    "counties": "s3://population-demographics-iceberg/silver/tiger/counties",
    "tracts": "s3://population-demographics-iceberg/silver/tiger/tracts",
}


def _build_silver_tiger_asset(geography: str):
    """Factory to create silver TIGER assets for each geography.

    Silver Iceberg table is partitioned by 'survey_year' and contains all years.
    Each partition run INSERT INTO the existing table (append behavior).
    """

    bronze_path_base = f"s3://population-demographics-iceberg/bronze/tiger/{geography}"
    silver_location = SILVER_LOCATIONS[geography]

    @dg.asset(
        name=f"silver_tiger_{geography}",
        partitions_def=YEAR_PARTITIONS,
        group_name="silver_tiger",
        deps=[f"bronze_tiger_{geography}"],
        required_resource_keys={"athena"},
    )
    def silver_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        athena = context.resources.athena
        year = context.partition_key
        bronze_path = f"{bronze_path_base}/year={year}"

        # Check if Iceberg table exists
        check_sql = f"""
        SELECT 1 FROM information_schema.tables
        WHERE table_name = 'silver_tiger_{geography}'
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
            CREATE TABLE {SILVER_DB}.silver_tiger_{geography} (
                geography_id STRING,
                survey_year INT,
                geography_name STRING,
                state_fips STRING,
                county_fips STRING,
                tract_fips STRING,
                namelsad STRING,
                mtfcc STRING,
                funcstat STRING,
                ALAND BIGINT,
                AWATER BIGINT,
                intptlat STRING,
                intptlon STRING,
                geometry_wkt STRING
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
                context.log.info(f"Created Iceberg table {SILVER_DB}.silver_tiger_{geography}")
            except Exception as e:
                if "already exists" in str(e).lower():
                    context.log.info(f"Iceberg table {SILVER_DB}.silver_tiger_{geography} already exists, skipping creation")
                else:
                    raise

        # Create external table over bronze parquet for this year
        ext_table_name = f"bronze_tiger_{geography}_ext_{year}"

        create_external_sql = f"""
        CREATE EXTERNAL TABLE {SILVER_DB}.{ext_table_name} (
            STATEFP STRING,
            COUNTYFP STRING,
            TRACTCE STRING,
            GEOID STRING,
            NAME STRING,
            NAMELSAD STRING,
            MTFCC STRING,
            FUNCSTAT STRING,
            ALAND BIGINT,
            AWATER BIGINT,
            INTPTLAT STRING,
            INTPTLON STRING,
            geometry_wkt STRING,
            survey_year BIGINT,
            ingest_date STRING
        )
        STORED AS PARQUET
        LOCATION '{bronze_path}'
        """

        # INSERT INTO Iceberg table from external table
        insert_sql = f"""
        INSERT INTO {SILVER_DB}.silver_tiger_{geography}
        SELECT
            GEOID AS geography_id,
            survey_year AS survey_year,
            NAME AS geography_name,
            STATEFP AS state_fips,
            COUNTYFP AS county_fips,
            TRACTCE AS tract_fips,
            NAMELSAD AS namelsad,
            MTFCC AS mtfcc,
            FUNCSTAT AS funcstat,
            ALAND,
            AWATER,
            INTPTLAT AS intptlat,
            INTPTLON AS intptlon,
            geometry_wkt
        FROM {SILVER_DB}.{ext_table_name}
        """

        # Execute - idempotent backfill pattern
        start_time = time_module.time()
        context.log.info(f"Starting silver_tiger_{geography} for year={year}")
        athena.execute_query(query=f"DROP TABLE IF EXISTS {SILVER_DB}.{ext_table_name}")
        context.log.info(f"Created external table over bronze: {bronze_path}")
        athena.execute_query(query=create_external_sql)
        context.log.info(f"Deleted existing data for survey_year={year}")
        athena.execute_query(query=f"""
            DELETE FROM {SILVER_DB}.silver_tiger_{geography}
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
                "silver_table": f"{SILVER_DB}.silver_tiger_{geography}",
                "action": "INSERT",
                "duration_seconds": round(elapsed, 2),
            }
        )

    return silver_asset


# Create the three silver TIGER assets
silver_tiger_states = _build_silver_tiger_asset("states")
silver_tiger_counties = _build_silver_tiger_asset("counties")
silver_tiger_tracts = _build_silver_tiger_asset("tracts")