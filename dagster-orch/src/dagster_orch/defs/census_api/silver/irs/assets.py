"""Silver IRS assets - cleaned and typed Iceberg tables for IRS migration outflows.

Reads from bronze parquet (raw IRS CSV) and INSERT INTO Iceberg tables per geography.
Bronze stores raw data unchanged (including 96/97/98 aggregate rows).
Silver filters out aggregate rows via WHERE clause during INSERT.
The Iceberg table is partitioned by 'survey_year' and contains all years (2012-2023).
"""

import time as time_module

import dagster as dg

from dagster_orch.defs.census_api.shared.constants import YEAR_PARTITIONS

SILVER_DB = "population_demographics_silver"

SILVER_LOCATIONS = {
    "state_outflows": "s3://population-demographics-iceberg/silver/irs/migration/state_outflows",
    "county_outflows": "s3://population-demographics-iceberg/silver/irs/migration/county_outflows",
}


def _build_silver_irs_asset(geography: str):
    """Factory to create silver IRS assets for state_outflows and county_outflows."""

    silver_location = SILVER_LOCATIONS[geography]

    @dg.asset(
        name=f"silver_irs_{geography}",
        partitions_def=YEAR_PARTITIONS,
        group_name="silver_irs",
        deps=[f"bronze_irs_{geography}"],
        required_resource_keys={"athena"},
        auto_materialize_policy=dg.AutoMaterializePolicy.eager(),
    )
    def silver_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        athena = context.resources.athena
        year = context.partition_key
        bronze_base = f"s3://population-demographics-iceberg/bronze/irs/migration/{geography}/year={year}"

        context.log.info(f"Starting silver_irs_{geography} for year={year}")

        # Check if Iceberg table exists
        check_sql = f"""
        SELECT 1 FROM information_schema.tables
        WHERE table_name = 'silver_irs_{geography}'
        AND table_schema = '{SILVER_DB}'
        """
        table_exists = False
        try:
            result = athena.execute_query(query=check_sql)
            table_exists = result is not None and len(result) > 0
        except Exception:
            table_exists = False

        if not table_exists:
            if geography == "state_outflows":
                create_iceberg_sql = f"""
                CREATE TABLE {SILVER_DB}.silver_irs_{geography} (
                    origin_geography_id STRING,
                    dest_geography_id STRING,
                    dest_state STRING,
                    dest_name STRING,
                    households BIGINT,
                    individuals BIGINT,
                    agi BIGINT,
                    survey_year INT
                )
                PARTITIONED BY (survey_year)
                LOCATION '{silver_location}'
                TBLPROPERTIES (
                    'table_type'='ICEBERG',
                    'format'='parquet',
                    'write_compression'='snappy'
                )
                """
            else:
                create_iceberg_sql = f"""
                CREATE TABLE {SILVER_DB}.silver_irs_{geography} (
                    origin_geography_id STRING,
                    dest_geography_id STRING,
                    dest_state STRING,
                    dest_name STRING,
                    households BIGINT,
                    individuals BIGINT,
                    agi BIGINT,
                    survey_year INT
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
                context.log.info(f"Created Iceberg table {SILVER_DB}.silver_irs_{geography}")
            except Exception as e:
                if "already exists" in str(e).lower():
                    context.log.info(f"Iceberg table {SILVER_DB}.silver_irs_{geography} already exists")
                else:
                    raise

        # Create external table over bronze parquet
        ext_table_name = f"bronze_irs_{geography}_ext_{year}"

        if geography == "state_outflows":
            create_external_sql = f"""
            CREATE EXTERNAL TABLE {SILVER_DB}.{ext_table_name} (
                y1_statefips STRING,
                y2_statefips STRING,
                y2_state STRING,
                y2_state_name STRING,
                n1 STRING,
                n2 STRING,
                AGI STRING,
                survey_year BIGINT,
                ingest_date STRING
            )
            STORED AS PARQUET
            LOCATION '{bronze_base}'
            TBLPROPERTIES ('parquet.compression' = 'SNAPPY')
            """
        else:
            create_external_sql = f"""
            CREATE EXTERNAL TABLE {SILVER_DB}.{ext_table_name} (
                y1_statefips STRING,
                y1_countyfips STRING,
                y2_statefips STRING,
                y2_countyfips STRING,
                y2_state STRING,
                y2_countyname STRING,
                n1 STRING,
                n2 STRING,
                agi STRING,
                survey_year BIGINT,
                ingest_date STRING
            )
            STORED AS PARQUET
            LOCATION '{bronze_base}'
            TBLPROPERTIES ('parquet.compression' = 'SNAPPY')
            """

        # Idempotent backfill: drop ext table if exists, create new, delete existing year, insert
        start_time = time_module.time()
        try:
            athena.execute_query(query=f"DROP TABLE IF EXISTS {SILVER_DB}.{ext_table_name}")
        except Exception:
            pass  # table may not exist
        context.log.info(f"Created external table over bronze: {bronze_base}")
        try:
            athena.execute_query(query=create_external_sql)
        except Exception as e:
            if "already exists" in str(e).lower():
                context.log.info(f"External table {SILVER_DB}.{ext_table_name} already exists")
            else:
                raise

        # Delete existing data for this survey_year before re-inserting
        try:
            athena.execute_query(query=f"""
                DELETE FROM {SILVER_DB}.silver_irs_{geography}
                WHERE survey_year = {year}
            """)
            context.log.info(f"Deleted existing data for survey_year={year}")
        except Exception as e:
            context.log.warning(f"Delete step failed (may be empty table): {e}")

        # Build geography_id expressions
        if geography == "state_outflows":
            origin_geo_expr = "LPAD(y1_statefips, 2, '0')"
            dest_geo_expr = "LPAD(y2_statefips, 2, '0')"
            insert_sql = f"""
            INSERT INTO {SILVER_DB}.silver_irs_{geography}
            SELECT
                {origin_geo_expr} AS origin_geography_id,
                {dest_geo_expr} AS dest_geography_id,
                y2_state AS dest_state,
                y2_state_name AS dest_name,
                CAST(n1 AS BIGINT) AS households,
                CAST(n2 AS BIGINT) AS individuals,
                CAST(AGI AS BIGINT) AS agi,
                survey_year AS survey_year
            FROM {SILVER_DB}.{ext_table_name}
            -- Exclude IRS aggregate codes and suppressed flows:
            --   96 = Total Migration US and Foreign
            --   97 = Total Migration US (same state + different state)
            --   98 = Total Migration Foreign (Puerto Rico + Abroad)
            --   59 = Puerto Rico aggregate flows (IRS internal code)
            --   57 = Foreign (separate code, distinct from 98)
            --   58 = Same-state aggregate flows (distinct from 57)
            --   n1/n2 = -1 means IRS suppressed the flow for privacy
            WHERE CAST(y2_statefips AS INT) NOT IN (96, 97, 98, 59, 57, 58)
              AND n1 != '-1'
              AND n2 != '-1'
            """
        else:
            origin_geo_expr = "CONCAT(LPAD(y1_statefips, 2, '0'), LPAD(y1_countyfips, 3, '0'))"
            dest_geo_expr = "CONCAT(LPAD(y2_statefips, 2, '0'), LPAD(y2_countyfips, 3, '0'))"
            insert_sql = f"""
            INSERT INTO {SILVER_DB}.silver_irs_{geography}
            SELECT
                {origin_geo_expr} AS origin_geography_id,
                {dest_geo_expr} AS dest_geography_id,
                y2_state AS dest_state,
                y2_countyname AS dest_name,
                CAST(n1 AS BIGINT) AS households,
                CAST(n2 AS BIGINT) AS individuals,
                CAST(agi AS BIGINT) AS agi,
                survey_year AS survey_year
            FROM {SILVER_DB}.{ext_table_name}
            -- Exclude IRS aggregate codes and suppressed flows:
            --   96 = Total Migration US and Foreign
            --   97 = Total Migration US (same state + different state)
            --   98 = Total Migration Foreign (Puerto Rico + Abroad)
            --   59 = Puerto Rico aggregate flows (IRS internal code)
            --   57 = Foreign (separate code, distinct from 98)
            --   58 = Same-state aggregate flows (distinct from 57)
            --   n1/n2 = -1 means IRS suppressed the flow for privacy
            WHERE CAST(y2_statefips AS INT) NOT IN (96, 97, 98, 59, 57, 58)
              AND n1 != '-1'
              AND n2 != '-1'
            """

        context.log.info(f"Inserting fresh data for survey_year={year}")
        athena.execute_query(query=insert_sql)
        context.log.info(f"Cleaning up external table")
        athena.execute_query(query=f"DROP TABLE {SILVER_DB}.{ext_table_name}")
        elapsed = time_module.time() - start_time

        return dg.MaterializeResult(
            metadata={
                "year": int(year),
                "geography": geography,
                "silver_table": f"{SILVER_DB}.silver_irs_{geography}",
                "action": "INSERT",
                "duration_seconds": round(elapsed, 2),
            }
        )

    return silver_asset


silver_irs_state_outflows = _build_silver_irs_asset("state_outflows")
silver_irs_county_outflows = _build_silver_irs_asset("county_outflows")