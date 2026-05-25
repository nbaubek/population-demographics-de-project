"""Gold IRS assets - promoted from silver with is_non_migrant flag.

IRS migration gold stays separate from ACS/TIGER gold — it's edge data (migration flows),
not node data (geography + demographics). No JOIN to ACS/TIGER needed.

Schema mirrors silver_irs_* with added is_non_migrant boolean:
- origin_geography_id, dest_geography_id, dest_state, dest_name,
  households, individuals, agi, survey_year, is_non_migrant

S3 paths:
  gold/irs/migration/state_outflows/
  gold/irs/migration/county_outflows/
"""

import time as time_module

import dagster as dg

from dagster_orch.defs.census_api.shared.athena_query import athena_query

SILVER_DB = "population_demographics_silver"
GOLD_DB = "population_demographics_gold"

GOLD_LOCATIONS = {
    "state_outflows": "s3://population-demographics-iceberg/gold/irs/migration/state_outflows",
    "county_outflows": "s3://population-demographics-iceberg/gold/irs/migration/county_outflows",
}


def _build_gold_irs_asset(geography: str):
    """Factory to create gold IRS assets for state_outflows and county_outflows."""

    gold_location = GOLD_LOCATIONS[geography]

    @dg.asset(
        name=f"gold_irs_{geography}",
        group_name="gold_irs",
        deps=[f"silver_irs_{geography}"],
        required_resource_keys={"athena"},
    )
    def gold_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        athena = context.resources.athena

        context.log.info(f"Starting gold_irs_{geography} — full refresh CTAS")
        start_time = time_module.time()

        # Drop existing table if exists (full refresh)
        drop_sql = f"DROP TABLE IF EXISTS {GOLD_DB}.gold_irs_{geography}"
        try:
            athena.execute_query(query=drop_sql)
            context.log.info(f"Dropped existing table {GOLD_DB}.gold_irs_{geography}")
        except Exception as e:
            context.log.warning(f"Table drop failed (may not exist), continuing: {e}")

        # Create Iceberg table with explicit schema
        create_table_sql = f"""
        CREATE TABLE {GOLD_DB}.gold_irs_{geography} (
            origin_geography_id STRING,
            dest_geography_id STRING,
            dest_state STRING,
            dest_name STRING,
            households BIGINT,
            individuals BIGINT,
            agi BIGINT,
            survey_year INT,
            is_non_migrant BOOLEAN
        )
        PARTITIONED BY (survey_year)
        LOCATION '{gold_location}'
        TBLPROPERTIES (
            'table_type'='ICEBERG',
            'format'='parquet',
            'write_compression'='snappy'
        )
        """
        context.log.info(f"Creating Iceberg table {GOLD_DB}.gold_irs_{geography}")
        athena.execute_query(query=create_table_sql)

        # Insert from silver with is_non_migrant flag
        insert_sql = f"""
        INSERT INTO {GOLD_DB}.gold_irs_{geography}
        SELECT
            origin_geography_id,
            dest_geography_id,
            dest_state,
            dest_name,
            households,
            individuals,
            agi,
            survey_year,
            CASE WHEN origin_geography_id = dest_geography_id
                 THEN true ELSE false
            END AS is_non_migrant
        FROM {SILVER_DB}.silver_irs_{geography}
        """
        context.log.info(f"Inserting data from {SILVER_DB}.silver_irs_{geography}")
        athena.execute_query(query=insert_sql)

        # Get row count for metadata
        count_result = athena_query(
            athena, f"SELECT COUNT(*) as cnt FROM {GOLD_DB}.gold_irs_{geography}"
        )
        row_count = count_result[0]["cnt"] if count_result else 0
        context.log.info(f"Gold table {GOLD_DB}.gold_irs_{geography} created with {row_count} rows")
        elapsed = time_module.time() - start_time
        context.log.info(f"Gold irs_{geography} completed in {elapsed:.1f}s")

        return dg.MaterializeResult(
            metadata={
                "geography": geography,
                "gold_table": f"{GOLD_DB}.gold_irs_{geography}",
                "row_count": row_count,
                "duration_seconds": round(elapsed, 2),
            }
        )

    return gold_asset


gold_irs_state_outflows = _build_gold_irs_asset("state_outflows")
gold_irs_county_outflows = _build_gold_irs_asset("county_outflows")