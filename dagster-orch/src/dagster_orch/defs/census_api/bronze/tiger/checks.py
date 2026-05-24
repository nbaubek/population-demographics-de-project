"""Asset checks for bronze TIGER tables.

Only check that matters for bronze: geometry must be present.
A missing geometry in bronze silently passes through to silver and breaks Kepler.gl downstream.

Each check creates a temporary external table over the S3 parquet path, validates geometry
is non-null, then drops the external table — keeping checks idempotent and independent.
"""

from dagster import AssetCheckResult, AssetCheckSeverity, asset_check

BRONZE_BUCKET = "s3://population-demographics-iceberg/bronze/tiger"
BRONZE_DB = "population_demographics_bronze"


def _check_geometry(athena, ext_table: str, geography: str) -> AssetCheckResult:
    """Helper: verify geometry_wkt is non-null for all rows in an external table."""
    result = athena.execute_query(f"""
        SELECT COUNT(*) as total, COUNT(geometry_wkt) as has_geometry
        FROM {BRONZE_DB}.{ext_table}
    """)
    total = result[0]["total"] if result else 0
    has_geometry = result[0]["has_geometry"] if result else 0
    return AssetCheckResult(
        passed=has_geometry == total,
        severity=AssetCheckSeverity.ERROR,
        metadata={"table": ext_table, "total": total, "has_geometry": has_geometry, "missing": total - has_geometry},
    )


def _drop_table_if_exists(athena, table: str):
    """Drop a table if it exists, ignoring errors."""
    try:
        athena.execute_query(query=f"DROP TABLE IF EXISTS {BRONZE_DB}.{table}")
    except Exception:
        pass


@asset_check(asset="bronze_tiger_states", name="check_geometry_not_null")
def check_bronze_tiger_states_geometry(context) -> AssetCheckResult:
    """Every state row must have a non-null geometry_wkt."""
    athena = context.resources.athena
    ext_table = "tmp_check_tiger_states"

    athena.execute_query(query=f"""
        CREATE EXTERNAL TABLE {BRONZE_DB}.{ext_table} (
            STATEFP STRING, COUNTYFP STRING, TRACTCE STRING, GEOID STRING,
            NAME STRING, NAMELSAD STRING, MTFCC STRING, FUNCSTAT STRING,
            ALAND BIGINT, AWATER BIGINT, INTPTLAT STRING, INTPTLON STRING,
            geometry_wkt STRING, survey_year BIGINT, ingest_date STRING
        )
        STORED AS PARQUET
        LOCATION '{BRONZE_BUCKET}/states'
        TBLPROPERTIES ('parquet.compression' = 'SNAPPY')
    """)

    try:
        return _check_geometry(athena, ext_table, "states")
    finally:
        _drop_table_if_exists(athena, ext_table)


@asset_check(asset="bronze_tiger_counties", name="check_geometry_not_null")
def check_bronze_tiger_counties_geometry(context) -> AssetCheckResult:
    """Every county row must have a non-null geometry_wkt."""
    athena = context.resources.athena
    ext_table = "tmp_check_tiger_counties"

    athena.execute_query(query=f"""
        CREATE EXTERNAL TABLE {BRONZE_DB}.{ext_table} (
            STATEFP STRING, COUNTYFP STRING, TRACTCE STRING, GEOID STRING,
            NAME STRING, NAMELSAD STRING, MTFCC STRING, FUNCSTAT STRING,
            ALAND BIGINT, AWATER BIGINT, INTPTLAT STRING, INTPTLON STRING,
            geometry_wkt STRING, survey_year BIGINT, ingest_date STRING
        )
        STORED AS PARQUET
        LOCATION '{BRONZE_BUCKET}/counties'
        TBLPROPERTIES ('parquet.compression' = 'SNAPPY')
    """)

    try:
        return _check_geometry(athena, ext_table, "counties")
    finally:
        _drop_table_if_exists(athena, ext_table)


@asset_check(asset="bronze_tiger_tracts", name="check_geometry_not_null")
def check_bronze_tiger_tracts_geometry(context) -> AssetCheckResult:
    """Every tract row must have a non-null geometry_wkt.

    Tracts are multi-partitioned (year × state), so we check the latest state=06
    partition as a representative sample — all states share the same schema.
    """
    athena = context.resources.athena
    ext_table = "tmp_check_tiger_tracts"

    # Check state=06 as representative sample (California, largest state)
    athena.execute_query(query=f"""
        CREATE EXTERNAL TABLE {BRONZE_DB}.{ext_table} (
            STATEFP STRING, COUNTYFP STRING, TRACTCE STRING, GEOID STRING,
            NAME STRING, NAMELSAD STRING, MTFCC STRING, FUNCSTAT STRING,
            ALAND BIGINT, AWATER BIGINT, INTPTLAT STRING, INTPTLON STRING,
            geometry_wkt STRING, survey_year BIGINT, ingest_date STRING
        )
        STORED AS PARQUET
        LOCATION '{BRONZE_BUCKET}/tracts/state=06'
        TBLPROPERTIES ('parquet.compression' = 'SNAPPY')
    """)

    try:
        return _check_geometry(athena, ext_table, "tracts (state=06 sample)")
    finally:
        _drop_table_if_exists(athena, ext_table)