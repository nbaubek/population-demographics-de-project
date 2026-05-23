"""Asset checks for bronze TIGER tables.

Only check that matters for bronze: geometry must be present.
A missing geometry in bronze silently passes through to silver and breaks Kepler.gl downstream.
"""

from dagster import AssetCheckResult, AssetCheckSeverity, asset_check

BRONZE_TIGER_BUCKET = "s3://population-demographics-iceberg/bronze/tiger"


@asset_check(asset="bronze_tiger_states", name="check_geometry_not_null")
def check_bronze_tiger_states_geometry(context) -> AssetCheckResult:
    """Every state row must have a non-null geometry_wkt."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT COUNT(*) as total, COUNT(geometry_wkt) as has_geometry
        FROM population_demographics_bronze.bronze_tiger_states
        WHERE ingest_date = (SELECT MAX(ingest_date) FROM population_demographics_bronze.bronze_tiger_states)
    """)
    total = result[0]["total"] if result else 0
    has_geometry = result[0]["has_geometry"] if result else 0
    return AssetCheckResult(
        passed=has_geometry == total,
        severity=AssetCheckSeverity.ERROR,
        metadata={"total": total, "has_geometry": has_geometry, "missing": total - has_geometry},
    )


@asset_check(asset="bronze_tiger_counties", name="check_geometry_not_null")
def check_bronze_tiger_counties_geometry(context) -> AssetCheckResult:
    """Every county row must have a non-null geometry_wkt."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT COUNT(*) as total, COUNT(geometry_wkt) as has_geometry
        FROM population_demographics_bronze.bronze_tiger_counties
        WHERE ingest_date = (SELECT MAX(ingest_date) FROM population_demographics_bronze.bronze_tiger_counties)
    """)
    total = result[0]["total"] if result else 0
    has_geometry = result[0]["has_geometry"] if result else 0
    return AssetCheckResult(
        passed=has_geometry == total,
        severity=AssetCheckSeverity.ERROR,
        metadata={"total": total, "has_geometry": has_geometry, "missing": total - has_geometry},
    )


@asset_check(asset="bronze_tiger_tracts", name="check_geometry_not_null")
def check_bronze_tiger_tracts_geometry(context) -> AssetCheckResult:
    """Every tract row must have a non-null geometry_wkt."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT COUNT(*) as total, COUNT(geometry_wkt) as has_geometry
        FROM population_demographics_bronze.bronze_tiger_tracts
        WHERE ingest_date = (SELECT MAX(ingest_date) FROM population_demographics_bronze.bronze_tiger_tracts)
    """)
    total = result[0]["total"] if result else 0
    has_geometry = result[0]["has_geometry"] if result else 0
    return AssetCheckResult(
        passed=has_geometry == total,
        severity=AssetCheckSeverity.ERROR,
        metadata={"total": total, "has_geometry": has_geometry, "missing": total - has_geometry},
    )