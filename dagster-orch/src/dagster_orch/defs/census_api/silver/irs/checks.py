"""Asset checks for silver IRS migration tables.

These checks run after each silver_irs_* materialization and catch data quality
issues before the data reaches downstream layers.
"""

import dagster as dg
from dagster import AssetCheckResult, AssetCheckSeverity

from dagster_orch.defs.census_api.shared.athena_query import athena_query

SILVER_DB = "population_demographics_silver"


@dg.asset_check(asset="silver_irs_state_outflows", name="check_no_aggregates", required_resource_keys={"athena"})
def check_silver_irs_state_outflows_no_aggregates(context) -> AssetCheckResult:
    """Verify no aggregate codes (57, 59, 96, 97, 98) exist in state outflows."""
    result = athena_query(context.resources.athena, f"""
        SELECT COUNT(*) as cnt
        FROM {SILVER_DB}.silver_irs_state_outflows
        WHERE CAST(dest_geography_id AS INT) IN (57, 59, 96, 97, 98)
    """)
    count = result[0]["cnt"] if result else 0
    return AssetCheckResult(
        passed=count == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"aggregate_rows_found": count},
    )


@dg.asset_check(asset="silver_irs_state_outflows", name="check_no_suppressed", required_resource_keys={"athena"})
def check_silver_irs_state_outflows_no_suppressed(context) -> AssetCheckResult:
    """Verify no IRS-suppressed rows (households=-1 or individuals=-1) exist."""
    result = athena_query(context.resources.athena, f"""
        SELECT COUNT(*) as cnt
        FROM {SILVER_DB}.silver_irs_state_outflows
        WHERE households = -1 OR individuals = -1
    """)
    count = result[0]["cnt"] if result else 0
    return AssetCheckResult(
        passed=count == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"suppressed_rows_found": count},
    )


@dg.asset_check(asset="silver_irs_state_outflows", name="check_all_years", required_resource_keys={"athena"})
def check_silver_irs_state_outflows_all_years(context) -> AssetCheckResult:
    """Verify all 12 survey years (2012–2023) are present."""
    result = athena_query(context.resources.athena, f"""
        SELECT COUNT(DISTINCT survey_year) as year_count
        FROM {SILVER_DB}.silver_irs_state_outflows
    """)
    year_count = result[0]["year_count"] if result else 0
    return AssetCheckResult(
        passed=year_count == 12,
        severity=AssetCheckSeverity.ERROR,
        metadata={"year_count": year_count, "expected": 12},
    )


@dg.asset_check(asset="silver_irs_state_outflows", name="check_no_null_keys", required_resource_keys={"athena"})
def check_silver_irs_state_outflows_no_null_keys(context) -> AssetCheckResult:
    """Verify no NULL origin or destination geography IDs exist."""
    result = athena_query(context.resources.athena, f"""
        SELECT COUNT(*) as cnt
        FROM {SILVER_DB}.silver_irs_state_outflows
        WHERE origin_geography_id IS NULL OR dest_geography_id IS NULL
    """)
    count = result[0]["cnt"] if result else 0
    return AssetCheckResult(
        passed=count == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"null_key_count": count},
    )


@dg.asset_check(asset="silver_irs_county_outflows", name="check_no_aggregates", required_resource_keys={"athena"})
def check_silver_irs_county_outflows_no_aggregates(context) -> AssetCheckResult:
    """Verify no aggregate codes (57, 59, 96, 97, 98) exist in county outflows."""
    result = athena_query(context.resources.athena, f"""
        SELECT COUNT(*) as cnt
        FROM {SILVER_DB}.silver_irs_county_outflows
        WHERE CAST(SUBSTRING(dest_geography_id, 1, 2) AS INT) IN (57, 59, 96, 97, 98)
    """)
    count = result[0]["cnt"] if result else 0
    return AssetCheckResult(
        passed=count == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"aggregate_rows_found": count},
    )


@dg.asset_check(asset="silver_irs_county_outflows", name="check_no_suppressed", required_resource_keys={"athena"})
def check_silver_irs_county_outflows_no_suppressed(context) -> AssetCheckResult:
    """Verify no IRS-suppressed rows (households=-1 or individuals=-1) exist."""
    result = athena_query(context.resources.athena, f"""
        SELECT COUNT(*) as cnt
        FROM {SILVER_DB}.silver_irs_county_outflows
        WHERE households = -1 OR individuals = -1
    """)
    count = result[0]["cnt"] if result else 0
    return AssetCheckResult(
        passed=count == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"suppressed_rows_found": count},
    )


@dg.asset_check(asset="silver_irs_county_outflows", name="check_no_null_keys", required_resource_keys={"athena"})
def check_silver_irs_county_outflows_no_null_keys(context) -> AssetCheckResult:
    """Verify no NULL origin or destination geography IDs exist."""
    result = athena_query(context.resources.athena, f"""
        SELECT COUNT(*) as cnt
        FROM {SILVER_DB}.silver_irs_county_outflows
        WHERE origin_geography_id IS NULL OR dest_geography_id IS NULL
    """)
    count = result[0]["cnt"] if result else 0
    return AssetCheckResult(
        passed=count == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"null_key_count": count},
    )