"""Asset checks for gold IRS migration tables.

Lightweight confirmations — heavy validation already happened in silver.
Only checks: row count sanity, key integrity, is_non_migrant flag coverage.
"""

import dagster as dg
from dagster import AssetCheckResult, AssetCheckSeverity

from dagster_orch.defs.census_api.shared.athena_query import athena_query

GOLD_DB = "population_demographics_gold"


@dg.asset_check(asset="gold_irs_state_outflows", name="check_row_count_per_year", required_resource_keys={"athena"})
def check_gold_irs_state_outflows_row_count(context) -> AssetCheckResult:
    """Every survey_year should have at least one row."""
    result = athena_query(context.resources.athena, f"""
        SELECT survey_year, COUNT(*) as row_count
        FROM {GOLD_DB}.gold_irs_state_outflows
        GROUP BY survey_year
        ORDER BY survey_year
    """)
    zero_years = {r["survey_year"]: r["row_count"] for r in result if r["row_count"] == 0}
    return AssetCheckResult(
        passed=len(zero_years) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "year_count": len(result),
            "zero_count_years": zero_years if zero_years else {"none": "all years have rows"},
        },
    )


@dg.asset_check(asset="gold_irs_state_outflows", name="check_no_null_keys", required_resource_keys={"athena"})
def check_gold_irs_state_outflows_no_null_keys(context) -> AssetCheckResult:
    """No NULL origin or destination geography IDs."""
    result = athena_query(context.resources.athena, f"""
        SELECT COUNT(*) as cnt
        FROM {GOLD_DB}.gold_irs_state_outflows
        WHERE origin_geography_id IS NULL OR dest_geography_id IS NULL
    """)
    count = result[0]["cnt"] if result else 0
    return AssetCheckResult(
        passed=count == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"null_key_count": count},
    )


@dg.asset_check(asset="gold_irs_state_outflows", name="check_is_non_migrant_coverage", required_resource_keys={"athena"})
def check_gold_irs_state_outflows_is_non_migrant_coverage(context) -> AssetCheckResult:
    """Verify is_non_migrant flag has both true and false values."""
    result = athena_query(context.resources.athena, f"""
        SELECT
            COUNT(CASE WHEN is_non_migrant = true THEN 1 END) as non_migrant_count,
            COUNT(CASE WHEN is_non_migrant = false THEN 1 END) as migrant_count
        FROM {GOLD_DB}.gold_irs_state_outflows
    """)
    non_migrant_count = result[0]["non_migrant_count"] if result else 0
    migrant_count = result[0]["migrant_count"] if result else 0
    passed = non_migrant_count > 0 and migrant_count > 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "non_migrant_count": non_migrant_count,
            "migrant_count": migrant_count,
        },
    )


@dg.asset_check(asset="gold_irs_county_outflows", name="check_row_count", required_resource_keys={"athena"})
def check_gold_irs_county_outflows_row_count(context) -> AssetCheckResult:
    """Every survey_year should have at least one row."""
    result = athena_query(context.resources.athena, f"""
        SELECT survey_year, COUNT(*) as row_count
        FROM {GOLD_DB}.gold_irs_county_outflows
        GROUP BY survey_year
        ORDER BY survey_year
    """)
    zero_years = {r["survey_year"]: r["row_count"] for r in result if r["row_count"] == 0}
    return AssetCheckResult(
        passed=len(zero_years) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "year_count": len(result),
            "zero_count_years": zero_years if zero_years else {"none": "all years have rows"},
        },
    )


@dg.asset_check(asset="gold_irs_county_outflows", name="check_no_null_keys", required_resource_keys={"athena"})
def check_gold_irs_county_outflows_no_null_keys(context) -> AssetCheckResult:
    """No NULL origin or destination geography IDs."""
    result = athena_query(context.resources.athena, f"""
        SELECT COUNT(*) as cnt
        FROM {GOLD_DB}.gold_irs_county_outflows
        WHERE origin_geography_id IS NULL OR dest_geography_id IS NULL
    """)
    count = result[0]["cnt"] if result else 0
    return AssetCheckResult(
        passed=count == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"null_key_count": count},
    )


@dg.asset_check(asset="gold_irs_county_outflows", name="check_is_non_migrant_coverage", required_resource_keys={"athena"})
def check_gold_irs_county_outflows_is_non_migrant_coverage(context) -> AssetCheckResult:
    """Verify is_non_migrant flag has both true and false values."""
    result = athena_query(context.resources.athena, f"""
        SELECT
            COUNT(CASE WHEN is_non_migrant = true THEN 1 END) as non_migrant_count,
            COUNT(CASE WHEN is_non_migrant = false THEN 1 END) as migrant_count
        FROM {GOLD_DB}.gold_irs_county_outflows
    """)
    non_migrant_count = result[0]["non_migrant_count"] if result else 0
    migrant_count = result[0]["migrant_count"] if result else 0
    passed = non_migrant_count > 0 and migrant_count > 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "non_migrant_count": non_migrant_count,
            "migrant_count": migrant_count,
        },
    )