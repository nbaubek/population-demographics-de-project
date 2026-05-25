"""Asset checks for silver ACS5 tables.

These checks run after each silver_acs5_* materialization and catch data quality
issues before the data reaches the gold layer.
"""

import dagster as dg
from dagster import AssetCheckResult, AssetCheckSeverity

from dagster_orch.defs.census_api.shared.athena_query import athena_query

SILVER_DB = "population_demographics_silver"

EXPECTED_STATE_COUNT = 52
APPROX_COUNTY_COUNT_MIN = 3212
MIN_TRACT_COUNT = 73000


@dg.asset_check(asset="silver_acs5_states", name="check_row_counts_per_year", required_resource_keys={"athena"})
def check_silver_acs5_states_row_count(context) -> AssetCheckResult:
    """Every survey_year should have exactly 52 state rows."""
    result = athena_query(context.resources.athena, f"""
        SELECT survey_year, COUNT(*) as row_count
        FROM {SILVER_DB}.silver_acs5_states
        GROUP BY survey_year
        ORDER BY survey_year
    """)
    failed = {r["survey_year"]: r["row_count"] for r in result if r["row_count"] != EXPECTED_STATE_COUNT}
    return AssetCheckResult(
        passed=len(failed) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "expected_per_year": EXPECTED_STATE_COUNT,
            "year_count": len(result),
            "failed_years": failed if failed else {"none": "all years have 52 rows"},
        },
    )


@dg.asset_check(asset="silver_acs5_counties", name="check_row_counts_per_year", required_resource_keys={"athena"})
def check_silver_acs5_counties_row_count(context) -> AssetCheckResult:
    """Each survey_year should have at least 3,212 county rows."""
    result = athena_query(context.resources.athena, f"""
        SELECT survey_year, COUNT(*) as row_count
        FROM {SILVER_DB}.silver_acs5_counties
        GROUP BY survey_year
        ORDER BY survey_year
    """)
    failed = {r["survey_year"]: r["row_count"] for r in result if r["row_count"] < APPROX_COUNTY_COUNT_MIN}
    return AssetCheckResult(
        passed=len(failed) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "min_expected": APPROX_COUNTY_COUNT_MIN,
            "year_count": len(result),
            "failed_years": failed if failed else {"none": f"all years >= {APPROX_COUNTY_COUNT_MIN}"},
        },
    )


@dg.asset_check(asset="silver_acs5_tracts", name="check_row_counts_per_year", required_resource_keys={"athena"})
def check_silver_acs5_tracts_row_count(context) -> AssetCheckResult:
    """Each survey_year should have at least 73,000 tract rows."""
    result = athena_query(context.resources.athena, f"""
        SELECT survey_year, COUNT(*) as row_count
        FROM {SILVER_DB}.silver_acs5_tracts
        GROUP BY survey_year
        ORDER BY survey_year
    """)
    failed = {r["survey_year"]: r["row_count"] for r in result if r["row_count"] < MIN_TRACT_COUNT}
    return AssetCheckResult(
        passed=len(failed) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "min_expected": MIN_TRACT_COUNT,
            "year_count": len(result),
            "failed_years": failed if failed else {"none": f"all years >= {MIN_TRACT_COUNT}"},
        },
    )


@dg.asset_check(asset="silver_acs5_states", name="check_no_null_geography_id", required_resource_keys={"athena"})
def check_silver_acs5_states_no_null_geo_id(context) -> AssetCheckResult:
    """No rows should have a null geography_id."""
    result = athena_query(context.resources.athena, f"""
        SELECT COUNT(*) as null_count
        FROM {SILVER_DB}.silver_acs5_states
        WHERE geography_id IS NULL
    """)
    null_count = result[0]["null_count"] if result else 0
    return AssetCheckResult(
        passed=null_count == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"null_count": null_count},
    )


@dg.asset_check(asset="silver_acs5_counties", name="check_no_null_geography_id", required_resource_keys={"athena"})
def check_silver_acs5_counties_no_null_geo_id(context) -> AssetCheckResult:
    """No rows should have a null geography_id."""
    result = athena_query(context.resources.athena, f"""
        SELECT COUNT(*) as null_count
        FROM {SILVER_DB}.silver_acs5_counties
        WHERE geography_id IS NULL
    """)
    null_count = result[0]["null_count"] if result else 0
    return AssetCheckResult(
        passed=null_count == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"null_count": null_count},
    )


@dg.asset_check(asset="silver_acs5_tracts", name="check_no_null_geography_id", required_resource_keys={"athena"})
def check_silver_acs5_tracts_no_null_geo_id(context) -> AssetCheckResult:
    """No rows should have a null geography_id."""
    result = athena_query(context.resources.athena, f"""
        SELECT COUNT(*) as null_count
        FROM {SILVER_DB}.silver_acs5_tracts
        WHERE geography_id IS NULL
    """)
    null_count = result[0]["null_count"] if result else 0
    return AssetCheckResult(
        passed=null_count == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"null_count": null_count},
    )


@dg.asset_check(asset="silver_acs5_states", name="check_no_duplicate_keys", required_resource_keys={"athena"})
def check_silver_acs5_states_no_duplicate_keys(context) -> AssetCheckResult:
    """No duplicate (geography_id, survey_year) composite keys."""
    result = athena_query(context.resources.athena, f"""
        SELECT geography_id, survey_year, COUNT(*) as cnt
        FROM {SILVER_DB}.silver_acs5_states
        GROUP BY geography_id, survey_year
        HAVING COUNT(*) > 1
    """)
    return AssetCheckResult(
        passed=len(result) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "duplicate_count": len(result),
            "sample": [{"geography_id": r["geography_id"], "survey_year": r["survey_year"]} for r in result[:3]],
        },
    )


@dg.asset_check(asset="silver_acs5_counties", name="check_no_duplicate_keys", required_resource_keys={"athena"})
def check_silver_acs5_counties_no_duplicate_keys(context) -> AssetCheckResult:
    """No duplicate (geography_id, survey_year) composite keys."""
    result = athena_query(context.resources.athena, f"""
        SELECT geography_id, survey_year, COUNT(*) as cnt
        FROM {SILVER_DB}.silver_acs5_counties
        GROUP BY geography_id, survey_year
        HAVING COUNT(*) > 1
    """)
    return AssetCheckResult(
        passed=len(result) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "duplicate_count": len(result),
            "sample": [{"geography_id": r["geography_id"], "survey_year": r["survey_year"]} for r in result[:3]],
        },
    )


@dg.asset_check(asset="silver_acs5_tracts", name="check_no_duplicate_keys", required_resource_keys={"athena"})
def check_silver_acs5_tracts_no_duplicate_keys(context) -> AssetCheckResult:
    """No duplicate (geography_id, survey_year) composite keys."""
    result = athena_query(context.resources.athena, f"""
        SELECT geography_id, survey_year, COUNT(*) as cnt
        FROM {SILVER_DB}.silver_acs5_tracts
        GROUP BY geography_id, survey_year
        HAVING COUNT(*) > 1
    """)
    return AssetCheckResult(
        passed=len(result) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "duplicate_count": len(result),
            "sample": [{"geography_id": r["geography_id"], "survey_year": r["survey_year"]} for r in result[:3]],
        },
    )


@dg.asset_check(asset="silver_acs5_states", name="check_all_years_present", required_resource_keys={"athena"})
def check_silver_acs5_states_all_years_present(context) -> AssetCheckResult:
    """All 13 years (2012-2024) should be present in the table."""
    result = athena_query(context.resources.athena, f"""
        SELECT COUNT(DISTINCT survey_year) as year_count
        FROM {SILVER_DB}.silver_acs5_states
    """)
    year_count = result[0]["year_count"] if result else 0
    return AssetCheckResult(
        passed=year_count == 13,
        severity=AssetCheckSeverity.ERROR,
        metadata={"year_count": year_count, "expected": 13, "missing": max(0, 13 - year_count)},
    )


@dg.asset_check(asset="silver_acs5_counties", name="check_all_years_present", required_resource_keys={"athena"})
def check_silver_acs5_counties_all_years_present(context) -> AssetCheckResult:
    """All 13 years (2012-2024) should be present in the table."""
    result = athena_query(context.resources.athena, f"""
        SELECT COUNT(DISTINCT survey_year) as year_count
        FROM {SILVER_DB}.silver_acs5_counties
    """)
    year_count = result[0]["year_count"] if result else 0
    return AssetCheckResult(
        passed=year_count == 13,
        severity=AssetCheckSeverity.ERROR,
        metadata={"year_count": year_count, "expected": 13, "missing": max(0, 13 - year_count)},
    )


@dg.asset_check(asset="silver_acs5_tracts", name="check_all_years_present", required_resource_keys={"athena"})
def check_silver_acs5_tracts_all_years_present(context) -> AssetCheckResult:
    """All 13 years (2012-2024) should be present in the table."""
    result = athena_query(context.resources.athena, f"""
        SELECT COUNT(DISTINCT survey_year) as year_count
        FROM {SILVER_DB}.silver_acs5_tracts
    """)
    year_count = result[0]["year_count"] if result else 0
    return AssetCheckResult(
        passed=year_count == 13,
        severity=AssetCheckSeverity.ERROR,
        metadata={"year_count": year_count, "expected": 13, "missing": max(0, 13 - year_count)},
    )