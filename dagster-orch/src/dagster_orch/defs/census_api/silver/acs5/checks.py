"""Asset checks for silver ACS5 tables.

These checks run after each silver_acs5_* materialization and catch data quality
issues before the data reaches the gold layer.
"""

from dagster import AssetCheckResult, AssetCheckSeverity, asset_check

SILVER_DB = "population_demographics_silver"

# Expected row counts per survey_year (ACS API returns 50 states + DC)
EXPECTED_STATE_COUNT = 52
# Counties vary slightly by year — track as assertion
APPROX_COUNTY_COUNT = 3222
APPROX_TRACT_COUNT_PER_STATE = 9129  # California example


@asset_check(asset="silver_acs5_states", name="check_row_counts_per_year")
def check_silver_acs5_states_row_count(context) -> AssetCheckResult:
    """Every survey_year should have exactly 52 state rows."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT survey_year, COUNT(*) as row_count
        FROM population_demographics_silver.silver_acs5_states
        GROUP BY survey_year
        ORDER BY survey_year
    """)
    failed_years = [r for r in result if r["row_count"] != EXPECTED_STATE_COUNT]
    return AssetCheckResult(
        passed=len(failed_years) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"failed_years": str(failed_years)} if failed_years else {"status": "all 52 rows present"},
    )


@asset_check(asset="silver_acs5_counties", name="check_row_counts_per_year")
def check_silver_acs5_counties_row_count(context) -> AssetCheckResult:
    """Each survey_year should have approximately 3222 county rows."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT survey_year, COUNT(*) as row_count
        FROM population_demographics_silver.silver_acs5_counties
        GROUP BY survey_year
        ORDER BY survey_year
    """)
    failed_years = [r for r in result if r["row_count"] < APPROX_COUNTY_COUNT - 10]
    return AssetCheckResult(
        passed=len(failed_years) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"failed_years": str(failed_years)} if failed_years else {"status": f"all years >= {APPROX_COUNTY_COUNT - 10}"},
    )


@asset_check(asset="silver_acs5_tracts", name="check_row_counts_per_year")
def check_silver_acs5_tracts_row_count(context) -> AssetCheckResult:
    """Each survey_year should have approximately 73,000+ tract rows (51 states)."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT survey_year, COUNT(*) as row_count
        FROM population_demographics_silver.silver_acs5_tracts
        GROUP BY survey_year
        ORDER BY survey_year
    """)
    # 51 states × ~1430 tracts ≈ 73,000
    min_expected = 73000
    failed_years = [r for r in result if r["row_count"] < min_expected]
    return AssetCheckResult(
        passed=len(failed_years) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"failed_years": str(failed_years)} if failed_years else {"status": f"all years >= {min_expected}"},
    )


@asset_check(asset="silver_acs5_states", name="check_no_null_geography_id")
def check_silver_acs5_states_no_null_geo_id(context) -> AssetCheckResult:
    """No rows should have a null geography_id."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT COUNT(*) as null_count
        FROM population_demographics_silver.silver_acs5_states
        WHERE geography_id IS NULL
    """)
    null_count = result[0]["null_count"] if result else 0
    return AssetCheckResult(
        passed=null_count == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"null_geography_id_count": null_count},
    )


@asset_check(asset="silver_acs5_counties", name="check_no_null_geography_id")
def check_silver_acs5_counties_no_null_geo_id(context) -> AssetCheckResult:
    """No rows should have a null geography_id."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT COUNT(*) as null_count
        FROM population_demographics_silver.silver_acs5_counties
        WHERE geography_id IS NULL
    """)
    null_count = result[0]["null_count"] if result else 0
    return AssetCheckResult(
        passed=null_count == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"null_geography_id_count": null_count},
    )


@asset_check(asset="silver_acs5_tracts", name="check_no_null_geography_id")
def check_silver_acs5_tracts_no_null_geo_id(context) -> AssetCheckResult:
    """No rows should have a null geography_id."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT COUNT(*) as null_count
        FROM population_demographics_silver.silver_acs5_tracts
        WHERE geography_id IS NULL
    """)
    null_count = result[0]["null_count"] if result else 0
    return AssetCheckResult(
        passed=null_count == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"null_geography_id_count": null_count},
    )


@asset_check(asset="silver_acs5_states", name="check_no_duplicate_keys")
def check_silver_acs5_states_no_duplicate_keys(context) -> AssetCheckResult:
    """No duplicate (geography_id, survey_year) composite keys."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT geography_id, survey_year, COUNT(*) as cnt
        FROM population_demographics_silver.silver_acs5_states
        GROUP BY geography_id, survey_year
        HAVING COUNT(*) > 1
    """)
    return AssetCheckResult(
        passed=len(result) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"duplicate_count": len(result), "duplicates": str([dict(r) for r in result[:5]])},
    )


@asset_check(asset="silver_acs5_counties", name="check_no_duplicate_keys")
def check_silver_acs5_counties_no_duplicate_keys(context) -> AssetCheckResult:
    """No duplicate (geography_id, survey_year) composite keys."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT geography_id, survey_year, COUNT(*) as cnt
        FROM population_demographics_silver.silver_acs5_counties
        GROUP BY geography_id, survey_year
        HAVING COUNT(*) > 1
    """)
    return AssetCheckResult(
        passed=len(result) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"duplicate_count": len(result), "duplicates": str([dict(r) for r in result[:5]])},
    )


@asset_check(asset="silver_acs5_tracts", name="check_no_duplicate_keys")
def check_silver_acs5_tracts_no_duplicate_keys(context) -> AssetCheckResult:
    """No duplicate (geography_id, survey_year) composite keys."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT geography_id, survey_year, COUNT(*) as cnt
        FROM population_demographics_silver.silver_acs5_tracts
        GROUP BY geography_id, survey_year
        HAVING COUNT(*) > 1
    """)
    return AssetCheckResult(
        passed=len(result) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"duplicate_count": len(result), "duplicates": str([dict(r) for r in result[:5]])},
    )


@asset_check(asset="silver_acs5_states", name="check_all_years_present")
def check_silver_acs5_states_all_years_present(context) -> AssetCheckResult:
    """All 13 years (2012-2024) should be present in the table."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT COUNT(DISTINCT survey_year) as year_count
        FROM population_demographics_silver.silver_acs5_states
    """)
    year_count = result[0]["year_count"] if result else 0
    return AssetCheckResult(
        passed=year_count == 13,
        severity=AssetCheckSeverity.ERROR,
        metadata={"year_count": year_count, "expected": 13},
    )


@asset_check(asset="silver_acs5_counties", name="check_all_years_present")
def check_silver_acs5_counties_all_years_present(context) -> AssetCheckResult:
    """All 13 years (2012-2024) should be present in the table."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT COUNT(DISTINCT survey_year) as year_count
        FROM population_demographics_silver.silver_acs5_counties
    """)
    year_count = result[0]["year_count"] if result else 0
    return AssetCheckResult(
        passed=year_count == 13,
        severity=AssetCheckSeverity.ERROR,
        metadata={"year_count": year_count, "expected": 13},
    )


@asset_check(asset="silver_acs5_tracts", name="check_all_years_present")
def check_silver_acs5_tracts_all_years_present(context) -> AssetCheckResult:
    """All 13 years (2012-2024) should be present in the table."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT COUNT(DISTINCT survey_year) as year_count
        FROM population_demographics_silver.silver_acs5_tracts
    """)
    year_count = result[0]["year_count"] if result else 0
    return AssetCheckResult(
        passed=year_count == 13,
        severity=AssetCheckSeverity.ERROR,
        metadata={"year_count": year_count, "expected": 13},
    )