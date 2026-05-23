"""Asset checks for silver TIGER tables.

These checks run after each silver_tiger_* materialization and catch data quality
issues before the data reaches the gold layer.
"""

from dagster import AssetCheckResult, AssetCheckSeverity, asset_check

# TIGER includes territories so counts are higher than ACS
EXPECTED_STATE_COUNT_TIGER = 56  # 50 states + DC + 4 territories
APPROX_COUNTY_COUNT_TIGER = 3235  # includes territory counties


@asset_check(asset="silver_tiger_states", name="check_row_counts_per_year")
def check_silver_tiger_states_row_count(context) -> AssetCheckResult:
    """Every survey_year should have exactly 56 state rows (50 states + DC + 4 territories)."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT survey_year, COUNT(*) as row_count
        FROM population_demographics_silver.silver_tiger_states
        GROUP BY survey_year
        ORDER BY survey_year
    """)
    failed_years = [r for r in result if r["row_count"] != EXPECTED_STATE_COUNT_TIGER]
    return AssetCheckResult(
        passed=len(failed_years) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"failed_years": str(failed_years)} if failed_years else {"status": "all 56 rows present"},
    )


@asset_check(asset="silver_tiger_counties", name="check_row_counts_per_year")
def check_silver_tiger_counties_row_count(context) -> AssetCheckResult:
    """Each survey_year should have approximately 3235 county rows."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT survey_year, COUNT(*) as row_count
        FROM population_demographics_silver.silver_tiger_counties
        GROUP BY survey_year
        ORDER BY survey_year
    """)
    failed_years = [r for r in result if r["row_count"] < APPROX_COUNTY_COUNT_TIGER - 10]
    return AssetCheckResult(
        passed=len(failed_years) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"failed_years": str(failed_years)} if failed_years else {"status": f"all years >= {APPROX_COUNTY_COUNT_TIGER - 10}"},
    )


@asset_check(asset="silver_tiger_tracts", name="check_row_counts_per_year")
def check_silver_tiger_tracts_row_count(context) -> AssetCheckResult:
    """Each survey_year should have approximately 73,000+ tract rows (51 states)."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT survey_year, COUNT(*) as row_count
        FROM population_demographics_silver.silver_tiger_tracts
        GROUP BY survey_year
        ORDER BY survey_year
    """)
    min_expected = 73000
    failed_years = [r for r in result if r["row_count"] < min_expected]
    return AssetCheckResult(
        passed=len(failed_years) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"failed_years": str(failed_years)} if failed_years else {"status": f"all years >= {min_expected}"},
    )


@asset_check(asset="silver_tiger_states", name="check_no_null_geography_id")
def check_silver_tiger_states_no_null_geo_id(context) -> AssetCheckResult:
    """No rows should have a null geography_id."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT COUNT(*) as null_count
        FROM population_demographics_silver.silver_tiger_states
        WHERE geography_id IS NULL
    """)
    null_count = result[0]["null_count"] if result else 0
    return AssetCheckResult(
        passed=null_count == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"null_geography_id_count": null_count},
    )


@asset_check(asset="silver_tiger_counties", name="check_no_null_geography_id")
def check_silver_tiger_counties_no_null_geo_id(context) -> AssetCheckResult:
    """No rows should have a null geography_id."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT COUNT(*) as null_count
        FROM population_demographics_silver.silver_tiger_counties
        WHERE geography_id IS NULL
    """)
    null_count = result[0]["null_count"] if result else 0
    return AssetCheckResult(
        passed=null_count == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"null_geography_id_count": null_count},
    )


@asset_check(asset="silver_tiger_tracts", name="check_no_null_geography_id")
def check_silver_tiger_tracts_no_null_geo_id(context) -> AssetCheckResult:
    """No rows should have a null geography_id."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT COUNT(*) as null_count
        FROM population_demographics_silver.silver_tiger_tracts
        WHERE geography_id IS NULL
    """)
    null_count = result[0]["null_count"] if result else 0
    return AssetCheckResult(
        passed=null_count == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"null_geography_id_count": null_count},
    )


@asset_check(asset="silver_tiger_states", name="check_no_duplicate_keys")
def check_silver_tiger_states_no_duplicate_keys(context) -> AssetCheckResult:
    """No duplicate (geography_id, survey_year) composite keys."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT geography_id, survey_year, COUNT(*) as cnt
        FROM population_demographics_silver.silver_tiger_states
        GROUP BY geography_id, survey_year
        HAVING COUNT(*) > 1
    """)
    return AssetCheckResult(
        passed=len(result) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"duplicate_count": len(result), "duplicates": str([dict(r) for r in result[:5]])},
    )


@asset_check(asset="silver_tiger_counties", name="check_no_duplicate_keys")
def check_silver_tiger_counties_no_duplicate_keys(context) -> AssetCheckResult:
    """No duplicate (geography_id, survey_year) composite keys."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT geography_id, survey_year, COUNT(*) as cnt
        FROM population_demographics_silver.silver_tiger_counties
        GROUP BY geography_id, survey_year
        HAVING COUNT(*) > 1
    """)
    return AssetCheckResult(
        passed=len(result) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"duplicate_count": len(result), "duplicates": str([dict(r) for r in result[:5]])},
    )


@asset_check(asset="silver_tiger_tracts", name="check_no_duplicate_keys")
def check_silver_tiger_tracts_no_duplicate_keys(context) -> AssetCheckResult:
    """No duplicate (geography_id, survey_year) composite keys."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT geography_id, survey_year, COUNT(*) as cnt
        FROM population_demographics_silver.silver_tiger_tracts
        GROUP BY geography_id, survey_year
        HAVING COUNT(*) > 1
    """)
    return AssetCheckResult(
        passed=len(result) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"duplicate_count": len(result), "duplicates": str([dict(r) for r in result[:5]])},
    )


@asset_check(asset="silver_tiger_states", name="check_all_years_present")
def check_silver_tiger_states_all_years_present(context) -> AssetCheckResult:
    """All 13 years (2012-2024) should be present in the table."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT COUNT(DISTINCT survey_year) as year_count
        FROM population_demographics_silver.silver_tiger_states
    """)
    year_count = result[0]["year_count"] if result else 0
    return AssetCheckResult(
        passed=year_count == 13,
        severity=AssetCheckSeverity.ERROR,
        metadata={"year_count": year_count, "expected": 13},
    )


@asset_check(asset="silver_tiger_counties", name="check_all_years_present")
def check_silver_tiger_counties_all_years_present(context) -> AssetCheckResult:
    """All 13 years (2012-2024) should be present in the table."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT COUNT(DISTINCT survey_year) as year_count
        FROM population_demographics_silver.silver_tiger_counties
    """)
    year_count = result[0]["year_count"] if result else 0
    return AssetCheckResult(
        passed=year_count == 13,
        severity=AssetCheckSeverity.ERROR,
        metadata={"year_count": year_count, "expected": 13},
    )


@asset_check(asset="silver_tiger_tracts", name="check_all_years_present")
def check_silver_tiger_tracts_all_years_present(context) -> AssetCheckResult:
    """All 13 years (2012-2024) should be present in the table."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT COUNT(DISTINCT survey_year) as year_count
        FROM population_demographics_silver.silver_tiger_tracts
    """)
    year_count = result[0]["year_count"] if result else 0
    return AssetCheckResult(
        passed=year_count == 13,
        severity=AssetCheckSeverity.ERROR,
        metadata={"year_count": year_count, "expected": 13},
    )