"""Asset checks for gold tables.

These checks run after each gold materialization and verify join correctness,
data completeness, and geometry integrity between ACS and TIGER.
"""

from dagster import AssetCheckResult, AssetCheckSeverity, asset_check

GOLD_DB = "population_demographics_gold"


@asset_check(asset="gold_states", name="check_row_counts_per_year")
def check_gold_states_row_count(context) -> AssetCheckResult:
    """Verify join didn't lose rows — approximately 52 per year."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT survey_year, COUNT(*) as row_count
        FROM population_demographics_gold.gold_states
        GROUP BY survey_year
        ORDER BY survey_year
    """)
    # ~52 rows per year (50 states + DC). Allow slight variation.
    failed_years = [r for r in result if r["row_count"] < 50]
    return AssetCheckResult(
        passed=len(failed_years) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"failed_years": str(failed_years)} if failed_years else {"status": "all years >= 50 rows"},
    )


@asset_check(asset="gold_counties", name="check_row_counts_per_year")
def check_gold_counties_row_count(context) -> AssetCheckResult:
    """Verify join didn't lose rows — approximately 3,212+ per year."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT survey_year, COUNT(*) as row_count
        FROM population_demographics_gold.gold_counties
        GROUP BY survey_year
        ORDER BY survey_year
    """)
    failed_years = [r for r in result if r["row_count"] < 3200]
    return AssetCheckResult(
        passed=len(failed_years) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"failed_years": str(failed_years)} if failed_years else {"status": "all years >= 3200 rows"},
    )


@asset_check(asset="gold_tracts", name="check_row_counts_per_year")
def check_gold_tracts_row_count(context) -> AssetCheckResult:
    """Verify join didn't lose rows — approximately 73,000+ per year."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT survey_year, COUNT(*) as row_count
        FROM population_demographics_gold.gold_tracts
        GROUP BY survey_year
        ORDER BY survey_year
    """)
    failed_years = [r for r in result if r["row_count"] < 73000]
    return AssetCheckResult(
        passed=len(failed_years) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"failed_years": str(failed_years)} if failed_years else {"status": "all years >= 73000 rows"},
    )


@asset_check(asset="gold_states", name="check_no_duplicate_keys")
def check_gold_states_no_duplicate_keys(context) -> AssetCheckResult:
    """No duplicate (geography_id, survey_year) composite keys after join."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT geography_id, survey_year, COUNT(*) as cnt
        FROM population_demographics_gold.gold_states
        GROUP BY geography_id, survey_year
        HAVING COUNT(*) > 1
    """)
    return AssetCheckResult(
        passed=len(result) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"duplicate_count": len(result)},
    )


@asset_check(asset="gold_counties", name="check_no_duplicate_keys")
def check_gold_counties_no_duplicate_keys(context) -> AssetCheckResult:
    """No duplicate (geography_id, survey_year) composite keys after join."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT geography_id, survey_year, COUNT(*) as cnt
        FROM population_demographics_gold.gold_counties
        GROUP BY geography_id, survey_year
        HAVING COUNT(*) > 1
    """)
    return AssetCheckResult(
        passed=len(result) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"duplicate_count": len(result)},
    )


@asset_check(asset="gold_tracts", name="check_no_duplicate_keys")
def check_gold_tracts_no_duplicate_keys(context) -> AssetCheckResult:
    """No duplicate (geography_id, survey_year) composite keys after join."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT geography_id, survey_year, COUNT(*) as cnt
        FROM population_demographics_gold.gold_tracts
        GROUP BY geography_id, survey_year
        HAVING COUNT(*) > 1
    """)
    return AssetCheckResult(
        passed=len(result) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"duplicate_count": len(result)},
    )


@asset_check(asset="gold_states", name="check_geometry_present")
def check_gold_states_geometry(context) -> AssetCheckResult:
    """Verify geometry_wkt is populated for all rows."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT COUNT(*) as total, COUNT(geometry_wkt) as has_geometry
        FROM population_demographics_gold.gold_states
    """)
    total = result[0]["total"] if result else 0
    has_geometry = result[0]["has_geometry"] if result else 0
    return AssetCheckResult(
        passed=has_geometry == total,
        severity=AssetCheckSeverity.ERROR,
        metadata={"total": total, "has_geometry": has_geometry, "missing": total - has_geometry},
    )


@asset_check(asset="gold_counties", name="check_geometry_present")
def check_gold_counties_geometry(context) -> AssetCheckResult:
    """Verify geometry_wkt is populated for all rows."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT COUNT(*) as total, COUNT(geometry_wkt) as has_geometry
        FROM population_demographics_gold.gold_counties
    """)
    total = result[0]["total"] if result else 0
    has_geometry = result[0]["has_geometry"] if result else 0
    return AssetCheckResult(
        passed=has_geometry == total,
        severity=AssetCheckSeverity.ERROR,
        metadata={"total": total, "has_geometry": has_geometry, "missing": total - has_geometry},
    )


@asset_check(asset="gold_tracts", name="check_geometry_present")
def check_gold_tracts_geometry(context) -> AssetCheckResult:
    """Verify geometry_wkt is populated for all rows."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT COUNT(*) as total, COUNT(geometry_wkt) as has_geometry
        FROM population_demographics_gold.gold_tracts
    """)
    total = result[0]["total"] if result else 0
    has_geometry = result[0]["has_geometry"] if result else 0
    return AssetCheckResult(
        passed=has_geometry == total,
        severity=AssetCheckSeverity.ERROR,
        metadata={"total": total, "has_geometry": has_geometry, "missing": total - has_geometry},
    )


@asset_check(asset="gold_states", name="check_acs_metrics_populated")
def check_gold_states_acs_metrics(context) -> AssetCheckResult:
    """Verify ACS metrics (total_population, median_household_income) are populated."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT
            COUNT(*) as total,
            COUNT(total_population) as has_population,
            COUNT(median_household_income) as has_income,
            COUNT(bachelors_degree) as has_education
        FROM population_demographics_gold.gold_states
    """)
    if not result:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            metadata={"error": "no data returned"},
        )
    total = result[0]["total"]
    has_population = result[0]["has_population"]
    has_income = result[0]["has_income"]
    has_education = result[0]["has_education"]
    return AssetCheckResult(
        passed=(has_population == total and has_income == total and has_education == total),
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "total": total,
            "has_population": has_population,
            "has_income": has_income,
            "has_education": has_education,
        },
    )


@asset_check(asset="gold_counties", name="check_acs_metrics_populated")
def check_gold_counties_acs_metrics(context) -> AssetCheckResult:
    """Verify ACS metrics are populated."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT
            COUNT(*) as total,
            COUNT(total_population) as has_population,
            COUNT(median_household_income) as has_income
        FROM population_demographics_gold.gold_counties
    """)
    if not result:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            metadata={"error": "no data returned"},
        )
    total = result[0]["total"]
    has_population = result[0]["has_population"]
    has_income = result[0]["has_income"]
    return AssetCheckResult(
        passed=(has_population == total and has_income == total),
        severity=AssetCheckSeverity.ERROR,
        metadata={"total": total, "has_population": has_population, "has_income": has_income},
    )


@asset_check(asset="gold_tracts", name="check_acs_metrics_populated")
def check_gold_tracts_acs_metrics(context) -> AssetCheckResult:
    """Verify ACS metrics are populated."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT
            COUNT(*) as total,
            COUNT(total_population) as has_population,
            COUNT(median_household_income) as has_income
        FROM population_demographics_gold.gold_tracts
    """)
    if not result:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            metadata={"error": "no data returned"},
        )
    total = result[0]["total"]
    has_population = result[0]["has_population"]
    has_income = result[0]["has_income"]
    return AssetCheckResult(
        passed=(has_population == total and has_income == total),
        severity=AssetCheckSeverity.ERROR,
        metadata={"total": total, "has_population": has_population, "has_income": has_income},
    )


@asset_check(asset="gold_states", name="check_california_population_sanity")
def check_gold_states_ca_population(context) -> AssetCheckResult:
    """Spot check: California total_population should be ~39M for recent years."""
    athena = context.resources.athena
    result = athena.execute_query("""
        SELECT survey_year, total_population, median_household_income
        FROM population_demographics_gold.gold_states
        WHERE state_fips = '06'
        ORDER BY survey_year
    """)
    # Check recent years (2022+) have plausible CA population
    recent = [r for r in result if r["survey_year"] >= 2022]
    passed = True
    for r in recent:
        pop = r.get("total_population")
        if pop and (int(pop) < 35000000 or int(pop) > 45000000):
            passed = False
            break
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "recent_ca_rows": str([{"year": r["survey_year"], "pop": r["total_population"]} for r in recent]),
        },
    )