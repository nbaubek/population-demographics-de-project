"""Asset checks for gold census tables (ACS+TIGER joined).

These checks run after each gold materialization and verify join correctness,
data completeness, and geometry integrity between ACS and TIGER.
"""

import dagster as dg
from dagster import AssetCheckResult, AssetCheckSeverity

from dagster_orch.defs.census_api.shared.athena_query import athena_query

GOLD_DB = "population_demographics_gold"

MIN_STATES_PER_YEAR = 50
MIN_COUNTIES_PER_YEAR = 3200
MIN_TRACTS_PER_YEAR = 73000


@dg.asset_check(asset="gold_states", name="check_row_counts_per_year", required_resource_keys={"athena"})
def check_gold_states_row_count(context) -> AssetCheckResult:
    """Verify join didn't lose rows — approximately 52 per year."""
    result = athena_query(context.resources.athena, f"""
        SELECT survey_year, COUNT(*) as row_count
        FROM {GOLD_DB}.gold_states
        GROUP BY survey_year
        ORDER BY survey_year
    """)
    failed = {r["survey_year"]: r["row_count"] for r in result if r["row_count"] < MIN_STATES_PER_YEAR}
    return AssetCheckResult(
        passed=len(failed) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "min_expected": MIN_STATES_PER_YEAR,
            "year_count": len(result),
            "failed_years": failed if failed else {"none": f"all years >= {MIN_STATES_PER_YEAR}"},
        },
    )


@dg.asset_check(asset="gold_counties", name="check_row_counts_per_year", required_resource_keys={"athena"})
def check_gold_counties_row_count(context) -> AssetCheckResult:
    """Verify join didn't lose rows — approximately 3,212+ per year."""
    result = athena_query(context.resources.athena, f"""
        SELECT survey_year, COUNT(*) as row_count
        FROM {GOLD_DB}.gold_counties
        GROUP BY survey_year
        ORDER BY survey_year
    """)
    failed = {r["survey_year"]: r["row_count"] for r in result if r["row_count"] < MIN_COUNTIES_PER_YEAR}
    return AssetCheckResult(
        passed=len(failed) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "min_expected": MIN_COUNTIES_PER_YEAR,
            "year_count": len(result),
            "failed_years": failed if failed else {"none": f"all years >= {MIN_COUNTIES_PER_YEAR}"},
        },
    )


@dg.asset_check(asset="gold_tracts", name="check_row_counts_per_year", required_resource_keys={"athena"})
def check_gold_tracts_row_count(context) -> AssetCheckResult:
    """Verify join didn't lose rows — approximately 73,000+ per year."""
    result = athena_query(context.resources.athena, f"""
        SELECT survey_year, COUNT(*) as row_count
        FROM {GOLD_DB}.gold_tracts
        GROUP BY survey_year
        ORDER BY survey_year
    """)
    failed = {r["survey_year"]: r["row_count"] for r in result if r["row_count"] < MIN_TRACTS_PER_YEAR}
    return AssetCheckResult(
        passed=len(failed) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "min_expected": MIN_TRACTS_PER_YEAR,
            "year_count": len(result),
            "failed_years": failed if failed else {"none": f"all years >= {MIN_TRACTS_PER_YEAR}"},
        },
    )


@dg.asset_check(asset="gold_states", name="check_no_duplicate_keys", required_resource_keys={"athena"})
def check_gold_states_no_duplicate_keys(context) -> AssetCheckResult:
    """No duplicate (geography_id, survey_year) composite keys after join."""
    result = athena_query(context.resources.athena, f"""
        SELECT geography_id, survey_year, COUNT(*) as cnt
        FROM {GOLD_DB}.gold_states
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


@dg.asset_check(asset="gold_counties", name="check_no_duplicate_keys", required_resource_keys={"athena"})
def check_gold_counties_no_duplicate_keys(context) -> AssetCheckResult:
    """No duplicate (geography_id, survey_year) composite keys after join."""
    result = athena_query(context.resources.athena, f"""
        SELECT geography_id, survey_year, COUNT(*) as cnt
        FROM {GOLD_DB}.gold_counties
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


@dg.asset_check(asset="gold_tracts", name="check_no_duplicate_keys", required_resource_keys={"athena"})
def check_gold_tracts_no_duplicate_keys(context) -> AssetCheckResult:
    """No duplicate (geography_id, survey_year) composite keys after join."""
    result = athena_query(context.resources.athena, f"""
        SELECT geography_id, survey_year, COUNT(*) as cnt
        FROM {GOLD_DB}.gold_tracts
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


@dg.asset_check(asset="gold_states", name="check_geometry_present", required_resource_keys={"athena"})
def check_gold_states_geometry(context) -> AssetCheckResult:
    """Verify geometry_wkt is populated for all rows."""
    result = athena_query(context.resources.athena, f"""
        SELECT COUNT(*) as total, COUNT(geometry_wkt) as has_geometry
        FROM {GOLD_DB}.gold_states
    """)
    total = result[0]["total"] if result else 0
    has_geometry = result[0]["has_geometry"] if result else 0
    return AssetCheckResult(
        passed=has_geometry == total,
        severity=AssetCheckSeverity.ERROR,
        metadata={"total": total, "has_geometry": has_geometry, "missing": total - has_geometry},
    )


@dg.asset_check(asset="gold_counties", name="check_geometry_present", required_resource_keys={"athena"})
def check_gold_counties_geometry(context) -> AssetCheckResult:
    """Verify geometry_wkt is populated for all rows."""
    result = athena_query(context.resources.athena, f"""
        SELECT COUNT(*) as total, COUNT(geometry_wkt) as has_geometry
        FROM {GOLD_DB}.gold_counties
    """)
    total = result[0]["total"] if result else 0
    has_geometry = result[0]["has_geometry"] if result else 0
    return AssetCheckResult(
        passed=has_geometry == total,
        severity=AssetCheckSeverity.ERROR,
        metadata={"total": total, "has_geometry": has_geometry, "missing": total - has_geometry},
    )


@dg.asset_check(asset="gold_tracts", name="check_geometry_present", required_resource_keys={"athena"})
def check_gold_tracts_geometry(context) -> AssetCheckResult:
    """Verify geometry_wkt is populated for all rows."""
    result = athena_query(context.resources.athena, f"""
        SELECT COUNT(*) as total, COUNT(geometry_wkt) as has_geometry
        FROM {GOLD_DB}.gold_tracts
    """)
    total = result[0]["total"] if result else 0
    has_geometry = result[0]["has_geometry"] if result else 0
    return AssetCheckResult(
        passed=has_geometry == total,
        severity=AssetCheckSeverity.ERROR,
        metadata={"total": total, "has_geometry": has_geometry, "missing": total - has_geometry},
    )


@dg.asset_check(asset="gold_states", name="check_acs_metrics_populated", required_resource_keys={"athena"})
def check_gold_states_acs_metrics(context) -> AssetCheckResult:
    """Verify ACS metrics (total_population, median_household_income) are populated."""
    result = athena_query(context.resources.athena, f"""
        SELECT
            COUNT(*) as total,
            COUNT(total_population) as has_population,
            COUNT(median_household_income) as has_income,
            COUNT(bachelors_degree) as has_education
        FROM {GOLD_DB}.gold_states
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


@dg.asset_check(asset="gold_counties", name="check_acs_metrics_populated", required_resource_keys={"athena"})
def check_gold_counties_acs_metrics(context) -> AssetCheckResult:
    """Verify ACS metrics are populated."""
    result = athena_query(context.resources.athena, f"""
        SELECT
            COUNT(*) as total,
            COUNT(total_population) as has_population,
            COUNT(median_household_income) as has_income
        FROM {GOLD_DB}.gold_counties
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


@dg.asset_check(asset="gold_tracts", name="check_acs_metrics_populated", required_resource_keys={"athena"})
def check_gold_tracts_acs_metrics(context) -> AssetCheckResult:
    """Verify ACS metrics are populated."""
    result = athena_query(context.resources.athena, f"""
        SELECT
            COUNT(*) as total,
            COUNT(total_population) as has_population,
            COUNT(median_household_income) as has_income
        FROM {GOLD_DB}.gold_tracts
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


@dg.asset_check(asset="gold_states", name="check_california_population_sanity", required_resource_keys={"athena"})
def check_gold_states_ca_population(context) -> AssetCheckResult:
    """Spot check: California total_population should be ~39M for recent years."""
    result = athena_query(context.resources.athena, f"""
        SELECT survey_year, total_population
        FROM {GOLD_DB}.gold_states
        WHERE state_fips = '06'
        ORDER BY survey_year
    """)
    recent = [r for r in result if r["survey_year"] >= 2022]
    failed_rows = [
        {"year": r["survey_year"], "pop": r["total_population"]}
        for r in recent
        if r["total_population"] and (int(r["total_population"]) < 35000000 or int(r["total_population"]) > 45000000)
    ]
    return AssetCheckResult(
        passed=len(failed_rows) == 0,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "recent_years_checked": len(recent),
            "failures": failed_rows if failed_rows else {"none": "all recent years plausible (~35-45M)"},
        },
    )