# Gold module - joined ACS5 + TIGER tables (census) and IRS migration (separate)
from dagster_orch.defs.census_api.gold.census import (
    gold_states,
    gold_counties,
    gold_tracts,
    check_gold_states_row_count,
    check_gold_counties_row_count,
    check_gold_tracts_row_count,
    check_gold_states_no_duplicate_keys,
    check_gold_counties_no_duplicate_keys,
    check_gold_tracts_no_duplicate_keys,
    check_gold_states_geometry,
    check_gold_counties_geometry,
    check_gold_tracts_geometry,
    check_gold_states_acs_metrics,
    check_gold_counties_acs_metrics,
    check_gold_tracts_acs_metrics,
    check_gold_states_ca_population,
)

__all__ = [
    "gold_states",
    "gold_counties",
    "gold_tracts",
    # Census checks
    "check_gold_states_row_count",
    "check_gold_counties_row_count",
    "check_gold_tracts_row_count",
    "check_gold_states_no_duplicate_keys",
    "check_gold_counties_no_duplicate_keys",
    "check_gold_tracts_no_duplicate_keys",
    "check_gold_states_geometry",
    "check_gold_counties_geometry",
    "check_gold_tracts_geometry",
    "check_gold_states_acs_metrics",
    "check_gold_counties_acs_metrics",
    "check_gold_tracts_acs_metrics",
    "check_gold_states_ca_population",
]