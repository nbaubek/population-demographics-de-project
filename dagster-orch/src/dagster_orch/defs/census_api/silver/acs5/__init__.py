# Silver ACS5 module - cleaned, typed, Iceberg registered
from dagster_orch.defs.census_api.silver.acs5.assets import (
    silver_acs5_states,
    silver_acs5_counties,
    silver_acs5_tracts,
)

# Asset checks
from dagster_orch.defs.census_api.silver.acs5.checks import (
    check_silver_acs5_states_row_count,
    check_silver_acs5_counties_row_count,
    check_silver_acs5_tracts_row_count,
    check_silver_acs5_states_no_null_geo_id,
    check_silver_acs5_counties_no_null_geo_id,
    check_silver_acs5_tracts_no_null_geo_id,
    check_silver_acs5_states_no_duplicate_keys,
    check_silver_acs5_counties_no_duplicate_keys,
    check_silver_acs5_tracts_no_duplicate_keys,
    check_silver_acs5_states_all_years_present,
    check_silver_acs5_counties_all_years_present,
    check_silver_acs5_tracts_all_years_present,
)

__all__ = [
    "silver_acs5_states",
    "silver_acs5_counties",
    "silver_acs5_tracts",
    # Checks
    "check_silver_acs5_states_row_count",
    "check_silver_acs5_counties_row_count",
    "check_silver_acs5_tracts_row_count",
    "check_silver_acs5_states_no_null_geo_id",
    "check_silver_acs5_counties_no_null_geo_id",
    "check_silver_acs5_tracts_no_null_geo_id",
    "check_silver_acs5_states_no_duplicate_keys",
    "check_silver_acs5_counties_no_duplicate_keys",
    "check_silver_acs5_tracts_no_duplicate_keys",
    "check_silver_acs5_states_all_years_present",
    "check_silver_acs5_counties_all_years_present",
    "check_silver_acs5_tracts_all_years_present",
]