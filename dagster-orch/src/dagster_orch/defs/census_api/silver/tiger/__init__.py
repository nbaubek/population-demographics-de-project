# Silver TIGER module - cleaned geography data, Iceberg registered
from dagster_orch.defs.census_api.silver.tiger.assets import (
    silver_tiger_states,
    silver_tiger_counties,
    silver_tiger_tracts,
)
from dagster_orch.defs.census_api.silver.tiger.jobs import silver_tiger_job
from dagster_orch.defs.census_api.silver.tiger.schedules import silver_tiger_schedule

# Asset checks
from dagster_orch.defs.census_api.silver.tiger.checks import (
    check_silver_tiger_states_row_count,
    check_silver_tiger_counties_row_count,
    check_silver_tiger_tracts_row_count,
    check_silver_tiger_states_no_null_geo_id,
    check_silver_tiger_counties_no_null_geo_id,
    check_silver_tiger_tracts_no_null_geo_id,
    check_silver_tiger_states_no_duplicate_keys,
    check_silver_tiger_counties_no_duplicate_keys,
    check_silver_tiger_tracts_no_duplicate_keys,
    check_silver_tiger_states_all_years_present,
    check_silver_tiger_counties_all_years_present,
    check_silver_tiger_tracts_all_years_present,
)

__all__ = [
    "silver_tiger_states",
    "silver_tiger_counties",
    "silver_tiger_tracts",
    "silver_tiger_job",
    "silver_tiger_schedule",
    # Checks
    "check_silver_tiger_states_row_count",
    "check_silver_tiger_counties_row_count",
    "check_silver_tiger_tracts_row_count",
    "check_silver_tiger_states_no_null_geo_id",
    "check_silver_tiger_counties_no_null_geo_id",
    "check_silver_tiger_tracts_no_null_geo_id",
    "check_silver_tiger_states_no_duplicate_keys",
    "check_silver_tiger_counties_no_duplicate_keys",
    "check_silver_tiger_tracts_no_duplicate_keys",
    "check_silver_tiger_states_all_years_present",
    "check_silver_tiger_counties_all_years_present",
    "check_silver_tiger_tracts_all_years_present",
]