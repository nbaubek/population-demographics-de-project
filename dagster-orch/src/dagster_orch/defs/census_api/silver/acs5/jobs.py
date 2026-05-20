"""Silver ACS5 jobs."""

import dagster as dg

from dagster_orch.defs.census_api.silver.acs5.assets import (
    silver_acs5_states,
    silver_acs5_counties,
    silver_acs5_tracts,
)

silver_acs5_job = dg.define_asset_job(
    name="silver_acs5_job",
    selection=[silver_acs5_states, silver_acs5_counties, silver_acs5_tracts],
    description="Transform bronze ACS5 data to silver Iceberg tables",
)