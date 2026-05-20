"""Bronze ACS5 jobs."""

import dagster as dg

from dagster_orch.defs.census_api.bronze.acs5.assets import (
    bronze_acs5_states,
    bronze_acs5_counties,
    bronze_acs5_tracts,
)

# Job for states and counties (single-partition by year)
bronze_acs5_job = dg.define_asset_job(
    name="bronze_acs5_job",
    selection=[bronze_acs5_states, bronze_acs5_counties],
    description="Ingest ACS 5-year Census data for states and counties",
)

# Separate job for tracts (multi-partition by year + state)
bronze_acs5_tracts_job = dg.define_asset_job(
    name="bronze_acs5_tracts_job",
    selection=[bronze_acs5_tracts],
    description="Ingest ACS 5-year Census data for all US tracts (multi-partitioned by year and state)",
)