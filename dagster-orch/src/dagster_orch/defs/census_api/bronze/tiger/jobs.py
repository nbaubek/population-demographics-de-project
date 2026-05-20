"""Bronze TIGER jobs."""

import dagster as dg

from dagster_orch.defs.census_api.bronze.tiger.assets import (
    bronze_tiger_states,
    bronze_tiger_counties,
    bronze_tiger_tracts,
)

# Job for states and counties (single-partition by year)
bronze_tiger_job = dg.define_asset_job(
    name="bronze_tiger_job",
    selection=[bronze_tiger_states, bronze_tiger_counties],
    description="Download TIGER geographic boundary data for states and counties",
)

# Separate job for tracts (multi-partition by year + state)
bronze_tiger_tracts_job = dg.define_asset_job(
    name="bronze_tiger_tracts_job",
    selection=[bronze_tiger_tracts],
    description="Download TIGER census tract boundaries for all US states (multi-partitioned by year and state)",
)