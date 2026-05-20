"""Silver TIGER jobs."""

import dagster as dg

from dagster_orch.defs.census_api.silver.tiger.assets import (
    silver_tiger_states,
    silver_tiger_counties,
    silver_tiger_tracts,
)

silver_tiger_job = dg.define_asset_job(
    name="silver_tiger_job",
    selection=[silver_tiger_states, silver_tiger_counties, silver_tiger_tracts],
    description="Transform bronze TIGER data to silver Iceberg tables",
)