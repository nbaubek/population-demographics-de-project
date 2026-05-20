"""Gold jobs."""

import dagster as dg

from dagster_orch.defs.census_api.gold.assets import (
    gold_states,
    gold_counties,
    gold_tracts,
)

gold_job = dg.define_asset_job(
    name="gold_job",
    selection=[gold_states, gold_counties, gold_tracts],
    description="Join silver ACS5 and TIGER tables into gold layer",
)