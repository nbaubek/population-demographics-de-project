"""Bronze IRS jobs."""

import dagster as dg

from dagster_orch.defs.census_api.bronze.irs.assets import (
    bronze_irs_state_outflows,
    bronze_irs_county_outflows,
)

bronze_irs_job = dg.define_asset_job(
    name="bronze_irs_job",
    selection=[bronze_irs_state_outflows, bronze_irs_county_outflows],
    description="Ingest IRS state and county migration outflows",
)