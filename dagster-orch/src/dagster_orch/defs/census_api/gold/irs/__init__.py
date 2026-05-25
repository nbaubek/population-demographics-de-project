# Gold IRS module - promoted silver migration outflows with is_non_migrant flag
from dagster_orch.defs.census_api.gold.irs.assets import gold_irs_state_outflows, gold_irs_county_outflows
from dagster_orch.defs.census_api.gold.irs.checks import (
    check_gold_irs_state_outflows_row_count,
    check_gold_irs_state_outflows_no_null_keys,
    check_gold_irs_state_outflows_is_non_migrant_coverage,
    check_gold_irs_county_outflows_row_count,
    check_gold_irs_county_outflows_no_null_keys,
    check_gold_irs_county_outflows_is_non_migrant_coverage,
)

__all__ = [
    "gold_irs_state_outflows",
    "gold_irs_county_outflows",
    "check_gold_irs_state_outflows_row_count",
    "check_gold_irs_state_outflows_no_null_keys",
    "check_gold_irs_state_outflows_is_non_migrant_coverage",
    "check_gold_irs_county_outflows_row_count",
    "check_gold_irs_county_outflows_no_null_keys",
    "check_gold_irs_county_outflows_is_non_migrant_coverage",
]