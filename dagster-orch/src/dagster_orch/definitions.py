from datetime import datetime
from pathlib import Path

import dagster as dg
from dagster_dbt import DbtCliResource
from dagster_orch.defs.census_api.dbt import population_demographics_dbt_assets, dbt_project
from dagster_orch.defs.census_api.shared.resources import athena_resource
from dagster_orch.defs.census_api.shared.constants import YEAR_PARTITIONS, STATE_FIPS_CODES, TRACT_PARTITIONS


def _get_current_acs_year() -> int:
    """Return the most recent ACS 5-year estimate year available.

    ACS 5-year data for year Y is released in December of year Y+1.
    e.g., 2024 ACS 5-year estimates released December 2025.
    """
    now = datetime.now()
    # If we're before December, the latest available ACS year is Y-2
    # If we're December or after, the latest available ACS year is Y-1
    if now.month < 12:
        return now.year - 2
    else:
        return now.year - 1


def _get_current_irs_year() -> int:
    """Return the most recent IRS migration year available.

    IRS migration data covers a 1-year period (e.g., 2022-2023).
    The partition key is the second year. Data is typically released
    in late November/December for the prior tax year.
    """
    now = datetime.now()
    # IRS migration for year Y (filed in year Y+1, released late year Y+1)
    # So if we're in 2025, 2023-2024 data (year=2024) might not be out yet
    # Use Y-1 to be conservative (IRS typically lags by 1-2 years)
    return now.year - 1


# Sensor to keep dynamic year partitions in sync with available data.
# Dagster evaluates sensors based on the daemon's `sensors` block polling interval
# (configured in dagster.yaml). When a new ACS or IRS year partition is added,
# all downstream assets with AutoMaterializePolicy.eager() will automatically
# be queued for materialization for that partition.
@dg.sensor(
    name="sync_year_partitions",
)
def sync_year_partitions_sensor(context: dg.SensorEvaluationContext):
    """Add new year partitions to YEAR_PARTITIONS when ACS or IRS data becomes available.

    ACS 5-year: available years start at 2012, latest depends on release schedule.
    IRS migration: available years are 2012-2023 (tax years, second year as partition).

    Also adds (year, state) multi-partition keys to TRACT_PARTITIONS for tract-level assets.
    """
    instance = context.instance

    years_to_add = []

    # Add ACS years up to current available year
    current_acs_year = _get_current_acs_year()
    for year in range(2012, current_acs_year + 1):
        years_to_add.append(str(year))

    # Add IRS years up to current available year (IRS lags ACS by ~1 year)
    current_irs_year = _get_current_irs_year()
    for year in range(2012, min(current_irs_year, 2023) + 1):
        years_to_add.append(str(year))

    if years_to_add:
        instance.add_dynamic_partitions(
            partitions_def_name=YEAR_PARTITIONS.name,
            partition_keys=years_to_add,
        )
        context.log.info(f"Added {len(years_to_add)} year partitions: {years_to_add}")

        # For tract multi-partitions, we also need to add (year, state) keys
        # whenever a new year partition is added, since TRACT_PARTITIONS is
        # MultiPartitionsDefinition([year: Dynamic, state: Static])
        for year in years_to_add:
            tract_keys = [f"{year}/{state}" for state in STATE_FIPS_CODES]
            instance.add_dynamic_partitions(
                partitions_def_name=TRACT_PARTITIONS.name,
                partition_keys=tract_keys,
            )
            context.log.info(f"Added {len(tract_keys)} tract partition keys for year={year}")

    return dg.SensorResult(run_requests=[])


defs = dg.Definitions.merge(
    dg.load_from_defs_folder(path_within_project=Path(__file__).parent),
    dg.Definitions(
        assets=[population_demographics_dbt_assets],
        resources={
            "dbt": DbtCliResource(project_dir=dbt_project),
            "athena": athena_resource,
        },
        sensors=[sync_year_partitions_sensor],
    ),
)
